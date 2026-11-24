import os
import uuid
import psycopg2
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional

DB_HOST = os.getenv("DB_HOST", "db-coordinator-service")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "coordinator_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password123")

SHARD_A_URL = os.getenv("SHARD_A_URL", "http://shard-a-service:8000")
SHARD_B_URL = os.getenv("SHARD_B_URL", "http://shard-b-service:8000")

app = FastAPI(title="Transaction Coordinator")

def get_db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

def recover_unresolved_transactions():
    print("[COORDINATOR] Running startup recovery...")
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT tx_id, source_acc, target_acc, state FROM coordinator_tx_log WHERE state NOT IN ('COMPLETED', 'ABORTED');")
            unresolved = cur.fetchall()
            for tx_id, src, tgt, state in unresolved:
                print(f"[RECOVERY] Found unresolved transaction {tx_id} in state {state}")
                if state == "COMMIT_DECIDED":
                    requests.post(f"{SHARD_A_URL}/2pc/commit", json={"tx_id": tx_id}, timeout=3)
                    requests.post(f"{SHARD_B_URL}/2pc/commit", json={"tx_id": tx_id}, timeout=3)
                    cur.execute("UPDATE coordinator_tx_log SET state = 'COMPLETED', updated_at = CURRENT_TIMESTAMP WHERE tx_id = %s;", (tx_id,))
                else:
                    requests.post(f"{SHARD_A_URL}/2pc/abort", json={"tx_id": tx_id}, timeout=3)
                    requests.post(f"{SHARD_B_URL}/2pc/abort", json={"tx_id": tx_id}, timeout=3)
                    cur.execute("UPDATE coordinator_tx_log SET state = 'ABORTED', updated_at = CURRENT_TIMESTAMP WHERE tx_id = %s;", (tx_id,))
            conn.commit()
    finally:
        conn.close()

@app.on_event("startup")
def startup():
    recover_unresolved_transactions()

@app.get("/health/live")
def liveness():
    return {"status": "ALIVE"}

@app.get("/health/ready")
def readiness():
    try:
        conn = get_db()
        conn.close()
        return {"status": "READY"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

class TransferRequest(BaseModel):
    source_acc: str
    target_acc: str
    amount: float
    idempotency_key: Optional[str] = None
    simulate_crash_after_decision: bool = False

@app.post("/transactions/execute")
def execute_2pc(payload: TransferRequest):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            # Check Idempotency
            if payload.idempotency_key:
                cur.execute("SELECT tx_id, state FROM coordinator_tx_log WHERE tx_id = %s;", (payload.idempotency_key,))
                existing = cur.fetchone()
                if existing:
                    return {"tx_id": existing[0], "status": existing[1], "idempotent_replay": True}

            tx_id = payload.idempotency_key if payload.idempotency_key else f"tx_{uuid.uuid4().hex[:12]}"

            # Init stable state
            cur.execute(
                "INSERT INTO coordinator_tx_log (tx_id, source_acc, target_acc, amount, state) VALUES (%s, %s, %s, %s, 'PREPARING');",
                (tx_id, payload.source_acc, payload.target_acc, payload.amount)
            )
            conn.commit()

            res_a = requests.post(f"{SHARD_A_URL}/2pc/prepare", json={
                "tx_id": tx_id, "action": "DEBIT", "account_id": payload.source_acc, "amount": payload.amount
            }, timeout=4).json()

            res_b = requests.post(f"{SHARD_B_URL}/2pc/prepare", json={
                "tx_id": tx_id, "action": "CREDIT", "account_id": payload.target_acc, "amount": payload.amount
            }, timeout=4).json()

            # Vote Checking
            if res_a.get("vote") == "YES" and res_b.get("vote") == "YES":
                cur.execute("UPDATE coordinator_tx_log SET state = 'COMMIT_DECIDED', updated_at = CURRENT_TIMESTAMP WHERE tx_id = %s;", (tx_id,))
                conn.commit()

                # Crash Simulation for testing fault tolerance 
                if payload.simulate_crash_after_decision:
                    print(f"[CHAOS] Crashing coordinator container mid-2PC after decision: {tx_id}")
                    os._exit(1)

                requests.post(f"{SHARD_A_URL}/2pc/commit", json={"tx_id": tx_id}, timeout=4)
                requests.post(f"{SHARD_B_URL}/2pc/commit", json={"tx_id": tx_id}, timeout=4)

                cur.execute("UPDATE coordinator_tx_log SET state = 'COMPLETED', updated_at = CURRENT_TIMESTAMP WHERE tx_id = %s;", (tx_id,))
                conn.commit()
                return {"tx_id": tx_id, "status": "COMPLETED", "source": payload.source_acc, "target": payload.target_acc, "amount": payload.amount}
            else:
                cur.execute("UPDATE coordinator_tx_log SET state = 'ABORT_DECIDED', updated_at = CURRENT_TIMESTAMP WHERE tx_id = %s;", (tx_id,))
                conn.commit()

                requests.post(f"{SHARD_A_URL}/2pc/abort", json={"tx_id": tx_id}, timeout=4)
                requests.post(f"{SHARD_B_URL}/2pc/abort", json={"tx_id": tx_id}, timeout=4)

                cur.execute("UPDATE coordinator_tx_log SET state = 'ABORTED', updated_at = CURRENT_TIMESTAMP WHERE tx_id = %s;", (tx_id,))
                conn.commit()
                raise HTTPException(status_code=400, detail="2PC Phase 1 rejected, transaction aborted")
    finally:
        conn.close()

@app.get("/transactions/{tx_id}/status")
def get_tx_status(tx_id: str):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT state, source_acc, target_acc, amount FROM coordinator_tx_log WHERE tx_id = %s;", (tx_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Transaction not found")
            return {"tx_id": tx_id, "state": row[0], "source": row[1], "target": row[2], "amount": float(row[3])}
    finally:
        conn.close()