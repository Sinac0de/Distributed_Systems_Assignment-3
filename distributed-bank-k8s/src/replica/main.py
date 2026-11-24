import os
import psycopg2
from psycopg2 import pool
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import Optional

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "bank_shard_a")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password123")
SHARD_NAME = os.getenv("SHARD_NAME", "Shard-A")
POD_NAME = os.getenv("POD_NAME", os.getenv("HOSTNAME", "replica-pod"))

app = FastAPI(title=f"Bank Replica ({SHARD_NAME})")

db_pool = None

@app.on_event("startup")
def startup():
    global db_pool
    db_pool = psycopg2.pool.SimpleConnectionPool(
        1, 20,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    # Initialize idempotency_keys table if it doesn't exist
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS idempotency_keys (
                    key VARCHAR(64) PRIMARY KEY,
                    account_id VARCHAR(10) NOT NULL,
                    amount NUMERIC(12, 2) NOT NULL,
                    balance NUMERIC(12, 2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
    finally:
        db_pool.putconn(conn)

@app.on_event("shutdown")
def shutdown():
    if db_pool:
        db_pool.closeall()

# --- Health PROBES ---
@app.get("/health/live")
def liveness():
    return {"status": "ALIVE", "pod": POD_NAME}

@app.get("/health/ready")
def readiness():
    if not db_pool:
        raise HTTPException(status_code=503, detail="DB pool not ready")
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
        db_pool.putconn(conn)
        return {"status": "READY", "pod": POD_NAME, "shard": SHARD_NAME}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {e}")

# --- DATA MODELS ---
class DepositPayload(BaseModel):
    amount: float
    idempotency_key: Optional[str] = None

class LocalTransferPayload(BaseModel):
    source_acc: str
    target_acc: str
    amount: float
    idempotency_key: Optional[str] = None

class PreparePayload(BaseModel):
    tx_id: str
    action: str  # 'DEBIT' OR 'CREDIT'
    account_id: str
    amount: float

class CommitAbortPayload(BaseModel):
    tx_id: str

# balance retrieval endpoint
@app.get("/accounts/{account_id}/balance")
def get_balance(account_id: str):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM accounts WHERE account_id = %s;", (account_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Account not found on this shard")
            return {
                "account_id": account_id,
                "balance": float(row[0]),
                "shard": SHARD_NAME,
                "serviced_by_pod": POD_NAME
            }
    finally:
        db_pool.putconn(conn)

# Idempotent deposit operation
@app.post("/accounts/{account_id}/deposit")
def deposit(account_id: str, payload: DepositPayload):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            # (Idempotency Check)
            if payload.idempotency_key:
                cur.execute("SELECT balance FROM idempotency_keys WHERE key = %s;", (payload.idempotency_key,))
                row = cur.fetchone()
                if row:
                    return {
                        "account_id": account_id,
                        "balance": float(row[0]),
                        "idempotent_replay": True,
                        "serviced_by_pod": POD_NAME
                    }

            cur.execute("BEGIN;")
            cur.execute("SELECT balance FROM accounts WHERE account_id = %s FOR UPDATE;", (account_id,))
            row = cur.fetchone()
            if not row:
                cur.execute("ROLLBACK;")
                raise HTTPException(status_code=404, detail="Account not found")

            new_balance = float(row[0]) + payload.amount
            cur.execute("UPDATE accounts SET balance = %s, updated_at = CURRENT_TIMESTAMP WHERE account_id = %s;", (new_balance, account_id))

            if payload.idempotency_key:
                cur.execute(
                    "INSERT INTO idempotency_keys (key, account_id, amount, balance) VALUES (%s, %s, %s, %s);",
                    (payload.idempotency_key, account_id, payload.amount, new_balance)
                )

            cur.execute("COMMIT;")
            return {
                "account_id": account_id,
                "balance": new_balance,
                "idempotent_replay": False,
                "serviced_by_pod": POD_NAME
            }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db_pool.putconn(conn)

# local transfer between accounts on the same shard with idempotency support
@app.post("/transfers/local")
def local_transfer(payload: LocalTransferPayload):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            if payload.idempotency_key:
                cur.execute("SELECT balance FROM idempotency_keys WHERE key = %s;", (payload.idempotency_key,))
                row = cur.fetchone()
                if row:
                    return {"status": "SUCCESS", "idempotent_replay": True, "serviced_by_pod": POD_NAME}

            first, second = sorted([payload.source_acc, payload.target_acc])
            cur.execute("BEGIN;")
            cur.execute("SELECT account_id, balance FROM accounts WHERE account_id IN (%s, %s) ORDER BY account_id FOR UPDATE;", (first, second))
            rows = {r[0]: float(r[1]) for r in cur.fetchall()}

            if len(rows) < 2 or rows[payload.source_acc] < payload.amount:
                cur.execute("ROLLBACK;")
                raise HTTPException(status_code=400, detail="Insufficient funds or account missing")

            cur.execute("UPDATE accounts SET balance = balance - %s WHERE account_id = %s;", (payload.amount, payload.source_acc))
            cur.execute("UPDATE accounts SET balance = balance + %s WHERE account_id = %s;", (payload.amount, payload.target_acc))

            if payload.idempotency_key:
                cur.execute(
                    "INSERT INTO idempotency_keys (key, account_id, amount, balance) VALUES (%s, %s, %s, %s);",
                    (payload.idempotency_key, payload.source_acc, payload.amount, rows[payload.source_acc] - payload.amount)
                )
            cur.execute("COMMIT;")
            return {"status": "SUCCESS", "idempotent_replay": False, "serviced_by_pod": POD_NAME}
    finally:
        db_pool.putconn(conn)

# --- فازهای پروتکل 2PC بومی پستگرس ---
@app.post("/2pc/prepare")
def prepare_tx(payload: PreparePayload):
    conn = db_pool.getconn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM accounts WHERE account_id = %s FOR UPDATE;", (payload.account_id,))
            row = cur.fetchone()
            if not row:
                conn.rollback()
                raise HTTPException(status_code=404, detail="Account not found")

            bal = float(row[0])
            if payload.action == "DEBIT":
                if bal < payload.amount:
                    conn.rollback()
                    raise HTTPException(status_code=400, detail="Insufficient funds")
                cur.execute("UPDATE accounts SET balance = balance - %s WHERE account_id = %s;", (payload.amount, payload.account_id))
            elif payload.action == "CREDIT":
                cur.execute("UPDATE accounts SET balance = balance + %s WHERE account_id = %s;", (payload.amount, payload.account_id))

            cur.execute(f"PREPARE TRANSACTION '{payload.tx_id}';")
        return {"vote": "YES", "tx_id": payload.tx_id}
    except Exception as e:
        conn.rollback()
        return {"vote": "NO", "error": str(e)}
    finally:
        db_pool.putconn(conn)

@app.post("/2pc/commit")
def commit_tx(payload: CommitAbortPayload):
    conn = db_pool.getconn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT gid FROM pg_prepared_xacts WHERE gid = %s;", (payload.tx_id,))
            if cur.fetchone():
                cur.execute(f"COMMIT PREPARED '{payload.tx_id}';")
        return {"status": "COMMITTED", "tx_id": payload.tx_id}
    finally:
        db_pool.putconn(conn)

@app.post("/2pc/abort")
def abort_tx(payload: CommitAbortPayload):
    conn = db_pool.getconn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT gid FROM pg_prepared_xacts WHERE gid = %s;", (payload.tx_id,))
            if cur.fetchone():
                cur.execute(f"ROLLBACK PREPARED '{payload.tx_id}';")
        return {"status": "ABORTED", "tx_id": payload.tx_id}
    finally:
        db_pool.putconn(conn)