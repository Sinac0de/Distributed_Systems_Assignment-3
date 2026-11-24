import os
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional

SHARD_A_SERVICE = os.getenv("SHARD_A_SERVICE", "http://shard-a-service:8000")
SHARD_B_SERVICE = os.getenv("SHARD_B_SERVICE", "http://shard-b-service:8000")
COORDINATOR_SERVICE = os.getenv("COORDINATOR_SERVICE", "http://coordinator-service:8000")

app = FastAPI(title="Cloud-Native Shard-Aware Gateway")

def resolve_shard(account_id: str) -> str:
    if not account_id.startswith("A") or len(account_id) != 5:
        raise HTTPException(status_code=400, detail="Invalid account format. Expected A0001 - A2000")
    try:
        num = int(account_id[1:])
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid account numeric format")

    if 1 <= num <= 1000:
        return SHARD_A_SERVICE
    elif 1001 <= num <= 2000:
        return SHARD_B_SERVICE
    else:
        raise HTTPException(status_code=404, detail="Account out of sharded range")

@app.get("/health/live")
def liveness():
    return {"status": "ALIVE"}

@app.get("/health/ready")
def readiness():
    return {"status": "READY"}

class TransferRequest(BaseModel):
    source: str
    target: str
    amount: float
    idempotency_key: Optional[str] = None

class DepositRequest(BaseModel):
    amount: float
    idempotency_key: Optional[str] = None

@app.get("/accounts/{account_id}/balance")
def get_balance(account_id: str):
    target_shard = resolve_shard(account_id)
    resp = requests.get(f"{target_shard}/accounts/{account_id}/balance", timeout=4)
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()

@app.post("/accounts/{account_id}/deposit")
def deposit(account_id: str, payload: DepositRequest):
    target_shard = resolve_shard(account_id)
    resp = requests.post(f"{target_shard}/accounts/{account_id}/deposit", json=payload.dict(), timeout=4)
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()

@app.post("/transfer")
def transfer(payload: TransferRequest):
    shard_src = resolve_shard(payload.source)
    shard_tgt = resolve_shard(payload.target)

    if shard_src == shard_tgt:
        # Local transfer
        resp = requests.post(
            f"{shard_src}/transfers/local",
            json={
                "source_acc": payload.source,
                "target_acc": payload.target,
                "amount": payload.amount,
                "idempotency_key": payload.idempotency_key
            },
            timeout=4
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()
    else:
        # 2PC cross shard transaction
        resp = requests.post(
            f"{COORDINATOR_SERVICE}/transactions/execute",
            json={
                "source_acc": payload.source,
                "target_acc": payload.target,
                "amount": payload.amount,
                "idempotency_key": payload.idempotency_key
            },
            timeout=8
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()