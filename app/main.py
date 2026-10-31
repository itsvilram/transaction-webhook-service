"""HTTP API: webhook intake, health check, transaction lookup."""
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app import config, db, worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


class Transaction(BaseModel):
    # Accept numeric ids like 12345 as "12345". Extra fields are ignored (Pydantic default).
    model_config = ConfigDict(coerce_numbers_to_str=True)

    transaction_id: str = Field(min_length=1)
    source_account: str = Field(min_length=1)
    destination_account: str = Field(min_length=1)
    amount: Decimal = Field(gt=0)
    currency: str = Field(pattern=r"^[A-Za-z]{3}$")

    @field_validator("currency")
    @classmethod
    def uppercase(cls, v: str) -> str:
        return v.upper()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    sweeper = asyncio.create_task(worker.sweep_forever())
    yield
    sweeper.cancel()
    await db.close()


app = FastAPI(title="Transaction Webhook Service", lifespan=lifespan)


def iso(dt):
    """UTC time like the spec: 2024-01-15T10:30:00Z."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


async def with_db(coro):
    """Run a DB call within the time limit; any failure becomes a 503."""
    try:
        return await asyncio.wait_for(coro, config.DB_TIMEOUT_SECONDS)
    except Exception:
        log.exception("database call failed")
        raise HTTPException(503, "Database unavailable, please retry")


@app.get("/")
async def health():
    now = iso(datetime.now(timezone.utc))
    try:
        await asyncio.wait_for(db.ping(), config.DB_TIMEOUT_SECONDS)
    except Exception:
        return JSONResponse({"status": "UNHEALTHY", "current_time": now}, status_code=503)
    return {"status": "HEALTHY", "current_time": now}


@app.post("/v1/webhooks/transactions", status_code=202)
async def receive_transaction(tx: Transaction):
    # Only say 202 once the row is stored. On 503 the provider retries, and a retry is safe either way.
    is_new = await with_db(db.insert_transaction(**tx.model_dump()))
    if is_new:
        worker.start(worker.process(tx.transaction_id))
    return {"status": "ACCEPTED", "transaction_id": tx.transaction_id, "duplicate": not is_new}


@app.get("/v1/transactions/{transaction_id}")
async def get_transaction(transaction_id: str):
    row = await with_db(db.get_transaction(transaction_id))
    if row is None:
        raise HTTPException(404, "Transaction not found")
    tx = dict(row)
    tx["amount"] = float(tx["amount"])
    tx["created_at"] = iso(tx["created_at"])
    tx["processed_at"] = iso(tx["processed_at"])
    return [tx]
