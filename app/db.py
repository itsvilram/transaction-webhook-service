"""Database pool, table and queries."""
from datetime import timedelta

import asyncpg

from app import config

pool: asyncpg.Pool | None = None
LEASE = timedelta(seconds=config.LEASE_SECONDS)

SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      TEXT PRIMARY KEY,
    source_account      TEXT NOT NULL,
    destination_account TEXT NOT NULL,
    amount              NUMERIC NOT NULL CHECK (amount > 0),
    currency            CHAR(3) NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('PROCESSING', 'PROCESSED')),
    locked_until        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at        TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_transactions_pending
    ON transactions (locked_until) WHERE status = 'PROCESSING';
"""


async def connect():
    global pool
    # Open every connection at startup and keep it open, so a burst of webhooks
    # never waits on new connection handshakes.
    pool = await asyncpg.create_pool(
        config.DATABASE_URL, min_size=10, max_size=10, max_inactive_connection_lifetime=0,
        command_timeout=config.DB_TIMEOUT_SECONDS,
    )
    await pool.execute(SCHEMA)


async def close():
    await pool.close()


async def ping():
    await pool.fetchval("SELECT 1")


async def insert_transaction(transaction_id, source_account, destination_account, amount, currency) -> bool:
    """Save a new transaction with its lease already taken. False if the id already exists."""
    row = await pool.fetchrow(
        """
        INSERT INTO transactions
            (transaction_id, source_account, destination_account, amount, currency, status, locked_until)
        VALUES ($1, $2, $3, $4, $5, 'PROCESSING', now() + $6::interval)
        ON CONFLICT (transaction_id) DO NOTHING
        RETURNING transaction_id
        """,
        transaction_id, source_account, destination_account, amount, currency, LEASE,
    )
    return row is not None


async def get_transaction(transaction_id):
    return await pool.fetchrow(
        """
        SELECT transaction_id, source_account, destination_account, amount, currency,
               status, created_at, processed_at
        FROM transactions WHERE transaction_id = $1
        """,
        transaction_id,
    )


async def find_unclaimed() -> list[str]:
    """Ids still PROCESSING whose lease is empty or expired."""
    rows = await pool.fetch(
        """
        SELECT transaction_id FROM transactions
        WHERE status = 'PROCESSING' AND (locked_until IS NULL OR locked_until < now())
        """
    )
    return [r["transaction_id"] for r in rows]


async def claim(transaction_id) -> bool:
    """Take the lease on an unfinished row. False if it's done or another worker holds it."""
    row = await pool.fetchrow(
        """
        UPDATE transactions SET locked_until = now() + $2::interval
        WHERE transaction_id = $1 AND status = 'PROCESSING'
          AND (locked_until IS NULL OR locked_until < now())
        RETURNING transaction_id
        """,
        transaction_id, LEASE,
    )
    return row is not None


async def mark_processed(transaction_id):
    await pool.execute(
        """
        UPDATE transactions SET status = 'PROCESSED', processed_at = now(), locked_until = NULL
        WHERE transaction_id = $1 AND status = 'PROCESSING'
        """,
        transaction_id,
    )
