"""Background processing: wait, then mark PROCESSED. The sweeper picks up anything left unfinished."""
import asyncio
import logging

from app import config, db

log = logging.getLogger(__name__)
_tasks = set()


def start(coro):
    """Run a coroutine in the background, keeping a reference until it's done."""
    task = asyncio.create_task(coro)
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


async def process(transaction_id, claim=False):
    """Simulate the external call, then save the result.

    New rows already hold the lease from the insert; rows found by the sweeper pass claim=True.
    """
    try:
        if claim and not await db.claim(transaction_id):
            return  # done already, or another worker has it
        await asyncio.sleep(config.PROCESSING_DELAY_SECONDS)
        await db.mark_processed(transaction_id)
        log.info("processed %s", transaction_id)
    except Exception:
        log.exception("processing %s failed; the sweeper retries it after the lease expires", transaction_id)


async def sweep_once():
    for transaction_id in await db.find_unclaimed():
        start(process(transaction_id, claim=True))


async def sweep_forever():
    while True:
        try:
            await sweep_once()
        except Exception:
            log.exception("sweep failed")
        await asyncio.sleep(config.SWEEP_INTERVAL_SECONDS)
