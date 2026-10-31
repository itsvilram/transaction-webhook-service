"""Smoke test against a running service: checks the assignment's success criteria.

Usage: python scripts/smoke_test.py [BASE_URL]    (default http://127.0.0.1:8000)
Takes about 40 seconds because of the 30-second processing delay.
"""
import asyncio
import statistics
import sys
import time
import uuid
from datetime import datetime

import httpx

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8000"
RUN = uuid.uuid4().hex[:8]  # unique ids per run
results = []


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""))


def webhook(tid):
    return {"transaction_id": tid, "source_account": "acc_user_789",
            "destination_account": "acc_merchant_456", "amount": 1500, "currency": "INR"}


async def post(client, tid):
    t0 = time.perf_counter()
    r = await client.post("/v1/webhooks/transactions", json=webhook(tid))
    return r, (time.perf_counter() - t0) * 1000


async def get_tx(client, tid):
    r = await client.get(f"/v1/transactions/{tid}")
    return r.json() if r.status_code == 200 else []


async def wait_processed(client, tids, timeout=45):
    deadline = time.monotonic() + timeout
    while True:
        found = await asyncio.gather(*[get_tx(client, t) for t in tids])
        if all(f and f[0]["status"] == "PROCESSED" for f in found) or time.monotonic() > deadline:
            return found
        await asyncio.sleep(2)


def seconds_between(start, end):
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return (datetime.strptime(end, fmt) - datetime.strptime(start, fmt)).total_seconds()


async def main():
    async with httpx.AsyncClient(base_url=BASE, timeout=10) as client:
        r = await client.get("/")
        print(f"Testing {BASE}\nGET / -> {r.status_code} {r.text}\n")

        # 1. Single transaction
        single = f"smoke_{RUN}_single"
        r, ms = await post(client, single)
        check("1a. Single: webhook returns 202", r.status_code == 202, f"{ms:.0f}ms")
        found = await get_tx(client, single)
        check("1b. Single: PROCESSING right after", found and found[0]["status"] == "PROCESSING")

        # 2. Duplicates: the same webhook 10 times at once, then 5 more one by one
        dup = f"smoke_{RUN}_dup"
        responses = [r for r, _ in await asyncio.gather(*[post(client, dup) for _ in range(10)])]
        for _ in range(5):
            responses.append((await post(client, dup))[0])
        new = sum(r.status_code == 202 and r.json()["duplicate"] is False for r in responses)
        check("2a. Duplicates: same webhook x15 -> all 202, only 1 accepted as new",
              all(r.status_code == 202 for r in responses) and new == 1, f"{new} new")

        # 3. Performance: 50 new webhooks at once, while the ones above are still processing
        load = [f"smoke_{RUN}_load_{i}" for i in range(50)]
        out = await asyncio.gather(*[post(client, t) for t in load])
        times = sorted(ms for _, ms in out)
        p50, p95 = statistics.median(times), statistics.quantiles(times, n=20)[-1]
        check("3a. Performance: 50 at once under load -> all 202, p95 under 500ms",
              all(r.status_code == 202 for r, _ in out) and p95 < 500,
              f"p50 {p50:.0f}ms, p95 {p95:.0f}ms, max {times[-1]:.0f}ms")

        # 4. Errors are handled, not crashes
        r = await client.post("/v1/webhooks/transactions", json={"transaction_id": "incomplete"})
        check("4a. Errors: bad body -> 422", r.status_code == 422)
        r = await client.get(f"/v1/transactions/smoke_{RUN}_missing")
        check("4b. Errors: unknown id -> 404", r.status_code == 404)

        print("\nWaiting for processing (about 30s)...")
        found = await wait_processed(client, [single, dup] + load)
        s, d, rest = found[0], found[1], found[2:]

        gap = seconds_between(s[0]["created_at"], s[0]["processed_at"]) if s and s[0]["processed_at"] else None
        check("1c. Single: PROCESSED after ~30s", gap is not None and 29 <= gap <= 40, f"{gap:.0f}s" if gap else "")
        check("2b. Duplicates: one transaction, PROCESSED once",
              len(d) == 1 and d[0]["status"] == "PROCESSED", f"{len(d)} record(s)")
        done = sum(bool(f) and f[0]["status"] == "PROCESSED" for f in rest)
        check("3b. Performance: all 50 PROCESSED too", done == len(load), f"{done}/{len(load)}")

    print(f"\n{sum(results)}/{len(results)} checks passed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
