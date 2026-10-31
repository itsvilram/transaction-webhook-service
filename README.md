# Transaction Webhook Service

Receives transaction webhooks from payment processors (like Razorpay), acknowledges them immediately with `202 Accepted`, and processes them reliably in the background (30-second simulated external call), storing the result in Postgres.

**Live API:** https://transaction-webhook-service-v4g2.onrender.com · Interactive docs at [`/docs`](https://transaction-webhook-service-v4g2.onrender.com/docs)

## Endpoints

| Method | Path | Response |
|---|---|---|
| `POST` | `/v1/webhooks/transactions` | `202` `{"status": "ACCEPTED", "transaction_id": "...", "duplicate": false}`. A repeat of the same `transaction_id` also gets `202`, with `"duplicate": true`, and is not processed again. |
| `GET` | `/` | `200` `{"status": "HEALTHY", "current_time": "2026-10-31T12:10:23Z"}`, or `503` `UNHEALTHY` if the database is unreachable |
| `GET` | `/v1/transactions/{transaction_id}` | `200` with a one-item list (as in the spec), or `404` |

Errors: `422` for an invalid body (missing field, `amount <= 0`, currency not 3 letters); `503` if the transaction couldn't be stored, so the sender retries.

## How it works

```
POST webhook ──► validate ──► INSERT ... ON CONFLICT (transaction_id) DO NOTHING ──► 202
                                   │ new row only
                                   ▼
                        background task: wait 30s ──► status = PROCESSED, processed_at = now()

sweeper (at startup + every 60s): finds PROCESSING rows nobody is working on ──► processes them
```

- **Fast acknowledgement.** The request path is one indexed `INSERT`. The 30-second work runs as an asyncio task after the response is sent.
- **Idempotency.** `transaction_id` is the primary key, so 20 identical webhooks arriving at the same moment still create one row and one background job. Duplicates get `202`, not an error.
- **No lost transactions.** `202` is returned only after the row is committed; if the database fails or takes longer than 2s, the endpoint returns `503` so the processor retries (a retry is safe because of idempotency). The transaction's state lives in the database, not in memory: if the server crashes or redeploys mid-wait, the sweeper finds the unfinished row and processes it.
- **No double processing.** Each row carries a short lease (`locked_until`). Only the holder of a valid lease works on it, and the final update only applies to rows still `PROCESSING`. Strictly, the status change is exactly-once and the (simulated) external call is at-least-once: if a worker stalls past its lease, a second worker may repeat the call. A real external call would pass `transaction_id` as an idempotency key.

## Technical choices

| Choice | Why |
|---|---|
| **FastAPI + uvicorn** | Async, so thousands of 30-second waits are cheap coroutines, not threads. Request validation built in. |
| **Neon Postgres** (managed, cloud) | Durable storage; the primary key gives idempotency for free with `ON CONFLICT DO NOTHING`. |
| **asyncpg**, plain SQL | One table, so no ORM. Fast async driver with a 10-connection pool opened at startup. |
| **In-process worker + DB sweeper** instead of Celery/Redis | Fewer moving parts. Durability comes from the row state in Postgres, so a separate queue adds no safety here. |
| **Render** (Singapore) | Long-running server, so the background worker lives in the app. Same region as the database. |

**Trade-offs and limits**
- After a crash, an in-flight transaction finishes after its lease expires (up to ~3.5 min instead of ~30s).
- Render's free plan sleeps after 15 min idle, so an external pinger calls `GET /` every 10 minutes to keep it awake. The free plan also has only 0.1 CPU, which limits latency under large bursts (see [Results](#results-against-the-live-url)).
- Webhook signatures aren't checked (the spec sends unsigned requests). For Razorpay, the endpoint would verify the `X-Razorpay-Signature` header: an HMAC-SHA256 of the raw request body with the webhook secret.

## Run locally

Needs Python 3.13 and a Postgres connection string (e.g. a free Neon database).

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env               # then put your DATABASE_URL in .env
uvicorn app.main:app --env-file .env
```

The table is created automatically on startup. Settings (all optional except `DATABASE_URL`):

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | – | Postgres connection string |
| `PROCESSING_DELAY_SECONDS` | `30` | Simulated external call |
| `SWEEP_INTERVAL_SECONDS` | `60` | How often the sweeper looks for unfinished rows |
| `LEASE_SECONDS` | `120` | How long a worker owns a row before another may take it |
| `DB_TIMEOUT_SECONDS` | `2` | Longest a request waits on the database before answering `503` |

## Test

```bash
BASE=http://127.0.0.1:8000    # or the live URL

# Send a webhook (send it again to see "duplicate": true)
curl -i -X POST $BASE/v1/webhooks/transactions -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'

# Check status: PROCESSING now, PROCESSED after ~30 seconds
curl $BASE/v1/transactions/txn_abc123def456

# Health
curl $BASE/
```

**Smoke test.** One script checks the success criteria (single transaction, duplicates, 50 webhooks at once with p50/p95 latency, error handling) and prints PASS/FAIL for each. It takes about 40 seconds.

```bash
python scripts/smoke_test.py $BASE
```

### Results against the live URL

Run on 31 Oct 2026 from India, against the Render free plan in Singapore:

```
PASS  1a. Single: webhook returns 202  (219ms)
PASS  1b. Single: PROCESSING right after
PASS  2a. Duplicates: same webhook x15 -> all 202, only 1 accepted as new  (1 new)
FAIL  3a. Performance: 50 at once under load -> all 202, p95 under 500ms  (p50 1055ms, p95 1715ms, max 1743ms)
PASS  4a. Errors: bad body -> 422
PASS  4b. Errors: unknown id -> 404
PASS  1c. Single: PROCESSED after ~30s  (30s)
PASS  2b. Duplicates: one transaction, PROCESSED once  (1 record(s))
PASS  3b. Performance: all 50 PROCESSED too  (50/50)

8/9 checks passed
```

**Latency by burst size.** Webhook response time as seen by the client, including the network round trip from India to Singapore:

| Webhooks sent at once | p95 | Under 500 ms? |
|---|---|---|
| 1 | ~100–200 ms | yes |
| 10 | 185 ms | yes |
| 25 | 420 ms | yes |
| 50, connections already open | 674 ms | no |
| 50, new connections | 1728 ms | no |

The 500 ms target is met up to about 25 simultaneous webhooks. Beyond that, the limit is the free plan's 0.1 CPU, not the design. Each request needs about 10 ms of that CPU, so a burst queues, and latency grows in line with the burst size. The 30-second background jobs don't add to it, because a waiting task uses no CPU. A paid instance (for example Render Starter, 0.5 CPU) would raise the limit with no code change.

## Project layout

```
app/
  main.py      API: request model, endpoints, startup/shutdown
  db.py        connection pool, table, SQL queries
  worker.py    background processing and the sweeper
  config.py    settings from environment variables
scripts/
  smoke_test.py
render.yaml    Render deployment
```
