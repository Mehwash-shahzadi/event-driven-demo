# Event-Driven Architecture Demo (FastAPI + Redis Streams)

> **A small demo built for learning purposes only.** It shows the basic ideas of event-driven architecture: producer, broker, consumer group, ACK, and pending messages. It is not production code (see [Limitations](#limitations)).

## Overview

Two independent services communicate only through a Redis Stream:

- **user-service** (FastAPI): `POST /users` creates a user and publishes a `user.created` event. It never sends email itself.
- **email-service** (consumer): reads `user.created` events and "sends" a welcome email.

```
POST /users -> user-service (producer) -> Redis Stream -> email-service (consumer group) -> XACK
```

## Project structure

```
event-driven-demo/
├── user_service/
│   ├── main.py        # FastAPI app (POST /users)
│   └── producer.py    # publishes events to the Redis Stream
├── email_service/
│   └── consumer.py    # consumer group worker: retries, dead-letters, ACKs
├── tests/             # unit tests (mocked Redis) + one integration test
├── .env.example       # config template: copy to .env (.env is git-ignored)
├── requirements.txt
└── README.md
```

## Key concepts

| Concept | How this demo uses it |
|---|---|
| **Stream** | An append-only log (`STREAM_NAME`). Each entry has a `payload` field holding the JSON event. |
| **Consumer group** | `GROUP_NAME`: Redis tracks what the group has received; each event goes to one worker in the group. |
| **Blocking read** | `XREADGROUP ... block` (`BLOCK_MS`) waits inside Redis for new events instead of polling. |
| **ACK (`XACK`)** | Sent only after the event is handled or dead-lettered. |
| **Pending list** | Events delivered but not ACKed (for example after a crash). They are not lost and are picked up again on consumer restart. |
| **Dead-letter stream** | `DEAD_STREAM_NAME`: where a message goes after `MAX_RETRIES` failed attempts. |

Event format:

```json
{
  "event_type": "user.created",
  "event_id": "uuid",
  "timestamp": "2026-10-10T01:00:00+00:00",
  "data": {"user_id": "uuid", "name": "Ali", "email": "ali@example.com"}
}
```

## Getting started

Requires Python 3 and Docker.

```bash
# Install
python3 -m venv .venv
source .venv/bin/activate   # prompt shows (.venv); repeat in every new terminal
pip install -r requirements.txt
cp .env.example .env

# Terminal 1: Redis
docker run --rm -p 6379:6379 --name redis redis:7

# Terminal 2: user-service
uvicorn user_service.main:app --port 8000

# Terminal 3: email consumer
python -m email_service.consumer

# Terminal 4: create a user
curl -X POST http://localhost:8000/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Ali", "email": "ali@example.com"}'
```

The consumer prints `Welcome email sent to ali@example.com`.

## Configuration

All settings live in `.env` (copied from `.env.example`, never committed). The code has no built-in defaults, so a missing setting fails immediately with a `KeyError`.

| Variable | Example | Meaning |
|---|---|---|
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Where Redis runs |
| `STREAM_NAME` | `user_events` | Stream name (same in both services) |
| `DEAD_STREAM_NAME` | `user_events_dead` | Where failed messages are moved |
| `EVENT_TYPE_USER_CREATED` | `user.created` | Event type name (same in both services) |
| `GROUP_NAME` / `CONSUMER_NAME` | `email_group` / `email_consumer_1` | Consumer group and this worker's name |
| `BLOCK_MS` | `5000` | How long a read waits for new events |
| `MAX_RETRIES` | `3` | Max attempts per message |
| `RETRY_DELAY_SECONDS` | `1` | Wait between attempts |
| `FAIL_EMAIL_PREFIX` | `fail` | Emails starting with this simulate a broken email server (demo) |
| `CRASH_ON_FAIL` | `0` | `1` = consumer dies after giving up (crash demo) |

## Tests

```bash
python -m pytest
```

Unit tests mock Redis. One integration test uses a real Redis on temporary streams and is skipped automatically if Redis is not running.

## Failure demo

Any email starting with `FAIL_EMAIL_PREFIX` (`fail`) simulates a broken email server.

**Failure without crash**

```bash
curl -X POST http://localhost:8000/users -H "Content-Type: application/json" \
  -d '{"name": "Bad", "email": "fail@example.com"}'
```

The consumer prints `Attempt 1/3 failed`, `2/3`, `3/3`, then `Giving up ... moved to '<DEAD_STREAM_NAME>'`. Inspect it:

```bash
docker exec redis redis-cli XPENDING user_events email_group   # 0 pending
docker exec redis redis-cli XRANGE user_events_dead - +        # the failed message
```

**Crash before ACK**

```bash
CRASH_ON_FAIL=1 python -m email_service.consumer   # or set it in .env
```

Send the `fail@` request again. After the 3rd failed attempt the consumer exits immediately and the message stays pending. Restart normally: the attempt count (3) is stored in Redis, so the message goes straight to the dead-letter stream.

## Retry behavior

- Each message gets at most `MAX_RETRIES` (3) attempts, with `RETRY_DELAY_SECONDS` between them.
- The failed-attempt count is stored in Redis (hash `<STREAM_NAME>_retries`), so it survives consumer restarts.
- After the last failed attempt the message is copied to the dead-letter stream and ACKed, so nothing stays pending forever.
- If the consumer crashes before that, the message stays pending and is picked up on restart, with its stored count.
- Delivery is at-least-once, so a message can be delivered twice. A real consumer should deduplicate using `event_id`.

## Limitations

Left out on purpose to keep the demo simple:

- reprocessing or alerting on the dead-letter stream
- `XAUTOCLAIM` to take over messages from a dead consumer
- stream trimming (`MAXLEN`)
- authentication and a database
