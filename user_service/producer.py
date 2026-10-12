# PRODUCER: builds events and publishes them to the Redis Stream.
import json
import os
import uuid
from datetime import datetime, timezone

import redis
from dotenv import load_dotenv

load_dotenv()

# All settings come from .env (see .env.example). A missing setting raises KeyError.
redis_client = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    decode_responses=True,
)

STREAM_NAME = os.environ["STREAM_NAME"]   # must match the consumer
EVENT_TYPE_USER_CREATED = os.environ["EVENT_TYPE_USER_CREATED"]   # must match the consumer


def publish_user_created(user: dict) -> str:
    event = {
        "event_type": EVENT_TYPE_USER_CREATED,
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": user,
    }

    # Stream entries are flat key/value pairs, so the event is stored as JSON under "payload".
    return redis_client.xadd(STREAM_NAME, {"payload": json.dumps(event)})
