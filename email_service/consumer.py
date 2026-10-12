# CONSUMER: reads events from the Redis Stream and reacts to them.
import json
import os
import time

import redis
from dotenv import load_dotenv

load_dotenv()

# All settings come from .env (see .env.example). A missing setting raises KeyError.
redis_client = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    decode_responses=True,
    socket_timeout=None,   # newer redis-py defaults to 5s, which races with the 5s blocking read
)

STREAM_NAME = os.environ["STREAM_NAME"]   # must match the producer
DEAD_STREAM_NAME = os.environ["DEAD_STREAM_NAME"]
RETRY_COUNTS_KEY = f"{STREAM_NAME}_retries"   # Redis hash: message id -> failed attempts
EVENT_TYPE_USER_CREATED = os.environ["EVENT_TYPE_USER_CREATED"]   # must match the producer
GROUP_NAME = os.environ["GROUP_NAME"]
CONSUMER_NAME = os.environ["CONSUMER_NAME"]
BLOCK_MS = int(os.environ["BLOCK_MS"])
MAX_RETRIES = int(os.environ["MAX_RETRIES"])
RETRY_DELAY_SECONDS = float(os.environ["RETRY_DELAY_SECONDS"])

# Failure demo: emails starting with this prefix simulate a broken email server.
FAIL_EMAIL_PREFIX = os.environ["FAIL_EMAIL_PREFIX"]
CRASH_ON_FAIL = os.environ["CRASH_ON_FAIL"] == "1"   # die before ACK to demo crash recovery


def create_group():
    # id="0": a new group starts from the beginning of the stream.
    # mkstream=True: create the stream if it doesn't exist yet.
    try:
        redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        print(f"Created group '{GROUP_NAME}'")
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):   # group already exists -> fine
            raise


def handle_event(event: dict):
    if event["event_type"] != EVENT_TYPE_USER_CREATED:
        return

    email = event["data"]["email"]

    if email.startswith(FAIL_EMAIL_PREFIX):
        raise RuntimeError(f"Email server down for {email}")

    print(f"Welcome email sent to {email}")


def process(message_id: str, fields: dict):
    # The attempt count lives in Redis, so it survives consumer restarts.
    attempts = int(redis_client.hget(RETRY_COUNTS_KEY, message_id) or 0)

    while attempts < MAX_RETRIES:
        try:
            handle_event(json.loads(fields["payload"]))
            break
        except Exception as e:
            attempts = redis_client.hincrby(RETRY_COUNTS_KEY, message_id, 1)
            print(f"Attempt {attempts}/{MAX_RETRIES} failed: {e}")
            if attempts < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
    else:
        # All attempts used up -> move the message to the dead-letter stream.
        if CRASH_ON_FAIL:
            print("Consumer crashing before ACK!")
            os._exit(1)   # sudden crash: the message stays pending
        redis_client.xadd(
            DEAD_STREAM_NAME, {"payload": fields.get("payload", ""), "original_id": message_id}
        )
        print(f"Giving up. Message {message_id} moved to '{DEAD_STREAM_NAME}'")

    # Handled successfully or dead-lettered -> done with this message.
    redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
    redis_client.hdel(RETRY_COUNTS_KEY, message_id)


def main():
    create_group()

    # id="0" returns MY messages that were delivered but never ACKed (crash recovery).
    pending = redis_client.xreadgroup(GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: "0"})
    for _, messages in pending:
        for message_id, fields in messages:
            print(f"Retrying pending message {message_id}")
            process(message_id, fields)

    print("Waiting for events...")
    while True:
        # id=">" returns only messages never delivered to this group.
        # block waits inside Redis for new events, so there is no busy loop.
        result = redis_client.xreadgroup(
            GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=1, block=BLOCK_MS
        )
        for _, messages in result:
            for message_id, fields in messages:
                process(message_id, fields)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
