"""Integration test against a real Redis. Skipped automatically if Redis is not running."""
import uuid
from unittest.mock import patch

import pytest
import redis

from email_service import consumer
from user_service import producer


@pytest.fixture
def real_redis():
    try:
        consumer.redis_client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis is not running")

    suffix = uuid.uuid4().hex[:8]   # temporary names so we never touch the demo streams
    names = {
        "STREAM_NAME": f"test_events_{suffix}",
        "DEAD_STREAM_NAME": f"test_dead_{suffix}",
        "RETRY_COUNTS_KEY": f"test_retries_{suffix}",
    }
    with patch.multiple(consumer, **names), \
         patch.object(producer, "STREAM_NAME", names["STREAM_NAME"]), \
         patch.object(consumer.time, "sleep"):
        yield consumer.redis_client
    consumer.redis_client.delete(*names.values())


def read_one(client):
    result = client.xreadgroup(consumer.GROUP_NAME, consumer.CONSUMER_NAME,
                               {consumer.STREAM_NAME: ">"}, count=1)
    return result[0][1][0]   # (message_id, fields)


def test_failing_message_ends_in_dead_stream_with_nothing_pending(real_redis):
    consumer.create_group()
    producer.publish_user_created({"email": "fail@example.com"})

    message_id, fields = read_one(real_redis)
    consumer.process(message_id, fields)

    assert real_redis.xpending(consumer.STREAM_NAME, consumer.GROUP_NAME)["pending"] == 0
    dead = real_redis.xrange(consumer.DEAD_STREAM_NAME)
    assert len(dead) == 1
    assert dead[0][1]["original_id"] == message_id
