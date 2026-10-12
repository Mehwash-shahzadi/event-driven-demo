import json
from unittest.mock import patch

import pytest

from email_service import consumer


def make_fields(email):
    event = {"event_type": consumer.EVENT_TYPE_USER_CREATED, "data": {"email": email}}
    return {"payload": json.dumps(event)}


@pytest.fixture(autouse=True)
def fake_redis():
    """Mocked Redis whose retry-count hash behaves like the real one."""
    counts = {}
    with patch.object(consumer, "redis_client") as fake, \
         patch.object(consumer.time, "sleep"):
        fake.hget.side_effect = lambda key, field: counts.get(field)
        fake.hincrby.side_effect = lambda key, field, n: counts.__setitem__(field, counts.get(field, 0) + n) or counts[field]
        fake.counts = counts
        yield fake


def test_success_is_acknowledged(fake_redis):
    consumer.process("1-0", make_fields("ali@example.com"))
    fake_redis.xack.assert_called_once_with(consumer.STREAM_NAME, consumer.GROUP_NAME, "1-0")
    fake_redis.xadd.assert_not_called()


def test_failure_is_retried_then_dead_lettered(fake_redis):
    fields = make_fields("fail@example.com")
    with patch.object(consumer, "handle_event", side_effect=RuntimeError("down")) as handler:
        consumer.process("1-0", fields)

    assert handler.call_count == consumer.MAX_RETRIES
    fake_redis.xadd.assert_called_once_with(
        consumer.DEAD_STREAM_NAME, {"payload": fields["payload"], "original_id": "1-0"}
    )
    fake_redis.xack.assert_called_once()


def test_attempt_count_survives_restart(fake_redis):
    fake_redis.counts["1-0"] = consumer.MAX_RETRIES - 1   # 2 attempts already used
    with patch.object(consumer, "handle_event", side_effect=RuntimeError("down")) as handler:
        consumer.process("1-0", make_fields("fail@example.com"))

    assert handler.call_count == 1   # only one attempt left
    fake_redis.xadd.assert_called_once()


def test_succeeds_on_a_later_attempt(fake_redis):
    with patch.object(consumer, "handle_event", side_effect=[RuntimeError("down"), None]):
        consumer.process("1-0", make_fields("ali@example.com"))

    fake_redis.xack.assert_called_once()
    fake_redis.xadd.assert_not_called()


def test_malformed_payload_is_dead_lettered_without_crashing(fake_redis):
    consumer.process("1-0", {"payload": "{not json"})
    fake_redis.xadd.assert_called_once()
    fake_redis.xack.assert_called_once()


def test_other_event_types_are_ignored_and_acknowledged(fake_redis):
    fields = {"payload": json.dumps({"event_type": "user.deleted", "data": {}})}
    consumer.process("1-0", fields)
    fake_redis.xack.assert_called_once()
    fake_redis.xadd.assert_not_called()
