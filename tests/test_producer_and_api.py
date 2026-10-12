import json
from unittest.mock import patch

from fastapi.testclient import TestClient

from user_service import producer
from user_service.main import app


def test_publish_user_created_writes_event_to_stream():
    with patch.object(producer, "redis_client") as fake_redis:
        fake_redis.xadd.return_value = "1-0"
        message_id = producer.publish_user_created({"email": "ali@example.com"})

    stream, fields = fake_redis.xadd.call_args.args
    event = json.loads(fields["payload"])
    assert message_id == "1-0"
    assert stream == producer.STREAM_NAME
    assert event["event_type"] == "user.created"
    assert event["data"] == {"email": "ali@example.com"}
    assert event["event_id"] and event["timestamp"]


def test_post_users_returns_user_and_publishes():
    with patch("user_service.main.publish_user_created", return_value="1-0") as publish:
        response = TestClient(app).post("/users", json={"name": "Ali", "email": "ali@example.com"})

    assert response.status_code == 200
    assert response.json()["stream_message_id"] == "1-0"
    assert response.json()["user"]["email"] == "ali@example.com"
    publish.assert_called_once()


def test_post_users_rejects_missing_email():
    response = TestClient(app).post("/users", json={"name": "Ali"})
    assert response.status_code == 422
