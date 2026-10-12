# user-service: FastAPI app that receives HTTP requests and publishes events.
import uuid

from fastapi import FastAPI
from pydantic import BaseModel

from user_service.producer import publish_user_created

app = FastAPI()


class UserIn(BaseModel):
    name: str
    email: str


@app.post("/users")
def create_user(body: UserIn):
    # No database: the user is just a dict with a generated id.
    user = {
        "user_id": str(uuid.uuid4()),
        "name": body.name,
        "email": body.email,
    }

    # The service only announces the event; it never sends the email itself.
    message_id = publish_user_created(user)

    return {"user": user, "stream_message_id": message_id}
