from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from templates import get_template
from sender import send_notification

app = FastAPI(
    title="Delfos Telegram bot API",
    description="Delfos Telegram bot sends notifications to Telegram based on predefined templates.",
    version="1.0.0",
)


class NotifyRequest(BaseModel):
    topic: str
    data: dict


@app.post("/notify")
def notify(request: NotifyRequest):
    try:
        notification = get_template(request.topic, request.data)
        response = send_notification(notification)
        print(f"Notificación enviada: {response.get('ok')}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
