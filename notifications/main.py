from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from telegrambot import send_notification as send_notification_telegram
from discordbot import send_notification as send_notification_discord

app = FastAPI(
    title="Delfos bot API",
    description="Delfos bot sends notifications to Telegram and Discord based on predefined templates.",
    version="1.0.0",
)


class NotifyRequest(BaseModel):
    text: str
    image_path: Optional[str] = None


@app.post("/notify")
def notify(request: NotifyRequest):
    try:
        response = send_notification_telegram(request)
        print(f"Notificación de Discord enviada: {response.get('ok')}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    try:
        response = send_notification_discord(request)
        print(f"Notificación de Discord enviada: {response.get('ok')}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
