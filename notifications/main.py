from fastapi import FastAPI, HTTPException
from models import Anomaly, Notification
from reporting import get_notification_message
from messaging import send_notification_telegram, send_notification_discord

app = FastAPI(
    title="Delfos bot API",
    description="Delfos bot sends notifications to Telegram and Discord based on predefined templates.",
    version="1.0.0",
)


@app.post("/notify")
def notify(request: Anomaly):
    notification_message: Notification = get_notification_message(request)
    print(f"Notificación generada: {notification_message}")
    try:
        response = send_notification_telegram(notification_message)
        print(f"Notificación de Telegram enviada: {response.get('ok')}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        response = send_notification_discord(notification_message)
        print(f"Notificación de Discord enviada: {response.get('ok')}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
