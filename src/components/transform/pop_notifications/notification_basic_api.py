import uvicorn
from fastapi import FastAPI
import pydantic
from pydantic import BaseModel

app = FastAPI()


class Notification(BaseModel):
    phone: str
    message: str


@app.post("/notifications_covid_tests")
def send_notifications(notification: Notification):
    print(f"notification sent to the phone {notification.phone}")
    print(notification.message)
    return {"status": "success- notification sent!!", "phone": notification.phone, "message": notification.message}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
