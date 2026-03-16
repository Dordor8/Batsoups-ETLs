from fastapi import FastAPI
import pydantic
from pydantic import BaseModel

app = FastAPI()

class Notification(BaseModel):
    
