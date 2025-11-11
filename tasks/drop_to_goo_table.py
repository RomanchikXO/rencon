from fastapi.testclient import TestClient
from fastapi_app.main import app
from loader import BEARER
from context_logger import ContextLogger
import logging

client = TestClient(app)

logger = ContextLogger(logging.getLogger("core"))

def do_something():
    try:
        response = client.get("/dimensions/", headers={"Authorization": f"Bearer {BEARER}"})
        logger.info(response.json())  # выведем JSON-ответ
    except Exception as e:
        logger.error(e)
        raise