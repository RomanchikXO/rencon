from fastapi_app.main import get_dimensions, database
from loader import BEARER
from context_logger import ContextLogger
import logging

logger = ContextLogger(logging.getLogger("core"))

async def do_something():
    try:
        # Подключаемся к базе вручную
        if not database.is_connected:
            await database.connect()

        # Вызываем эндпоинт напрямую
        result = await get_dimensions(token=BEARER)
        logger.info(result)

    except Exception as e:
        logger.error(f"Ошибка при выполнении get_dimensions: {e}")

    finally:
        # Закрываем соединение, чтобы не держать лишние подключения
        if database.is_connected:
            await database.disconnect()
