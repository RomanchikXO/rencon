from fastapi_app.main import get_dimensions
from loader import BEARER
from context_logger import ContextLogger
import logging


logger = ContextLogger(logging.getLogger("core"))

async def do_something():
    try:
        result = await get_dimensions(token=BEARER)
    except Exception as e:
        logger.error(e)
        raise
    logger.info(result)