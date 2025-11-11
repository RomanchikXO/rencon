from fastapi_app.main import get_dimensions, database
from loader import BEARER
from context_logger import ContextLogger
import logging
from decorators import with_db_connection

logger = ContextLogger(logging.getLogger("core"))

@with_db_connection
async def do_something():
    result = await get_dimensions(token=BEARER)
    logger.info(result)
