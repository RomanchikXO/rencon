import asyncio
from datetime import datetime, timedelta

from database.DataBase import async_connect_to_database
from google.functions import fetch_google_sheet_data, update_google_sheet_data

import logging
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("core"))


async def get_quantity_in_db(supplierarticles: list) -> dict:
    conn = await async_connect_to_database()

    if not conn:
        logger.warning(f"Ошибка подключения к БД в fetch_data__get_adv_id")
        raise
    try:
        request = ("SELECT supplierarticle, sum(quantity) AS total "
                   "FROM myapp_stocks "
                   "WHERE supplierarticle = ANY($1) "
                   "AND EXISTS (SELECT 1 FROM myapp_wblk WHERE myapp_wblk.id = myapp_stocks.lk_id)"
                   "GROUP BY supplierarticle")
        all_fields = await conn.fetch(request, supplierarticles)
        result = {row["supplierarticle"]: row["total"] for row in all_fields}
        return result
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_stocks. Запрос {request}. Error: {e}")
    finally:
        await conn.close()


# loop = asyncio.get_event_loop()
# res = loop.run_until_complete(set_orders_quantity_in_google())