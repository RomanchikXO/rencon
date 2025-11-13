import asyncio
from datetime import datetime, timedelta

from database.DataBase import async_connect_to_database
from google.functions import fetch_google_sheet_data, update_google_sheet_data

import logging
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("core"))


async def get_orders_in_db() -> dict:
    conn = await async_connect_to_database()
    date_from = str((datetime.now() + timedelta(hours=3) - timedelta(days=14)).replace(hour=0, minute=0, second=0, microsecond=0))
    date_to = str((datetime.now() + timedelta(hours=3)).replace(hour=0, minute=0, second=0, microsecond=0))

    if not conn:
        logger.warning(f"Ошибка подключения к БД в fetch_data__get_adv_id")
        raise
    try:
        request = (f"SELECT supplierarticle, COUNT(*) AS total "
                   f"FROM myapp_orders "
                   f"WHERE date >= '{date_from}' AND date < '{date_to}' "
                   f"AND EXISTS (SELECT 1 FROM myapp_wblk WHERE myapp_wblk.id = myapp_orders.lk_id)"
                   f"GROUP BY supplierarticle")
        all_fields = await conn.fetch(request)
        result = {row["supplierarticle"]: row["total"] for row in all_fields}
        return result
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_orders. Запрос {request}. Error: {e}")
    finally:
        await conn.close()


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