import json
import math
import asyncio
from datetime import datetime, timedelta

from database.DataBase import async_connect_to_database
from google.functions import fetch_google_sheet_data, update_google_prices_data_with_format, update_google_sheet_data
from parsers.wildberies import get_products_and_prices, parse, get_prices_from_lk
from database.funcs_db import get_data_from_db

import logging
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("core"))


async def get_black_price_spp():
    conn = await async_connect_to_database()

    if not conn:
        logger.warning(f"Ошибка подключения к БД в fetch_data__get_adv_id")
        return

    request = ("SELECT cookie, authorizev3 "
               "FROM myapp_wblk")
    try:
        all_fields = await conn.fetch(request)
        lks = [
            {
                "cookie": row["cookie"],
                "authorizev3": row["authorizev3"]
            }
            for row in all_fields
        ]
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_wblk в get_black_price_spp. Запрос {request}. Error: {e}")
        raise
    finally:
        await conn.close()

    data = None
    try:
        data = (get_prices_from_lk(lk) for lk in lks)
        response = await asyncio.gather(*data)
    except Exception as e:
        logger.error(f"Ошибка получения данных из ЛК в get_black_price_spp. Ошибка {e}")
        raise

    conn = await async_connect_to_database()
    if not conn:
        logger.warning(f"Ошибка подключения к БД в set_wallet_discount")
        return
    try:
        request = ("SELECT nmid, wallet_discount "
                   "FROM myapp_price ")
        all_fields = await conn.fetch(request)
        result = {int(row["nmid"]): (row["wallet_discount"]) for row in all_fields}

    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_price. Запрос {request}. Error: {e}")
    finally:
        await conn.close()


    try:
        updates = {
            nmid["nmID"]: {
                "blackprice": math.floor((nmid["discountedPrices"][0] / 100) * (100 - (nmid.get("discountOnSite") or 0))),
                "spp": nmid.get("discountOnSite") or 0,
                "redprice": math.floor(
                    round((nmid["discountedPrices"][0] / 100) * (100 - (nmid.get("discountOnSite") or 0))) * ((100 - result[int(nmid["nmID"])])/100)
                )
            }
            for item in response
            for nmid in item["data"]["listGoods"]
        }
    except Exception as e:
        logger.error(f"Ошибка: {e}. Response: {response}")
        return

    try:
        values = [(nmid, data["blackprice"], data["spp"], data["redprice"]) for nmid, data in updates.items()]
    except Exception as e:
        logger.error(f"Ошибка преобразования данных для записи в БД в get_black_price_spp. Ошибка {e}")
        raise

    conn = await async_connect_to_database()
    if not conn:
        logger.warning("Ошибка подключения к БД в add_set_data_from_db")
        return
    try:
        query = """
            UPDATE myapp_price AS p 
            SET
                blackprice = v.blackprice,
                spp = v.spp,
                redprice = v.redprice
            FROM (VALUES
                {}
            ) AS v(nmid, blackprice, spp, redprice)
            WHERE v.nmid = p.nmid
        """.format(", ".join(
            f"({nmid}, {blackprice}, {spp}, {redprice})" for nmid, blackprice, spp, redprice in values
        ))
        await conn.execute(query)
    except Exception as e:
        logger.error(f"Ошибка обновления spp и blackprice в myapp_price. Error: {e}")
    finally:
        await conn.close()

# loop = asyncio.get_event_loop()
# res = loop.run_until_complete(set_prices_on_google())