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

async def set_prices_on_google():
    """
    - Получаем nmid и цену из бд
    - Получаем данные из гугл таблицы
    - Сопостовляем и обновляем гугл таблицу
    - Записываем данные в гугл таблицу
    :return:
    """

    url = "https://docs.google.com/spreadsheets/d/19hbmos6dX5WGa7ftRagZtbCVaY-bypjGNE2u0d9iltk/edit?gid=1041007463#gid=1041007463"

    data = await get_data_from_db(
        table_name="myapp_price",
        columns=["nmid", "sizes", "discount", "spp", "wallet_discount", "redprice"],
        additional_conditions="EXISTS (SELECT 1 FROM myapp_wblk WHERE myapp_wblk.id = myapp_price.lk_id)"
    )
    result_dict = {
        item['nmid']: {
            "sizes": json.loads(item["sizes"]),
            "discount": item["discount"],
            "spp": item["spp"],
            "wallet_discount": item["wallet_discount"],
            "redprice": item["redprice"]
        }
        for item in data
    }

    try:
        google_data = fetch_google_sheet_data(
            spreadsheet_url=url,
            sheet_identifier="Цены с WB",

        )
    except Exception as e:
        logger.error(f"Ошибка получения данных с листа 'Цены с WB': {e}")

    try:
        for index, _string in enumerate(google_data):
            if index == 0: continue
            nmID = int(_string[2])

            if nm_info:=result_dict.get(nmID):
                price = int(nm_info["sizes"][0]["price"])
                discount_table = str(nm_info["discount"]) + "%"
                spp_table = str(nm_info["spp"]) + "%"
                wallet_discount_table = str(nm_info["wallet_discount"]) + "%"
                redprice = int(nm_info["redprice"])

                google_data[index][8] = price
                google_data[index][10] = discount_table
                google_data[index][11] = spp_table
                google_data[index][12] = wallet_discount_table
                google_data[index][13] = redprice
            else:
                google_data[index][8] = "0"
                google_data[index][10] = "0%"
                google_data[index][11] = "0%"
                google_data[index][12] = "0%"
                google_data[index][13] = "0"
    except Exception as e:
        logger.error(f"Ошибка обработки данных в set_prices_on_google {e}")
        raise

    try:
        update_google_prices_data_with_format(
            url, int(url.split("=")[-1]), 0, 0, google_data
        )
    except Exception as e:
        logger.error(f"Ошибка обновления листа 'Цены с WB': {e}")
        raise

    # СКОРОСТЬ ПРОДАЖ
    now_msk = datetime.now() + timedelta(hours=3)
    yesterday_end = now_msk.replace(hour=23, minute=59, second=59, microsecond=0) - timedelta(days=1)
    seven_days_ago = yesterday_end - timedelta(days=7)

    conn = await async_connect_to_database()
    if not conn:
        logger.warning(f"Ошибка подключения к БД в set_prices_on_google")
        return

    request = ("SELECT supplierarticle, COUNT(id) AS total_orders "
               "FROM myapp_orders "
               "WHERE date >= $1 "
               "GROUP BY supplierarticle")
    try:
        all_fields = await conn.fetch(request, seven_days_ago)
        data = [
            [row["supplierarticle"].rstrip(), round(row["total_orders"] / 7, 2)]
            for row in all_fields
        ]
        for _ in range(50):
            data.append(["", ""])
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_orders в set_prices_on_google. Запрос {request}. Error: {e}")
        raise
    finally:
        await conn.close()

    url = "https://docs.google.com/spreadsheets/d/19hbmos6dX5WGa7ftRagZtbCVaY-bypjGNE2u0d9iltk/edit?gid=573978297#gid=573978297"
    try:
        update_google_sheet_data(
            url, "Скорость продаж", f"A1:B{len(data)}", data
        )
    except Exception as e:
        logger.error(f"Ошибка обновления листа 'Скорость продаж': {e}")

    # ОСТАТКИ
    conn = await async_connect_to_database()
    if not conn:
        logger.warning(f"Ошибка подключения к БД в set_prices_on_google")
        return

    request = ("SELECT supplierarticle, SUM(quantity) AS total_quantity "
               "FROM myapp_stocks "
               "GROUP BY supplierarticle")
    try:
        all_fields = await conn.fetch(request)
        result_dict = [
            [row["supplierarticle"].rstrip(), int(row["total_quantity"])]
            for row in all_fields
        ]
        for _ in range(50):
            result_dict.append(["", ""])
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_stocks в set_prices_on_google. Запрос {request}. Error: {e}")
        raise
    finally:
        await conn.close()

    url = "https://docs.google.com/spreadsheets/d/19hbmos6dX5WGa7ftRagZtbCVaY-bypjGNE2u0d9iltk/edit?gid=2136512051#gid=2136512051"
    try:
        update_google_sheet_data(
            url, "Остатки", f"A2:B{len(result_dict)+1}", result_dict
        )
    except Exception as e:
        logger.error(f"Ошибка обновления листа 'Остатки': {e}")
        raise

    # КлиентWB
    conn = await async_connect_to_database()
    if not conn:
        logger.warning(f"Ошибка подключения к БД в set_prices_on_google")
        return

    request = ("SELECT supplierarticle, SUM(inwaytoclient) AS total_inwaytoclient, SUM(inwayfromclient) AS total_inwayfromclient "
               "FROM myapp_stocks "
               "GROUP BY supplierarticle")
    try:
        all_fields = await conn.fetch(request)
        result_dict = [
            [row["supplierarticle"], int(row["total_inwaytoclient"]), int(row["total_inwayfromclient"])]
            for row in all_fields
        ]
        for _ in range(50):
            result_dict.append(["", ""])
    except Exception as e:
        logger.error(
            f"Ошибка получения данных из myapp_stocks в set_prices_on_google. Запрос {request}. Error: {e}")
        raise
    finally:
        await conn.close()

    url = "https://docs.google.com/spreadsheets/d/19hbmos6dX5WGa7ftRagZtbCVaY-bypjGNE2u0d9iltk/edit?gid=742171447#gid=742171447"
    try:
        update_google_sheet_data(
            url, "КлиентWB", f"A2:C{len(result_dict) + 1}", result_dict
        )
    except Exception as e:
        logger.error(f"Ошибка обновления листа 'КлиентWB': {e}")
        raise


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