import asyncio

from fastapi_app.main import wblk_table, get_dimensions, get_adv_conversion, ProductsStatRequest
from sqlalchemy import select
from loader import BEARER
from context_logger import ContextLogger
import logging
from decorators import with_db_connection
from google.functions import update_google_sheet_data, fetch_google_sheet_data
from datetime import date


logger = ContextLogger(logging.getLogger("core"))


async def get_sloy() -> dict:
    """
    Получить данные с листа СЛОИ
    response: {"0907FCЧерный-2": "2 слой"}
    """
    url = "https://docs.google.com/spreadsheets/d/1djlCANhJ5eOWsHB95Gh7Duz0YWlF6cOT035dYsqOZQ4/edit?gid=1970091091#gid=1970091091"
    name = "СЛОИ"
    response = fetch_google_sheet_data(
        url, name, None
    )

    response = {i[0]: i[1] for i in response[1:]}
    return response

@with_db_connection
async def upload_dimensions_to_google():
    url = "https://docs.google.com/spreadsheets/d/1djlCANhJ5eOWsHB95Gh7Duz0YWlF6cOT035dYsqOZQ4/edit?gid=968779387#gid=968779387"
    name = "Dimensions"
    result = await get_dimensions(token=BEARER)

    headers = ["inn", "article_code", "img_url", "nmid", "subjectname", "height", "length", "width", "Слой"]
    data = [headers]

    sloi = await get_sloy()

    try:
        reform_data = [
            [
                value["inn"],
                key,
                value["img_url"],
                value["nmid"],
                value["subjectname"],
                value["dimensions"]["height"],
                value["dimensions"]["lenght"],
                value["dimensions"]["width"],
                sloi.get(key, "Слой не обнаружен")
            ]
            for key, value in result.items()
        ]
    except Exception as e:
        logger.error(f"Ошибка обработки данных в upload_dimensions_to_google: {e}")
        raise

    data += reform_data

    try:
        clear_rows = max(1000, len(data) + 300)
        clear_data = [["" for _ in range(9)] for _ in range(clear_rows)]

        update_google_sheet_data(
            spreadsheet_url=url,
            sheet_identifier=name,
            data_range=f"A1:I{clear_rows}",
            values=clear_data
        )

        update_google_sheet_data(
            spreadsheet_url=url,
            sheet_identifier=name,
            data_range=f"A1:I{len(data)}",
            values=data
        )
    except Exception as e:
        logger.error(f"Ошибка загрузки dimensions в таблицу: {e}")


@with_db_connection
async def upload_advconconversion_to_google():
    url = "https://docs.google.com/spreadsheets/d/1djlCANhJ5eOWsHB95Gh7Duz0YWlF6cOT035dYsqOZQ4/edit?gid=661019855#gid=661019855"
    name = "AdvConversion"

    sloi = await get_sloy()

    today = date.today()
    date_from_str = date(today.year - (today.month == 1), (today.month - 1) or 12, 1).strftime("%Y-%m-%d")

    query_inns = select(
        wblk_table.c.inn
    )
    rows = await database.fetch_all(query_inns)
    inns = [int(row["inn"]) for row in rows]

    payloads = [
        ProductsStatRequest(inn=inn, date_from=date_from_str)
        for inn in inns
    ]

    # Запускаем все запросы параллельно
    results_list = await asyncio.gather(
        *[get_adv_conversion(payload=payload, token=BEARER) for payload in payloads]
    )

    # Создаем словарь: ключ — inn, значение — результат
    results_by_inn = {inn: result for inn, result in zip(inns, results_list)}


    headers = ["инн", "артикул продавца", "клики", "показы", "корзина", "заказ", "артикул wb", "дата", "цвет", "слой"]
    data = [headers]

    try:
        reform_data = [
            [
                inn,
                stat["vendorcode"],
                stat["clicks"],
                stat["views"],
                stat["atbs"],
                stat["orders"],
                stat["nmid"],
                stat["date_wb"],
                stat["color"],
                sloi.get(stat["vendorcode"], "Слой не обнаружен")
            ]
            for inn, stats_list in results_by_inn.items()
            for stat in stats_list
        ]
    except Exception as e:
        logger.error(f"Ошибка обработки данных в upload_advconconversion_to_google: {e}")
        raise

    data += reform_data

    try:
        clear_rows = max(1000, len(data) + 300)
        clear_data = [["" for _ in range(9)] for _ in range(clear_rows)]

        update_google_sheet_data(
            spreadsheet_url=url,
            sheet_identifier=name,
            data_range=f"A1:J{clear_rows}",
            values=clear_data
        )

        update_google_sheet_data(
            spreadsheet_url=url,
            sheet_identifier=name,
            data_range=f"A1:J{len(data)}",
            values=data
        )
    except Exception as e:
        logger.error(f"Ошибка загрузки advconconversion в таблицу: {e}")