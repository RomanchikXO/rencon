import asyncio

from fastapi_app.main import (wblk_table, get_dimensions, get_adv_conversion, ProductsStatRequest, get_adv_cost,
                              get_adv_reg_sales, ProductsQuantRequest, products_quantity_endpoint,
                              products_stat_endpoint, fin_report_endpoint, FinReportRequest)
from sqlalchemy import select
from loader import BEARER
from context_logger import ContextLogger
import logging
from decorators import with_db_connection
from google.functions import (update_google_sheet_data, fetch_google_sheet_data, gspread, SCOPES, Credentials,
                              CREDENTIALS_FILE, clear_list)
from datetime import date, datetime, timedelta
from fastapi_app.main import database


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
        clear_list(url, name)

        update_google_sheet_data(
            spreadsheet_url=url,
            sheet_identifier=name,
            data_range=f"A1:I{len(data)}",
            values=data
        )
    except Exception as e:
        logger.error(f"Ошибка загрузки dimensions в таблицу: {e}")


async def get_first_day_last_month() -> str:
    """
    получить первое число прошлого месяца
    :return:
    """
    today = date.today()
    date_from_str = date(today.year - (today.month == 1), (today.month - 1) or 12, 1).strftime("%Y-%m-%d")
    return date_from_str


async def get_last_week_monday() -> str:
    """
    Получить понедельник прошлой недели
    """
    today = date.today()
    monday = today - timedelta(days=today.weekday() + 7)
    return monday.strftime("%Y-%m-%d")


async def get_last_week_sunday() -> str:
    """
    Получить вскр прошлой недели
    :return:
    """
    today = date.today()
    sunday = today - timedelta(days=today.weekday() + 1)
    return sunday.strftime("%Y-%m-%d")


@with_db_connection
async def upload_advconversion_to_google(mode):
    if mode == "Dima":
        url = "https://docs.google.com/spreadsheets/d/1djlCANhJ5eOWsHB95Gh7Duz0YWlF6cOT035dYsqOZQ4/edit?gid=661019855#gid=661019855"
    elif mode == "Anna":
        url = "https://docs.google.com/spreadsheets/d/1uxHnuWtUbg5MNEvUzuxUO8VoVG6Th-TE9MwgNotGDH0/edit?gid=0#gid=0"

    name_list = "AdvConversion"

    sloi = await get_sloy()

    query_inns = select(
        wblk_table.c.inn,
        wblk_table.c.name
    )
    rows = await database.fetch_all(query_inns)
    inns = {int(row["inn"]): row["name"] for row in rows}

    if mode == "Dima":
        date_from_str = await get_first_day_last_month()
        payloads = [
            ProductsStatRequest(inn=inn, date_from=date_from_str)
            for inn, name in inns.items()
        ]
    elif mode == "Anna":
        date_from_str = await get_last_week_monday()
        date_to_str = await get_last_week_sunday()

        payloads = [
            ProductsStatRequest(inn=inn, date_from=date_from_str, date_to=date_to_str)
            for inn, name in inns.items()
        ]

    # Запускаем все запросы параллельно
    results_list = await asyncio.gather(
        *[get_adv_conversion(payload=payload, token=BEARER) for payload in payloads]
    )

    # Создаем словарь: ключ — inn, значение — результат
    results_by_inn = {inn: result for inn, result in zip(inns, results_list)}

    reform_data = {}

    if mode == "Dima":
        headers = ["инн", "артикул продавца", "клики", "показы", "корзина", "заказ", "артикул wb", "дата", "цвет", "слой"]
        try:
            intermed_data = [
                [
                    inn,
                    stat["vendorcode"],
                    stat["clicks"],
                    stat["views"],
                    stat["atbs"],
                    stat["orders"],
                    stat["nmid"],
                    stat["date_wb"].strftime("%d.%m.%Y"),
                    stat["color"],
                    sloi.get(stat["vendorcode"], "Слой не обнаружен")
                ]
                for inn, stats_list in results_by_inn.items()
                for stat in stats_list
            ]

            reform_data[""] = intermed_data
        except Exception as e:
            logger.error(f"Ошибка обработки данных в upload_advconversion_to_google для {mode}: {e}")
            raise
    elif mode == "Anna":
        headers = ["артикул продавца", "клики", "показы", "корзина", "заказ", "артикул wb", "дата", "цвет", "слой"]
        try:
            for inn, stats_list in results_by_inn.items():
                intermed_data = [
                    [
                        stat["vendorcode"],
                        stat["clicks"],
                        stat["views"],
                        stat["atbs"],
                        stat["orders"],
                        stat["nmid"],
                        stat["date_wb"].strftime("%d.%m.%Y"),
                        stat["color"],
                        sloi.get(stat["vendorcode"], "Слой не обнаружен")
                    ]
                    for stat in stats_list
                ]

                reform_data[inns.get(inn)] = intermed_data

        except Exception as e:
            logger.error(f"Ошибка обработки данных в upload_advconversion_to_google для {mode}: {e}")


    for name, intermed_data in reform_data.items():
        data = [headers]
        data += intermed_data

        letter = "J" if mode == "Dima" else "I"
        name = name_list + name

        try:
            try:
                clear_list(url, name)
            except:
                pass

            update_google_sheet_data(
                spreadsheet_url=url,
                sheet_identifier=name,
                data_range=f"A1:{letter}{len(data)}",
                values=data
            )
        except Exception as e:
            logger.error(f"Ошибка загрузки advconversion в таблицу: {e}")


@with_db_connection
async def upload_advcost_to_google():
    url = "https://docs.google.com/spreadsheets/d/1djlCANhJ5eOWsHB95Gh7Duz0YWlF6cOT035dYsqOZQ4/edit?gid=1726292074#gid=1726292074"
    name = "AdvCost"

    sloi = await get_sloy()

    date_from_str = await get_first_day_last_month()

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
        *[get_adv_cost(payload=payload, token=BEARER) for payload in payloads]
    )

    # Создаем словарь: ключ — inn, значение — результат
    results_by_inn = {inn: result for inn, result in zip(inns, results_list)}


    headers = ["inn", "артикул продавца", "nmid", "cost", "color", "date_wb", "слой"]
    data = [headers]

    try:
        reform_data = [
            [
                inn,
                stat["vendorcode"],
                stat["nmid"],
                stat["cost"],
                stat["color"],
                stat["date_wb"].strftime("%d.%m.%Y"),
                sloi.get(stat["vendorcode"], "Слой не обнаружен")
            ]
            for inn, stats_list in results_by_inn.items()
            for stat in stats_list
        ]
    except Exception as e:
        logger.error(f"Ошибка обработки данных в upload_advcost_to_google: {e}")
        raise

    data += reform_data

    try:
        clear_list(url, name)

        update_google_sheet_data(
            spreadsheet_url=url,
            sheet_identifier=name,
            data_range=f"A1:G{len(data)}",
            values=data
        )
    except Exception as e:
        logger.error(f"Ошибка загрузки advcost в таблицу: {e}")


@with_db_connection
async def upload_salesreport_to_google():
    url = "https://docs.google.com/spreadsheets/d/1djlCANhJ5eOWsHB95Gh7Duz0YWlF6cOT035dYsqOZQ4/edit?gid=240040532#gid=240040532"
    name = "SalesReport"

    sloi = await get_sloy()

    date_from_str = await get_first_day_last_month()

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
        *[get_adv_reg_sales(payload=payload, token=BEARER) for payload in payloads]
    )

    # Создаем словарь: ключ — inn, значение — результат
    results_by_inn = {inn: result for inn, result in zip(inns, results_list)}


    headers = ["inn", "vendorcode", "rub", "sht", "nmid", "color", "date_wb", "Слой"]
    data = [headers]

    try:
        reform_data = [
            [
                inn,
                stat["vendorcode"],
                stat["rub"],
                stat["sht"],
                stat["nmid"],
                stat["color"],
                stat["date_wb"].strftime("%d.%m.%Y"),
                sloi.get(stat["vendorcode"], "Слой не обнаружен")
            ]
            for inn, stats_list in results_by_inn.items()
            for stat in stats_list
        ]
    except Exception as e:
        logger.error(f"Ошибка обработки данных в upload_salesreport_to_google: {e}")
        raise

    data += reform_data

    try:
        clear_list(url, name)

        update_google_sheet_data(
            spreadsheet_url=url,
            sheet_identifier=name,
            data_range=f"A1:H{len(data)}",
            values=data
        )
    except Exception as e:
        logger.error(f"Ошибка загрузки salesreport в таблицу: {e}")

@with_db_connection
async def upload_ostatki_to_google(mode):
    if mode == "Dima":
        url = "https://docs.google.com/spreadsheets/d/1djlCANhJ5eOWsHB95Gh7Duz0YWlF6cOT035dYsqOZQ4/edit?gid=177198408#gid=177198408"
    elif mode == "Anna":
        url = "https://docs.google.com/spreadsheets/d/1uxHnuWtUbg5MNEvUzuxUO8VoVG6Th-TE9MwgNotGDH0/edit?gid=0#gid=0"
    name_list = "Ostatki"

    sloi = await get_sloy()

    query_inns = select(
        wblk_table.c.inn,
        wblk_table.c.name
    )
    rows = await database.fetch_all(query_inns)
    inns = {int(row["inn"]): row["name"] for row in rows}

    payloads = [
        ProductsQuantRequest(inn=inn)
        for inn, name in inns.items()
    ]

    # Запускаем все запросы параллельно
    results_list = await asyncio.gather(
        *[products_quantity_endpoint(payload=payload, token=BEARER) for payload in payloads]
    )

    # Создаем словарь: ключ — inn, значение — результат
    results_by_inn = {inn: result for inn, result in zip(inns, results_list)}
    reform_data = {}

    if mode == "Dima":
        headers = [
            "inn", "vendorcode", "nmid", "quantity", "inwaytoclient", "inwayfromclient", "warehouse", "size", "color", "Слой"
        ]

        try:
            intermed_data = [
                [
                    inn,
                    stat["vendorcode"],
                    stat["nmid"],
                    stat["quantity"],
                    stat["inwaytoclient"],
                    stat["inwayfromclient"],
                    stat["warehouse"],
                    stat["size"],
                    stat["color"],
                    sloi.get(stat["vendorcode"], "Слой не обнаружен")
                ]
                for inn, stats_list in results_by_inn.items()
                for stat in stats_list
            ]

            reform_data[""] = intermed_data
        except Exception as e:
            logger.error(f"Ошибка обработки данных в upload_ostatki_to_google для {mode}: {e}")
            raise
    elif mode == "Anna":
        headers = ["vendorcode", "nmid", "quantity", "inwaytoclient", "inwayfromclient", "warehouse", "size", "color",
                   "Слой"]
        try:
            for inn, stats_list in results_by_inn.items():
                intermed_data = [
                    [
                        stat["vendorcode"],
                        stat["nmid"],
                        stat["quantity"],
                        stat["inwaytoclient"],
                        stat["inwayfromclient"],
                        stat["warehouse"],
                        stat["size"],
                        stat["color"],
                        sloi.get(stat["vendorcode"], "Слой не обнаружен")
                    ]
                    for stat in stats_list
                ]

                reform_data[inns.get(inn)] = intermed_data

        except Exception as e:
            logger.error(f"Ошибка обработки данных в upload_ostatki_to_google для {mode}: {e}")

    for name, intermed_data in reform_data.items():
        data = [headers]
        data += intermed_data

        letter = "J" if mode == "Dima" else "I"
        name = name_list + name

        try:
            try:
                clear_list(url, name)
            except:
                pass

            update_google_sheet_data(
                spreadsheet_url=url,
                sheet_identifier=name,
                data_range=f"A1:{letter}{len(data)}",
                values=data
            )
        except Exception as e:
            logger.error(f"Ошибка загрузки ostatki в таблицу: {e}")


@with_db_connection
async def upload_products_stat_to_google():
    url = "https://docs.google.com/spreadsheets/d/1djlCANhJ5eOWsHB95Gh7Duz0YWlF6cOT035dYsqOZQ4/edit?gid=1628086389#gid=1628086389"
    name = "ProductsStat"

    sloi = await get_sloy()

    date_from_str = await get_first_day_last_month()

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
        *[products_stat_endpoint(payload=payload, token=BEARER) for payload in payloads]
    )

    # Создаем словарь: ключ — inn, значение — результат
    results_by_inn = {inn: result for inn, result in zip(inns, results_list)}

    headers = [
        "inn", "vendorcode", "nmid", "date_wb", "color", "ordersSumRub", "ordersCount", "Слой"
    ]
    data = [headers]

    try:
        reform_data = [
            [
                inn,
                stat["vendorcode"],
                stat["nmid"],
                stat["date_wb"].strftime("%d.%m.%Y"),
                stat["color"],
                stat["ordersSumRub"],
                stat["ordersCount"],
                sloi.get(stat["vendorcode"], "Слой не обнаружен")
            ]
            for inn, stats_list in results_by_inn.items()
            for stat in stats_list
        ]
    except Exception as e:
        logger.error(f"Ошибка обработки данных в upload_products_stat_to_google: {e}")
        raise

    data += reform_data

    try:
        clear_list(url, name)

        update_google_sheet_data(
            spreadsheet_url=url,
            sheet_identifier=name,
            data_range=f"A1:H{len(data)}",
            values=data
        )
    except Exception as e:
        logger.error(f"Ошибка загрузки products_stat в таблицу: {e}")


@with_db_connection
async def upload_fin_report_to_google():
    url = "https://docs.google.com/spreadsheets/d/1djlCANhJ5eOWsHB95Gh7Duz0YWlF6cOT035dYsqOZQ4/edit?gid=1889074269#gid=1889074269"
    name = "FinancialData"

    sloi = await get_sloy()

    date_from_str = await get_first_day_last_month()

    query_inns = select(wblk_table.c.inn)
    rows = await database.fetch_all(query_inns)
    inns = [int(row["inn"]) for row in rows]

    payloads = [FinReportRequest(
        inn=inn,
        date_from=date_from_str,
        supplier_oper_name=['возврат', 'добровольная компенсация при возврате',
                            'компенсация скидки по программе лояльности', 'компенсация ущерба', 'коррекция логистики',
                            'логистика', 'платная приемка', 'продажа', 'стоимость участия в программе лояльности',
                            'сумма удержанная за начисленные баллы программы лояльности', 'удержание', 'штраф'])
        for inn in inns]

    # Запускаем все запросы параллельно
    results_list = await asyncio.gather(*[fin_report_endpoint(payload=payload, token=BEARER) for payload in payloads])

    # Создаем словарь: ключ — inn, значение — результат
    results_by_inn = {inn: result["data"] for inn, result in zip(inns, results_list)}

    headers = [
        "inn", "nmid", "retail_price", "retail_amount", "ppvz_for_pay", "delivery_rub", "acceptance", "penalty", "date_wb",
        "sale_dt", "color", "supplier_oper_name", "subjectname", "vendorcode", "Слой",
        "Стоимость закупа", "Комиссия"
    ]
    data = [headers]

    try:
        reform_data = [
            [
                inn,
                stat["nmid"],
                stat["retail_price"],
                stat["retail_amount"],
                stat["ppvz_for_pay"],
                stat["delivery_rub"],
                stat["acceptance"],
                stat.get("penalty", 0),
                stat["date_wb"].strftime("%d.%m.%Y") if isinstance(stat["date_wb"], (date, datetime)) else stat["date_wb"],
                stat["sale_dt"].strftime("%d.%m.%Y") if stat.get("sale_dt") and isinstance(stat["sale_dt"],
                                                                                  (date, datetime)) else (
                            stat.get("sale_dt") or ""),
                stat["color"],
                stat["supplier_oper_name"],
                stat["subjectname"],
                stat["vendorcode"],
                sloi.get(stat["vendorcode"], "Слой не обнаружен"),
                "",
                stat["retail_amount"] - stat["ppvz_for_pay"]
            ]
            for inn, stats_list in results_by_inn.items()
            for stat in stats_list if stat["supplier_oper_name"] not in ['', 'хранение']
        ]
    except Exception as e:
        logger.error(f"Ошибка обработки данных в upload_fin_report_to_google: {e}")
        raise

    data += reform_data

    BATCH_SIZE = 50000  # количество строк за один запрос
    NUM_COLS = 17  # столбцы A-Q
    def batch_update(sheet_name, data, as_user_input=False):
        total_rows = len(data)
        for start in range(0, total_rows, BATCH_SIZE):
            end = min(start + BATCH_SIZE, total_rows)
            batch = data[start:end]
            range_str = f"A{start + 1}:Q{end}"
            update_google_sheet_data(url, sheet_name, range_str, batch, as_user_input=as_user_input)

    try:
        # 1️⃣ Перетираем старые данные батчами
        clear_list(url, name)

        # 2️⃣ Загружаем новые данные батчами
        batch_update(name, data, as_user_input=True)

        # 3️⃣ Вставляем формулу отдельно
        sheet = gspread.authorize(
            Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        ).open_by_url(url).worksheet(name)

        sheet.update_acell(
            "P2",
            """=ARRAYFORMULA(IF(N2:N="";"";IFERROR(VLOOKUP(N2:N;'Себес'!B:C;2;FALSE))))"""
        )

    except Exception as e:
        logger.error(f"Ошибка загрузки fin_report в таблицу: {e}")


@with_db_connection
async def upload_save_data_to_google():
    url = "https://docs.google.com/spreadsheets/d/1djlCANhJ5eOWsHB95Gh7Duz0YWlF6cOT035dYsqOZQ4/edit?gid=711193990#gid=711193990"
    name = "SavesData"

    sloi = await get_sloy()

    date_from_str = await get_first_day_last_month()

    query_inns = select(wblk_table.c.inn)
    rows = await database.fetch_all(query_inns)
    inns = [int(row["inn"]) for row in rows]

    payloads = [FinReportRequest(
        inn=inn,
        date_from=date_from_str,
        supplier_oper_name=['хранение', 'хранение товара с низким индексом остатка'])
        for inn in inns]

    # Запускаем все запросы параллельно
    results_list = await asyncio.gather(*[fin_report_endpoint(payload=payload, token=BEARER) for payload in payloads])

    # Создаем словарь: ключ — inn, значение — результат
    results_by_inn = {inn: result["data"] for inn, result in zip(inns, results_list)}

    headers = [
        "inn", "nmid", "vendorcode", "subjectname", "warehousePrice", "color", "supplier_oper_name", "Слой", "date_wb"
    ]
    data = [headers]

    try:
        reform_data = [
            [
                inn,
                stat["nmid"],
                stat["vendorcode"],
                stat["subjectname"],
                stat["warehousePrice"],
                stat["color"],
                stat["supplier_oper_name"],
                sloi.get(stat["vendorcode"], "Слой не обнаружен"),
                stat["date_wb"].strftime("%d.%m.%Y") if isinstance(stat["date_wb"], (date, datetime)) else stat[
                    "date_wb"],
            ]
            for inn, stats_list in results_by_inn.items()
            for stat in stats_list if stat["supplier_oper_name"] in ['хранение']
        ]
    except Exception as e:
        logger.error(f"Ошибка обработки данных в upload_save_data_to_google: {e}")
        raise

    data += reform_data

    BATCH_SIZE = 50000  # количество строк за один запрос
    NUM_COLS = 9  # столбцы A-Q
    def batch_update(sheet_name, data, as_user_input=False):
        total_rows = len(data)
        for start in range(0, total_rows, BATCH_SIZE):
            end = min(start + BATCH_SIZE, total_rows)
            batch = data[start:end]
            range_str = f"A{start + 1}:I{end}"
            update_google_sheet_data(url, sheet_name, range_str, batch, as_user_input=as_user_input)

    try:
        # 1️⃣ Перетираем старые данные батчами
        clear_list(url, name)

        # 2️⃣ Загружаем новые данные батчами
        batch_update(name, data, as_user_input=True)

    except Exception as e:
        logger.error(f"Ошибка загрузки save_data в таблицу: {e}")