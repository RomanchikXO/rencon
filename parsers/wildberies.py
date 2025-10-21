import asyncio
import random
import httpx
from typing import Union, List
import time
import aiohttp
from asgiref.sync import sync_to_async
from django.utils.timezone import now, timedelta as td
from django.db.models import Q

from database.DataBase import async_connect_to_database
from database.funcs_db import get_data_from_db, add_set_data_from_db
from datetime import datetime, timedelta
from django.utils.dateparse import parse_datetime
import json
import uuid
import zipfile
import math
import logging
import io
import csv
from context_logger import ContextLogger
from itertools import chain
from myapp.models import Price, Adverts

logger = ContextLogger(logging.getLogger("parsers"))


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",
    "Content-Type": "application/json; charset=UTF-8",
}


def get_uuid()-> str:
    generated_uuid = str(uuid.uuid4())
    return generated_uuid


def generate_random_user_agent() -> str:
    browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
    platforms = [
        "Windows NT 10.0",
        "Windows NT 6.1",
        "Macintosh; Intel Mac OS X 10_15_7",
        "X11; Linux x86_64",
    ]
    versions = [
        lambda: f"{random.randint(70, 110)}.0.{random.randint(0, 9999)}.{random.randint(0, 150)}",
        lambda: f"{random.randint(70, 110)}.0.{random.randint(0, 9999)}",
    ]
    browser = random.choice(browsers)
    platform = random.choice(platforms)
    version = random.choice(versions)()
    return f"Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) {browser}/{version} Safari/537.36"


def get_data(method: str, url: str, response_type="json", **kwargs):
    attempt, max_attemps = 0, 4
    if headers := kwargs.pop("headers"):
        headers["User-Agent"] = generate_random_user_agent()
    while attempt <= max_attemps:
        time.sleep(5)
        # proxies = {"http://": f"http://{random.choice(proxies_all)}"}
        timeout = httpx.Timeout(10.0, connect=5.0)
        try:
            with httpx.Client(
                timeout=timeout,
                # proxies=proxies
            ) as client:
                response = client.request(
                    method.upper(), url, headers=headers, **kwargs
                )
            if response.status_code == 404:
                logger.info(f"Bad link 404. {url}")
                return None
            if response_type == "json":
                result = response.json()
            elif response_type == "text":
                result = response.text
            return result
        except:
            attempt += 1
            logger.info(f"Can't get data, retry {attempt}")
            time.sleep(attempt * 2)
    logger.error(f"Can't get data, URl: {url}")


def calculate_card_price(price: Union[int, float]) -> int:
    card_price = int((price * 0.97))
    if price >= 15000:
        card_price = 0
    return card_price


def parse_link(
    link: Union[int, str], disc: int
) -> tuple:
    api_url = "https://card.wb.ru/cards/v4/detail"
    params = {
        "spp": "0",
        "reg": "0",
        "appType": "1",
        "emp": "0",
        "dest": -4734876,
        "nm": link,
    }
    data = get_data("get", api_url, "json", headers=headers, params=params)

    if not data or not data["products"][0]:
        logger.info(f"Fail {link}. Функция: parse_link. Data: {data}")
        return 0, 0

    sku = data["products"][0]

    try:
        price = int(int(sku["sizes"][0]["price"]["product"] / 100 * 0.97) * ((100-disc) / 100))
    except Exception as e:
        logger.info(f"Не нашли цену для артикула {link}. Ошибка {e}")
        price = 0

    rating = sku.get("reviewRating", 0)

    return price, rating


def safe_parse_link(link, disc: int) -> tuple:
    try:
        data = parse_link(link, disc)
        return data
    except Exception as e:
        logger.error(f"Can't parse link. Url: {link}. Error: {e}")

def parse_by_links(links: list, disc: int) -> List[tuple]:
    tasks = [
        safe_parse_link(link, disc)
        for link in links
    ]
    return tasks


def parse(links: list, disc: int) -> List[tuple]:
    response = parse_by_links(links, disc)
    return response


async def wb_api(session, param):
    """
    Асинхронная функция для получения данных по API Wildberries.
    :param param:
    :return:
    """

    API_URL = ''
    view = ''
    data = {}
    params = {}

    if param["type"] == "info_about_rks":
        API_URL = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"
        data = param['id_lks']  # максимум 50 рк
        view = "post"

    if param["type"] == "list_adverts_id":
        API_URL = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
        view = "get"

    if param["type"] == "fin_report":
        API_URL = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"

        params = {
            "dateFrom": param["date_from"],
            "dateTo": param["date_to"],
            "rrdid": param.get("rrdid", 0),
        }

        view = "get"

    if param["type"] == "make_save_rep":
        """Можно получить отчёт максимум за 8 дней."""
        API_URL = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage"

        params = {
            "dateFrom": param["date_from"],
            "dateTo": param["date_to"],
        }

        view = "get"

    if param["type"] == "get_save_report":
        """Можно получить отчёт максимум за 8 дней."""
        API_URL = f"https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{param['task_id']}/download"

        params = {
            "task_id": param["task_id"],
        }

        view = "get"

    if param["type"] == 'region_sale':
        """Метод возвращает отчёт с данными продаж, сгруппированных по регионам стран."""
        API_URL = "https://seller-analytics-api.wildberries.ru/api/v1/analytics/region-sale"

        params = {
            "dateFrom": param["dateFrom"],
            "dateTo": param["dateTo"],
        }

        view = "get"

    if param["type"] == "fullstatsadv":
        """
        Метод формирует статистику для всех кампаний, независимо от типа.
        Данные вернутся для кампаний в статусах:

        9 — активно
        7 — завершено
        11 — кампания на паузе
        Если в запросе указан только ID кампании, по ней вернутся данные только за последние сутки.
        """
        API_URL = "https://advert-api.wildberries.ru/adv/v3/fullstats"

        params = param["settings"]

        view = "get"

    if param["type"] == "get_balance_seller":
        API_URL = "https://finance-api.wildberries.ru/api/v1/account/balance"
        view = "get"

    if param["type"] == "get_balance_lk":
        # получить balance-счет net-баланс bonus-бонусы личный кабинет
        # Максимум 1 запрос в секунду на один аккаунт продавца
        API_URL = "https://advert-api.wildberries.ru/adv/v1/balance"
        view = "get"

    if param["type"] == "orders":
        # Максимум 1 запрос в минуту на один аккаунт продавца
        # Данные обновляются раз в 30 минут.
        API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
        params = {
            "dateFrom": param["date_from"],
            # Дата и время последнего изменения по заказу. `2019-06-20` `2019-06-20T23:59:59`
            "flag": param["flag"],  # если flag=1 то только за выбранный день если 0 то
            # со дня до сегодня но не более 100000 строк
        }
        view = "get"

    if param["type"] == "sales":
        API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
        params = {
            "dateFrom": param["date_from"],
            # Дата и время последнего изменения по продажам. `2019-06-20` `2019-06-20T23:59:59`
            "flag": param["flag"],  # если flag=1 то только за выбранный день если 0 то
            # со дня до сегодня но не более 100000 строк
        }
        view = "get"

    if param["type"] == "start_advert":
        # запустить рекламу
        # Максимум 5 запросов в секунду на один аккаунт продавца
        API_URL = "https://advert-api.wildberries.ru/adv/v0/start"
        params = {
            "id": param["advert_id"],  # int
        }
        view = "get"

    if param["type"] == "budget_advert":
        # получить бюджет кампании
        # Максимум 4 запроса в секунду на один аккаунт продавца
        API_URL = "https://advert-api.wildberries.ru/adv/v1/budget"
        params = {
            "id": param["advert_id"],  # int
        }
        view = "get"

    if param["type"] == "add_bidget_to_adv":
        # пополнить бюджет рекламной кампании
        # Максимум 1 запрос в секунду на один аккаунт продавца
        API_URL = "https://advert-api.wildberries.ru/adv/v1/budget/deposit"
        params = {
            "id": param["advert_id"],
        }
        data = {
            "sum": param["sum"],  # int
            "type": param["source"],  # int: 0-счет 1-баланс 3-бонусы
            "return": param["return"],  # bool: в ответе вернется обновлённый размер бюджета кампании если True
        }

        view = "post"

    if param["type"] == "get_nmids":
        # получить все артикулы
        # Максимум 100 запросов в минуту для всех методов категории Контент на один аккаунт продавца
        API_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"

        data = {
            "settings": {
                "cursor": {
                    "limit": 100
                },
                "filter": {
                    "withPhoto": -1
                },
            }
        }
        if param.get("updatedAt"):
            data["settings"]["cursor"]["updatedAt"] = param["updatedAt"]
        if param.get("nmID"):
            data["settings"]["cursor"]["nmID"] = param["nmID"]
        view = "post"

    if param["type"] == "get_delivery_fbw":
        API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/incomes"

        params = {
            "dateFrom": param["dateFrom"]
        }

        view = "get"

    if param["type"] == "get_products_and_prices":
        # получить товары с ценами
        # максимальный лимит 1000
        API_URL = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
        params = {
            "limit": param.get("limit", 1000)
        }
        view = "get"

    # сортировка по nmID/предметам/брендам/тегам
    if param["type"] == 'get_stat_cart_sort_nm':
        API_URL = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail"
        data = {
            "period": {
                "begin": param["begin"],
                "end": param["end"],
            },
            "page": 1
        }
        view = "post"

    if param["type"] == "get_feedback":
        # Максимум 1 запрос в секунду
        # Если превысить лимит в 3 запроса в секунду, отправка запросов будет заблокирована на 60 секунд
        API_URL = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"
        params = {
            "isAnswered": param["isAnswered"],  # str: Обработанные отзывы (True) или необработанные отзывы(False)
            "take": param["take"],  # int: Количество отзывов (max. 5 000)
            "skip": param["skip"],  # int: Количество отзывов для пропуска (max. 199990)

        }
        if param.get("nmId"):  # по артикулу
            params["nmId"] = param["nmId"]
        if param.get("order"):  # str: сортировка по дате "dateAsc" "dateDesc"
            params["order"] = param["order"]
        if param.get("dateFrom"):  # int: Дата начала периода в формате Unix timestamp
            params["dateFrom"] = param["dateFrom"]
        if param.get("dateTo"):  # int: Дата конца периода в формате Unix timestamp
            params["dateTo"] = param["dateTo"]
        view = "get"

    if param["type"] == "warehouse_data":
        # Максимум 3 запроса в минуту на один аккаунт продавца
        # Метод формирует набор данных об остатках по складам.
        # Данные по складам Маркетплейс (FBS) приходят в агрегированном виде — по всем сразу, без детализации по
        # конкретным складам — эти записи будут с "regionName":"Маркетплейс" и "offices":[].
        API_URL = "https://seller-analytics-api.wildberries.ru/api/v2/stocks-report/offices"

        data = {
            "currentPeriod": {
                "start": param["start"],  # "2024-02-10" Не позднее end. Не ранее 3 месяцев от текущей даты
                "end": param["end"],  # Дата окончания периода. Не ранее 3 месяцев от текущей даты
            },
            "stockType": "" if not param.get("stockType") else param["stockType"],
            # "" — все wb—Склады WB mp—Склады Маркетплейс (FBS)
            "skipDeletedNm": True if not param.get("skipDeletedNm") else param["skipDeletedNm"],
            # Скрыть удалённые товары
        }

        view = "post"

    if param["type"] == "docs_cat":
        """Метод возвращает категории документов для получения списка документов продавца."""
        API_URL = "https://documents-api.wildberries.ru/api/v1/documents/categories"
        view = "get"

    if param["type"] == "list_docs":
        API_URL = "https://documents-api.wildberries.ru/api/v1/documents/list"
        view = "get"

    if param["type"] == "get_docs":
        API_URL = "https://documents-api.wildberries.ru/api/v1/documents/download/all"
        view = "post"

    if param["type"] == "seller_analytics_generate":
        # Метод создаёт задание на генерацию отчёта с расширенной аналитикой продавца.
        # Максимальное количество отчётов, генерируемых в сутки — 20.
        # Максимум 3 запроса в минуту на один аккаунт продавца
        API_URL = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/downloads"

        # https://dev.wildberries.ru/openapi/analytics#tag/Analitika-prodavca-CSV/paths/~1api~1v2~1nm-report~1downloads/post
        # Ниже типы reportType
        # DETAIL_HISTORY_REPORT GROUPED_HISTORY_REPORT SEARCH_QUERIES_PREMIUM_REPORT_GROUP
        # SEARCH_QUERIES_PREMIUM_REPORT_PRODUCT SEARCH_QUERIES_PREMIUM_REPORT_TEXT STOCK_HISTORY_REPORT_CSV

        statuses = [
            "deficient",
            "actual",
            "balanced",
            "nonActual",
            "nonLiquid",
            "invalidData"
        ]

        data = {
            "id": param["id"],  # ID отчёта в UUID-формате
            "reportType": param["reportType"],
            "userReportName": param["userReportName"],  # Название отчета
        }
        if param["reportType"] == "DETAIL_HISTORY_REPORT":
            data["params"] = {
                "startDate": param["start"],  # str
                "endDate": param["end"],
                "skipDeletedNm": param.get("skipDeletedNm", True),  # скрыть удаленные товары
            }
        elif param["reportType"] == "STOCK_HISTORY_REPORT_CSV":
            data["params"] = {
                "currentPeriod": {
                    "start": param["start"],
                    "end": param["end"],
                },  # str
                "stockType": param.get("stockType", ""),
                "skipDeletedNm": param.get("skipDeletedNm", True),  # скрыть удаленные товары
                "availabilityFilters": param.get("availabilityFilters", statuses),  # List[str]
                "orderBy": {
                    "field": param.get("orderBy", "officeMissingTime"),
                    "mode": param.get("mode", "desc"),
                }
            }

        view = "post"

    if param["type"] == "seller_analytics_report":
        # Можно получить отчёт, который сгенерирован за последние 48 часов.
        # Отчёт будет загружен внутри архива ZIP в формате CSV.
        # Максимум 3 запроса в минуту на один аккаунт продавца
        API_URL = f"https://seller-analytics-api.wildberries.ru/api/v2/nm-report/downloads/file/{param['downloadId']}"

        params = {
            "downloadId": param["downloadId"],  # string <uuid>
        }
        view = "get"

    if param["type"] == "get_stocks_data":
        # Метод предоставляет количество остатков товаров на складах WB.
        # Данные обновляются раз в 30 минут.
        # Максимум 1 запрос в минуту на один аккаунт продавца

        params = {"dateFrom": param["dateFrom"]}  # "2019-06-20"  Время передаётся в часовом поясе Мск (UTC+3).
        API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"

        view = "get"

    if param["type"] == "set_price_and_discount":
        # Метод устанавливает цены и скидки для товаров.
        # Максимум 10 запросов за 6 секунд
        # Максимум 1 000 товаров
        # Цена и скидка не могут быть пустыми одновременно
        # Если новая цена со скидкой будет хотя бы в 3 раза меньше старой, она попадёт в карантин, и товар будет продаваться по старой цене
        API_URL = "https://discounts-prices-api.wildberries.ru/api/v2/upload/task"

        data = {
            "data": param["data"]
        }  # List[dict]  где dict {"nmID": int, "price": int, "discount": int}
        view = "post"

    if param["type"] == "get_question":
        # Метод предоставляет список вопросов по заданным фильтрам.
        # Можно получить максимум 10 000 вопросов в одном ответе
        # Максимум 1 запрос в секунду
        # Если превысить лимит в 3 запроса в секунду, отправка запросов будет заблокирована на 60 секунд

        API_URL = "https://feedbacks-api.wildberries.ru/api/v1/questions"
        params = {
            "isAnswered": param["isAnswered"],  # bool отвеченные (True)
            "take": param.get("take", 10000),
            # Количество запрашиваемых вопросов (максимально допустимое значение для параметра - 10 000, при этом сумма значений параметров take и skip не должна превышать 10 000)
            "skip": param.get("skip", 0),
            # Количество вопросов для пропуска (максимально допустимое значение для параметра - 10 000, при этом сумма значений параметров take и skip не должна превышать 10 000)
        }
        view = "get"

    try:
        headers = {
            "Authorization": f"Bearer {param['API_KEY']}"  # Или просто API_KEY, если нужно
        }
    except Exception as e:
        logger.error(
            f"Ошибка в wb_api при формировании заголовка: {e}. Параметры: {param}"
        )
        # param.pop("API_KEY", None)
        return None

    if view == 'get':
        async with session.get(API_URL, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=60), ssl=False) as response:
            if param["type"] == "seller_analytics_report":
                try:
                    content = await response.read()
                    return content
                except Exception as e:
                    return e
            response_text = await response.text()
            try:
                response.raise_for_status()
                return json.loads(response_text)
            except Exception as e:
                logger.error(
                    f"Ошибка в wb_api (get запрос): {e}. Ответ: {response_text}. Параметры: {param}"
                )
                # param.pop("API_KEY", None)
                return None

    if view == 'post':
        async with session.post(API_URL, headers=headers, params=params, json=data, timeout=aiohttp.ClientTimeout(total=60),
                                ssl=False) as response:
            response_text = await response.text()
            try:
                response.raise_for_status()
                return json.loads(response_text)
            except Exception as e:
                logger.error(
                    f"Ошибка в wb_api (post запрос): {e}.  Ответ: {response_text}. Параметры: {param}"
                )
                return None


async def insert_in_chunks(pool, query, data, chunk_size=1000):
    """
    Выполняет пакетную вставку данных в БД с разбиением на чанки и использованием транзакций.

    Функция делит переданный список данных на части фиксированного размера (чанки)
    и поочередно выполняет вставку для каждой части в рамках отдельной транзакции.
    В случае ошибки в конкретном чанке транзакция откатывается, остальные чанки
    продолжают загружаться. Таким образом достигается контроль атомарности и
    повышается устойчивость загрузки больших объёмов данных.

    Args:
        pool: Асинхронное соединение с базой данных (объект asyncpg connection).
        query (str): SQL-запрос для выполнения вставки (с плейсхолдерами $1, $2, ...).
        data (list[tuple]): Список данных для вставки, где каждая запись представлена кортежем.
        chunk_size (int, optional): Размер чанка (количество записей в одной транзакции).
            По умолчанию 1000.

    Raises:
        Exception: Пробрасывает исключение, если вставка чанка завершилась с ошибкой.
                   В этом случае транзакция откатывается.
    """
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i+chunk_size]
        async with pool.acquire() as conn:
            async with conn.transaction():  # ✅ транзакция на чанк
                try:
                    await conn.executemany(query, chunk)
                except Exception:
                    logger.exception(f"Ошибка загрузки чанка {i // chunk_size + 1}")
                    raise


async def get_products_and_prices():
    """
    получаем товары и цены и пишем их в бд
    :return:
    """

    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"])

    data = {}

    async with aiohttp.ClientSession() as session:
        for cab in cabinets:
            param = {
                "type": "get_products_and_prices",
                "API_KEY": cab["token"],
            }

            data[cab["id"]] = wb_api(session, param)

        results = await asyncio.gather(*data.values())
        id_to_result = {name: result for name, result in zip(data.keys(), results)}      
        status_rep = Price.objects.order_by('id').values_list('main_status', flat=True).first()

        try:
            conn = await async_connect_to_database()
            if not conn:
                logger.error("Ошибка подключения к БД")
                raise
            for key, value in id_to_result.items():
                value = value["data"]["listGoods"]
                data = []
                try:
                    for item in value:
                        data.append(
                            add_set_data_from_db(
                                conn=conn,
                                table_name="myapp_price",
                                data=dict(
                                    lk_id=key,
                                    nmid=item["nmID"],
                                    vendorcode=item["vendorCode"],
                                    sizes=json.dumps(item["sizes"]),
                                    discount=item["discount"],
                                    clubdiscount=item["clubDiscount"],
                                    editablesizeprice=item["editableSizePrice"],
                                    main_status=status_rep,
                                ),
                                conflict_fields=["nmid", "lk_id"]
                            )
                        )
                    results = await asyncio.gather(*data)
                except Exception as e:
                    logger.error(f"Ошибка при добавлении продуктов и цен {e}")
        except:
            return
        finally:
            await conn.close()


async def get_nmids():
    # получаем все карточки товаров
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"])

    for cab in cabinets:
        async with aiohttp.ClientSession() as session:
            param = {
                "type": "get_nmids",
                "API_KEY": cab["token"],
            }
            while True:
                response = await wb_api(session, param)

                if response.get("cursor"):
                    if response["cursor"]["total"] == 0:
                        break

                if not response.get("cards"):
                    logger.error(f"Ошибка при получении артикулов для {cab['name']}: {response}")
                    raise
                conn = await async_connect_to_database()
                if not conn:
                    logger.error("Ошибка подключения к БД")
                    raise
                try:
                    for resp in response["cards"]:
                        await add_set_data_from_db(
                            conn=conn,
                            table_name="myapp_nmids",
                            data=dict(
                                lk_id=cab["id"],
                                nmid=resp["nmID"],
                                imtid=resp["imtID"],
                                nmuuid=resp["nmUUID"],
                                subjectid=resp["subjectID"],
                                subjectname=resp["subjectName"],
                                vendorcode=resp["vendorCode"],
                                brand=resp["brand"],
                                title=resp["title"],
                                description=resp.get("description", ""),
                                needkiz=resp["needKiz"],
                                dimensions=json.dumps(resp["dimensions"]),
                                characteristics=json.dumps(resp["characteristics"]),
                                sizes=json.dumps(resp["sizes"]),
                                tag_ids = json.dumps([]),
                                created_at=parse_datetime(resp["createdAt"]),
                                updated_at=parse_datetime(resp["updatedAt"]),
                                added_db=datetime.now()
                            ),
                            conflict_fields=["nmid", "lk_id"]
                        )
                except Exception as e:
                    logger.error(f"Ошибка при добавлении артикулов в бд {e}")
                    raise
                finally:
                    await conn.close()


                if response["cursor"]["total"] < 100:
                    break
                else:
                    param["updatedAt"] = response["cursor"]["updatedAt"]
                    param["nmID"] = response["cursor"]["nmID"]
                    # await asyncio.sleep(60)


async def get_stocks_data_2_weeks():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"])

    for cab in cabinets:
        async with aiohttp.ClientSession() as session:
            conn = await async_connect_to_database()
            if not conn:
                logger.error("Ошибка подключения к БД")
                raise

            req_is_rows_in_db = """
                SELECT * from myapp_stocks WHERE lk_id = $1 LIMIT 1 
            """
            all_fields = await conn.fetch(req_is_rows_in_db, cab["id"])

            if all_fields:
                days = 1
            else:
                logger.info("Пишим остатки в БД впервые")
                days = 250

            param = {
                "type": "get_stocks_data",
                "API_KEY": cab["token"],
                "dateFrom": str(datetime.now() - timedelta(days=days)),
            }
            response = await wb_api(session, param)

            try:
                for quant in response:
                    await add_set_data_from_db(
                        conn=conn,
                        table_name="myapp_stocks",
                        data=dict(
                            lk_id=cab["id"],
                            lastchangedate=parse_datetime(quant["lastChangeDate"]),
                            warehousename=quant["warehouseName"],
                            supplierarticle=quant["supplierArticle"],
                            nmid=quant["nmId"],
                            barcode=int(quant["barcode"]) if quant.get("barcode") else None,
                            quantity=quant["quantity"],
                            inwaytoclient=quant["inWayToClient"],
                            inwayfromclient=quant["inWayFromClient"],
                            quantityfull=quant["quantityFull"],
                            category=quant["category"],
                            techsize=quant["techSize"],
                            issupply=quant["isSupply"],
                            isrealization=quant["isRealization"],
                            sccode=quant["SCCode"],
                            added_db=datetime.now()

                        ),
                        conflict_fields=['nmid', 'lk_id', 'supplierarticle', 'warehousename']
                    )
            except Exception as e:
                logger.error(f"Ошибка при добавлении остатков в БД. Error: {e}")
            finally:
                await conn.close()


async def get_orders():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"])
    for cab in cabinets:
        async with aiohttp.ClientSession() as session:
            date_from = (datetime.now() - timedelta(days=14)).replace(hour=0, minute=0, second=0, microsecond=0)
            param = {
                "type": "orders",
                "API_KEY": cab["token"],
                "date_from": str(date_from),
                "flag": 0
            }
            response = await wb_api(session, param)
            conn = await async_connect_to_database()
            if not conn:
                logger.warning("Ошибка подключения к БД")
                raise
            try:
                for order in response:
                    await add_set_data_from_db(
                        conn=conn,
                        table_name="myapp_orders",
                        data=dict(
                            lk_id=cab["id"],
                            date=parse_datetime(order["date"]),
                            lastchangedate=parse_datetime(order["lastChangeDate"]),
                            warehousename=order["warehouseName"].replace("Виртуальный ", "") if order["warehouseName"].startswith("Виртуальный") else order["warehouseName"],
                            warehousetype=order["warehouseType"],
                            countryname=order["countryName"],
                            oblastokrugname=order["oblastOkrugName"],
                            regionname=order["regionName"],
                            supplierarticle=order["supplierArticle"],
                            nmid=order["nmId"],
                            barcode=int(order["barcode"]) if order.get("barcode") else None,
                            category=order["category"],
                            subject=order["subject"],
                            brand=order["brand"],
                            techsize=order["techSize"],
                            incomeid=order["incomeID"],
                            issupply=order["isSupply"],
                            isrealization=order["isRealization"],
                            totalprice=order["totalPrice"],
                            discountpercent=order["discountPercent"],
                            spp=order["spp"],
                            finishedprice=float(order["finishedPrice"]),
                            pricewithdisc=float(order["priceWithDisc"]),
                            iscancel=order["isCancel"],
                            canceldate=parse_datetime(order["cancelDate"]),
                            sticker=order["sticker"],
                            gnumber=order["gNumber"],
                            srid=order["srid"],
                        ),
                        conflict_fields=['nmid', 'lk_id', 'srid']
                    )
            except Exception as e:
                logger.error(f"Ошибка при добавлении заказов в БД. Error: {e}")
            finally:
                await conn.close()


async def get_prices_from_lk(lk: dict):
    """
    Получаем данные о ценах прямо из личного кабинета
    Returns:
    """

    cookie_str = lk["cookie"]
    cookie_list = cookie_str.split(";")
    cookie_dict = {i.split("=")[0]: i.split("=")[1] for i in cookie_list}

    authorizev3 = lk["authorizev3"]

    proxy = "31806a1a:6846a6171a@45.13.192.129:30018"

    cookies = {
        'external-locale': 'ru',
        '_wbauid': cookie_dict["_wbauid"],
        'wbx-validation-key': cookie_dict["wbx-validation-key"],
        'WBTokenV3': authorizev3,
        'x-supplier-id-external': cookie_dict["x-supplier-id-external"],
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'ru-RU,ru;q=0.9',
        'authorizev3': authorizev3,
        'content-type': 'application/json',
        'origin': 'https://seller.wildberries.ru',
        'priority': 'u=1, i',
        'referer': 'https://seller.wildberries.ru/',
        'sec-ch-ua': '"Not.A/Brand";v="99", "Chromium";v="136"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }

    json_data = {
        'limit': 200,
        'offset': 0,
        'facets': [],
        'filterWithoutPrice': False,
        'filterWithLeftovers': False,
        'sort': 'price',
        'sortOrder': 0,
    }
    url = "https://discounts-prices.wildberries.ru/ns/dp-api/discounts-prices/suppliers/api/v1/list/goods/filter"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, cookies=cookies, json=json_data, timeout=60, #proxy=f"http://{proxy}",
                                ssl=False) as response:
                response_text = await response.text()
                try:
                    response.raise_for_status()
                    return json.loads(response_text)
                except Exception as e:
                    logger.error(
                        f"Ошибка в get_prices_from_lk: {e}.  Ответ: {response_text}"
                    )
                    return None
    except Exception as e:
        raise Exception(e)


async def get_qustions():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"])
    async def get_data(cab: dict):
        """
        Получаем неотвеченный вопросы
        """
        async with aiohttp.ClientSession() as session:
            param = {
                "type": "get_question",
                "API_KEY": cab["token"],
                "isAnswered": 0,
            }
            response = await wb_api(session, param)
            response = response["data"]["questions"]

            data = [
                {
                    "id_question": i["id"],
                    "nmid": i["productDetails"]["nmId"],
                    "createdDate": i["createdDate"],
                    "question": i["text"]
                }
                for i in response
            ]

            return data


    tasks = [
        get_data(cab)
        for cab in cabinets
    ]
    data = await asyncio.gather(*tasks)
    data = list(chain.from_iterable(data))

    api_ids_questions = [i["id_question"] for i in data]

    ids_db_is_not_ans = await get_data_from_db("myapp_questions", ["id_question"], {"is_answered": False})
    ids_db_is_not_ans = [i["id_question"] for i in ids_db_is_not_ans]

    ids_need_change_to_true = list(set(ids_db_is_not_ans) - set(api_ids_questions))

    if ids_need_change_to_true:
        conn = await async_connect_to_database()
        if not conn:
            logger.error("Ошибка подключения к БД в get_qustions")
            raise
        try:
            placeholders = ','.join(f'${i + 1}' for i in range(len(ids_need_change_to_true)))
            query = f"""
                UPDATE myapp_questions 
                SET
                    is_answered = TRUE
                WHERE id_question IN ({placeholders})
            """
            await conn.execute(query, *ids_need_change_to_true)
        except Exception as e:
            logger.error(
                f"Ошибка обновления отвеченных вопросов в myapp_questions. Error: {e}"
            )
            raise
        finally:
            await conn.close()

    data = [i for i in data if i["id_question"] not in ids_need_change_to_true]

    if data:
        conn = await async_connect_to_database()
        if not conn:
            logger.error("Ошибка подключения к БД в get_qustions")
            raise
        conn = await conn.acquire()
        try:
            async with conn.transaction():
                for quant in data:
                    await conn.execute(
                        """
                        INSERT INTO myapp_questions (nmid, id_question, created_at, question, answer, is_answered)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (nmid, id_question) DO NOTHING
                        """,
                        quant["nmid"], quant["id_question"], parse_datetime(quant["createdDate"]),
                        quant["question"], "", False
                    )
        except Exception as e:
            logger.error(f"Ошибка при добавлении вопросов в БД. Error: {e}")
        finally:
            await conn.close()


async def get_stock_age_by_period():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"])

    async def get_analitics(cab: dict, period_get: int, id_report):
        async with aiohttp.ClientSession() as session:
            param = {
                "type": "seller_analytics_generate",
                "API_KEY": cab["token"],
                "reportType": "STOCK_HISTORY_REPORT_CSV",
                "start": (datetime.now() - timedelta(days=period_get)).strftime('%Y-%m-%d'),
                "end": (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'), #вчера с текущим временем
                "id": id_report, #'685d17f6-ed17-44b4-8a86-b8382b05873c'
                "userReportName": get_uuid(),
            }
            response = await wb_api(session, param)
            logger.info(f"Генерируем отчет для {cab['name']}. ID: {id_report}. Period: {period_get}")

            if not (response and response.get("data") and response["data"] == "Началось формирование файла/отчета"):
                logger.error(f"Ошибка формирования отчета. Период {period_get}. Кабинет: {cab['name']}. Ответ: {response}")
                raise

            for attempt in range(4):
                if attempt == 3:
                    logger.error(f"‼️Ошибка получения данных в get_stock_age_by_period. Кабинет {cab['name']}. ID: {id_report}. Period: {period_get}")
                    raise

                await asyncio.sleep(10)
                param = {
                    "type": "seller_analytics_report",
                    "API_KEY": cab["token"],
                    "downloadId": id_report
                }

                response = await wb_api(session, param)
                if not isinstance(response, bytes):
                    await asyncio.sleep(55)
                else:
                    try:
                        text = response.decode('utf-8')
                        if "check correctness of download id or supplier id" in text:
                            await asyncio.sleep(55)
                            logger.info(f"ВНИМАНИЕ!!!: check correctness of download id or supplier id. ПОПЫТКА: {attempt + 1}. Кабинет {cab['name']}. ID: {id_report}. Period: {period_get}")
                            continue
                        text = json.loads(text)
                        if text.get("title"):
                            await asyncio.sleep(55)
                            continue
                    except:
                        break

            with zipfile.ZipFile(io.BytesIO(response)) as zip_file:
                for file_name in zip_file.namelist():
                    with zip_file.open(file_name) as csv_file:
                        # читаем CSV построчно
                        reader = csv.reader(io.TextIOWrapper(csv_file, encoding='utf-8'))

                        data = []
                        header = next(reader)
                        OfficeMissingTime_index = header.index("OfficeMissingTime")
                        nmid_index = header.index("NmID")
                        OfficeName_index = header.index("OfficeName") # может быть пустой строкой
                        for index, row in enumerate(reader):
                            if index == 0: continue # пропускаем шапку
                            if row[OfficeName_index] == "": continue # если пустое название склада
                            data.append(
                                (
                                    int(row[nmid_index]),
                                    row[OfficeName_index].replace("Виртуальный ", "").replace("СЦ ", "").replace(" WB", "").replace(", Молодежненское", " (Молодежненское)").replace(" Сталелитейная", ""),
                                    math.floor((period_get*24-int(row[OfficeMissingTime_index]))/24) if row[OfficeMissingTime_index] not in ["-1", "-2", "-3", "-4"] else 0,
                                )
                            )

                        conn = await async_connect_to_database()
                        if not conn:
                            logger.error("Ошибка подключения к БД в add_set_data_from_db")
                            raise

                        try:
                            column_map = {
                                3: "days_in_stock_last_3",
                                7: "days_in_stock_last_7",
                                14: "days_in_stock_last_14",
                                30: "days_in_stock_last_30"
                            }
                            column_period = column_map.get(period_get)
                            if not column_period:
                                raise ValueError(f"Неподдерживаемый период: {period_get}")

                            # Подготовка VALUES и параметров
                            values_placeholders = []
                            values_data = []
                            for idx, (nmid, warehousename, OfficeMissingTime) in enumerate(data):
                                base = idx * 3
                                values_placeholders.append(f"(${base + 1}::integer, ${base + 2}::text, ${base + 3}::integer)")
                                values_data.extend([nmid, warehousename, OfficeMissingTime])

                            query = f"""
                                UPDATE myapp_stocks AS p 
                                SET
                                    {column_period} = v.OfficeMissingTime
                                FROM (
                                    VALUES {', '.join(values_placeholders)}
                                ) AS v(nmid, warehousename, OfficeMissingTime)
                                WHERE v.nmid = p.nmid 
                                    AND p.warehousename ILIKE '%' || v.warehousename || '%'
                            """
                            await conn.execute(query, *values_data)

                        except Exception as e:
                            logger.error(
                                f"Ошибка обновления nmid, warehousename, column_period в myapp_stocks. Error: {e}"
                            )
                            raise
                        finally:
                            await conn.close()

    for period in [3, 7, 14, 30]:
        tasks = []
        for cab in cabinets:
            id_report = get_uuid()  # 👉 делаем тут
            tasks.append(get_analitics(cab, period, id_report))
        await asyncio.gather(*tasks)
        await asyncio.sleep(60)


async def get_stat_products():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"])

    async def get_analitics(cab: dict, period_get: dict):
        async with aiohttp.ClientSession() as session:
            id_report = get_uuid()
            param = {
                "type": "seller_analytics_generate",
                "API_KEY": cab["token"],
                "reportType": "DETAIL_HISTORY_REPORT",
                "start": period["start"],
                "end": period["end"],
                "id": id_report,  # '685d17f6-ed17-44b4-8a86-b8382b05873c'
                "userReportName": get_uuid(),
            }
            response = await wb_api(session, param)

            if not (response and response.get("data") and response["data"] == "Началось формирование файла/отчета"):
                logger.error(
                    f"Ошибка формирования отчета. Период {period_get}. Кабинет: {cab['name']}. Ответ: {response}")
                raise

            for attempt in range(4):
                if attempt == 3:
                    logger.error(
                        f"‼️Ошибка получения данных в get_stat_products. Кабинет {cab['name']}. ID: {id_report}. Period: {period_get}")
                    raise
                await asyncio.sleep(10)
                param = {
                    "type": "seller_analytics_report",
                    "API_KEY": cab["token"],
                    "downloadId": id_report
                }

                response = await wb_api(session, param)
                if not isinstance(response, bytes):
                    await asyncio.sleep(55)
                else:
                    try:
                        text = response.decode('utf-8')
                        if "check correctness of download id or supplier id" in text:
                            await asyncio.sleep(55)
                            logger.info(
                                f"ВНИМАНИЕ!!!: check correctness of download id or supplier id. ПОПЫТКА: {attempt + 1}. Кабинет {cab['name']}. ID: {id_report}. Period: {period_get}")
                            continue
                        text = json.loads(text)
                        if text.get("title"):
                            await asyncio.sleep(55)
                            continue
                    except Exception as e:
                        break
            with zipfile.ZipFile(io.BytesIO(response)) as zip_file:
                for file_name in zip_file.namelist():
                    with zip_file.open(file_name) as csv_file:
                        # читаем CSV построчно
                        reader = csv.reader(io.TextIOWrapper(csv_file, encoding='utf-8'))

                        data = []
                        header = next(reader)

                        nmid_index = header.index("nmID")
                        date_wb = header.index("dt")
                        openCardCount = header.index("openCardCount")
                        addToCartCount = header.index("addToCartCount")
                        ordersCount = header.index("ordersCount")
                        ordersSumRub = header.index("ordersSumRub")
                        buyoutsCount = header.index("buyoutsCount")
                        buyoutsSumRub = header.index("buyoutsSumRub")
                        cancelCount = header.index("cancelCount")
                        cancelSumRub = header.index("cancelSumRub")
                        addToCartConversion = header.index("addToCartConversion")
                        cartToOrderConversion = header.index("cartToOrderConversion")
                        buyoutPercent = header.index("buyoutPercent")


                        for index, row in enumerate(reader):
                            if index == 0: continue  # пропускаем шапку
                            data.append(
                                (
                                    int(row[nmid_index]),
                                    parse_datetime(row[date_wb]),
                                    int(row[openCardCount]),
                                    int(row[addToCartCount]),
                                    int(row[ordersCount]),
                                    int(row[ordersSumRub]),
                                    int(row[buyoutsCount]),
                                    int(row[buyoutsSumRub]),
                                    int(row[cancelCount]),
                                    int(row[cancelSumRub]),
                                    int(row[addToCartConversion]),
                                    int(row[cartToOrderConversion]),
                                    int(row[buyoutPercent]),
                                )
                            )

                        conn = await async_connect_to_database()
                        if not conn:
                            logger.error("Ошибка подключения к БД в get_stat_products")
                            raise

                        try:
                            BATCH_SIZE = 1000
                            for batch_start in range(0, len(data), BATCH_SIZE):
                                batch = data[batch_start:batch_start + BATCH_SIZE]
                                # Подготовка VALUES и параметров
                                values_placeholders = []
                                values_data = []

                                for idx, (
                                        nmid, date_wb, openCardCount, addToCartCount, ordersCount, ordersSumRub, buyoutsCount,
                                        buyoutsSumRub, cancelCount, cancelSumRub, addToCartConversion, cartToOrderConversion,
                                        buyoutPercent) in enumerate(batch):
                                    base = idx * 13
                                    values_placeholders.append(
                                        f"(${base + 1}::integer, ${base + 2}, ${base + 3}::integer, "
                                        f"${base + 4}::integer, ${base + 5}::integer, ${base + 6}::integer, "
                                        f"${base + 7}::integer, ${base + 8}::integer, ${base + 9}::integer, "
                                        f"${base + 10}::integer, ${base + 11}::integer, ${base + 12}::integer, "
                                        f"${base + 13}::integer)"
                                    )
                                    values_data.extend([
                                        nmid, date_wb, openCardCount, addToCartCount, ordersCount, ordersSumRub,
                                        buyoutsCount, buyoutsSumRub, cancelCount, cancelSumRub,
                                        addToCartConversion, cartToOrderConversion, buyoutPercent
                                    ])

                                query = f"""
                                    INSERT INTO myapp_productsstat (
                                        nmid, date_wb, "openCardCount", "addToCartCount", "ordersCount", "ordersSumRub",
                                        "buyoutsCount", "buyoutsSumRub", "cancelCount", "cancelSumRub",
                                        "addToCartConversion", "cartToOrderConversion", "buyoutPercent"
                                    )
                                    VALUES {', '.join(values_placeholders)}
                                    ON CONFLICT (nmid, date_wb) DO UPDATE SET
                                        "openCardCount" = EXCLUDED."openCardCount",
                                        "addToCartCount" = EXCLUDED."addToCartCount",
                                        "ordersCount" = EXCLUDED."ordersCount",
                                        "ordersSumRub" = EXCLUDED."ordersSumRub",
                                        "buyoutsCount" = EXCLUDED."buyoutsCount",
                                        "buyoutsSumRub" = EXCLUDED."buyoutsSumRub",
                                        "cancelCount" = EXCLUDED."cancelCount",
                                        "cancelSumRub" = EXCLUDED."cancelSumRub",
                                        "addToCartConversion" = EXCLUDED."addToCartConversion",
                                        "cartToOrderConversion" = EXCLUDED."cartToOrderConversion",
                                        "buyoutPercent" = EXCLUDED."buyoutPercent";
                                """
                                await conn.execute(query, *values_data)

                        except Exception as e:
                            logger.error(
                                f"Ошибка обновления данных в myapp_productsstat. Error: {e}"
                            )
                            raise
                        finally:
                            await conn.close()
    periods = [
        {
            "start": (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            "end": datetime.now().strftime('%Y-%m-%d')
        },
        {
            "start": (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d'),
            "end": (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        }
    ]
    for index, period in enumerate(periods):
        tasks = [get_analitics(cab, period) for cab in cabinets]
        await asyncio.gather(*tasks)
        if index != len(periods) - 1:
            await asyncio.sleep(60)


async def get_supplies():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"])
    async def get_analitics(cab, period_get: int):
        async with aiohttp.ClientSession() as session:
            param = {
                "type": "get_delivery_fbw",
                "API_KEY": cab["token"],
                "dateFrom": (datetime.now() - timedelta(days=period_get)).strftime('%Y-%m-%d')
            }
            response = await wb_api(session, param)
            data = [
                (
                    i["nmId"], i["incomeId"], i["number"], parse_datetime(i["date"]), parse_datetime(i["lastChangeDate"]),
                    i["supplierArticle"], i["techSize"], i["barcode"], i["quantity"], i["totalPrice"], parse_datetime(i["dateClose"]),
                    i["warehouseName"], i["status"]
            )
                for i in response
                if i["status"] == "Принято"
            ]
            conn = await async_connect_to_database()
            if not conn:
                logger.error("Ошибка подключения к БД")
                raise
            try:
                query = f"""
                    INSERT INTO myapp_supplies (
                        nmid, "incomeId", "number", "date_post", "lastChangeDate", "supplierArticle",
                        "techSize", "barcode", "quantity", "totalPrice",
                        "dateClose", "warehouseName", "status"
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6,
                        $7, $8, $9, $10, $11,
                        $12, $13
                    )
                    ON CONFLICT (nmid, "incomeId") DO UPDATE SET
                        "number" = EXCLUDED."number",
                        "date_post" = EXCLUDED."date_post",
                        "lastChangeDate" = EXCLUDED."lastChangeDate",
                        "supplierArticle" = EXCLUDED."supplierArticle",
                        "techSize" = EXCLUDED."techSize",
                        "barcode" = EXCLUDED."barcode",
                        "quantity" = EXCLUDED."quantity",
                        "totalPrice" = EXCLUDED."totalPrice",
                        "dateClose" = EXCLUDED."dateClose",
                        "warehouseName" = EXCLUDED."warehouseName",
                        "status" = EXCLUDED."status";
                """
                await conn.executemany(query, data)
            except Exception as e:
                logger.error(
                    f"Ошибка обновления данных в myapp_supplies. Error: {e}"
                )
                raise
            finally:
                await conn.close()



    tasks = [get_analitics(cab, 7) for cab in cabinets]
    await asyncio.gather(*tasks)


import time


async def get_advs_stat():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"])

    # КРИТИЧЕСКИ ВАЖНО: создаём блокировки ДО запуска задач
    token_locks = {}
    token_last_call = {}

    # Предварительно создаём блокировки для всех уникальных токенов
    unique_tokens = set(cab["token"] for cab in cabinets)
    for token in unique_tokens:
        token_locks[token] = asyncio.Lock()
        token_last_call[token] = 0

    logger.info(f"Инициализировано {len(unique_tokens)} уникальных токенов для rate limiting")

    async def get_data_advs(cab):
        conn = await async_connect_to_database()
        if not conn:
            logger.error(f"Ошибка подключения к БД в {cab['name']}")
            raise
        try:
            token = cab["token"]

            yesterday = now() - td(days=1)
            advs_ids = await sync_to_async(list)(
                Adverts.objects.filter(
                    Q(status__in=[9, 11], lk_id=cab["id"]) |
                    Q(status=7, lk_id=cab["id"], changeTime__gte=yesterday)
                ).values_list("advert_id", flat=True)
            )

            if not advs_ids:
                logger.info(f"Нет активных РК для кабинета {cab['name']}")
                return

            async with aiohttp.ClientSession() as session:
                param = {"type": "fullstatsadv"}

                # Уменьшаем батч до 50 для надёжности
                BATCH_SIZE = 50
                MIN_INTERVAL = 90  # секунды между запросами
                MAX_RETRIES = 3

                for i in range(0, len(advs_ids), BATCH_SIZE):
                    param["API_KEY"] = cab["token"]
                    articles = [str(art) for art in advs_ids[i:i + BATCH_SIZE]]

                    startperiod = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
                    endperiod = datetime.now().strftime('%Y-%m-%d')
                    param["settings"] = {
                        "ids": ",".join(articles),
                        "beginDate": startperiod,
                        "endDate": endperiod
                    }

                    # Retry логика для обработки временных ошибок
                    response = None
                    batch_num = i // BATCH_SIZE + 1
                    total_batches = (len(advs_ids) + BATCH_SIZE - 1) // BATCH_SIZE

                    for attempt in range(1, MAX_RETRIES + 1):
                        # Rate limiting для конкретного токена
                        async with token_locks[token]:
                            current_time = time.time()
                            time_since_last_call = current_time - token_last_call[token]

                            if time_since_last_call < MIN_INTERVAL:
                                wait_time = MIN_INTERVAL - time_since_last_call
                                logger.info(f"Rate limiting для {cab['name']}: ждём {wait_time:.1f} сек")
                                await asyncio.sleep(wait_time)

                            if attempt > 1:
                                logger.info(
                                    f"Повтор {attempt}/{MAX_RETRIES} для {cab['name']}, батч {batch_num}/{total_batches}")
                            else:
                                logger.info(f"Запрос fullstats для {cab['name']}, батч {batch_num}/{total_batches}")

                            response = await wb_api(session, param)
                            token_last_call[token] = time.time()

                        # Проверяем успешность запроса
                        if response is not None:
                            break

                        # Если это не последняя попытка и получили None
                        if attempt < MAX_RETRIES:
                            # Экспоненциальная задержка перед retry
                            retry_delay = 10 * attempt
                            logger.warning(
                                f"Пустой ответ для {cab['name']}, батч {batch_num}. "
                                f"Ждём {retry_delay} сек перед повтором"
                            )
                            await asyncio.sleep(retry_delay)

                    # Обработка ответа
                    data_for_upload = []
                    if not response:
                        logger.error(
                            f"Не удалось получить данные после {MAX_RETRIES} попыток "
                            f"для {cab['name']}, батч {batch_num}"
                        )
                        continue

                    for advert in response:
                        advert_id = advert["advertId"]
                        for day in advert.get("days", []):
                            try:
                                date_wb = datetime.strptime(day['date'][:10], '%Y-%m-%d').date()
                            except (KeyError, ValueError) as e:
                                logger.warning(f"Ошибка парсинга даты для {cab['name']}: {e}")
                                continue

                            for app in day.get("apps", []):
                                app_type = app.get("appType")
                                if not app_type:
                                    continue

                                for nm in app.get("nms", []):
                                    nmid = nm.get("nmId")
                                    if not nmid:
                                        continue

                                    data_for_upload.append((
                                        advert_id, date_wb, app_type, nmid,
                                        nm.get("orders", 0), nm.get("atbs", 0), nm.get("canceled", 0),
                                        nm.get("clicks", 0), nm.get("cpc", 0), nm.get("cr", 0),
                                        nm.get("ctr", 0), nm.get("shks", 0), nm.get("sum", 0),
                                        nm.get("sum_price", 0), nm.get("views", 0)
                                    ))

                    # Загрузка в БД
                    if data_for_upload:
                        try:
                            query = """
                                INSERT INTO myapp_advstat (
                                    "advert_id", "date_wb", "app_type", "nmid", "orders", 
                                    "atbs", "canceled", "clicks", "cpc", "cr", "ctr", "shks", 
                                    "sum_cost", "sum_price", "views"
                                )
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                                ON CONFLICT ("nmid", "date_wb", "app_type", "advert_id") 
                                DO UPDATE SET
                                    "orders" = EXCLUDED."orders",
                                    "atbs" = EXCLUDED."atbs",
                                    "canceled" = EXCLUDED."canceled",
                                    "clicks" = EXCLUDED."clicks",
                                    "cpc" = EXCLUDED."cpc",
                                    "cr" = EXCLUDED."cr",
                                    "ctr" = EXCLUDED."ctr",
                                    "shks" = EXCLUDED."shks",
                                    "sum_cost" = EXCLUDED."sum_cost",
                                    "sum_price" = EXCLUDED."sum_price",
                                    "views" = EXCLUDED."views";
                            """
                            await conn.executemany(query, data_for_upload)
                            logger.info(f"Загружено {len(data_for_upload)} записей для {cab['name']}, батч {batch_num}")
                        except Exception as e:
                            logger.error(f"Ошибка обновления данных для {cab['name']}, батч {batch_num}: {e}")
                            raise
                    else:
                        logger.info(f"Нет данных для загрузки, батч {batch_num} для {cab['name']}")

        except Exception as e:
            logger.error(f"Ошибка в get_data_advs для {cab['name']}: {e}")
            raise
        finally:
            await conn.close()

    # Запускаем все кабинеты параллельно
    tasks = [get_data_advs(cab) for cab in cabinets]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Подсчёт успешных и неуспешных обработок
    successful = sum(1 for r in results if not isinstance(r, Exception))
    failed = len(results) - successful

    logger.info(f"Обработка завершена: успешно {successful}/{len(cabinets)}, ошибок {failed}")

    for cab, result in zip(cabinets, results):
        if isinstance(result, Exception):
            logger.error(f"Ошибка обработки кабинета {cab['name']}: {result}")


async def get_advs():
    """
    Получить списки всех рекламных кампаний продавца с их ID.
    """

    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"])

    semaphore = asyncio.Semaphore(3)

    async def get_advs_for_inn(cab):
        conn = await async_connect_to_database()
        if not conn:
            logger.error(f"Ошибка подключения к БД в {cab['name']}")
            raise
        try:
            param = {
                "type": "list_adverts_id",
                "API_KEY": cab["token"],
            }
            async with aiohttp.ClientSession() as session:
                response = await wb_api(session, param)
                response = response["adverts"]

            data_for_upload = [
                (
                    cab["id"],
                    adv["advertId"],
                    adverts["status"],
                    adverts["type"],
                    datetime.strptime(adv["changeTime"][:10], "%Y-%m-%d").date()
                )
                for adverts in response
                for adv in adverts["advert_list"]
            ]

            try:
                query = f"""
                    INSERT INTO myapp_adverts (
                        "lk_id", "advert_id", "status", "type_adv", "changeTime"
                    )
                    VALUES (
                        $1, $2, $3, $4, $5
                    )
                    ON CONFLICT ("advert_id") DO UPDATE SET
                        "status" = EXCLUDED."status",
                        "changeTime" = EXCLUDED."changeTime";
                """
                await conn.executemany(query, data_for_upload)
            except Exception as e:
                raise Exception(f"Ошибка обновления данных в myapp_adverts. Error: {e}")
        except Exception as e:
            logger.error(f"Ошибка в get_advs_for_inn: {e}")
        finally:
            await conn.close()

    async def get_advs_limited(cab):
        async with semaphore:
            return await get_advs_for_inn(cab)

    tasks = [get_advs_limited(cab) for cab in cabinets]
    await asyncio.gather(*tasks)


async def get_fin_report():
    """
    Получить фин отчет
    """

    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"])

    semaphore = asyncio.Semaphore(3)
    async def fin_report_by_lk(cab):
        conn = await async_connect_to_database()
        if not conn:
            logger.error(f"Ошибка подключения к БД в {cab['name']}")
            raise
        try:
            param = {
                "type": "fin_report",
                "API_KEY": cab["token"],
                "date_from": (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d'),
                "date_to": datetime.now().strftime('%Y-%m-%d')
            }

            async with aiohttp.ClientSession() as session:
                response = await wb_api(session, param)

            try:
                data_for_upload = [
                    (
                        cab["id"],
                        str(row["rrd_id"]),
                        datetime.strptime(row["rr_dt"][:10], "%Y-%m-%d").date(),
                        row.get("nm_id"),
                        datetime.strptime(row["order_dt"][:10], "%Y-%m-%d").date(),
                        datetime.strptime(row["sale_dt"][:10], "%Y-%m-%d").date(),
                        str(row.get("shk_id")) if row.get("shk_id") is not None else None,
                        row["ts_name"].lower(),
                        row["supplier_oper_name"].lower(),
                        float(row["retail_price"]),
                        row.get("retail_amount"),
                        row.get("ppvz_for_pay"),
                        row.get("delivery_rub"),
                        row.get("storage_fee"),
                        row.get("deduction"),
                        float(row["acceptance"]),
                    )
                    for row in response
                ]
            except Exception as e:
                raise Exception(f"ошибка подготовки данных {e}")

            try:
                query = f"""
                    INSERT INTO myapp_findata (
                        "lk_id", "rrd_id", "rr_dt", "nmid", "order_dt", "sale_dt", "shk_id", "ts_name", "supplier_oper_name",
                        "retail_price", "retail_amount", "ppvz_for_pay", "delivery_rub", "storage_fee", "deduction",
                        "acceptance"
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
                    )
                    ON CONFLICT ("rrd_id") DO UPDATE SET
                        "sale_dt" = EXCLUDED."sale_dt",
                        "retail_price" = EXCLUDED."retail_price",
                        "retail_amount" = EXCLUDED."retail_amount",
                        "ppvz_for_pay" = EXCLUDED."ppvz_for_pay",
                        "delivery_rub" = EXCLUDED."delivery_rub",
                        "storage_fee" = EXCLUDED."storage_fee",
                        "deduction" = EXCLUDED."deduction",
                        "acceptance" = EXCLUDED."acceptance";
                """
                await insert_in_chunks(conn, query, data_for_upload, chunk_size=1000)
            except Exception as e:
                raise Exception(f"Ошибка обновления данных в myapp_findata. Error: {e}")
        except Exception:
            logger.exception(f"Ошибка в fin_report_by_lk")
        finally:
            await conn.close()

    async def get_fin_rep_limited(cab):
        async with semaphore:
            return await fin_report_by_lk(cab)

    tasks = [get_fin_rep_limited(cab) for cab in cabinets]
    await asyncio.gather(*tasks)


async def make_and_get_save_report():
    """
    Отчет о платном хранении
    """

    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"])

    semaphore = asyncio.Semaphore(3)
    async def save_dates(cab):
        param = {
            "type": "make_save_rep",
            "API_KEY": cab["token"],
            "date_from": (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d'),
            "date_to": datetime.now().strftime('%Y-%m-%d')
        }

        async with aiohttp.ClientSession() as session:
            response = await wb_api(session, param)
            try:
                task_id = response["data"]["taskId"]
            except Exception as e:
                logger.error(f"Ошибка при запросе отчета платного хранения для кабинета {cab['name']}")
                raise

        param["type"] = "get_save_report"
        param["task_id"] = task_id

        for i in range(1,5):
            await asyncio.sleep(10*i)
            if i == 4:
                logger.error(f"Ошибка получения данных о платном хранении для {cab['name']}")
                return
            async with aiohttp.ClientSession() as session:
                response = await wb_api(session, param)
                try:
                    conn = await async_connect_to_database()
                    if not conn:
                        raise Exception(f"Ошибка подключения к БД в {cab['name']}")

                    try:
                        data_for_upload = [
                            (
                                cab["id"],
                                datetime.strptime(row["date"][:10], "%Y-%m-%d").date(),
                                row["logWarehouseCoef"],
                                row["officeId"],
                                row["warehouse"],
                                row["warehouseCoef"],
                                row["giId"],
                                row["chrtId"],
                                row["size"],
                                row["barcode"],
                                row["subject"],
                                row["brand"],
                                row["vendorCode"],
                                row["nmId"],
                                row["volume"],
                                row["calcType"],
                                row["warehousePrice"],
                                row["barcodesCount"],
                                row["palletPlaceCode"],
                                row["palletCount"],
                                datetime.strptime(row["originalDate"][:10], "%Y-%m-%d").date(),
                                row["loyaltyDiscount"],
                                datetime.strptime(row["tariffFixDate"][:10], "%Y-%m-%d").date() if row.get("tariffFixDate") else None,
                                datetime.strptime(row["tariffLowerDate"][:10], "%Y-%m-%d").date() if row.get("tariffLowerDate") else None
                            )
                            for row in response
                        ]
                    except Exception as e:
                        raise Exception(f"ошибка подготовки данных {e}")

                    try:
                        query = f"""
                            INSERT INTO myapp_savedata (
                                "lk_id", "date_wb", "logWarehouseCoef", "officeId", "warehouse", "warehouseCoef", "giId",
                                "chrtId", "size", "barcode", "subject", "brand", "vendorcode", "nmid", "volume",
                                "calcType", "warehousePrice", "barcodesCount", "palletPlaceCode", "palletCount", 
                                "originalDate", "loyaltyDiscount", "tariffFixDate", "tariffLowerDate"
                            )
                            VALUES (
                                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, 
                                $20, $21, $22, $23, $24
                            )
                            ON CONFLICT ("date_wb", "nmid", "calcType", "size") DO UPDATE SET
                                "warehousePrice" = EXCLUDED."warehousePrice";
                        """
                        await insert_in_chunks(conn, query, data_for_upload, chunk_size=1000)
                    except Exception as e:
                        raise Exception(f"Ошибка обновления данных. Error: {e}")

                except Exception as e:
                    logger.error(f"Ошибка в save_dates: {e}")
                finally:
                    await conn.close()
            break

    async def get_save_rep_limited(cab):
        async with semaphore:
            return await save_dates(cab)

    tasks = [get_save_rep_limited(cab) for cab in cabinets]
    await asyncio.gather(*tasks)


async def get_region_sales():
    """
    Получить продажи по регионам
    :return:
    """
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"])

    semaphore = asyncio.Semaphore(3)

    async def sale_dates(cab):
        try:
            conn = await async_connect_to_database()
            if not conn:
                raise Exception(f"Ошибка подключения к БД в {cab['name']}")

            req_is_rows_in_db = """
                SELECT * from myapp_regionsales WHERE lk_id = $1 LIMIT 1 
            """
            all_fields = await conn.fetch(req_is_rows_in_db, cab["id"])

            if all_fields:
                dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 3)]
            else:
                logger.info(f"Данные о продажах в регионе для {cab['name']} отсутствуют в БД")
                dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 16)]

            for _date in dates:
                param = {
                    "type": "region_sale",
                    "dateFrom": _date,
                    "dateTo": _date,
                    "API_KEY": cab["token"],
                }

                async with aiohttp.ClientSession() as session:
                    response = await wb_api(session, param)
                    await asyncio.sleep(25)
                    try:
                        data_for_upload = [
                            (
                                cab["id"],
                                datetime.strptime(_date, "%Y-%m-%d").date(),
                                row["nmID"],
                                row["cityName"],
                                row["countryName"],
                                row["foName"],
                                row["regionName"],
                                row["sa"],
                                row["saleInvoiceCostPrice"],
                                row["saleInvoiceCostPricePerc"],
                                row["saleItemInvoiceQty"]
                            )
                            for row in response["report"]
                        ]
                    except Exception as e:
                        raise Exception(f"ошибка подготовки данных {e}")

                    try:
                        query = f"""
                            INSERT INTO myapp_regionsales (
                                "lk_id", "date_wb", "nmid", "cityName", "countryName", "foName", "regionName",
                                "sa", "saleInvoiceCostPrice", "saleInvoiceCostPricePerc", "saleItemInvoiceQty"
                            )
                            VALUES (
                                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
                            )
                            ON CONFLICT ("date_wb", "nmid", "sa", "cityName", "regionName") DO UPDATE SET
                                "saleInvoiceCostPrice" = EXCLUDED."saleInvoiceCostPrice",
                                "saleInvoiceCostPricePerc" = EXCLUDED."saleInvoiceCostPricePerc",
                                "saleItemInvoiceQty" = EXCLUDED."saleItemInvoiceQty";
                        """
                        await insert_in_chunks(conn, query, data_for_upload, chunk_size=1000)
                    except Exception as e:
                        raise Exception(f"Ошибка обновления данных. Error: {e}")

        except Exception as e:
                logger.error(f"Ошибка в sale_dates: {e}")
        finally:
            try:
                await conn.close()
            except:
                pass

    async def get_region_sales_limited(cab):
        async with semaphore:
            return await sale_dates(cab)

    tasks = [get_region_sales_limited(cab) for cab in cabinets]
    await asyncio.gather(*tasks)



# loop = asyncio.get_event_loop()
# res = loop.run_until_complete(test_addv())


