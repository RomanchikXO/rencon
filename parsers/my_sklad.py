from loader import LOGIN_MY_SKLAD, PASS_MY_SKLAD
import base64
import asyncio
import aiohttp
import json
from context_logger import ContextLogger
import logging
from typing import Dict
import time
from database.DataBase import async_connect_to_database
from database.funcs_db import add_set_data_from_db
from django.utils.dateparse import parse_datetime

logger = ContextLogger(logging.getLogger("parsers"))

# Кодируем в Base64
credentials = f"{LOGIN_MY_SKLAD}:{PASS_MY_SKLAD}"
encoded = base64.b64encode(credentials.encode()).decode()

# Заголовки
headers = {
    "Authorization": f"Basic {encoded}",
    "Accept-Encoding": "gzip",
}

MAX_RETRIES = 5
RETRY_BACKOFF = 2

# Ограничения API МойСклад
MAX_REQUESTS_PER_3_SEC = 45  # максимум 45 запросов за 3 секунды
MAX_PARALLEL_REQUESTS = 5  # максимум 20 параллельных запросов
TIME_WINDOW = 3.0  # временное окно в секундах


class RateLimiter:
    """Контроллер ограничения скорости запросов для API МойСклад."""

    def __init__(self, max_requests: int, time_window: float, max_parallel: int):
        self.max_requests = max_requests
        self.time_window = time_window
        self.max_parallel = max_parallel
        self.semaphore = asyncio.Semaphore(max_parallel)
        self.request_times = []
        self.lock = asyncio.Lock()

    async def acquire(self):
        """Ожидает, пока не станет доступен слот для запроса."""
        # Ограничиваем параллельность
        await self.semaphore.acquire()

        async with self.lock:
            now = time.time()

            # Убираем запросы старше временного окна
            self.request_times = [t for t in self.request_times if now - t < self.time_window]

            # Если достигли лимита, ждём
            if len(self.request_times) >= self.max_requests:
                sleep_time = self.time_window - (now - self.request_times[0]) + 0.1
                logger.info(f"Достигнут лимит запросов. Ожидание {sleep_time:.2f} сек")
                await asyncio.sleep(sleep_time)

                # Очищаем старые запросы после ожидания
                now = time.time()
                self.request_times = [t for t in self.request_times if now - t < self.time_window]

            # Добавляем текущий запрос
            self.request_times.append(now)

    def release(self):
        """Освобождает слот параллельного запроса."""
        self.semaphore.release()


# Глобальный rate limiter
rate_limiter = RateLimiter(
    max_requests=MAX_REQUESTS_PER_3_SEC,
    time_window=TIME_WINDOW,
    max_parallel=MAX_PARALLEL_REQUESTS
)


async def get_data(session, url):
    """Асинхронный GET-запрос с механизмом повторных попыток и rate limiting."""
    for attempt in range(1, MAX_RETRIES + 1):
        # Ждём разрешения на запрос
        await rate_limiter.acquire()

        try:
            async with session.get(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=60),
                    ssl=False
            ) as response:
                response_text = await response.text()

                # Если слишком много запросов — ждем и пробуем снова
                if response.status == 429:
                    wait_time = RETRY_BACKOFF ** attempt
                    logger.warning(
                        f"Получен 429 Too Many Requests. Повтор через {wait_time} сек. Попытка {attempt}/{MAX_RETRIES}"
                    )
                    await asyncio.sleep(wait_time)
                    continue

                response.raise_for_status()
                return json.loads(response_text)

        except aiohttp.ClientResponseError as e:
            logger.error(f"HTTP ошибка {e.status} при запросе {url}: {e.message}")
            if 500 <= e.status < 600 and attempt < MAX_RETRIES:
                wait_time = RETRY_BACKOFF ** attempt
                logger.warning(f"Серверная ошибка {e.status}, повтор через {wait_time} сек")
                await asyncio.sleep(wait_time)
                continue
            return None

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Ошибка соединения: {e}. Попытка {attempt}/{MAX_RETRIES}")
            await asyncio.sleep(RETRY_BACKOFF ** attempt)
            continue

        except Exception as e:
            logger.error(f"Неожиданная ошибка в get_data: {e}. Url: {url}")
            return None

        finally:
            # Всегда освобождаем семафор
            rate_limiter.release()

    logger.error(f"Превышено количество попыток ({MAX_RETRIES}) для {url}")
    return None


async def get_date_and_id() -> Dict[str, str]:
    """
    Получить дату и id заказов поставщикам
    :return: словарь {id: дата}
    """
    response_data = {}
    url = "https://api.moysklad.ru/api/remap/1.2/entity/purchaseorder"

    async with aiohttp.ClientSession() as session:
        data = await get_data(session, url)

    if not data:
        return response_data

    rows = data.get("rows", [])

    for row in rows:
        response_data[row["id"]] = row["moment"]

    return response_data


async def get_info_order(session, id: str, date_time: str) -> Dict[str, dict]:
    """
    Получить кол-во, себес, отправлено, принято, url для запроса имени и характеристик
    :param session: aiohttp сессия
    :param id: ID заказа
    :param date_time: дата заказа
    :return: словарь с данными позиций
    """
    response_data = {}
    url = f"https://api.moysklad.ru/api/remap/1.2/entity/purchaseorder/{id}/positions"

    data = await get_data(session, url)

    if not data:
        return response_data

    rows = data.get("rows", [])

    for row in rows:
        key = row["assortment"]["meta"]["href"]

        response_data[key] = dict(
            quantity=row["quantity"],
            price=round(row["price"] / 100, 2),
            shipped=row["shipped"],
            accepted=row["shipped"] * round(row["price"] / 100, 2),
            date_time=date_time
        )

    return response_data


async def get_info_position(session, url: str) -> Dict[str, any]:
    """
    Получить имя и характеристики позиции
    :param session: aiohttp сессия
    :param url: URL позиции
    :return: словарь с данными позиции
    """
    response_data = {}

    data = await get_data(session, url)

    if not data:
        return response_data

    characteristics = data.get("characteristics", [])

    response_data = dict(name=data.get("name", ""))

    for characteristic in characteristics:
        response_data[characteristic["name"]] = characteristic["value"]

    return response_data


async def collect_all_data() -> Dict:
    """
    Собирает всю информацию с "Мой склад" с учетом ограничений API
    :return: словарь со всеми данными
    """
    logger.info("Начало сбора данных из МойСклад")

    # Получаем ID и даты заказов
    id_and_date = await get_date_and_id()
    logger.info(f"Получено {len(id_and_date)} заказов")

    # Используем одну сессию для всех запросов
    async with aiohttp.ClientSession() as session:
        # 1️⃣ Получаем информацию по заказам
        tasks_orders = [
            get_info_order(session, idd, date_time)
            for idd, date_time in id_and_date.items()
        ]

        results_orders = await asyncio.gather(*tasks_orders)
        logger.info("Получена информация по всем заказам")

        # Преобразуем результаты
        id_to_result = {url: values for res in results_orders for url, values in res.items()}
        logger.info(f"Всего позиций для обработки: {len(id_to_result)}")

        # 2️⃣ Получаем информацию по позициям
        tasks_positions = [
            get_info_position(session, url)
            for url in id_to_result.keys()
        ]

        results_positions = await asyncio.gather(*tasks_positions)
        logger.info("Получена информация по всем позициям")

    # 3️⃣ Объединяем результаты
    final_data = {}
    for (url, order_values), position_values in zip(id_to_result.items(), results_positions):
        merged = {**order_values, **{"positions": position_values}}
        final_data[url] = merged

    return final_data


async def get_and_save_mysklad_data() -> None:
    """
    здесь получаем данные из collect_all_data
    и сохарняем их в БД
    :return:
    """

    all_data = await collect_all_data()

    try:
        conn = await async_connect_to_database()
        if not conn:
            logger.error("Ошибка подключения к БД")
            raise

        data = []
        try:
            for key, value in all_data.items():
                key_id = key.split("/")[-1]
                color = value["positions"]["Цвет"]
                name = value["positions"]["name"]
                size = value["positions"]["Размер"]
                size_ru = value["positions"]["Размер РФ"]
                articul = name.split("_")[1].split(" ")[0]
                data.append(
                    add_set_data_from_db(
                        conn=conn,
                        table_name="myapp_mysklad",
                        data=dict(
                            key_id=key_id,
                            name=name,
                            articul=articul,
                            date_time=parse_datetime(value["date_time"]),
                            price=value["price"],
                            quantity=value["quantity"],
                            shipped=value["shipped"],
                            accepted=value["accepted"],
                            color=color,
                            size=size,
                            size_ru=size_ru
                        ),
                            conflict_fields=["key_id"]
                    )
                )
        except Exception as e:
            logger.error(f"Ошибка подготовки данных перед загрузкой в БД {e}")

        try:
            results = await asyncio.gather(*data)
        except Exception as e:
            logger.error(f"Ошибка при добавлении данных в БД Мой Склад {e}")
    except:
        return
    finally:
        await conn.close()

# if __name__ == "__main__":
#     loop = asyncio.get_event_loop()
#     res = loop.run_until_complete(collect_all_data())
#     print(json.dumps(res, indent=2, ensure_ascii=False))