import time
from typing import List
from parsers.wildberies import parse
from google.functions import fetch_google_sheet_data

import logging
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("core"))


url_prices = "https://docs.google.com/spreadsheets/d/1EhjutxGw8kHlW1I3jbdgD-UMA5sE20aajMO865RzrlA/edit?gid=1101267699#gid=1101267699"


def get_count_pages(url: str) -> int:
    """
    Получить кол-во листов
    :param url: url таблы
    :return: кол-во листов в таблице
    """
    i = 0
    while i < 5:
        try:
            data = fetch_google_sheet_data(url, sheet_identifier=None)
            return len(data)
        except Exception as e:
            i += 1
            time.sleep(i * 2)
            logger.error(f"Ошибка: {e}. Функция get_count_pages. Параметры: {url}")


def get_data_lists(url: str) -> List[list]:
    """
    получть информацию из всех листов таблицы
    :param url:
    :return: массив с информацуией на каждом листе
    """
    count_pages = get_count_pages(url)

    result = []

    for page in range(count_pages):
        data = fetch_google_sheet_data(url, sheet_identifier=page)
        result.append(data)

    return result


