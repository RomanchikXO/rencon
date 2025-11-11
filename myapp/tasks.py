from celery import shared_task
import asyncio

from parsers.wildberies import (get_nmids, get_stocks_data_2_weeks, get_orders, get_stock_age_by_period,
                                get_qustions, get_stat_products, get_advs, get_advs_stat, get_fin_report,
                                make_and_get_save_report, get_region_sales)
from parsers.my_sklad import get_and_save_mysklad_data, update_google_table_mysklad
from tasks.google_our_prices import get_products_and_prices
from tasks.google_reviews import fetch_data__get_feedback
from tasks.drop_to_goo_table import do_something

import logging
from decorators import with_task_context
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("myapp"))


@shared_task
@with_task_context("update_google_table_mysklad")
def update_google_table_mysklad_task():
    logger.info("🟢 Обновляем в Мой склад google")
    asyncio.run(update_google_table_mysklad())
    logger.info("Мой склад google обновлена")


@shared_task
@with_task_context("get_and_save_mysklad_data")
def get_and_save_mysklad_data_task():
    logger.info("🟢 Обновляем в БД Мой склад")
    asyncio.run(get_and_save_mysklad_data())
    logger.info("Мой склад в БД обновлены")


@shared_task
@with_task_context("get_region_sales")
def get_region_sales_task():
    logger.info("🟢 Обновляем ПРОДАЖИ регион в БД")
    asyncio.run(get_region_sales())
    logger.info("ПРОДАЖИ регион в БД обновлены")


@shared_task
@with_task_context("make_and_get_save_report")
def make_and_get_save_report_task():
    logger.info("🟢 Обновляем ХРАНЕНИЕ отчет в БД")
    asyncio.run(make_and_get_save_report())
    logger.info("ХРАНЕНИЕ отчет в БД обновлены")


@shared_task
@with_task_context("get_fin_report")
def get_fin_report_task():
    logger.info("🟢 Обновляем ФИН отчеты в БД")
    asyncio.run(get_fin_report())
    logger.info("ФИН отчеты в БД обновлены")


@shared_task
@with_task_context("get_advs_stat")
def get_advs_stat_task():
    logger.info("🟢 Обновляем рекламнyю СТАТУ в БД")
    asyncio.run(get_advs_stat())
    logger.info("Рекламная СТАТА в БД обновлены")


@shared_task
@with_task_context("get_advs")
def get_advs_task():
    logger.info("🟢 Обновляем рекламы в БД")
    asyncio.run(get_advs())
    logger.info("Рекламы в БД обновлены")


@shared_task
@with_task_context("get_stat_products_task")
def get_stat_products_task():
    logger.info("🟢 Обновляем стату по товарам в БД")
    asyncio.run(get_stat_products())
    logger.info("Стата по товарам в БД обновлены")


@shared_task
@with_task_context("get_questions_task")
def get_questions_task():
    logger.info("🟢 Обновляем вопросы в БД")
    asyncio.run(get_qustions())
    logger.info("Вопросы в БД обновлены")


@shared_task
@with_task_context("get_stock_age_by_period_task")
def get_stock_age_by_period_task():
    logger.info("🟢 Получаем время нахождения товара на складах за пероиды")
    asyncio.run(get_stock_age_by_period())
    logger.info("Время нахождения товара на складах за пероиды получено")


@shared_task
@with_task_context("get_prices_and_products")
def get_prices_and_products():
    logger.info("🟢 Собираем товары и цены в БД")
    asyncio.run(get_products_and_prices())
    logger.info("Товары и цены собраны в БД")


@shared_task
@with_task_context("some_task")
def some_task():
    logger.info("Старт тестового таска")
    asyncio.run(do_something())


@shared_task
@with_task_context("get_nmids_to_db")
def get_nmids_to_db():
    logger.info("🟢 Обновляем таблицу со всеми артикулами в бд")
    asyncio.run(get_nmids())
    logger.info("Таблица со всеми артикулами обновлена")


@shared_task
@with_task_context("get_stocks_to_db")
def get_stocks_to_db():
    logger.info("🟢 Обновляем таблицу с остатками товаров на складах в бд")
    asyncio.run(get_stocks_data_2_weeks())
    logger.info("Таблица с остатками товаров на складах обновлена")


@shared_task
@with_task_context("get_orders_to_db")
def get_orders_to_db():
    logger.info("🟢 Обновляем таблицу с заказами в бд")
    asyncio.run(get_orders())
    logger.info("Таблица с заказами в бд обновлена")


@shared_task
@with_task_context("otzivi")
def get_otzivi():
    logger.info("🟢 Получаем отзывы")
    asyncio.run(fetch_data__get_feedback())
    logger.info("✅ Отзывы получены")