import math

import lxml.html
from playwright.async_api import async_playwright
import asyncio
from database.DataBase import async_connect_to_database
import logging
from context_logger import ContextLogger
from playwright_utils import ask_user_for_input
from BOT.states import get_status


logger = ContextLogger(logging.getLogger("wallet_discount_updater"))


async def login_and_get_context():
    conn = await async_connect_to_database()
    if not conn:
        logger.warning(f"Ошибка подключения к БД в login_and_get_context")
        return
    try:
        request = ("SELECT phone_number, tg_id "
                   "FROM myapp_customuser "
                   "WHERE id = 15")
        all_fields = await conn.fetch(request)
        result = [{"number": row["phone_number"], "tg_id": int(row["tg_id"])} for row in all_fields]
        result = result[0]
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_customuser. Запрос {request}. Error: {e}")
    finally:
        await conn.close()

    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-software-rasterizer",
            "--start-maximized",
            "--disable-blink-features=AutomationControlled",
            "--disable-extensions",
            "--disable-gpu",
        ]
    )
    context = await browser.new_context(
        timezone_id="Europe/Moscow",  # Устанавливаем часовой пояс на Москву
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        # Стандартный пользовательский агент
        locale="ru-RU",  # Устанавливаем локаль
        geolocation={"latitude": 55.7558, "longitude": 37.6173},  # Геолокация Москвы
        permissions=["geolocation"],  # Разрешаем использование геолокации
    )

    page = await context.new_page()

    await page.goto("https://www.wildberries.ru/security/login?returnUrl=https%3A%2F%2Fwww.wildberries.ru%2F")

    # Ожидание появления инпута и ввод номера
    await page.wait_for_selector("input.input--BeCbN[inputmode='tel']", timeout=30000)
    input_selector = "input.input--BeCbN[inputmode='tel']"
    await page.click(input_selector)  # сфокусировать
    await asyncio.sleep(2)
    await page.type(input_selector, f"{result['number']}", delay=100)  # имитируем ручной ввод


    # Ожидание активности кнопки и клик
    await page.wait_for_selector("button#requestCode:not([disabled])", timeout=10000)
    await page.click("button#requestCode")

    # Ожидание появления полей для ввода кода
    await page.wait_for_selector("input.j-b-charinput[inputmode='numeric']", timeout=10000)

    # Ввод кода из консоли
    ask_user_for_input(result['tg_id'])

    sms_code = None
    while not sms_code:
        status = get_status(result["tg_id"])
        if status and status.startswith("code_"):
            sms_code = str(status.replace("code_", ""))
            break
        await asyncio.sleep(10)

    # Получение всех инпутов и заполнение их по одной цифре
    inputs = await page.query_selector_all("input.j-b-charinput[inputmode='numeric']")
    for i, digit in enumerate(sms_code.strip()):
        if i < len(inputs):
            await inputs[i].fill(digit)

    # (Опционально) можно дождаться редиректа или подтверждения входа
    await page.wait_for_timeout(5000)  # или ждём элемент, указывающий на успешный вход

    while True:
        # Зацикливаем с паузой в 5 минут

        # Шаг 3: Переход на страницу продавца
        await page.goto("https://www.wildberries.ru/seller/1209217", wait_until="domcontentloaded")

        # Шаг 4: Поиск первого блока карточки товара
        await page.wait_for_selector("div.product-card__wrapper", timeout=60000)

        first_card = await page.query_selector("div.product-card__wrapper a.product-card__link")
        url = await first_card.get_attribute("href")

        # переходим в карточку
        await page.goto(url, wait_until="domcontentloaded")

        await page.wait_for_selector("div.priceBlockPriceWrap--G4F0p", timeout=10000)

        # Красная цена (с WB-кошельком)
        red_price_el = await page.query_selector("span.priceBlockWalletPrice--RJGuT")
        red_price = int(
            (await red_price_el.inner_text()).replace("\xa0", "").replace("₽", "")
        )

        # Чёрная цена (финальная без кошелька)
        black_price_el = await page.query_selector("ins.priceBlockFinalPrice--iToZR")
        black_price = int(
            (await black_price_el.inner_text()).replace("\xa0", "").replace("₽", "")
        )

        discount = math.floor((black_price - red_price) / (black_price / 100))

        conn = await async_connect_to_database()
        if not conn:
            logger.warning(f"Ошибка подключения к БД в set_wallet_discount")
            return
        try:
            request = ("UPDATE myapp_price "
                       "SET wallet_discount = $1")
            await conn.execute(request, discount)
        except Exception as e:
            logger.error(f"Ошибка обновления wallet_discount в myapp_price. Запрос {request}. Error: {e}")
        finally:
            await conn.close()

        await asyncio.sleep(300)
        try:
            await page.reload(timeout=60000, wait_until="domcontentloaded")
        except Exception as e:
            logger.error(f"Ошибка в основном цикле playwright_discount_updater в функции login_and_get_context: {e}")
            await asyncio.sleep(60)  # отдохнём минуту перед повтором
            await page.reload(timeout=60000, wait_until="domcontentloaded")



# loop = asyncio.get_event_loop()
# res = loop.run_until_complete(login_and_get_context())