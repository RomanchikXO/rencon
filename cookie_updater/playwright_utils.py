import asyncio
import time

from bot.loader_bot import bot
from playwright.async_api import async_playwright
from database.DataBase import async_connect_to_database
import logging
from context_logger import ContextLogger
from bot.states import set_status, get_status
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
import os
from datetime import datetime


logger = ContextLogger(logging.getLogger("cookie_updater"))

os.makedirs('/app/logs', exist_ok=True)

def ask_user_for_input(user_id):
    bot.send_message(user_id, "Введите код из SMS:")
    set_status("get_sms_code", user_id)


async def get_datetime():
    response = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return response


async def login_and_get_context(page=None, context=None):
    """
        Функция авторизации.
        Если page=None - создает новый браузер и сохраняет состояние
        Если page передан - использует существующий контекст
        """

    try:
        await page.wait_for_selector('input[data-testid="phone-input"]')
    except Exception as e:
        time_now = await get_datetime()
        logger.error(f"{time_now} Ошибка при ожидании поля ввода номера: {e}")
        html_content = await page.content()
        with open('/app/logs/error_page.html', 'w', encoding='utf-8') as f:
            f.write(html_content)
        await page.screenshot(path='/app/logs/error_screenshot.png', full_page=True)
        raise

    # Введём номер (формат: 9999999999)
    conn = await async_connect_to_database()
    if not conn:
        logger.warning(f"Ошибка подключения к БД в login_and_get_context")
        return
    try:
        request = ("SELECT number, tg_id "
                   "FROM myapp_wblk "
                   "LIMIT 1")
        all_fields = await conn.fetch(request)
        result = [{ "number": row["number"], "tg_id": row["tg_id"] } for row in all_fields]
        result = result[0]
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_wblk. Запрос {request}. Error: {e}")
        raise
    finally:
        await conn.close()

    await page.fill('input[data-testid="phone-input"]', str(result["number"]))

    # Клик по кнопке со стрелкой
    await page.locator('img[src*="arrow-circle-right"]').click()

    # Ждём появления поля для ввода SMS-кода
    await page.wait_for_selector('input[data-testid="sms-code-input"]')

    # спрашиваем в боте код
    ask_user_for_input(result["tg_id"])

    sms_code = None
    while not sms_code:
        status = get_status(result["tg_id"])
        if status and status.startswith("code_"):
            sms_code = str(status.replace("code_", ""))
            break
        time.sleep(10)
    try:
        await page.fill('input[data-testid="sms-code-input"]', str(sms_code))
    except Exception as e:
        logger.error(f"Ошибка при вставке смс. Код из смс: {sms_code}. Ошибка: {e}")
        raise

    # сохраняем состояние
    await asyncio.sleep(20)
    await context.storage_state(path="auth_state.json")

    # не раскомичиваем
    # if created_here:
    #     await browser.close()
    #     await playwright.stop()

    return context


async def close_any_popup(page):
    """Закрывает всплывающие окна"""

    # Вариант 1: Специфичные селекторы для кнопок закрытия
    candidates = [
        # Кнопки с крестиком внутри модальных окон/drawer
        "[class*='Drawer'] [class*='clear-icon'] button",
        "[class*='Drawer'] [class*='close'] button",
        "[class*='modal'] [class*='clear-icon'] button",
        "[class*='modal'] [class*='close'] button",
        "[class*='Informer'] [class*='clear-icon'] button",

        # Help Center drawer специфично
        "[class*='Help-center-drawer'] button:has(svg path[d*='22.7782'])",

        # Общие паттерны для кнопок закрытия
        "[class*='clear-icon'] button:has(svg)",
        "[class*='close-icon'] button:has(svg)",

        # Кнопки с текстом
        "button:has-text('×')",
        "button:has-text('✕')",
        "button:has-text('Закрыть')",

        # По роли
        "[role=dialog] button:has(svg path[clip-rule='evenodd'])",

        # Круглые кнопки-иконки (обычно для закрытия)
        # "button[class*='circle']:has(svg path[clip-rule='evenodd'])",
        # "button[class*='onlyIcon']:has(svg path[clip-rule='evenodd'])",
    ]

    for selector in candidates:
        try:
            count = await page.locator(selector).count()
            if count > 0:
                await page.locator(selector).first.click(timeout=500)
                print(f"✅ Закрыто через: {selector}")
                return True
        except:
            pass

    return False


async def get_and_store_cookies(page=None):
    """Основная функция получения и сохранения cookies"""

    if not page:
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

    try:
        # Пытаемся загрузить сохраненное состояние
        if not page:
            context = await browser.new_context(storage_state="auth_state.json")
            page = await context.new_page()
            await page.goto("https://seller.wildberries.ru/")

    except FileNotFoundError:
        # Файла auth_state.json нет - создаем с нуля
        context = await browser.new_context(
            timezone_id="Europe/Moscow",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            locale="ru-RU",
            geolocation={"latitude": 55.7558, "longitude": 37.6173},
            permissions=["geolocation"],
        )
        page = await context.new_page()
        await page.goto("https://seller.wildberries.ru/")

    # Проверяем, нужна ли авторизация
    try:
        button = await page.wait_for_selector(
            'button:has-text("Войти")',
            timeout=10000
        )
        if button:
            await button.click()
            await page.wait_for_load_state("load")
            # Передаем page И context для сохранения состояния
            await login_and_get_context(page, context)
            # После авторизации возвращаемся на главную
            await page.goto("https://seller.wildberries.ru/")
            await page.wait_for_load_state("load")
    except:
        # Кнопки "Войти" нет - значит уже авторизованы
        pass

    # Закрываем всплывающие окна
    try:
        page.on("dialog", lambda dialog: dialog.dismiss())
        await close_any_popup(page)

        popup = page.locator("xpath=//*[number(translate(@style,'^0-9',''))>1000]")
        await popup.locator("button, div, span").first.click()
    except:
        pass

    # Основная логика работы с cookies
    # Кликаем по кнопке для открытия списка поставщиков
    try:
        chips_button = page.locator('button[data-testid="desktop-profile-select-button-chips-component"]')
        await chips_button.wait_for(state="visible", timeout=10000)
        await chips_button.hover()
        await asyncio.sleep(0.5)  # Небольшая задержка для открытия выпадающего списка

        cookies_need = [
            "wbx-validation-key",
            "_wbauid",
            "x-supplier-id-external",
        ]

        conn = await async_connect_to_database()
        if not conn:
            logger.warning(f"Ошибка подключения к БД в login_and_get_context")
            return
        try:
            request = ("SELECT id, inn "
                       "FROM myapp_wblk")
            all_fields = await conn.fetch(request)
            inns = [{ "id": row["id"], "inn": row["inn"] } for row in all_fields]
        except Exception as e:
            raise Exception(f"Ошибка получения данных из myapp_wblk. Запрос {request}. Error: {e}")
        finally:
            await conn.close()

        current_handler = None

        for index, inn in enumerate(inns): # тут inns это массив с инн с БД
            # authorizev3 = None

            # Удаляем старый обработчик
            if current_handler:
                page.remove_listener("request", current_handler)

            async def log_request(request):
                nonlocal authorizev3
                if "authorizev3" in request.headers:
                    authorizev3 = request.headers["authorizev3"]

            current_handler = log_request
            page.on("request", current_handler)

            target_text = f"ИНН {inn['inn']}"

            if index > 0:
                # если элемент не первый то раскрываем выбор ЛК
                await chips_button.wait_for(state="visible", timeout=5000)
                await chips_button.hover()
                await asyncio.sleep(0.5)

            supplier_radio_label = page.locator(
                f"li.suppliers-list_SuppliersList__item__GPkdU:has-text('{target_text}') label[data-testid='supplier-checkbox-checkbox']"
            )

            try:
                await supplier_radio_label.wait_for(state="visible", timeout=5000)
                await asyncio.sleep(5)
                await supplier_radio_label.click()
            except PlaywrightTimeoutError:
                time_now = await get_datetime()
                logger.warning(f"{time_now} Элемент поставщика '{target_text}' не найден или не появился вовремя")
                continue

            await asyncio.sleep(3)
            await close_any_popup(page)

            cookies = await page.context.cookies()
            cookies_str = ";".join(f"{cookie['name']}={cookie['value']}" for cookie in cookies if cookie.get("name", "") in cookies_need)

            conn = await async_connect_to_database()
            if not conn:
                logger.warning("Ошибка подключения к БД в get_and_store_cookies")
                return
            try:
                query = """
                        UPDATE myapp_wblk
                        SET
                            cookie = $1,
                            authorizev3 = $2
                        WHERE id = $3
                    """
                await conn.execute(query, cookies_str, authorizev3, inn["id"])
            except Exception as e:
                raise Exception(f"Ошибка обновления кукков в лк. Error: {e}")
            finally:
                await conn.close()
            await asyncio.sleep(300)
            await page.reload()
            await get_and_store_cookies(page)
    except Exception as e:
        time_now = await get_datetime()
        logger.error(f"{time_now} Ошибка: {e}")
        pass
    finally:
        try:
            await browser.close()
        except:
            pass
        await get_and_store_cookies()
