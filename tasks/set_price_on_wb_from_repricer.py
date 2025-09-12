import asyncio
import math
import sympy as sp
from database.DataBase import async_connect_to_database
import logging
from context_logger import ContextLogger
from typing import List

from myapp.models import Price
from parsers.wildberies import wb_api
import aiohttp
from asgiref.sync import sync_to_async


logger = ContextLogger(logging.getLogger("core"))


async def get_price_from_db_dor_wb()->List[dict]:
    """
    Получить товары которым надо устанавливать цену
    Returns:

    """

    conn = await async_connect_to_database()
    if not conn:
        logger.warning("Ошибка подключения к БД в set_price_wb")
        return
    try:
        query = """
            SELECT 
                rp.nmid as nmid, 
                wblk.token as token, 
                rp.keep_price as keep_price,
                rp.price_plan as price_plan,
                rp.marg_or_price as marg_or_price,
                price.redprice as redprice,
                price.spp as spp,
                price.discount as discount,
                price.wallet_discount as wallet_discount,
                price.cost_price as cost_price,
                price.reject as reject,
                price.commission as commission,
                price.acquiring as acquiring,
                price.drr as drr,
                price.usn as usn,
                price.nds as nds
            FROM myapp_repricer rp
            INNER JOIN myapp_wblk wblk ON wblk.id = rp.lk_id 
            INNER JOIN myapp_price price ON price.nmid = rp.nmid
            WHERE 
                rp.is_active IS TRUE 
                AND price.cost_price != 0.0
        """
        rows = await conn.fetch(query)
        columns = [dict(row) for row in rows]
        return columns
    except Exception as e:
        logger.error(f"Ошибка получения цен из БД для репрайсера. Error: {e}")
    finally:
        await conn.close()


def get_price(
        res_val: int,
        cost_price: float,
        spp: int,
        discount_seller: float,
        disc_wb: int,
        nds: int,
        reject: int,
        commission: int,
        acquiring: int,
        drr: int,
        usn: int
)-> tuple:
    """
    получить цену без скидок по желаемой марже
    """

    spp = int(spp)/100
    nds = int(nds)/100
    reject = int(reject)/100
    commission = int(commission)/100
    acquiring = int(acquiring)/100
    drr = int(drr)/100
    usn = int(usn)/100
    discount = 1 - int(discount_seller)/100

    # Переменные
    x = sp.symbols('x', positive=True, real=True)
    result = sp.symbols('result', real=True)

    # Вычисления, заменяем floor на просто выражение (приближенно)
    j = x * discount

    n = cost_price + cost_price * reject + j * commission + j * acquiring + j * drr
    p = j * nds + (j - j * nds) * usn
    s = n + p

    # Выражение для result (без round для аналитики)
    expr = ((j - s) / j) * 100

    # Приравниваем к result и решаем уравнение для x
    equation = sp.Eq(expr, result)

    # Выведем решения
    solutions = sp.solve(equation.subs(result, res_val), x)
    try:
        price_without_disc = math.ceil(solutions[0])
    except Exception as e:
        logger.error(f"Ошибка в get_price: {e}. solution: {solutions}")
        return None, None
    black_price = math.floor(math.floor(price_without_disc * discount) * (1 - spp))
    return price_without_disc, black_price


def get_price_with_all_disc(
        res_val: int,
        spp: int,
        discount_seller: float,
        disc_wb: int,
) -> tuple:
    """
    получить цену без скидок по желаемой красной цене с учетом спп и скидки кошелька
    """
    spp = int(spp)
    disc_wb = int(disc_wb)
    discount = int(discount_seller)

    black_price = math.ceil(res_val / (100-disc_wb) * 100)
    price_with_disc = math.ceil(black_price / (100 - spp) * 100)
    price_without_disc = math.ceil(price_with_disc / (100 - discount) * 100)
    return price_without_disc, black_price


def get_marg(
    price_without_disc: int,
    discount: int,
    cost_price: float,
    reject: int,
    commission: int,
    acquiring: int,
    nds: int,
    usn: int,
    drr: int
) -> int:
    """
    Посчитать маржинальность
    """
    j = math.floor(price_without_disc * ((100 - discount) / 100))

    n = round(cost_price +
         cost_price * (reject / 100) +
         j * (commission / 100) +
         j * (acquiring / 100) +
         j * (drr / 100),
              2
         )
    p = round(j * (nds / 100) + (j - j * (nds / 100)) * (usn / 100), 2)
    s = round(n + p, 2)

    marg = round((j - s) / j * 100)
    return marg


def set_current_list(data: List[dict])-> dict:
    response = {}

    try:
        for i in data:
            if not response.get(i["token"]):
                response[i["token"]] = []
            try:
                if i["marg_or_price"]:
                    price, black_price = get_price(i["keep_price"], i["cost_price"], i["spp"], i["discount"], i["wallet_discount"], i["nds"], i["reject"], i["commission"], i["acquiring"], i["drr"], i["usn"])
                else:
                    price, black_price = get_price_with_all_disc(i["price_plan"], i["spp"], i["discount"], i["wallet_discount"])
            except:
                raise Exception(f"Ошибка при формировании price, black_price. Данные: {i}")
            if not price and not black_price:
                logger.info(f"Не нашлось цены для {i}")
                continue
            try:
                red_price = math.floor(black_price * (1 - int(i["wallet_discount"])/100))
            except:
                raise Exception(f"Ошибка при формировании red_price. Данные: {i}")
            try:
                response[i["token"]].append(
                    {
                        "nmID":int(i["nmid"]),
                        "price": int(price),
                        "black_price": black_price,
                        "discount": int(i["discount"]),
                        "keep_price": i["keep_price"], #это маржа
                        "redprice": red_price,
                    }
                )
            except:
                raise Exception(f"Ошибка при добавлении в response. Данные: {i}")
    except Exception as e:
        logger.error(f"в set_current_list: {e}")
        raise
    return response


@sync_to_async
def get_main_status():
    return Price.objects.order_by('id').values_list('main_status', flat=True).first()


async def set_price_on_wb_from_repricer():
    result = await get_price_from_db_dor_wb()

    if not result:
        logger.info("Отсутствуют товары для установки цен")
        return

    try:
        articles = set_current_list(result)
        combined = sum(articles.values(), [])  # получаем массив со словарями [{}, {}]
        articles = {
            k: [{k2: v2 for k2, v2 in d.items() if k2 not in ["keep_price", "black_price", "redprice"]} for d in v]
            for k, v in articles.items()
        }
    except Exception as e:
        logger.error(f"Новые цены не установлены. Ошибка: {e}")
        return

    param = [
        {
            "API_KEY": key,
            "type": "set_price_and_discount",
            "data": value,
        }
        for key, value in articles.items()
    ]
    if not param:
        logger.info("Нет товаров для обновления цены")
        return

    request = {}

    status_rep = False
    try:
        status_rep = await get_main_status()
    except Exception as e:
        logger.error(f"Ошибка получения main_status в set_price_on_wb_from_repricer: {e}")
        return

    if status_rep:
        try:
            async with aiohttp.ClientSession() as session:
                for seller in param:
                    request[seller["API_KEY"]] = wb_api(session, seller)
                await asyncio.gather(*request.values())
        except Exception as e:
            logger.error(f"Цены не установлены. Ошибка: {e}")
            return

    conn = await async_connect_to_database()
    if not conn:
        logger.warning("Ошибка подключения к БД в set_price_on_wb_from_repricer")
        return
    try:
        values = [(item["nmID"], int(item["keep_price"]), int(item["price"]), int(item["black_price"]), int(item["redprice"])) for item in combined]
        groups = []
        for idx in range(len(values)):
            # base — сдвиг для этой тройки
            base = idx * 5
            groups.append(f"(${base+1}::integer, ${base+2}::numeric, ${base+3}::numeric, ${base+4}::numeric, ${base+5}::numeric)")
        row_placeholders = ", ".join(groups)
        flat_params = [x for triple in values for x in triple]
        query = f"""
            UPDATE myapp_price AS mp
            SET 
              redprice = d.redprice,
              sizes = (
                SELECT jsonb_agg(
                  jsonb_set(elem, '{{price}}', to_jsonb(d.price), false)
                )
                FROM jsonb_array_elements(mp.sizes) AS elem
              ),
              blackprice = d.black_price
            FROM (
              VALUES
                {row_placeholders}
            ) AS d(nmid, keep_price, price, black_price, redprice)
            WHERE mp.nmid = d.nmid;
        """

        await conn.execute(query, *flat_params)

    except Exception as e:
        logger.error(f"Ошибка обновления цен в БД myapp_price после репрайсинга. Error: {e}")
    finally:
        await conn.close()

