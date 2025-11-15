import json
from collections import defaultdict
from typing import Dict, Union, Optional, List

from fastapi import FastAPI, HTTPException
from sqlalchemy import MetaData, Table, select, create_engine, func, cast, String, text
from .database import database
from pydantic import BaseModel, Field, RootModel
from loader import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from context_logger import ContextLogger
import logging
from dateutil.parser import parse as parse_date
from datetime import timedelta, datetime, date as date_dt
from fastapi import Depends
from .auth import verify_token


logger = ContextLogger(logging.getLogger("fastapi_app"))

app = FastAPI(root_path="/api")

# SQLAlchemy только для описания схемы (reflect)
sync_engine = create_engine(
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@db:5432/{POSTGRES_DB}"
)  # sync для рефлекта
metadata = MetaData()
metadata.reflect(bind=sync_engine)


products_table = metadata.tables.get("myapp_productsstat")
nmids_table = metadata.tables.get("myapp_nmids")
wblk_table = metadata.tables.get("myapp_wblk")
stocks_table = metadata.tables.get("myapp_stocks")
advstat_table = metadata.tables.get("myapp_advstat")
advs_table = metadata.tables.get("myapp_adverts")
findata_table = metadata.tables.get("myapp_findata")
savedata_table = metadata.tables.get("myapp_savedata")
sales_reg_table = metadata.tables.get("myapp_regionsales")

if None in [products_table, nmids_table, wblk_table, stocks_table, advstat_table, findata_table, savedata_table,
            sales_reg_table]:
    logger.error("Одна из таблиц (ProductsStat, nmids, WbLk, Stocks) не найдена.")
    raise RuntimeError()


color_expr = cast(
        func.jsonb_path_query_first(
            nmids_table.c.characteristics,
            '$[*] ? (@.id == 14177449).value[0]'
        ),
        String
    ).label("color")


# Подключаем/отключаем БД при старте/остановке приложения
@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


class FinReportRequest(BaseModel):
    inn: int = Field(..., description="ИНН клиента, обязательное поле")
    date_from: str | None = Field(None, description="Дата начала выборки в формате YYYY-MM-DD, необязательное поле")
    date_to: str | None = Field(None, description="Дата окончания выборки в формате YYYY-MM-DD, необязательное поле")
    articles: list[int] | None = Field(None,
                                       description="Список артикулов, по которым фильтровать, необязательное поле")
    colors: list[str] | None = Field(None, description="Список цветов для фильтрации, необязательное поле")
    sizes: list[str] | None = Field(None, description="Список размеров для фильтрации, необязательное поле")
    supplier_oper_name: list[str] | None = Field(None, description="Обоснование для оплаты, необязательное поле")


class FinReportResponse(BaseModel):
    subjectname: Optional[str] = Field(None, description="Предмет")
    vendorcode: Optional[str] = Field(None, description="Артикул продавца")
    nmid: int = Field(..., description="Артикул WB")
    date_wb: date_dt = Field(..., description="Дата операции")
    sale_dt: Optional[date_dt] = Field(None, description="Дата продажи")
    color: Optional[str] = Field(None, description="Цвет товара")
    retail_price: Optional[float] = Field(None, description="Цена розничная")
    retail_amount: Optional[float] = Field(None, description="Сумма реализации")
    ppvz_for_pay: Optional[float] = Field(None, description="К перечислению продавцу")
    delivery_rub: Optional[float] = Field(None, description="Стоимость доставки")
    acceptance: Optional[float] = Field(None, description="Платная приемка")
    warehousePrice: Optional[float] = Field(None, description="Сумма хранения")
    supplier_oper_name: Optional[str] = Field(None, description="Основание для оплаты")
    penalty: Optional[float] = Field(None, description="Штрафы")

class FinReportResponseWithDeduction(BaseModel):
    data: List[FinReportResponse]
    deduction: float = Field(..., description="Удержание")

@app.post(
    "/fin_report/",
    response_model=FinReportResponseWithDeduction,
    summary="Получить фин. отчет",
    description="Возвращает фин отчет по NMID. "
                "Можно фильтровать по дате, артикулам и цветам, размерам и обоснованием для оплаты"
)
async def fin_report_endpoint(
        payload: FinReportRequest,
        token: str = Depends(verify_token)
):

    try:
        # Парсим даты
        date_from = (parse_date(payload.date_from) - timedelta(days=1)).date() if payload.date_from else None
        date_to = (parse_date(payload.date_to) + timedelta(days=1)).date() if payload.date_to else None

        # Получаем lk
        query_lk = select(wblk_table).where(wblk_table.c.inn == payload.inn)
        lk_row = await database.fetch_one(query_lk)
        if not lk_row:
            raise HTTPException(status_code=404, detail="WbLk не найден")

        lk_id = lk_row["id"]

        # Получаем список nmid по lk
        query_nmids = select(
            nmids_table.c.nmid,
            nmids_table.c.characteristics
        ).where(nmids_table.c.lk_id == lk_id)

        if payload.articles:
            query_nmids = query_nmids.where(nmids_table.c.nmid.in_(payload.articles))

        nmids_rows = await database.fetch_all(query_nmids)

        nmids_list = []

        for row in nmids_rows:
            nmid = row["nmid"]
            if payload.colors:
                characteristics = row["characteristics"]  # JSONField из Django
                if characteristics is None:
                    continue

                try:
                    parsed = (
                        characteristics
                        if isinstance(characteristics, list)
                        else json.loads(characteristics)
                    )
                except Exception:
                    continue

                color_entry = next(
                    (item for item in parsed if item.get("id") == 14177449), None
                )
                if not color_entry:
                    continue

                value = color_entry.get("value")
                if not value or not isinstance(value, list):
                    continue

                color_value = value[0].lower()  # берём первое значение
                if color_value in [c.lower() for c in payload.colors]:
                    nmids_list.append(nmid)
            else:
                nmids_list.append(nmid)

        if not nmids_list:
            return []

        # Фильтруем ProductsStat
        query_stats = (select(
            nmids_table.c.subjectname,
            nmids_table.c.vendorcode,
            findata_table.c.nmid,
            findata_table.c.retail_price,
            findata_table.c.retail_amount,
            findata_table.c.ppvz_for_pay,
            findata_table.c.delivery_rub,
            findata_table.c.acceptance,
            findata_table.c.rr_dt, # дата операции
            findata_table.c.sale_dt,  # дата операции
            findata_table.c.supplier_oper_name,
            findata_table.c.ts_name, # размер
            findata_table.c.penalty, # штрафы
            color_expr
        )
        .join(nmids_table, nmids_table.c.nmid == findata_table.c.nmid)
       .where(findata_table.c.nmid.in_(nmids_list)))

        query_save = (select(
            nmids_table.c.subjectname,
            nmids_table.c.vendorcode,
            savedata_table.c.nmid,
            savedata_table.c.warehousePrice,
            savedata_table.c.date_wb,
            savedata_table.c.size,
            color_expr
        )
        .join(nmids_table, nmids_table.c.nmid == savedata_table.c.nmid)
        .where(savedata_table.c.nmid.in_(nmids_list)))

        if date_from:
            query_stats = query_stats.where(findata_table.c.rr_dt > date_from)
            query_save = query_save.where(savedata_table.c.date_wb > date_from)
        if date_to:
            query_stats = query_stats.where(findata_table.c.rr_dt < date_to)
            query_save = query_save.where(savedata_table.c.date_wb < date_to)
        if payload.supplier_oper_name:
            supplier_oper_name = [i.lower() for i in payload.supplier_oper_name]
            query_stats = query_stats.where(func.lower(findata_table.c.supplier_oper_name).in_(supplier_oper_name))
        if payload.sizes:
            sizes = [i.lower() for i in payload.sizes]
            query_stats = query_stats.where(findata_table.c.ts_name.in_(sizes))
            query_save = query_save.where(savedata_table.c.size.in_(sizes))

        stats_rows = await database.fetch_all(query_stats)
        art_per_day = [dict(row._mapping) for row in stats_rows]

        save_rows = await database.fetch_all(query_save)
        art_per_day_saves = [dict(row._mapping) for row in save_rows]

        # Отдельный запрос для deduction с сортировкой по дате
        query_deduction = select(
            findata_table.c.deduction,
            findata_table.c.rr_dt
        ).order_by(findata_table.c.rr_dt)

        if date_from:
            query_deduction = query_deduction.where(findata_table.c.rr_dt > date_from)
        if date_to:
            query_deduction = query_deduction.where(findata_table.c.rr_dt < date_to)

        deduction_rows = await database.fetch_all(query_deduction)
        deductions = sum(row.deduction for row in deduction_rows) if deduction_rows else 0

        def merge_data(arr1, arr2):
            response = []

            # первый массив (финансовые данные)
            for i in arr1:
                response.append({
                    "subjectname": i["subjectname"],
                    "vendorcode": i["vendorcode"],
                    "nmid": i["nmid"],
                    "retail_price": i["retail_price"],
                    "retail_amount": i["retail_amount"],
                    "ppvz_for_pay": i["ppvz_for_pay"],
                    "delivery_rub": i["delivery_rub"],
                    "acceptance": i["acceptance"],
                    "date_wb": datetime.fromisoformat(str(i["rr_dt"])).date(),  # приводим к единому имени
                    "sale_dt": datetime.fromisoformat(str(i["sale_dt"])).date(),
                    "color": i["color"].strip('"') if i.get("color") else 'Цвет не указан',
                    "supplier_oper_name": i["supplier_oper_name"],
                    "warehousePrice": 0,
                    "penalty": i["penalty"] if i.get("penalty") else 0
                })

            # второй массив (цены склада)
            for i in arr2:
                response.append({
                    "subjectname": i["subjectname"],
                    "vendorcode": i["vendorcode"],
                    "nmid": i["nmid"],
                    "date_wb": datetime.fromisoformat(str(i["date_wb"])).date(),
                    "warehousePrice": i["warehousePrice"],
                    "retail_price": 0,
                    "retail_amount": 0,
                    "ppvz_for_pay": 0,
                    "delivery_rub": 0,
                    "acceptance": 0,
                    "color": i["color"].strip('"') if i.get("color") else 'Цвет не указан',
                    "supplier_oper_name": "хранение",
                })

            return response

        result = merge_data(art_per_day, art_per_day_saves)

        return {
            "data": result,
            "deduction": deductions
        }


    except Exception as e:
        logger.exception("Ошибка в fin_report_endpoint")
        raise HTTPException(status_code=500, detail="Internal server error")


# Pydantic-модель для входящего POST
class ProductsStatRequest(BaseModel):
    inn: int = Field(..., description="ИНН клиента, обязательное поле")
    date_from: str | None = Field(None, description="Дата начала выборки в формате YYYY-MM-DD, необязательное поле")
    date_to: str | None = Field(None, description="Дата окончания выборки в формате YYYY-MM-DD, необязательное поле")
    articles: list[int] | None = Field(None,
                                       description="Список артикулов, по которым фильтровать, необязательное поле")
    colors: list[str] | None = Field(None, description="Список цветов для фильтрации, необязательное поле")


class ProductStatResponse(BaseModel):
    vendorcode: Optional[str] = Field(None, description="Артикул продавца")
    nmid: int = Field(..., description="Артикул WB")
    date_wb: date_dt = Field(..., description="Дата отчёта (YYYY-MM-DD)")
    color: Optional[str] = Field(None, description="Цвет товара")
    ordersSumRub: float = Field(..., description="Сумма заказов в рублях")
    ordersCount: int = Field(..., description="Количество заказов")


@app.post(
    "/products_stat/",
    response_model=List[ProductStatResponse],
    summary="Получить информацию о заказах",
    description="Возвращает суммарную информацию о заказах по NMID: количество и сумма в рублях. "
                "Можно фильтровать по дате, артикулам и цветам."
)
async def products_stat_endpoint(
        payload: ProductsStatRequest,
        token: str = Depends(verify_token)
):

    try:
        # Парсим даты
        date_from = parse_date(payload.date_from).date() if payload.date_from else None
        date_to = parse_date(payload.date_to).date() if payload.date_to else None

        # Получаем lk
        query_lk = select(wblk_table).where(wblk_table.c.inn == payload.inn)
        lk_row = await database.fetch_one(query_lk)
        if not lk_row:
            raise HTTPException(status_code=404, detail="WbLk не найден")

        lk_id = lk_row["id"]

        # Получаем список nmid по lk
        query_nmids = select(
            nmids_table.c.nmid,
            nmids_table.c.characteristics
        ).where(nmids_table.c.lk_id == lk_id)

        if payload.articles:
            query_nmids = query_nmids.where(nmids_table.c.nmid.in_(payload.articles))

        nmids_rows = await database.fetch_all(query_nmids)

        nmids_list = []

        for row in nmids_rows:
            nmid = row["nmid"]
            if payload.colors:
                characteristics = row["characteristics"]  # JSONField из Django
                if characteristics is None:
                    continue

                try:
                    parsed = (
                        characteristics
                        if isinstance(characteristics, list)
                        else json.loads(characteristics)
                    )
                except Exception:
                    continue

                color_entry = next(
                    (item for item in parsed if item.get("id") == 14177449), None
                )
                if not color_entry:
                    continue

                value = color_entry.get("value")
                if not value or not isinstance(value, list):
                    continue

                color_value = value[0].lower()  # берём первое значение
                if color_value in [c.lower() for c in payload.colors]:
                    nmids_list.append(nmid)
            else:
                nmids_list.append(nmid)

        if not nmids_list:
            return []

        # Фильтруем ProductsStat
        query_stats = (select(
            nmids_table.c.vendorcode,
            products_table.c.nmid,
            products_table.c.date_wb,
            products_table.c.ordersSumRub,
            products_table.c.ordersCount,
            color_expr
        )
       .join(nmids_table, nmids_table.c.nmid == products_table.c.nmid)
       .where(products_table.c.nmid.in_(nmids_list)))

        if date_from:
            query_stats = query_stats.where(products_table.c.date_wb >= date_from)
        if date_to:
            query_stats = query_stats.where(products_table.c.date_wb <= date_to)

        stats_rows = await database.fetch_all(query_stats)
        all_data = []
        for row in stats_rows:
            r = dict(row._mapping)
            if isinstance(r["date_wb"], datetime):
                r["date_wb"] = datetime.fromisoformat(str(r["date_wb"])).date()
            r["color"] = r["color"].strip('"') if r["color"] else 'Цвет не указан'
            all_data.append(r)

        return all_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Pydantic-модель для входящего POST
class ProductsQuantRequest(BaseModel):
    inn: int
    articles: list[int] | None = None
    colors: list[str] | None = None
    sizes: list[int] | None = None
    warhouses: list[str] | None = None


class ProductQuantResponse(BaseModel):
    vendorcode: Optional[str] = Field(None, description="Артикул продавца")
    nmid: int = Field(..., description="Артикул WB")
    quantity: float = Field(..., description="Остатки")
    inwaytoclient: int = Field(..., description="В пути к клиенту")
    inwayfromclient: int = Field(..., description="В пути от клиента")
    warehouse: str = Field(..., description="Склад")
    size: str = Field(..., description="Размер")
    color: str = Field(..., description="Цвет")


@app.post(
    "/quantity/",
    response_model=List[ProductQuantResponse],
    summary="Получить остатки для артикулов"
)
async def products_quantity_endpoint(
        payload: ProductsQuantRequest,
        token: str = Depends(verify_token)
):
    try:
        # Получаем lk
        query_lk = select(wblk_table).where(wblk_table.c.inn == payload.inn)
        lk_row = await database.fetch_one(query_lk)
        if not lk_row:
            raise HTTPException(status_code=404, detail="WbLk не найден")

        lk_id = lk_row["id"]

        query_stocks = (select(
            nmids_table.c.vendorcode,
            stocks_table.c.nmid,
            stocks_table.c.techsize,
            stocks_table.c.inwaytoclient,
            stocks_table.c.inwayfromclient,
            stocks_table.c.quantity,
            stocks_table.c.warehousename,
            color_expr,
            ).join(nmids_table, stocks_table.c.nmid == nmids_table.c.nmid)
            .where(nmids_table.c.lk_id == lk_id)
        )

        if payload.articles:
            query_stocks = query_stocks.where(stocks_table.c.nmid.in_(payload.articles))

        if payload.sizes:
            query_stocks = query_stocks.where(stocks_table.c.techsize.in_(payload.sizes))

        if payload.warhouses:
            lower_warehouses = [w.lower() for w in payload.warhouses]
            query_stocks = query_stocks.where(
                func.lower(stocks_table.c.warehousename).in_(lower_warehouses)
            )

        stock_rows = await database.fetch_all(query_stocks)
        row_data = [dict(row._mapping) for row in stock_rows]

        colors_lower = [c.lower() for c in payload.colors] if payload.colors else []

        all_data = []
        for i in row_data:
            color = i["color"].strip('"').lower() if i.get("color") else 'Цвет не указан'
            if colors_lower and color not in colors_lower:
                continue

            all_data.append(dict(
                vendorcode=i["vendorcode"],
                nmid=i["nmid"],
                inwaytoclient=i["inwaytoclient"],
                inwayfromclient=i["inwayfromclient"],
                quantity=i["quantity"],
                size=i["techsize"],
                warehouse=i["warehousename"],
                color=color
            ))

        return all_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/warehouses/", summary="Получить список уникальных складов")
async def get_unique_warehouses(
        token: str = Depends(verify_token)
):
    try:
        query = select(func.distinct(stocks_table.c.warehousename))
        rows = await database.fetch_all(query)

        warehouses = [row[0] for row in rows if row[0] is not None]
        return warehouses
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/supplier_oper_name/", summary="Получить список уникальных оснований для оплаты")
async def get_supplier_oper_name(
        token: str = Depends(verify_token)
):
    try:
        query = select(func.distinct(findata_table.c.supplier_oper_name))
        rows = await database.fetch_all(query)

        supplier_oper_names = [row[0] for row in rows if row[0] is not None]
        return supplier_oper_names
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dimensions/", summary="Получить данные о товарах")
async def get_dimensions(
        token: str = Depends(verify_token)
):
    try:
        query_data = (
            select(
                nmids_table.c.vendorcode,
                nmids_table.c.nmid,
                nmids_table.c.subjectname,
                nmids_table.c.title,
                nmids_table.c.dimensions,
                wblk_table.c.inn,
                # достаём первый элемент photos, где big оканчивается на /1.webp
                func.coalesce(
                    text("""
                        (
                            SELECT p->>'big'
                            FROM jsonb_array_elements(photos) AS p
                            WHERE p->>'big' LIKE '%/1.webp'
                            LIMIT 1
                        )
                        """),
                    text("'пока пусто'")
                ).label("img_url")
            )
            .select_from(
                nmids_table.join(wblk_table, nmids_table.c.lk_id == wblk_table.c.id)
            )
        )
        rows = await database.fetch_all(query_data)

        response = {
            row["vendorcode"] : {
                "inn": row["inn"],
                "img_url": row["img_url"],
                "nmid": row["nmid"],
                "subjectname": row["subjectname"],
                "dimensions" : {
                    "height": row["dimensions"]["height"],
                    "lenght": row["dimensions"]["length"],
                    "width": row["dimensions"]["width"],
                }
            }
            for row in rows
        }
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class AdvCostResponse(BaseModel):
    vendorcode: Optional[str] = Field(None, description="Артикул продавца")
    nmid: int = Field(..., description="Артикул WB")
    cost: float = Field(..., description="Затраты")
    color: str = Field(..., description="Цвет")
    date_wb: date_dt = Field(..., description="Дата отчёта (YYYY-MM-DD)")


@app.post(
    "/adv_cost/",
    response_model=List[AdvCostResponse],
    summary="Получить затраты на рекламу поартикульно"
)
async def get_adv_cost(
        payload: ProductsStatRequest,
        token: str = Depends(verify_token)
):
    try:
        # Парсим даты
        date_from = parse_date(payload.date_from).date() if payload.date_from else None
        date_to = parse_date(payload.date_to).date() if payload.date_to else None

        # Получаем lk
        query_lk = select(wblk_table).where(wblk_table.c.inn == payload.inn)
        lk_row = await database.fetch_one(query_lk)
        if not lk_row:
            raise HTTPException(status_code=404, detail="WbLk не найден")

        lk_id = lk_row["id"]

        # Получаем список nmid по lk
        query_nmids = (
            select(
                nmids_table.c.vendorcode,
                advstat_table.c.nmid,
                advstat_table.c.sum_cost,
                advstat_table.c.date_wb,
                color_expr,
            ).join(nmids_table, advstat_table.c.nmid == nmids_table.c.nmid)
            .where(nmids_table.c.lk_id == lk_id)
        )

        if payload.articles:
            query_nmids = query_nmids.where(advstat_table.c.nmid.in_(payload.articles))

        if date_from:
            query_nmids = query_nmids.where(advstat_table.c.date_wb >= date_from)

        if date_to:
            query_nmids = query_nmids.where(advstat_table.c.date_wb <= date_to)

        colors_lower = [c.lower() for c in payload.colors] if payload.colors else []

        nmids_rows = await database.fetch_all(query_nmids)
        result = []
        for row in nmids_rows:
            color = row["color"].strip('"').lower() if row["color"] else 'Цвет не указан'
            if colors_lower and color not in colors_lower:
                continue

            result.append(dict(
                vendorcode=row["vendorcode"],
                nmid=row["nmid"],
                cost=row["sum_cost"] or 0,
                color=color,
                date_wb=datetime.fromisoformat(str(row["date_wb"])).date()
            ))


        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class AdvConversionResponse(BaseModel):
    vendorcode: Optional[str] = Field(None, description="Артикул продавца")
    clicks: int
    views: int
    atbs: int
    orders: int
    nmid: int = Field(..., description="Артикул WB")
    type_adv: int = Field(..., description="Тип РК")
    date_wb: date_dt = Field(..., description="Дата отчёта (YYYY-MM-DD)")
    color: Optional[str] = Field(None, description="Цвет товара")


@app.post(
    "/adv_conversion/",
    response_model=List[AdvConversionResponse],
    summary="Получить данные по конверсии"
)
async def get_adv_conversion(
        payload: ProductsStatRequest,
        token: str = Depends(verify_token)
):
    try:
        # Парсим даты
        date_from = parse_date(payload.date_from).date() if payload.date_from else None
        date_to = parse_date(payload.date_to).date() if payload.date_to else None

        # Получаем lk
        query_lk = select(wblk_table).where(wblk_table.c.inn == payload.inn)
        lk_row = await database.fetch_one(query_lk)
        if not lk_row:
            raise HTTPException(status_code=404, detail="WbLk не найден")

        lk_id = lk_row["id"]

        # Получаем список nmid по lk
        query_nmids = (
            select(
                nmids_table.c.vendorcode,
                advstat_table.c.nmid,
                advs_table.c.type_adv,
                advstat_table.c.clicks,
                advstat_table.c.views,
                advstat_table.c.atbs,
                advstat_table.c.orders,
                nmids_table.c.characteristics,
                advstat_table.c.date_wb,
                advstat_table.c.advert_id,
                color_expr,
            )
            .join(nmids_table, advstat_table.c.nmid == nmids_table.c.nmid)
            .join(advs_table, advstat_table.c.advert_id == advs_table.c.advert_id, isouter=True)
            .where(nmids_table.c.lk_id == lk_id)
        )

        if payload.articles:
            query_nmids = query_nmids.where(advstat_table.c.nmid.in_(payload.articles))

        if date_from:
            query_nmids = query_nmids.where(advstat_table.c.date_wb >= date_from)

        if date_to:
            query_nmids = query_nmids.where(advstat_table.c.date_wb <= date_to)

        colors_lower = [c.lower() for c in payload.colors] if payload.colors else []

        nmids_rows = await database.fetch_all(query_nmids)
        result = []
        for row in nmids_rows:
            color = row["color"].strip('"').lower() if row["color"] else 'Цвет не указан'
            if colors_lower and color not in colors_lower:
                continue

            result.append(dict(
                vendorcode=row["vendorcode"],
                nmid=row["nmid"],
                type_adv = row["type_adv"] or 0,
                clicks=row["clicks"] or 0,
                views=row["views"] or 0,
                atbs=row["atbs"] or 0,
                orders=row["orders"] or 0,
                date_wb=datetime.fromisoformat(str(row["date_wb"])).date(),
                color=color,
            ))
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class ProductSaleResponse(BaseModel):
    vendorcode: Optional[str] = Field(None, description="Артикул продавца")
    rub: float = Field(..., description="Сумма заказов в рублях")
    sht: int = Field(..., description="Количество заказов")
    nmid: int = Field(..., description="Артикул WB")
    color: Optional[str] = Field(None, description="Цвет товара")
    date_wb: date_dt = Field(..., description="Дата отчёта (YYYY-MM-DD)")

@app.post(
    "/region_sales/",
    response_model=List[ProductSaleResponse],
    summary="Получить данные по продажам по регионам"
)
async def get_adv_reg_sales(
        payload: ProductsStatRequest,
        token: str = Depends(verify_token)
):
    try:
        # Парсим даты
        date_from = (parse_date(payload.date_from) - timedelta(days=1)).date() if payload.date_from else None
        date_to = parse_date(payload.date_to).date() if payload.date_to else None

        # Получаем lk
        query_lk = select(wblk_table).where(wblk_table.c.inn == payload.inn)
        lk_row = await database.fetch_one(query_lk)
        if not lk_row:
            raise HTTPException(status_code=404, detail="WbLk не найден")

        lk_id = lk_row["id"]

        colors_lower = [c.lower() for c in payload.colors] if payload.colors else []

        # Фильтруем ProductsStat
        query_stats = (select(
            nmids_table.c.vendorcode,
            sales_reg_table.c.nmid,
            sales_reg_table.c.date_wb,
            sales_reg_table.c.saleInvoiceCostPrice,
            sales_reg_table.c.saleItemInvoiceQty,
            color_expr
            ).join(nmids_table, sales_reg_table.c.nmid == nmids_table.c.nmid)
            .where(nmids_table.c.lk_id == lk_id)
       )

        if payload.articles:
            query_stats = query_stats.where(query_stats.c.nmid.in_(payload.articles))
        if date_from:
            query_stats = query_stats.where(sales_reg_table.c.date_wb >= date_from)
        if date_to:
            query_stats = query_stats.where(sales_reg_table.c.date_wb <= date_to)

        stats_rows = await database.fetch_all(query_stats)
        art_per_day = [dict(row._mapping) for row in stats_rows]

        all_data = []
        for i in art_per_day:
            color = i["color"].strip('"').lower() if i.get("color") else 'Цвет не указан'
            if colors_lower and color not in colors_lower:
                continue
            vendorcode = i["vendorcode"]
            nmid = i["nmid"]
            rub = i["saleInvoiceCostPrice"]
            sht = i["saleItemInvoiceQty"]

            all_data.append(dict(
                vendorcode=vendorcode,
                nmid=nmid,
                rub=rub,
                sht=sht,
                color=color,
                date_wb=datetime.fromisoformat(str(i["date_wb"])).date(),
            ))


        return all_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))