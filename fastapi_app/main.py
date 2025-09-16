import json
from typing import Dict

from fastapi import FastAPI, HTTPException
from sqlalchemy import MetaData, Table, select, create_engine, func
from .database import database
from pydantic import BaseModel, Field
from loader import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from context_logger import ContextLogger
import logging
from dateutil.parser import parse as parse_date

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

if None in [products_table, nmids_table, wblk_table, stocks_table]:
    logger.error("Одна из таблиц (ProductsStat, nmids, WbLk, Stocks) не найдена.")
    raise RuntimeError()


# Подключаем/отключаем БД при старте/остановке приложения
@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


# Pydantic-модель для входящего POST
class ProductsStatRequest(BaseModel):
    inn: int = Field(..., description="ИНН клиента, обязательное поле")
    date_from: str | None = Field(None, description="Дата начала выборки в формате YYYY-MM-DD, необязательное поле")
    date_to: str | None = Field(None, description="Дата окончания выборки в формате YYYY-MM-DD, необязательное поле")
    articles: list[int] | None = Field(None,
                                       description="Список артикулов, по которым фильтровать, необязательное поле")
    colors: list[str] | None = Field(None, description="Список цветов для фильтрации, необязательное поле")


class ProductStatResponse(BaseModel):
    rub: float = Field(..., description="Сумма заказов в рублях")
    sht: int = Field(..., description="Количество заказов")


@app.post(
    "/products_stat/",
    response_model=Dict[int, ProductStatResponse],
    summary="Получить информацию о заказах",
    description="Возвращает суммарную информацию о заказах по NMID: количество и сумма в рублях. "
                "Можно фильтровать по дате, артикулам и цветам."
)
async def products_stat_endpoint(payload: ProductsStatRequest):

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
        query_stats = select(products_table).where(products_table.c.nmid.in_(nmids_list))
        if date_from:
            query_stats = query_stats.where(products_table.c.date_wb >= date_from)
        if date_to:
            query_stats = query_stats.where(products_table.c.date_wb <= date_to)

        stats_rows = await database.fetch_all(query_stats)
        art_per_day = [dict(row._mapping) for row in stats_rows]

        all_data = {}
        for i in art_per_day:
            nmid = i["nmid"]
            rub = i["ordersSumRub"]
            sht = i["ordersCount"]

            if nmid in all_data:
                all_data[nmid]["rub"] += rub
                all_data[nmid]["sht"] += sht
            else:
                all_data[nmid] = {"rub": rub, "sht": sht}
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
    quantity: float
    inwaytoclient: int
    inwayfromclient: int


@app.post(
    "/quantity/",
    response_model=Dict[int, ProductQuantResponse],
    summary="Получить остатки для артикулов"
)
async def products_quantity_endpoint(payload: ProductsQuantRequest):
    try:
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

        query_stocks = select(stocks_table).where(
            stocks_table.c.nmid.in_(nmids_list)
        )

        if payload.sizes:
            query_stocks = query_stocks.where(stocks_table.c.techsize.in_(payload.sizes))

        if payload.warhouses:
            lower_warehouses = [w.lower() for w in payload.warhouses]
            query_stocks = query_stocks.where(
                func.lower(stocks_table.c.warehousename).in_(lower_warehouses)
            )

        stock_rows = await database.fetch_all(query_stocks)
        row_data = [dict(row._mapping) for row in stock_rows]

        all_data = {}
        for i in row_data:
            nmid = i["nmid"]
            inwaytoclient = i["inwaytoclient"]
            inwayfromclient = i["inwayfromclient"]
            quantity = i["quantity"]

            if nmid in all_data:
                all_data[nmid]["quantity"] += quantity
                all_data[nmid]["inwaytoclient"] += inwaytoclient
                all_data[nmid]["inwayfromclient"] += inwayfromclient
            else:
                all_data[nmid] = {}
                all_data[nmid]["quantity"] = quantity
                all_data[nmid]["inwaytoclient"] = inwaytoclient
                all_data[nmid]["inwayfromclient"] = inwayfromclient
        return all_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/warehouses/", summary="Получить список уникальных складов")
async def get_unique_warehouses():
    try:
        query = select(func.distinct(stocks_table.c.warehousename))
        rows = await database.fetch_all(query)

        warehouses = [row[0] for row in rows if row[0] is not None]
        return warehouses
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))