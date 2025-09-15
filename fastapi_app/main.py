import json

from fastapi import FastAPI, HTTPException
from sqlalchemy import MetaData, Table, select, create_engine
from .database import database
from pydantic import BaseModel, Field
from loader import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from context_logger import ContextLogger
import logging
from dateutil.parser import parse as parse_date

logger = ContextLogger(logging.getLogger("fastapi"))

app = FastAPI()

# SQLAlchemy только для описания схемы (reflect)
sync_engine = create_engine(
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@db:5432/{POSTGRES_DB}"
)  # sync для рефлекта
metadata = MetaData()
metadata.reflect(bind=sync_engine)


products_table = metadata.tables.get("myapp_productsstat")
nmids_table = metadata.tables.get("myapp_nmids")
wblk_table = metadata.tables.get("myapp_wblk")

if None in [products_table, nmids_table, wblk_table]:
    logger.error("Одна из таблиц (ProductsStat, nmids, WbLk) не найдена.")
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
    inn: int
    date_from: str | None = None
    date_to: str | None = None
    articles: list[int] | None = None
    colors: list[str] | None = None


@app.post("/products_stat/")
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

        # nmids_list = [row["nmid"] for row in await database.fetch_all(query_nmids)]
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
                all_data[nmid]["руб"] += rub
                all_data[nmid]["шт"] += sht
            else:
                all_data[nmid] = {"руб": rub, "шт": sht}
        return all_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
