from fastapi import FastAPI, HTTPException
from sqlalchemy import MetaData, Table, select, create_engine
from .database import database
from loader import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

app = FastAPI()

# SQLAlchemy только для описания схемы (reflect)
sync_engine = create_engine(
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@db:5432/{POSTGRES_DB}"
)  # sync для рефлекта
metadata = MetaData()
metadata.reflect(bind=sync_engine)

# Берём таблицу, созданную Django
products_table = metadata.tables.get("myapp_productsstat")
if products_table is None:
    raise RuntimeError("Таблица myapp_product не найдена. Проверь имя в БД.")

# Подключаем/отключаем БД при старте/остановке приложения
@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

@app.get("/products/")
async def list_products():
    query = select(products_table)
    rows = await database.fetch_all(query)
    return [dict(row._mapping) for row in rows]

@app.get("/products/{product_id}")
async def get_product(product_id: int):
    query = select(products_table).where(products_table.c.nmid == product_id)
    row = await database.fetch_one(query)
    if not row:
        raise HTTPException(status_code=404, detail="Product not found")
    return dict(row._mapping)
