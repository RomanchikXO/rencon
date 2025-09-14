import databases
from loader import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

DATABASE_URL = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@db:5432/{POSTGRES_DB}"

# асинхронное подключение
database = databases.Database(DATABASE_URL)
