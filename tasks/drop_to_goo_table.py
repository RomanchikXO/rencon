from fastapi_app.main import get_dimensions
from loader import BEARER
from context_logger import ContextLogger
import logging
from decorators import with_db_connection
from google.functions import update_google_sheet_data

logger = ContextLogger(logging.getLogger("core"))

@with_db_connection
async def do_something():
    url = "https://docs.google.com/spreadsheets/d/1djlCANhJ5eOWsHB95Gh7Duz0YWlF6cOT035dYsqOZQ4/edit?gid=968779387#gid=968779387"
    name = "Dimensions"
    result = await get_dimensions(token=BEARER)

    headers = ["inn", "article_code", "img_url", "nmid", "subjectname", "height", "length", "width", ""]
    data = [headers]

    try:
        reform_data = [
            [value["inn"],
             key,
             value["img_url"],
             value["nmid"],
             value["subjectname"],
             value["dimensions"]["height"],
             value["dimensions"]["length"],
             value["dimensions"]["width"],
             ""]
            for key, value in result.items()
        ]
    except Exception as e:
        logger.error(f"Ошибка обработки данных в do_something: {e}")
        raise

    data += reform_data

    try:
        clear_rows = max(1000, len(data) + 300)
        clear_data = [["" for _ in range(9)] for _ in range(clear_rows)]

        update_google_sheet_data(
            spreadsheet_url=url,
            sheet_identifier=name,
            data_range=f"A1:I{clear_rows}",
            values=clear_data
        )

        update_google_sheet_data(
            spreadsheet_url=url,
            sheet_identifier=name,
            data_range=f"A1:I{len(data)}",
            values=data
        )
    except Exception as e:
        logger.error(f"Ошибка загрузки dimensions в таблицу: {e}")

