from log_context import task_context
from functools import wraps
from fastapi_app.main import database


def with_task_context(task_name):
    def decorator(func):
        @wraps(func)  # <-- сохраняем имя и метаданные
        def wrapper(*args, **kwargs):
            token = task_context.set({'task_name': task_name})
            try:
                return func(*args, **kwargs)
            finally:
                task_context.reset(token)
        return wrapper
    return decorator


def with_db_connection(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        connected_here = False
        try:
            # Подключаемся, если база не активна
            if not database.is_connected:
                await database.connect()
                connected_here = True

            return await func(*args, **kwargs)

        finally:
            # Закрываем соединение, если именно этот вызов его открыл
            if connected_here and database.is_connected:
                await database.disconnect()

    return wrapper