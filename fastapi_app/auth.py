from fastapi import Header, HTTPException, Depends
from loader import BEARER as VALID_TOKEN


async def verify_token(authorization: str = Header(...)):
    """
    Проверяет наличие и валидность токена в заголовке Authorization.
    Ждёт формат: Authorization: Bearer <token>
    """
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Некорректный формат токена")

    token = authorization.split(" ", 1)[1]
    if token != VALID_TOKEN:
        raise HTTPException(status_code=401, detail="Неверный токен")

    return token
