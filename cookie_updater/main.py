import asyncio
from playwright_utils import get_and_store_cookies

async def main():
    await get_and_store_cookies()

if __name__ == "__main__":
    asyncio.run(main())