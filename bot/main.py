from loader_bot import bot
import handlers  # noqa
from utils.set_bot_commands import set_default_commands
import logging
from context_logger import ContextLogger
import os

logger = ContextLogger(logging.getLogger("bot"))

DEBUG = os.environ.get("DEBUG", "1")

DEBUG = DEBUG.lower() in ("1", "true", "yes")

if __name__ == "__main__":
    if not DEBUG:
        set_default_commands(bot)
        logger.info("Бот запущен")
        bot.infinity_polling(skip_pending=True)