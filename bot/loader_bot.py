from telebot import TeleBot
import os


BOT_TOKEN = os.environ.get('BOT_TOKEN')

bot = TeleBot(token=BOT_TOKEN, skip_pending=True)


