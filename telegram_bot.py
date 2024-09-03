import telegram
from config import telegram_config

token = telegram_config['token']
bot = telegram.Bot(token)
chat_id = telegram_config['chat_id']

async def send_message(message): 
    await bot.send_message(chat_id, message)
