import csv
from telethon import TelegramClient
from environs import Env

env = Env()
env.read_env()

api_id = env.int('API_ID')
api_hash = env('API_HASH')
phone = env('PHONE')
password = env('PASSWORD', None)
channel_link = env('CHANNEL_LINK')

output_file = "channel_messages.csv"
session_name = "parser"

client = TelegramClient(session_name, api_id, api_hash)


async def main():
    with open(output_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["date", "text"])

        async for message in client.iter_messages(channel_link, reverse=True):
            if message.text:
                writer.writerow([message.date.isoformat(), message.text])

    print(f"Все сообщения сохранены в {output_file}")


if __name__ == "__main__":
    with client.start(phone, password):
        client.loop.run_until_complete(main())
