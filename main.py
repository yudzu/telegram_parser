import csv
import asyncio
from environs import Env
from telethon import TelegramClient
from telethon.tl.types import ReactionEmoji
from telethon import utils

env = Env()
env.read_env()

api_id = env.int('API_ID')
api_hash = env('API_HASH')
phone = env('PHONE')
password = env('PASSWORD', None)
channel_link = env('CHANNEL_LINK')

output_file_posts = "channel_posts.csv"
output_file_comments = "channel_comments.csv"
session_name = "parser"

client = TelegramClient(session=session_name, api_id=api_id, api_hash=api_hash,
                        device_model="Samsung Galaxy S24 Ultra", system_version="Android 16", app_version="11.14.1")  # можно поменять в соответствии с выбранной платформой приложения


async def main():
    channel = await client.get_entity(channel_link)

    with (open(output_file_posts, "w", newline="", encoding="utf-8") as file_posts,
          open(output_file_comments, "w", newline="", encoding="utf-8") as file_comments):

        writer_posts = csv.writer(file_posts)
        writer_comments = csv.writer(file_comments)

        writer_posts.writerow(["post_id", "date", "text", "reactions", "comments_count"])
        writer_comments.writerow(["post_id", "comment_id", "date", "author_id", "author_username", "text"])

        async for message in client.iter_messages(channel, reverse=True):
            if message.text:
                post_id = message.id
                date = message.date.isoformat()
                text = message.text

                reactions_data = None
                if message.reactions:
                    parts = []
                    for reaction in message.reactions.results:
                        emoji = reaction.reaction.emoticon if isinstance(reaction.reaction, ReactionEmoji) else str(reaction.reaction)
                        parts.append(f"{emoji}:{reaction.count}")
                    reactions_data = "; ".join(parts)

                comments_count = message.replies.replies if message.replies else 0

                writer_posts.writerow([post_id, date, text, reactions_data, comments_count])

                if comments_count > 0:
                    async for comment in client.iter_messages(channel, reply_to=post_id, reverse=True):

                        if comment.text:
                            if comment.sender:
                                if comment.sender_id < 0:
                                    author_id, peer_type = utils.resolve_id(comment.sender_id)
                                else:
                                    author_id = comment.sender_id

                                if comment.sender.username:
                                    author_username = f"@{comment.sender.username}"
                                else:
                                    first_name = comment.sender.first_name or ""
                                    last_name = comment.sender.last_name or ""
                                    author_username = (first_name + " " + last_name).strip()
                            else:
                                author_id = channel.id
                                author_username = f"@{channel.username}" if channel.username else channel.title

                            writer_comments.writerow([
                                post_id,
                                comment.id,
                                comment.date.isoformat(),
                                author_id,
                                author_username,
                                comment.text
                            ])

                        await asyncio.sleep(0.1)  #
                                                  # для избежания FloodWaitError
                await asyncio.sleep(0.3)          #

    print(f"Посты сохранены в {output_file_posts}")
    print(f"Комментарии сохранены в {output_file_comments}")


if __name__ == "__main__":
    with client.start(phone=phone, password=password):
        client.loop.run_until_complete(main())
