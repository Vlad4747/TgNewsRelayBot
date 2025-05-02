import aiohttp
from bs4 import BeautifulSoup
import yaml
from pyrogram import Client, filters
from pyrogram.types import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
import asyncio
import logging
from collections import deque
import gc
import platform
import sys
import time
import threading
from aiohttp_socks import ProxyConnector, ProxyError
import psutil
from datetime import datetime, timezone
import sqlite3
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@contextmanager
def db_connection(db_file):
    conn = sqlite3.connect(db_file)
    try:
        yield conn
    finally:
        conn.close()

def init_db(db_file):
    with db_connection(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS news_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                link TEXT NOT NULL UNIQUE,
                timestamp REAL NOT NULL,
                source_url TEXT NOT NULL
            )
        """)
        conn.commit()

def load_config():
    try:
        with open("config.yml", "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except yaml.YAMLError as e:
        logger.error(f"Ошибка загрузки config.yml: {e}")
        raise
    except FileNotFoundError:
        logger.error("Файл config.yml не найден")
        raise

def save_config(config):
    try:
        with open("config.yml", "w", encoding="utf-8") as f:
            yaml.safe_dump(config, f, allow_unicode=True)
    except Exception as e:
        logger.error(f"Ошибка сохранения config.yml: {e}")
        raise

def load_history(db_file, max_size):
    history = deque(maxlen=max_size)
    with db_connection(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT title, link, timestamp, source_url FROM news_history ORDER BY timestamp DESC LIMIT ?", (max_size,))
        for row in cursor.fetchall():
            history.append({
                "title": row[0],
                "link": row[1],
                "timestamp": row[2],
                "source_url": row[3]
            })
    return history

def save_history(history, db_file, max_size):
    with db_connection(db_file) as conn:
        cursor = conn.cursor()
        for item in history:
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO news_history (title, link, timestamp, source_url) VALUES (?, ?, ?, ?)",
                    (item["title"], item["link"], item["timestamp"], item["source_url"])
                )
            except sqlite3.IntegrityError:
                continue
        conn.commit()
        cursor.execute("DELETE FROM news_history WHERE id NOT IN (SELECT id FROM news_history ORDER BY timestamp DESC LIMIT ?)", (max_size,))
        conn.commit()

async def parse_news(url, max_articles, proxy_config):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    try:
        required_keys = ['scheme', 'hostname', 'port', 'username', 'password']
        missing_keys = [key for key in required_keys if key not in proxy_config or not proxy_config[key]]
        if missing_keys:
            logger.error(f"Отсутствуют или пустые ключи в proxy_config: {missing_keys}")
            return []
        
        proxy_url = f"{proxy_config['scheme']}://{proxy_config['username']}:{proxy_config['password']}@{proxy_config['hostname']}:{proxy_config['port']}"
        logger.info(f"Попытка парсинга {url} с proxy_url: {proxy_url}")
        
        connector = ProxyConnector.from_url(proxy_url)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url, headers=headers, timeout=10) as response:
                response.raise_for_status()
                text = await response.text()
                soup = BeautifulSoup(text, "html.parser")
                
                news_list = []
                articles = soup.find_all("article", class_="tm-articles-list__item")[:max_articles]
                for article in articles:
                    title_tag = article.find("a", class_="tm-title__link")
                    if not title_tag:
                        continue
                    title = title_tag.text.strip()
                    link = "https://habr.com" + title_tag["href"]
                    description_tag = article.find("div", class_="tm-article-snippet__lead")
                    description = description_tag.text.strip()[:150] + "..." if description_tag else ""
                    
                    news_list.append({
                        "title": title,
                        "link": link,
                        "description": description,
                        "source_url": url
                    })
                
                logger.info(f"Спарсено {len(news_list)} новостей с {url} через прокси")
                soup.decompose()
                return news_list
    except ProxyError as e:
        logger.error(f"Ошибка прокси для {url}: {e}")
        return []
    except ValueError as e:
        logger.error(f"Ошибка значения в настройках прокси для {url}: {e}")
        return []
    except TypeError as e:
        logger.error(f"Ошибка типа в настройках прокси для {url}: {e}")
        return []
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка клиента HTTP для {url}: {e}")
        return []
    except Exception as e:
        logger.error(f"Неожиданная ошибка при парсинге {url}: {e}")
        return []
    finally:
        gc.collect()

def publish_news(app, config, history):
    published_links = {item["link"] for item in history}
    published_titles = {item["title"] for item in history}
    
    new_posts = []
    for site in config["parsing"]["sites"]:
        news_list = asyncio.run(parse_news(site["url"], site["max_articles"], config["proxy"]))
        for news in news_list:
            if news["link"] not in published_links and news["title"] not in published_titles:
                post_text = f"📰 {news['title']}\n\n{news['description']}\n\n🔗 {news['link']}"
                for channel_id in config["telegram"]["channels"]:
                    try:
                        app.send_message(
                            chat_id=channel_id,
                            text=post_text
                        )
                        logger.info(f"Новость опубликована в {channel_id}: {news['title']}")
                    except Exception as e:
                        logger.error(f"Ошибка публикации в {channel_id}: {e}")
                new_posts.append({
                    "title": news["title"],
                    "link": news["link"],
                    "timestamp": asyncio.get_event_loop().time(),
                    "source_url": news["source_url"]
                })
    
    if new_posts:
        history.extend(new_posts)
        save_history(new_posts, config["parsing"]["history_db"], config["parsing"]["max_history_size"])
    return len(new_posts)

main_menu = ReplyKeyboardMarkup(
    [
        ["/status", "/stats"],
        ["/parse", "/listposts"],
        ["/listsites", "/addsite"],
        ["/removesite", "/addchannel"],
        ["/setinterval", "/stop"],
        ["/resume"]
    ],
    resize_keyboard=True,
    is_persistent=True
)

config = load_config()
app = Client(
    "news_bot",
    api_id=config["telegram"]["api_id"],
    api_hash=config["telegram"]["api_hash"],
    bot_token=config["telegram"]["token"]
)



@app.on_message(filters.command("start") & filters.user([config["telegram"]["admin_id"]]))
async def start_command(client, message):
    logger.info(f"Получена команда /start от пользователя {message.from_user.id}, admin_id: {config['telegram']['admin_id']}")
    await message.reply(
        "👋 Привет! Я бот для парсинга новостей. Используй кнопки ниже для управления.",
        reply_markup=main_menu
    )
    logger.info(f"Пользователь {message.from_user.id} запустил бота")

@app.on_message(filters.command("status") & filters.user([config["telegram"]["admin_id"]]))
async def status_command(client, message):
    config = load_config()
    sites = "\n".join([f"- {site['url']} (max {site['max_articles']} articles)" for site in config["parsing"]["sites"]])
    channels = "\n".join([f"- {channel}" for channel in config["telegram"]["channels"]])
    status_text = (
        f"📊 **Статус бота**\n"
        f"Каналы:\n{channels}\n"
        f"Сайты для парсинга:\n{sites}\n"
        f"Интервал: {config['parsing']['interval_seconds']} секунд\n"
        f"История: {len(load_history(config['parsing']['history_db'], config['parsing']['max_history_size']))} записей\n"
        f"Прокси: {config['proxy']['hostname']}:{config['proxy']['port']}"
    )
    await message.reply(status_text, reply_markup=main_menu)

@app.on_message(filters.command("stats") & filters.user([config["telegram"]["admin_id"]]))
async def stats_command(client, message):
    config = load_config()
    history = load_history(config["parsing"]["history_db"], config["parsing"]["max_history_size"])
    
    posts_by_source = {}
    for item in history:
        source = item.get("source_url", "Unknown")
        posts_by_source[source] = posts_by_source.get(source, 0) + 1
    
    last_posts = sorted(history, key=lambda x: x["timestamp"], reverse=True)[:3]
    last_posts_text = "\n".join([
        f"- {item['title']} ({datetime.fromtimestamp(item['timestamp'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')})"
        for item in last_posts
    ]) or "Нет публикаций"
    
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024
    
    stats_text = (
        f"📈 **Статистика бота**\n"
        f"Всего публикаций: {len(history)}\n"
        f"Публикации по сайтам:\n" +
        "\n".join([f"- {source}: {count}" for source, count in posts_by_source.items()]) +
        f"\nПоследние публикации:\n{last_posts_text}\n"
        f"Интервал парсинга: {config['parsing']['interval_seconds']} секунд\n"
        f"Авто-парсинг: {'включен' if auto_parsing else 'выключен'}\n"
        f"Потребление памяти: {memory_mb:.2f} МБ"
    )
    await message.reply(stats_text, reply_markup=main_menu)

@app.on_message(filters.command("parse") & filters.user([config["telegram"]["admin_id"]]))
async def parse_command(client, message):
    config = load_config()
    history = load_history(config["parsing"]["history_db"], config["parsing"]["max_history_size"])
    count = await publish_news(client, config, history)
    await message.reply(f"📰 Парсинг завершен. Опубликовано {count} новых новостей.", reply_markup=main_menu)

@app.on_message(filters.command("listposts") & filters.user([config["telegram"]["admin_id"]]))
async def list_posts_command(client, message):
    config = load_config()
    history = load_history(config["parsing"]["history_db"], config["parsing"]["max_history_size"])
    last_posts = sorted(history, key=lambda x: x["timestamp"], reverse=True)[:5]
    posts_text = "\n\n".join([
        f"📰 {item['title']}\n🔗 {item['link']}\n📅 {datetime.fromtimestamp(item['timestamp'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}\n🌐 {item['source_url']}"
        for item in last_posts
    ]) or "Нет публикаций"
    await message.reply(f"📜 **Последние 5 публикаций**\n\n{posts_text}", reply_markup=main_menu)

@app.on_message(filters.command("listsites") & filters.user([config["telegram"]["admin_id"]]))
async def list_sites_command(client, message):
    config = load_config()
    sites = "\n".join([f"- {site['url']} (max {site['max_articles']} articles)" for site in config["parsing"]["sites"]])
    await message.reply(f"🌐 **Сайты для парсинга**\n\n{sites or 'Нет сайтов'}", reply_markup=main_menu)

@app.on_message(filters.command("addsite") & filters.user([config["telegram"]["admin_id"]]))
async def add_site_command(client, message):
    try:
        _, url, max_articles = message.text.split(maxsplit=2)
        max_articles = int(max_articles)
        if max_articles <= 0:
            raise ValueError("max_articles должно быть положительным")
        
        config = load_config()
        config["parsing"]["sites"].append({"url": url, "max_articles": max_articles})
        save_config(config)
        await message.reply(f"✅ Сайт {url} добавлен (max {max_articles} статей).", reply_markup=main_menu)
    except ValueError as e:
        await message.reply(f"❌ Ошибка: {e}. Используй: /addsite <url> <max_articles>", reply_markup=main_menu)

@app.on_message(filters.command("removesite") & filters.user([config["telegram"]["admin_id"]]))
async def remove_site_command(client, message):
    try:
        _, url = message.text.split(maxsplit=1)
        config = load_config()
        if not any(site["url"] == url for site in config["parsing"]["sites"]):
            await message.reply(f"❌ Сайт {url} не найден.", reply_markup=main_menu)
            return
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить", callback_data=f"remove_site:{url}")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
        ])
        await message.reply(f"Вы уверены, что хотите удалить сайт {url}?", reply_markup=keyboard)
    except ValueError:
        await message.reply("❌ Используй: /removesite <url>", reply_markup=main_menu)

@app.on_message(filters.command("addchannel") & filters.user([config["telegram"]["admin_id"]]))
async def add_channel_command(client, message):
    try:
        _, channel_id = message.text.split(maxsplit=1)
        channel_id = int(channel_id)
        
        try:
            chat = await client.get_chat(channel_id)
            if not (chat.permissions and chat.permissions.can_send_messages):
                await message.reply(f"❌ Бот не имеет прав на отправку сообщений в {channel_id}. Добавьте бота как администратора с правом публикации.", reply_markup=main_menu)
                return
        except Exception as e:
            await message.reply(f"❌ Ошибка проверки канала {channel_id}: {e}. Убедитесь, что бот добавлен в канал.", reply_markup=main_menu)
            return
        
        config = load_config()
        if channel_id in config["telegram"]["channels"]:
            await message.reply(f"❌ Канал {channel_id} уже добавлен.", reply_markup=main_menu)
            return
        
        config["telegram"]["channels"].append(channel_id)
        save_config(config)
        await message.reply(f"✅ Канал {channel_id} добавлен для публикации.", reply_markup=main_menu)
    except ValueError:
        await message.reply("❌ Используй: /addchannel <channel_id>", reply_markup=main_menu)

@app.on_message(filters.command("setinterval") & filters.user([config["telegram"]["admin_id"]]))
async def set_interval_command(client, message):
    try:
        _, seconds = message.text.split(maxsplit=1)
        seconds = int(seconds)
        if seconds < 60:
            raise ValueError("Интервал должен быть >= 60 секунд")
        
        config = load_config()
        config["parsing"]["interval_seconds"] = seconds
        save_config(config)
        await message.reply(f"✅ Интервал установлен: {seconds} секунд.", reply_markup=main_menu)
    except ValueError as e:
        await message.reply(f"❌ Ошибка: {e}. Используй: /setinterval <seconds>", reply_markup=main_menu)

@app.on_message(filters.command("stop") & filters.user([config["telegram"]["admin_id"]]))
async def stop_command(client, message):
    global auto_parsing
    auto_parsing = False
    await message.reply("🛑 Автоматический парсинг остановлен.", reply_markup=main_menu)

@app.on_message(filters.command("resume") & filters.user([config["telegram"]["admin_id"]]))
async def resume_command(client, message):
    global auto_parsing
    auto_parsing = True
    await message.reply("▶️ Автоматический парсинг возобновлен.", reply_markup=main_menu)

@app.on_message(filters.command("test"))
async def test_command(client, message):
    logger.info(f"Получена команда /test от пользователя {message.from_user.id}")
    await message.reply("Команда /test получена!")

@app.on_callback_query(filters.regex(r"remove_site:(.+)") & filters.user([config["telegram"]["admin_id"]]))
async def confirm_remove_site(client, callback_query):
    url = callback_query.data.split(":", 1)[1]
    config = load_config()
    config["parsing"]["sites"] = [site for site in config["parsing"]["sites"] if site["url"] != url]
    save_config(config)
    await callback_query.message.edit_text(f"✅ Сайт {url} удален.", reply_markup=None)
    await callback_query.message.reply("Вернуться в меню?", reply_markup=main_menu)

@app.on_callback_query(filters.regex(r"cancel") & filters.user([config["telegram"]["admin_id"]]))
async def cancel_action(client, callback_query):
    await callback_query.message.edit_text("❌ Действие отменено.", reply_markup=None)
    await callback_query.message.reply("Вернуться в меню?", reply_markup=main_menu)


def main():
    global auto_parsing
    auto_parsing = True
    
    init_db(config["parsing"]["history_db"])
    history = load_history(config["parsing"]["history_db"], config["parsing"]["max_history_size"])
    def loop():
        time.sleep(60)
        try:
            logger.info("Бот успешно запущен")
            while True:
                if auto_parsing:
                    publish_news(app, config, history)
                time.sleep(config["parsing"]["interval_seconds"])
        except Exception as e:
            logger.error(f"Ошибка запуска бота: {e}")
            raise
        finally:
            gc.collect()
    threading.Thread(target=loop).start()
    app.run()

if __name__ == "__main__":
    main()