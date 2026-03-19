import asyncio
import random
import re
from pathlib import Path
from datetime import datetime
import httpx
from bs4 import BeautifulSoup
import sqlite3

# === Конфигурация ===
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

LINKS_FILE = "raw_links.txt"
DONE_FILE = Path("done_lenta.txt")
DEAD_FILE = Path("dead_links_lenta.txt")
DB_PATH = "lenta.db"
REQUEST_TIMEOUT = 15
NUM_WORKERS = 30
DELAY_RANGE = (2, 3)

client: httpx.AsyncClient


# === Загрузка ссылок из TXT файла ===
def load_urls_from_txt(filepath: str) -> list[str]:
    """
    Читает TXT файл, где каждая строка содержит одну ссылку.
    Игнорирует пустые строки и комментарии (начинающиеся с #).
    Возвращает список уникальных URL.
    """
    urls = []
    seen = set()

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                url = line.strip()
                if not url or url.startswith("#"):
                    continue

                if url not in seen:
                    urls.append(url)
                    seen.add(url)

    except FileNotFoundError:
        print(f"❌ Ошибка: Файл {filepath} не найден!")
        return []
    except Exception as e:
        print(f"❌ Ошибка при чтении файла {filepath}: {e}")
        return []

    return urls


# === Инициализация БД ===
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Создаем таблицу с минимальным набором полей
    cur.execute("""
                CREATE TABLE IF NOT EXISTS articles
                (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    published_at TEXT
                )
                """)

    cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url)
                """)

    # Опционально: индекс по дате, если планируешь сортировать статьи по времени
    cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_articles_date ON articles(published_at)
                """)
    conn.commit()
    conn.close()
    print("✅ База данных оптимизирована и готова к работе.")


# === Сохранение статьи ===
def save_article(data):
    """
    data ожидается словарем с ключами: url, title, description, published_at
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT OR IGNORE INTO articles (url, title, description, published_at)
            VALUES (?, ?, ?, ?)
        """, (
            data["url"],
            data["title"],
            data["description"],
            data["published_at"]
        ))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Ошибка БД при сохранении {data['url']}: {e}")
    finally:
        conn.close()


# === Очистка текста ===
def clean_text(html_content: str) -> str:
    soup = BeautifulSoup(html_content, "lxml")

    for tag in soup.select("img, video, audio, iframe, figure, script, style, .topic-body__content-foot"):
        tag.decompose()
    for a in soup.find_all("a"):
        text = a.get_text(strip=True)
        if text:
            a.replace_with(f" {text} ")

    paragraphs = soup.select("p.topic-body__content-text")

    texts = []
    for p in paragraphs:
        p_text = p.get_text(separator=" ",
                            strip=True)
        if p_text:
            texts.append(p_text)

    result = "\n".join(texts)
    result = re.sub(r'\s+', ' ', result).strip()

    return result

# === Парсинг даты из строки вида "15:37, 10 октября 2012" ===
def parse_lenta_date(date_str: str) -> str | None:
    ru_months = {
        "января": "01", "февраля": "02", "марта": "03", "апреля": "04",
        "мая": "05", "июня": "06", "июля": "07", "августа": "08",
        "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12"
    }
    for ru, num in ru_months.items():
        date_str = date_str.replace(ru, num)
    try:
        dt = datetime.strptime(date_str, "%H:%M, %d %m %Y")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


# === Парсинг статьи с lenta.ru ===
def parse_article(html: str, url: str) -> dict | None:
    soup = BeautifulSoup(html, "lxml")

    title_span = soup.select_one("h1.topic-body__titles > span.topic-body__title")
    if not title_span:
        print(f"  → Пропущено: нет заголовка на {url}")
        return None

    title = title_span.get_text(strip=True)
    if not title:
        return None

    content_div = soup.select_one("div.topic-body__content.js-topic-body-content")
    if not content_div:
        print(f"  → Пропущено: нет контейнера текста на {url}")
        return None

    description = clean_text(str(content_div))
    if not description:
        print(f"  → Пропущено: текст пуст на {url}")
        return None

    time_tag = soup.select_one("a.topic-header__item.topic-header__time")
    published_at = None
    if time_tag:
        date_text = time_tag.get_text(strip=True)
        published_at = parse_lenta_date(date_text)

    return {
        "url": url,
        "title": title,
        "description": description,
        "published_at": published_at
    }

# === Воркер ===
async def worker(worker_id: int, url_queue: asyncio.Queue):
    while not url_queue.empty():
        url = await url_queue.get()
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            resp = await client.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 404:
                print(f"[{worker_id}] 404 — мёртвая ссылка: {url}")
                with open(DEAD_FILE, "a", encoding="utf-8") as f:
                    f.write(url + "\n")
                url_queue.task_done()
                continue

            if resp.status_code != 200:
                print(f"[{worker_id}] HTTP {resp.status_code} — {url}")
                url_queue.task_done()
                continue

            article = parse_article(resp.text, url)
            if article:
                save_article(article)
                print(f"[{worker_id}] Сохранено: {article['title'][:40]}... {article['url']}")
            else:
                print(f"[{worker_id}] Пропущено (пусто): {url}")

            with open(DONE_FILE, "a", encoding="utf-8") as f:
                f.write(url + "\n")

        except Exception as e:
            print(f"[{worker_id}] Ошибка на {url}: {e}")

        delay = random.uniform(*DELAY_RANGE)
        await asyncio.sleep(delay)
        url_queue.task_done()


# === Главная функция ===
async def main():
    global client
    init_db()

    # Загрузка URL из XML
    all_urls = load_urls_from_txt(LINKS_FILE)
    unique_urls = list(dict.fromkeys(all_urls))
    print(f"📥 Всего уникальных URL из {LINKS_FILE}: {len(unique_urls)}")

    # Прогресс
    done = set(DONE_FILE.read_text().splitlines()) if DONE_FILE.exists() else set()
    dead = set(DEAD_FILE.read_text().splitlines()) if DEAD_FILE.exists() else set()

    to_process = [url for url in unique_urls if url not in done and url not in dead]
    print(f"⏳ К обработке: {len(to_process)} ссылок")

    if not to_process:
        print("✅ Всё уже обработано.")
        return

    # Очередь
    queue = asyncio.Queue()
    for url in to_process:
        queue.put_nowait(url)

    # Запуск
    async with httpx.AsyncClient(http2=False, timeout=REQUEST_TIMEOUT) as client:
        workers = [
            asyncio.create_task(worker(i + 1, queue))
            for i in range(NUM_WORKERS)
        ]
        await asyncio.gather(*workers)

    print("✅ Парсинг завершён.")


if __name__ == "__main__":
    asyncio.run(main())