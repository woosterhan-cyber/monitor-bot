import os
import sqlite3
import hashlib
import requests
import feedparser
from dateutil import parser as dateparser
from datetime import datetime, timezone

DB_PATH = "hashed_mentions.db"
KEYWORDS = ['Hashed', '해시드']

# 환경변수로 받는 걸 추천 (로컬에선 .env 써도 되고)
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "").strip()   # xoxb-...
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "").strip()       # 예: media-monitoring (또는 C0123...)
# 예: SLACK_CHANNEL="media-monitoring" 또는 "C0123456789"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mentions (
            id TEXT PRIMARY KEY,
            source TEXT,
            title TEXT,
            url TEXT,
            published_at TEXT,
            fetched_at TEXT
        )
    """)
    conn.commit()
    conn.close()

def make_id(source, url):
    raw = f"{source}|{url}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

def already_exists(mention_id):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM mentions WHERE id = ?", (mention_id,))
    exists = cur.fetchone() is not None
    conn.close()
    return exists

def save_mention(m):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO mentions (id, source, title, url, published_at, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (m["id"], m["source"], m["title"], m["url"], m["published_at"], m["fetched_at"]))
    conn.commit()
    conn.close()

# -------------------------
# Slack Bot API helpers
# -------------------------
def slack_headers():
    if not SLACK_BOT_TOKEN.startswith("xoxb-"):
        raise RuntimeError("SLACK_BOT_TOKEN is missing or invalid. (should start with xoxb-...)")
    return {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json; charset=utf-8"
    }

def resolve_channel_id(channel_or_id: str) -> str:
    """
    Accepts a channel ID like C0123... or a channel name like 'media-monitoring'
    Returns channel ID.
    """
    if not channel_or_id:
        raise RuntimeError("SLACK_CHANNEL is missing. Set env SLACK_CHANNEL to channel name or ID.")

    # already channel ID
    if channel_or_id.startswith("C") or channel_or_id.startswith("G"):
        return channel_or_id

    # otherwise treat as channel name (without '#')
    target_name = channel_or_id.lstrip("#")

    # conversations.list requires channels:read / groups:read scopes depending on channel types
    cursor = None
    for _ in range(10):
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        r = requests.get("https://slack.com/api/conversations.list", headers=slack_headers(), params=params, timeout=15)
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(f"Slack conversations.list error: {data.get('error')}")

        for ch in data.get("channels", []):
            if ch.get("name") == target_name:
                return ch.get("id")

        cursor = data.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    raise RuntimeError(f"Channel name not found: {target_name}. Make sure bot has access and scopes.")

def slack_post_mention(channel_id: str, mention: dict):
    title = mention["title"]
    url = mention["url"]
    source = mention["source"]
    published = mention["published_at"]

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "🟣 Hashed Mentions Alert", "emoji": True}},
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*<{url}|{title}>*\n\n*Source:* `{source}`\n*Published:* `{published}`"}
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "자동 모니터링 봇 (Google News RSS + GDELT)"}]
        }
    ]

    payload = {
        "channel": channel_id,
        "text": f"[{source}] {title}",
        "blocks": blocks
    }

    r = requests.post("https://slack.com/api/chat.postMessage", headers=slack_headers(), json=payload, timeout=15)
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack chat.postMessage error: {data.get('error')}")

# -------------------------
# Sources
# -------------------------
def fetch_google_news_rss(query: str):
    url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=ko&gl=KR&ceid=KR:ko"
    feed = feedparser.parse(url)

    results = []
    for entry in feed.entries:
        title = entry.title
        link = entry.link
        published = entry.get("published")
        if published:
            published_dt = dateparser.parse(published).astimezone(timezone.utc)
        else:
            published_dt = datetime.now(timezone.utc)

        results.append({
            "source": "GoogleNewsRSS",
            "title": title,
            "url": link,
            "published_at": published_dt.isoformat(),
        })
    return results

def fetch_gdelt(query: str, max_records=50):
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": max_records,
        "sort": "HybridRel"
    }

    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()

    results = []
    for item in data.get("articles", []):
        title = item.get("title", "")
        link = item.get("url", "")
        seendate = item.get("seendate")
        if seendate:
            published_dt = datetime.strptime(seendate, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        else:
            published_dt = datetime.now(timezone.utc)

        results.append({
            "source": "GDELT",
            "title": title,
            "url": link,
            "published_at": published_dt.isoformat(),
        })
    return results

# -------------------------
# Main
# -------------------------
def run():
    init_db()
    fetched_at = datetime.now(timezone.utc).isoformat()

    combined_query = " OR ".join([f'"{k}"' for k in KEYWORDS])

    all_results = []
    all_results += fetch_google_news_rss(combined_query)
    all_results += fetch_gdelt(combined_query)

    channel_id = resolve_channel_id(SLACK_CHANNEL)

    new_mentions = []
    for m in all_results:
        m["id"] = make_id(m["source"], m["url"])
        m["fetched_at"] = fetched_at

        if not already_exists(m["id"]):
            save_mention(m)
            new_mentions.append(m)

    if new_mentions:
        print(f"✅ New mentions: {len(new_mentions)}")
        # 폭주 방지: 한 번에 최대 10개만 전송
        for m in new_mentions[:10]:
            print(f"[{m['source']}] {m['title']} - {m['url']}")
            slack_post_mention(channel_id, m)
    else:
        print("No new mentions.")

if __name__ == "__main__":
    run()
