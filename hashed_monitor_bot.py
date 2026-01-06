import os
import json
import hashlib
import requests
import feedparser
import gspread
from google.oauth2.service_account import Credentials
from dateutil import parser as dateparser
from datetime import datetime, timezone

KEYWORDS = ["Hashed", "해시드"]

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "").strip()
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "").strip()   # 채널 ID(C...) 권장
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

# ===== Slack =====
def slack_headers():
    if not SLACK_BOT_TOKEN.startswith("xoxb-"):
        raise RuntimeError("SLACK_BOT_TOKEN is missing or invalid. (should start with xoxb-...)")
    return {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json; charset=utf-8"
    }

def slack_post_mention(channel_id: str, mention: dict):
    title = mention["title"]
    url = mention["url"]
    source = mention["source"]
    published = mention["published_at"]

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "🟣 Hashed Mentions Alert", "emoji": True}},
        {"type": "divider"},
        {"type": "section",
         "text": {"type": "mrkdwn",
                  "text": f"*<{url}|{title}>*\n\n*Source:* `{source}`\n*Published:* `{published}`"}},
        {"type": "divider"},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": "자동 모니터링 봇 (Google News RSS + GDELT)"}]}
    ]

    payload = {"channel": channel_id, "text": f"[{source}] {title}", "blocks": blocks}
    r = requests.post("https://slack.com/api/chat.postMessage", headers=slack_headers(), json=payload, timeout=15)
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack chat.postMessage error: {data.get('error')}")

# ===== Google Sheet (DB) =====
def get_sheet():
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("GOOGLE_SHEET_ID is missing.")
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is missing.")

    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = sh.sheet1
    return ws

def sheet_get_existing_ids(ws, limit=5000):
    """
    시트의 id 컬럼(A열)을 읽어서 이미 본 기사 id set 생성
    limit: 너무 커질 경우를 대비한 보호장치(최근 N개만 확인)
    """
    # A열 전체 가져오기 (첫 row는 header)
    col = ws.col_values(1)  # id column
    if not col:
        return set()
    ids = col[1:]  # skip header
    if len(ids) > limit:
        ids = ids[-limit:]
    return set(ids)

def sheet_append_rows(ws, rows):
    """
    rows: list of [id, fetched_at, published_at, source, title, url]
    """
    if not rows:
        return
    ws.append_rows(rows, value_input_option="RAW")

# ===== ID 생성 =====
def make_id(source, url):
    raw = f"{source}|{url}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

# ===== Sources =====
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

def fetch_gdelt(query: str, max_records=50, retries=3):
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": max_records,
        "sort": "HybridRel"
    }
    headers = {"User-Agent": "HashedMonitorBot/1.0"}

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=20)

            if r.status_code != 200:
                last_err = f"GDELT HTTP {r.status_code}"
                print(f"[GDELT] attempt {attempt}/{retries} failed: {last_err}")
                continue

            if not r.text or len(r.text.strip()) == 0:
                last_err = "GDELT empty response"
                print(f"[GDELT] attempt {attempt}/{retries} failed: {last_err}")
                continue

            ctype = r.headers.get("Content-Type", "")
            if "application/json" not in ctype:
                last_err = f"GDELT non-json content-type: {ctype}"
                print(f"[GDELT] attempt {attempt}/{retries} failed: {last_err}")
                print("[GDELT] response head:", r.text[:200])
                continue

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

        except Exception as e:
            last_err = str(e)
            print(f"[GDELT] attempt {attempt}/{retries} exception: {last_err}")

    print(f"[GDELT] giving up after {retries} attempts. last_err={last_err}")
    return []

# ===== Main =====
def run():
    fetched_at = datetime.now(timezone.utc).isoformat()

    if not SLACK_CHANNEL:
        raise RuntimeError("SLACK_CHANNEL is missing. Use channel ID like C0123... recommended.")

    # ✅ OR 괄호 포함 (GDELT 문법)
    combined_query = "(" + " OR ".join([f'"{k}"' for k in KEYWORDS]) + ")"

    # 1) Fetch
    all_results = []
    all_results += fetch_google_news_rss(combined_query)
    all_results += fetch_gdelt(combined_query)

    # 2) Connect sheet
    ws = get_sheet()

    # 3) existing IDs
    existing_ids = sheet_get_existing_ids(ws)
    print(f"[Sheet] existing ids loaded: {len(existing_ids)}")

    # 4) filter new
    new_mentions = []
    for m in all_results:
        m_id = make_id(m["source"], m["url"])
        m["id"] = m_id
        m["fetched_at"] = fetched_at
        if m_id not in existing_ids:
            new_mentions.append(m)

    # 5) append to sheet + notify slack
    if new_mentions:
        print(f"✅ New mentions: {len(new_mentions)}")

        rows = []
        for m in new_mentions:
            rows.append([
                m["id"],
                m["fetched_at"],
                m["published_at"],
                m["source"],
                m["title"],
                m["url"],
            ])

        # ✅ 먼저 시트 저장
        sheet_append_rows(ws, rows)

        # ✅ Slack 전송 (폭주 방지: 최대 10개)
        for m in new_mentions[:20]:
            slack_post_mention(SLACK_CHANNEL, m)

    else:
        print("No new mentions.")

if __name__ == "__main__":
    run()

