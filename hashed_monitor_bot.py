import os
import json
import hashlib
import time
import requests
import feedparser
import gspread
from google.oauth2.service_account import Credentials
from dateutil import parser as dateparser
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

# =========================
# Config
# =========================
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "").strip()
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "").strip()  # 채널 ID(C...) 권장
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

# 🔧 키워드 (원하면 추가 가능)
BASE_KEYWORDS = ["Hashed", "해시드"]

# Slack 폭주 방지
MAX_SLACK_ALERTS = 10

# Sheet에서 ID 읽는 row 수 제한(너무 커질 경우 대비)
SHEET_ID_LOAD_LIMIT = 8000


# =========================
# Helpers
# =========================
def normalize_url(url: str) -> str:
    """URL에서 트래킹 파라미터 등을 제거해 id 안정성 개선."""
    if not url:
        return url
    url = url.strip()
    p = urlparse(url)

    # fragment 제거
    p = p._replace(fragment="")

    # query에서 트래킹 제거
    q = parse_qsl(p.query, keep_blank_values=True)
    filtered = []
    for k, v in q:
        lk = k.lower()
        if lk.startswith("utm_"):
            continue
        if lk in ("fbclid", "gclid", "mc_cid", "mc_eid"):
            continue
        filtered.append((k, v))
    new_query = urlencode(filtered, doseq=True)
    p = p._replace(query=new_query)

    # 호스트 소문자
    p = p._replace(netloc=p.netloc.lower())

    return urlunparse(p)


def today_midnight_kst_utc() -> datetime:
    """오늘 0:00(KST)를 UTC로 변환해 반환."""
    kst = ZoneInfo("Asia/Seoul")
    now_kst = datetime.now(kst)
    midnight_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight_kst.astimezone(timezone.utc)


def make_id(source: str, url: str) -> str:
    """기사 중복 판정용 ID."""
    url = normalize_url(url)
    raw = f"{source}|{url}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


# =========================
# Slack
# =========================
def slack_headers():
    if not SLACK_BOT_TOKEN.startswith("xoxb-"):
        raise RuntimeError("SLACK_BOT_TOKEN is missing or invalid. (should start with xoxb-...)")
    return {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json; charset=utf-8"
    }


def slack_post_with_retry(payload, retries=3):
    """Slack rate limit 등을 대비해 재시도."""
    for attempt in range(1, retries + 1):
        r = requests.post("https://slack.com/api/chat.postMessage", headers=slack_headers(), json=payload, timeout=15)
        data = r.json()
        if data.get("ok"):
            return True
        err = data.get("error")
        if err == "rate_limited":
            time.sleep(2 * attempt)
            continue
        print("Slack post failed:", err)
        return False
    return False


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
    ok = slack_post_with_retry(payload)
    if not ok:
        raise RuntimeError("Slack chat.postMessage failed after retries.")


def slack_post_digest(channel_id: str, mentions: list):
    """Slack 폭주 방지: 남은 항목은 digest로 1번에 보냄."""
    if not mentions:
        return

    lines = "\n".join([f"• <{m['url']}|{m['title']}>" for m in mentions[:20]])
    extra = len(mentions) - min(len(mentions), 20)
    if extra > 0:
        lines += f"\n… and {extra} more."

    payload = {
        "channel": channel_id,
        "text": f"🧾 Hashed Mentions Digest: {len(mentions)} more",
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": f"🧾 Digest: {len(mentions)} more mentions", "emoji": True}},
            {"type": "section", "text": {"type": "mrkdwn", "text": lines}},
        ]
    }
    slack_post_with_retry(payload)


# =========================
# Google Sheet
# =========================
def get_gspread_client():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is missing.")
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("GOOGLE_SHEET_ID is missing.")
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def get_worksheets():
    """sheet1(기사 DB) + meta 시트(since 저장)"""
    gc = get_gspread_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    ws = sh.sheet1
    try:
        meta_ws = sh.worksheet("meta")
    except Exception:
        raise RuntimeError("Worksheet 'meta' not found. Please create a sheet tab named 'meta' with key/value rows.")

    return ws, meta_ws


def sheet_get_existing_ids(ws, limit=SHEET_ID_LOAD_LIMIT):
    """시트 A열(id) 읽어서 set 구성."""
    col = ws.col_values(1)  # id column
    if not col:
        return set()
    ids = col[1:]  # skip header
    if len(ids) > limit:
        ids = ids[-limit:]
    return set(ids)


def sheet_append_rows(ws, rows):
    """rows: list of [id, fetched_at, published_at, source, title, url]"""
    if not rows:
        return
    ws.append_rows(rows, value_input_option="RAW")


def meta_get_since(meta_ws):
    values = meta_ws.get_all_values()
    for row in values[1:]:
        if len(row) >= 2 and row[0] == "since":
            return row[1].strip() if row[1] else None
    return None


def meta_set_since(meta_ws, iso_time):
    values = meta_ws.get_all_values()
    for i, row in enumerate(values[1:], start=2):  # row index in sheet
        if len(row) >= 1 and row[0] == "since":
            meta_ws.update_cell(i, 2, iso_time)
            return
    meta_ws.append_row(["since", iso_time], value_input_option="RAW")


# =========================
# Sources
# =========================
def fetch_google_news_rss(query: str):
    # Google News RSS는 URL 인코딩된 쿼리로 받음
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
    """
    GDELT는 가끔 HTML 오류를 주므로 방어 + 재시도.
    실패해도 [] 반환 → 전체 봇은 계속 동작.
    """
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


# =========================
# Main
# =========================
def run():
    if not SLACK_CHANNEL:
        raise RuntimeError("SLACK_CHANNEL is missing. Use channel ID like C0123... recommended.")

    now_utc = datetime.now(timezone.utc)
    fetched_at = now_utc.isoformat()
    midnight_utc = today_midnight_kst_utc()

    # ✅ 시트 연결
    ws, meta_ws = get_worksheets()

    # ✅ since 가져오기
    since_str = meta_get_since(meta_ws)

    if not since_str:
        # ✅ 첫 실행: 지금부터 시작(오늘 기사라도 과거는 안 보냄)
        meta_set_since(meta_ws, now_utc.isoformat())
        print("First run: since initialized to now. No notifications this run.")
        return

    since_dt = dateparser.parse(since_str).astimezone(timezone.utc)
    # ✅ 오늘 0:00(KST) 이전으로는 내려가지 않게 보정
    if since_dt < midnight_utc:
        since_dt = midnight_utc

    # ✅ 쿼리 설계: Google News는 넓게, GDELT도 일단 넓게(원하면 좁혀도 됨)
    google_query = '("Hashed" OR "해시드")'
    # GDELT는 OR 괄호 필수
    gdelt_query = '("Hashed" OR "해시드")'

    # ✅ Fetch
    all_results = []
    all_results += fetch_google_news_rss(google_query)
    all_results += fetch_gdelt(gdelt_query)

    # ✅ 날짜 필터: since 이후 + 오늘 0:00(KST) 이후
    filtered = []
    for m in all_results:
        try:
            pub_dt = dateparser.parse(m["published_at"]).astimezone(timezone.utc)
        except Exception:
            pub_dt = now_utc  # published_at이 이상하면 지금으로 처리

        if pub_dt >= since_dt and pub_dt >= midnight_utc:
            filtered.append(m)

    # ✅ 중복 제거 (시트 기반)
    existing_ids = sheet_get_existing_ids(ws)
    new_mentions = []

    for m in filtered:
        m["url"] = normalize_url(m["url"])
        m_id = make_id(m["source"], m["url"])
        if m_id not in existing_ids:
            m["id"] = m_id
            m["fetched_at"] = fetched_at
            new_mentions.append(m)

    if new_mentions:
        print(f"✅ New mentions: {len(new_mentions)}")

        # ✅ 시트 저장
        rows = []
        for m in new_mentions:
            rows.append([m["id"], m["fetched_at"], m["published_at"], m["source"], m["title"], m["url"]])
        sheet_append_rows(ws, rows)

        # ✅ Slack 전송 (상위 N개는 개별, 나머지는 digest)
        to_send = new_mentions[:MAX_SLACK_ALERTS]
        remaining = new_mentions[MAX_SLACK_ALERTS:]

        for m in to_send:
            slack_post_mention(SLACK_CHANNEL, m)

        if remaining:
            slack_post_digest(SLACK_CHANNEL, remaining)

    else:
        print("No new mentions.")

    # ✅ 실행 종료 시 since 갱신 → 다음 실행은 이번 실행 이후 기사만
    meta_set_since(meta_ws, now_utc.isoformat())
    print(f"[meta] since updated to {now_utc.isoformat()}")


if __name__ == "__main__":
    run()


