import os
import json
import hashlib
import time
import requests
import feedparser
import gspread
from google.oauth2.service_account import Credentials
from dateutil import parser as dateparser
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode


# ============================================================
# CONFIG
# ============================================================
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "").strip()
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "").strip()  # 채널 ID(C...) 권장

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

# (필요하면 키워드 확장 가능)
BASE_KEYWORDS = ["Hashed", "해시드"]

# Slack 폭주 방지
MAX_SLACK_ALERTS = 10

# 시트에서 이미 본 ID 읽어오는 최대 개수 (너무 커질 경우 대비)
SHEET_ID_LOAD_LIMIT = 8000

# 오래된 기사 방지 안전장치 (보험)
MAX_LOOKBACK_DAYS = 7

# ============================================================
# UTILS
# ============================================================
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


def safe_parse_dt(value: str):
    """
    published_at 파싱.
    IMPORTANT:
      - 파싱 실패하면 None 반환 (절대 now로 대체하지 않음)
      - 이게 2023년 기사(혹은 이상한 기사)가 새 기사로 판정되는 버그를 막음
    """
    if not value:
        return None
    try:
        dt = dateparser.parse(value)
        if not dt:
            return None
        if not dt.tzinfo:
            # timezone 정보가 없으면 UTC로 간주
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


# ============================================================
# SLACK
# ============================================================
def slack_headers():
    if not SLACK_BOT_TOKEN.startswith("xoxb-"):
        raise RuntimeError("SLACK_BOT_TOKEN is missing or invalid. (should start with xoxb-...)")
    return {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json; charset=utf-8"
    }


def slack_post_with_retry(payload, retries=3):
    """Slack rate limit 대비 재시도."""
    for attempt in range(1, retries + 1):
        r = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers=slack_headers(),
            json=payload,
            timeout=15
        )
        data = r.json()
        if data.get("ok"):
            return True

        err = data.get("error")
        if err == "rate_limited":
            time.sleep(2 * attempt)
            continue

        print("[Slack] post failed:", err)
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
    """Slack 폭주 방지: 남은 항목은 digest 1번으로 요약."""
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


# ============================================================
# GOOGLE SHEETS (DB)
# ============================================================
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
    """sheet1(기사 저장) + meta(since 저장)"""
    gc = get_gspread_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    ws = sh.sheet1
    try:
        meta_ws = sh.worksheet("meta")
    except Exception:
        raise RuntimeError("Worksheet 'meta' not found. Please create a sheet tab named 'meta' with key/value rows.")
    return ws, meta_ws


def sheet_get_existing_ids(ws, limit=SHEET_ID_LOAD_LIMIT):
    """A열(id)을 읽어서 이미 본 기사 set 구성."""
    col = ws.col_values(1)
    if not col:
        return set()
    ids = col[1:]  # header 제외
    if len(ids) > limit:
        ids = ids[-limit:]
    return set(ids)


def sheet_append_rows(ws, rows):
    """rows: [id, fetched_at, published_at, source, title, url]"""
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
    for i, row in enumerate(values[1:], start=2):
        if len(row) >= 1 and row[0] == "since":
            meta_ws.update_cell(i, 2, iso_time)
            return
    meta_ws.append_row(["since", iso_time], value_input_option="RAW")


# ============================================================
# SOURCES
# ============================================================
def fetch_google_news_rss(query: str):
    url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=ko&gl=KR&ceid=KR:ko"
    feed = feedparser.parse(url)

    results = []
    for entry in feed.entries:
        title = entry.title
        link = entry.link
        published = entry.get("published")

        published_dt = safe_parse_dt(published) if published else None
        if not published_dt:
            # ✅ published_at이 없으면 이 소스에서는 스킵하는게 안전
            # (오래된 기사/깨진 기사들이 now로 간주되는 문제 방지)
            continue

        results.append({
            "source": "GoogleNewsRSS",
            "title": title,
            "url": link,
            "published_at": published_dt.isoformat(),
        })
    return results


def fetch_gdelt(query: str, max_records=50, retries=3):
    """
    GDELT는 HTML 오류를 주기도 하므로 방어 + 재시도.
    실패해도 [] 반환(전체 봇은 계속 동작).
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
                seendate = item.get("seendate")  # yyyymmddHHMMSS

                published_dt = None
                if seendate:
                    try:
                        published_dt = datetime.strptime(seendate, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                    except Exception:
                        published_dt = None

                # ✅ 파싱 실패하면 스킵 (now 대체 절대 금지)
                if not published_dt:
                    continue

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


# ============================================================
# MAIN
# ============================================================
def run():
    if not SLACK_CHANNEL:
        raise RuntimeError("SLACK_CHANNEL is missing. Use channel ID like C0123... recommended.")

    now_utc = datetime.now(timezone.utc)
    fetched_at = now_utc.isoformat()

    midnight_utc = today_midnight_kst_utc()
    too_old_cutoff = now_utc - timedelta(days=MAX_LOOKBACK_DAYS)
    future_cutoff = now_utc + timedelta(minutes=5)

    # 1) Sheet 연결
    ws, meta_ws = get_worksheets()

    # 2) since 읽기
    since_str = meta_get_since(meta_ws)

    if not since_str:
        # ✅ 첫 실행: '지금부터 시작'
        meta_set_since(meta_ws, now_utc.isoformat())
        print("First run: since initialized to now. No notifications this run.")
        return

    since_dt = safe_parse_dt(since_str)
    if not since_dt:
        # meta since가 깨진 경우에도 안전하게 now로 리셋
        meta_set_since(meta_ws, now_utc.isoformat())
        print("since value invalid → reset to now, skipping this run.")
        return

    # ✅ 오늘 0:00(KST) 이전은 무조건 방지
    if since_dt < midnight_utc:
        since_dt = midnight_utc

    # 3) Query (GDELT OR 괄호 규칙 준수)
    google_query = '("Hashed" OR "해시드")'
    gdelt_query = '("Hashed" OR "해시드")'

    # 4) Fetch
    all_results = []
    all_results += fetch_google_news_rss(google_query)
    all_results += fetch_gdelt(gdelt_query)

    # 5) 날짜 필터 (여기가 이번 문제의 핵심)
    filtered = []
    for m in all_results:
        pub_dt = safe_parse_dt(m.get("published_at"))

        # ✅ 파싱 실패한 건 절대 now로 처리하지 말고 스킵
        if not pub_dt:
            print("[WARN] published_at parse failed → skip:", m.get("source"), m.get("title"))
            continue

        # ✅ 이상하게 오래된 기사/미래 기사 보험
        if pub_dt < too_old_cutoff:
            # 예: 2023년 기사 같은 것들 강제 차단
            continue
        if pub_dt > future_cutoff:
            continue

        # ✅ “오늘 0:00 이후” + “since 이후”만
        if pub_dt >= midnight_utc and pub_dt >= since_dt:
            filtered.append(m)

    # 6) 시트 기반 중복 제거
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

        # ✅ 시트 저장 먼저
        rows = []
        for m in new_mentions:
            rows.append([m["id"], m["fetched_at"], m["published_at"], m["source"], m["title"], m["url"]])
        sheet_append_rows(ws, rows)

        # ✅ Slack 전송 (상위 N개 개별 + 나머지 digest)
        to_send = new_mentions[:MAX_SLACK_ALERTS]
        remaining = new_mentions[MAX_SLACK_ALERTS:]

        for m in to_send:
            slack_post_mention(SLACK_CHANNEL, m)

        if remaining:
            slack_post_digest(SLACK_CHANNEL, remaining)

    else:
        print("No new mentions.")

    # 7) since 갱신: 다음 실행은 이번 실행 이후 기사만
    meta_set_since(meta_ws, now_utc.isoformat())
    print(f"[meta] since updated to {now_utc.isoformat()}")


if __name__ == "__main__":
    run()


