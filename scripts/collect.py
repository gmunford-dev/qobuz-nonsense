#!/usr/bin/env python3
"""
Qobuz Nonsense — Data Collector
Fetches Reddit posts/comments, news articles, and X/Twitter posts
mentioning Qobuz switching campaigns.

Runs every 3 hours via GitHub Actions.
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import hashlib
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Add scripts dir to path for sibling imports
sys.path.insert(0, str(Path(__file__).parent))
from tag_narratives import tag_narratives, detect_platform_from
from bot_score import score_account

# ─── Paths ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
ARCHIVE_DIR = DATA_DIR / "archive"
POSTS_FILE = DATA_DIR / "posts.json"
METADATA_FILE = DATA_DIR / "metadata.json"

DATA_DIR.mkdir(exist_ok=True)
ARCHIVE_DIR.mkdir(exist_ok=True)

# ─── Reddit search config ─────────────────────────────────────────────────────
REDDIT_QUERIES = [
    # Pro-Qobuz / switch recommendation
    "switch to Qobuz",
    "switched to Qobuz",
    "switching to Qobuz",
    "Spotify to Qobuz",
    "Amazon to Qobuz",
    "Apple Music to Qobuz",
    "YouTube to Qobuz",
    "Tidal to Qobuz",
    "Qobuz alternative",
    "quit Spotify Qobuz",
    "leave Spotify Qobuz",
    "Qobuz better than Spotify",
    "Qobuz over Spotify",
    # Criticism / issues
    "Qobuz app not working",
    "Qobuz broken",
    "Qobuz bug",
    "Qobuz crash",
    "Qobuz missing album",
    "Qobuz catalog",
    "Qobuz customer service",
    "Qobuz refund",
    "Qobuz cancel subscription",
    "Qobuz price",
    "Qobuz problem",
    "Qobuz issue",
    "Qobuz complaint",
    "Qobuz vs Tidal",
    "Qobuz vs Apple Music",
    "leaving Qobuz",
    "switched from Qobuz",
    "cancel Qobuz",
]

REDDIT_SUBREDDITS = [
    "Music", "audiophile", "hifi", "BoycottIsrael", "degoogle",
    "TIdaL", "fantanoforever", "audiofiliabrasil", "spotify",
    "headphones", "vinyl", "letstalkmusic", "indieheads",
    "qobuz",  # official Qobuz community — prime source for complaints
]

# ─── News search config ───────────────────────────────────────────────────────
NEWS_QUERIES = [
    "Qobuz switch Spotify",
    "Qobuz alternative Spotify",
    "Qobuz streaming review",
    "Qobuz pays artists",
    "switch music streaming Qobuz",
    "Qobuz app problems",
    "Qobuz criticism",
    "Qobuz complaints users",
]

# Direct RSS feeds from music industry trades — no API key needed
DIRECT_RSS_FEEDS = [
    ("Music Business Worldwide", "https://www.musicbusinessworldwide.com/feed/"),
    ("Digital Music News", "https://www.digitalmusicnews.com/feed/"),
    ("The Ear", "https://the-ear.net/feed/"),
    ("What Hi-Fi", "https://www.whathifi.com/rss"),
    ("Stereophile", "https://www.stereophile.com/rss.xml"),
    # Hifi News removed — returns HTTP 404
]

# ─── Direction detection ──────────────────────────────────────────────────────

CRITICISM_KEYWORDS = [
    "not working", "broken", "bug", "crash", "issue", "problem",
    "complaint", "cancel", "refund", "leaving qobuz", "switched from qobuz",
    "worse than", "qobuz sucks", "disappointed", "poor customer",
    "missing album", "missing artist", "catalog gap", "no support",
    "customer service", "billing", "overpriced", "price hike",
    "too expensive", "app is bad", "app is terrible", "qobuz lacks",
]

PRO_KEYWORDS = [
    "switch to qobuz", "switched to qobuz", "switching to qobuz",
    "recommend qobuz", "qobuz is better", "moved to qobuz",
    "love qobuz", "qobuz over", "best streaming", "from spotify to qobuz",
]


def detect_direction(text: str, narratives: list[str]) -> str:
    """
    Classify whether a post is pro-Qobuz, critical of Qobuz, or neutral.
    Returns 'pro', 'critical', or 'neutral'.
    """
    if not text:
        return "neutral"
    lower = text.lower()
    is_critical = any(kw in lower for kw in CRITICISM_KEYWORDS)
    is_pro = any(kw in lower for kw in PRO_KEYWORDS) or "switch-recommendation" in narratives
    if is_critical and not is_pro:
        return "critical"
    if is_pro:
        return "pro"
    return "neutral"


# ─── Utility ──────────────────────────────────────────────────────────────────

def make_id(source: str, raw_id: str) -> str:
    return f"{source}_{raw_id}"


def load_existing() -> list[dict]:
    if POSTS_FILE.exists():
        try:
            return json.loads(POSTS_FILE.read_text())
        except Exception:
            return []
    return []


def save_posts(posts: list[dict]):
    posts.sort(key=lambda p: p.get("date", ""), reverse=True)
    POSTS_FILE.write_text(json.dumps(posts, indent=2, ensure_ascii=False))


def archive_old_posts(posts: list[dict]) -> list[dict]:
    """Move posts older than 30 days to monthly archive files. Return recent posts."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    recent = []
    by_month: dict[str, list] = {}

    for post in posts:
        date_str = post.get("date", "")
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if dt < cutoff:
                month_key = dt.strftime("%Y-%m")
                by_month.setdefault(month_key, []).append(post)
            else:
                recent.append(post)
        except Exception:
            recent.append(post)

    # Write/merge monthly archives, keep max 12 months
    for month_key, month_posts in by_month.items():
        archive_file = ARCHIVE_DIR / f"{month_key}.json"
        existing = []
        if archive_file.exists():
            try:
                existing = json.loads(archive_file.read_text())
            except Exception:
                pass
        existing_ids = {p["id"] for p in existing}
        merged = existing + [p for p in month_posts if p["id"] not in existing_ids]
        merged.sort(key=lambda p: p.get("date", ""), reverse=True)
        archive_file.write_text(json.dumps(merged, indent=2, ensure_ascii=False))
        print(f"  Archived {len(month_posts)} posts to {month_key}.json")

    # Prune archives older than 13 months
    year_ago = (datetime.now(timezone.utc) - timedelta(days=395)).strftime("%Y-%m")
    for af in ARCHIVE_DIR.glob("*.json"):
        if af.stem < year_ago:
            af.unlink()
            print(f"  Pruned old archive: {af.name}")

    return recent


def fetch_url(url: str, timeout: int = 15) -> str | None:
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; QobuzNonsense/1.0)"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"  Fetch error {url}: {e}")
        return None


# ─── Reddit ───────────────────────────────────────────────────────────────────

def collect_reddit(existing_ids: set) -> list[dict]:
    """Collect Reddit posts using PRAW."""
    try:
        import praw
    except ImportError:
        print("PRAW not installed, skipping Reddit.")
        return []

    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
    username = os.environ.get("REDDIT_USERNAME")
    password = os.environ.get("REDDIT_PASSWORD")

    if not all([client_id, client_secret, username, password]):
        print("Reddit credentials not set, skipping.")
        return []

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        username=username,
        password=password,
        user_agent="QobuzNonsense:1.0 (competitive intelligence monitor)",
        check_for_async=False,
    )

    now = datetime.now(timezone.utc).isoformat()
    posts = []
    seen_this_run = set()

    def process_submission(sub):
        pid = make_id("reddit", sub.id)
        if pid in existing_ids or pid in seen_this_run:
            return
        seen_this_run.add(pid)

        # Combined text for analysis
        full_text = (sub.title or "") + " " + (sub.selftext or "")

        # Skip if Qobuz not mentioned at all
        if "qobuz" not in full_text.lower():
            return

        try:
            author = sub.author
            age_days = None
            karma = None
            if author:
                created = getattr(author, "created_utc", None)
                if created:
                    age_days = int(
                        (datetime.now(timezone.utc) - datetime.fromtimestamp(created, timezone.utc)).days
                    )
                karma = getattr(author, "link_karma", 0) + getattr(author, "comment_karma", 0)
                author_name = str(author)
            else:
                author_name = "[deleted]"
        except Exception:
            author_name = "[unknown]"
            age_days = None
            karma = None

        narratives = tag_narratives(full_text)
        post = {
            "id": pid,
            "source": "reddit",
            "type": "post",
            "platform_from": detect_platform_from(full_text),
            "narratives": narratives,
            "direction": detect_direction(full_text, narratives),
            "url": f"https://reddit.com{sub.permalink}",
            "title": sub.title[:300],
            "text": sub.selftext[:600] if sub.selftext else "",
            "author": author_name,
            "author_age_days": age_days,
            "author_karma": karma,
            "subreddit": f"r/{sub.subreddit.display_name}",
            "date": datetime.fromtimestamp(sub.created_utc, timezone.utc).isoformat(),
            "score": sub.score,
            "num_comments": sub.num_comments,
            "discovered": now,
            "bot_score": 0.0,
            "bot_signals": [],
            "campaign_burst": False,
        }
        posts.append(post)

    def process_comment(comment, subreddit_name):
        pid = make_id("reddit_comment", comment.id)
        if pid in existing_ids or pid in seen_this_run:
            return
        seen_this_run.add(pid)

        body = getattr(comment, "body", "") or ""
        if "qobuz" not in body.lower():
            return
        if len(body) < 20:
            return

        try:
            author = comment.author
            age_days = None
            karma = None
            if author:
                created = getattr(author, "created_utc", None)
                if created:
                    age_days = int(
                        (datetime.now(timezone.utc) - datetime.fromtimestamp(created, timezone.utc)).days
                    )
                karma = getattr(author, "link_karma", 0) + getattr(author, "comment_karma", 0)
                author_name = str(author)
            else:
                author_name = "[deleted]"
        except Exception:
            author_name = "[unknown]"
            age_days = None
            karma = None

        # Try to get parent post URL
        try:
            parent_url = f"https://reddit.com/r/{subreddit_name}/comments/{comment.link_id.replace('t3_', '')}"
        except Exception:
            parent_url = f"https://reddit.com/r/{subreddit_name}"

        comment_narratives = tag_narratives(body)
        post = {
            "id": pid,
            "source": "reddit",
            "type": "comment",
            "platform_from": detect_platform_from(body),
            "narratives": comment_narratives,
            "direction": detect_direction(body, comment_narratives),
            "url": parent_url + f"/_/{comment.id}",
            "title": body[:120] + ("…" if len(body) > 120 else ""),
            "text": body[:600],
            "author": author_name,
            "author_age_days": age_days,
            "author_karma": karma,
            "subreddit": f"r/{subreddit_name}",
            "date": datetime.fromtimestamp(comment.created_utc, timezone.utc).isoformat(),
            "score": comment.score,
            "num_comments": 0,
            "discovered": now,
            "bot_score": 0.0,
            "bot_signals": [],
            "campaign_burst": False,
        }
        posts.append(post)

    print("Collecting Reddit posts...")

    # 1. Broad search across r/all
    for query in REDDIT_QUERIES:
        try:
            print(f"  Search: '{query}'")
            for sub in reddit.subreddit("all").search(
                query, sort="new", time_filter="month", limit=25
            ):
                process_submission(sub)
        except Exception as e:
            print(f"  Search error for '{query}': {e}")

    # 2. Subreddit-specific searches for Qobuz
    for sr_name in REDDIT_SUBREDDITS:
        try:
            sr = reddit.subreddit(sr_name)
            for sub in sr.search("Qobuz", sort="new", time_filter="month", limit=15):
                process_submission(sub)
            # Also grab recent comments mentioning Qobuz
            for comment in sr.comments(limit=100):
                process_comment(comment, sr_name)
        except Exception as e:
            print(f"  Subreddit r/{sr_name} error: {e}")

    # 3. Historical backfill (first run): search r/all past year
    try:
        print("  Historical backfill search...")
        for query in ["Qobuz Spotify", "switch Qobuz", "Qobuz alternative"]:
            for sub in reddit.subreddit("all").search(
                query, sort="top", time_filter="year", limit=50
            ):
                process_submission(sub)
    except Exception as e:
        print(f"  Backfill error: {e}")

    print(f"  Reddit: {len(posts)} new items found")
    return posts


# ─── News (Google News RSS) ───────────────────────────────────────────────────

def collect_news(existing_ids: set) -> list[dict]:
    """Collect news articles via Google News RSS search."""
    now = datetime.now(timezone.utc).isoformat()
    posts = []
    seen_this_run = set()

    print("Collecting news articles...")

    for query in NEWS_QUERIES:
        encoded = urllib.parse.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"

        xml_content = fetch_url(url)
        if not xml_content:
            continue

        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            print(f"  XML parse error: {e}")
            continue

        channel = root.find("channel")
        if channel is None:
            continue

        for item in channel.findall("item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            description = (item.findtext("description") or "").strip()
            source_elem = item.find("source")
            source_name = source_elem.text if source_elem is not None else "Unknown"

            full_text = title + " " + description

            # Must mention Qobuz
            if "qobuz" not in full_text.lower():
                continue

            # Make stable ID from URL
            pid = make_id("news", hashlib.md5(link.encode()).hexdigest()[:12])
            if pid in existing_ids or pid in seen_this_run:
                continue
            seen_this_run.add(pid)

            # Parse date
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(pub_date)
                date_iso = dt.isoformat()
            except Exception:
                date_iso = now

            news_narratives = tag_narratives(full_text)
            post = {
                "id": pid,
                "source": "news",
                "type": "article",
                "platform_from": detect_platform_from(full_text),
                "narratives": news_narratives,
                "direction": detect_direction(full_text, news_narratives),
                "url": link,
                "title": title[:300],
                "text": description[:600],
                "author": source_name,
                "author_age_days": None,
                "author_karma": None,
                "subreddit": None,
                "date": date_iso,
                "score": 0,
                "num_comments": 0,
                "discovered": now,
                "bot_score": 0.0,
                "bot_signals": [],
            }
            posts.append(post)

    # ── Direct RSS feeds from music industry trades ──
    from email.utils import parsedate_to_datetime as _parse_rfc_date
    for feed_name, feed_url in DIRECT_RSS_FEEDS:
        print(f"  RSS: {feed_name}")
        xml_content = fetch_url(feed_url)
        if not xml_content:
            continue
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError:
            continue
        channel = root.find("channel")
        if channel is None:
            continue
        for item in channel.findall("item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            description = (item.findtext("description") or "").strip()
            full_text = title + " " + description
            if "qobuz" not in full_text.lower():
                continue
            pid = make_id("news", hashlib.md5(link.encode()).hexdigest()[:12])
            if pid in existing_ids or pid in seen_this_run:
                continue
            seen_this_run.add(pid)
            try:
                dt = _parse_rfc_date(pub_date)
                date_iso = dt.isoformat()
            except Exception:
                date_iso = now
            rss_narratives = tag_narratives(full_text)
            post = {
                "id": pid,
                "source": "news",
                "type": "article",
                "platform_from": detect_platform_from(full_text),
                "narratives": rss_narratives,
                "direction": detect_direction(full_text, rss_narratives),
                "url": link,
                "title": title[:300],
                "text": description[:600],
                "author": feed_name,
                "author_age_days": None,
                "author_karma": None,
                "subreddit": None,
                "date": date_iso,
                "score": 0,
                "num_comments": 0,
                "discovered": now,
                "bot_score": 0.0,
                "bot_signals": [],
                "campaign_burst": False,
            }
            posts.append(post)

    print(f"  News: {len(posts)} new articles found")
    return posts


# ─── X / Twitter ─────────────────────────────────────────────────────────────

def collect_twitter(existing_ids: set) -> list[dict]:
    """X/Twitter collection disabled.

    snscrape was removed — it uses the find_module() import API that was
    removed in Python 3.12 (AttributeError at import time). The library is
    also unmaintained since Twitter/X closed its public API in 2023.

    To re-enable: implement via the official X API v2 (requires Bearer token
    in TWITTER_BEARER_TOKEN secret) using the requests library.
    """
    print("  X/Twitter: skipped (snscrape removed — see collect.py for details)")
    return []


# ─── Reddit (public JSON API — no auth required) ──────────────────────────────

def _process_reddit_public_post(p: dict, posts: list, seen: set,
                                 existing_ids: set, now: str):
    """Parse one post dict from Reddit's public JSON API and append to posts."""
    raw_id = p.get("id", "")
    if not raw_id:
        return

    pid = make_id("reddit", raw_id)
    if pid in existing_ids or pid in seen:
        return
    seen.add(pid)

    title = (p.get("title") or "").strip()
    body = (p.get("selftext") or "").strip()
    full_text = title + " " + body

    # Skip if Qobuz not mentioned (catches noise from broad subreddit scans)
    if "qobuz" not in full_text.lower():
        return

    # Skip deleted/removed posts
    if body in ("[deleted]", "[removed]"):
        body = ""
        full_text = title

    author_name = str(p.get("author") or "[deleted]")
    subreddit_name = p.get("subreddit_name_prefixed") or f"r/{p.get('subreddit', '?')}"

    created_utc = p.get("created_utc") or p.get("created", 0)
    try:
        date_iso = datetime.fromtimestamp(float(created_utc), timezone.utc).isoformat()
    except Exception:
        date_iso = now

    permalink = p.get("permalink", "")
    url = f"https://reddit.com{permalink}" if permalink else f"https://reddit.com/r/{p.get('subreddit', '')}"

    narratives = tag_narratives(full_text)
    post = {
        "id": pid,
        "source": "reddit",
        "type": "post",
        "platform_from": detect_platform_from(full_text),
        "narratives": narratives,
        "direction": detect_direction(full_text, narratives),
        "url": url,
        "title": title[:300],
        "text": body[:600],
        "author": author_name,
        "author_age_days": None,   # not available from public API
        "author_karma": None,      # not available from public API
        "subreddit": subreddit_name,
        "date": date_iso,
        "score": int(p.get("score") or 0),
        "num_comments": int(p.get("num_comments") or 0),
        "discovered": now,
        "bot_score": 0.0,
        "bot_signals": [],
        "campaign_burst": False,
    }
    posts.append(post)


def collect_reddit_public(existing_ids: set) -> list[dict]:
    """Collect Reddit posts via the public JSON API (no credentials required).

    Used as fallback when PRAW OAuth secrets are not configured.
    Rate-limited to ~1 req/s to stay well within Reddit's anonymous limits.
    """
    BASE = "https://www.reddit.com"
    HEADERS = {"User-Agent": "qobuz-nonsense-monitor/1.0 (github.com/gmunford-dev/qobuz-nonsense)"}
    posts: list[dict] = []
    seen: set[str] = set()
    now = datetime.now(timezone.utc).isoformat()

    def fetch_reddit_json(url: str) -> dict | None:
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode("utf-8", errors="ignore"))
        except Exception as e:
            print(f"    Reddit public fetch error ({url[:60]}…): {e}")
            return None

    print("Collecting Reddit posts (public API — no auth)...")

    # 1. Cross-Reddit keyword searches (first 20 queries to avoid rate limits)
    for query in REDDIT_QUERIES[:20]:
        encoded = urllib.parse.quote(query)
        url = f"{BASE}/search.json?q={encoded}&sort=new&t=month&limit=25&type=link"
        data = fetch_reddit_json(url)
        if data:
            for child in data.get("data", {}).get("children", []):
                _process_reddit_public_post(child.get("data", {}), posts, seen, existing_ids, now)
        time.sleep(1.1)  # ~54 req/min max; Reddit anonymous limit is ~60/min

    # 2. Subreddit-targeted scans — search for "qobuz" within each subreddit
    for sr_name in REDDIT_SUBREDDITS:
        url = f"{BASE}/r/{sr_name}/search.json?q=qobuz&sort=new&t=month&limit=25&restrict_sr=1"
        data = fetch_reddit_json(url)
        if data:
            for child in data.get("data", {}).get("children", []):
                _process_reddit_public_post(child.get("data", {}), posts, seen, existing_ids, now)
        time.sleep(1.1)

    print(f"  Reddit (public API): {len(posts)} new posts found")
    return posts


# ─── Burst Detection ─────────────────────────────────────────────────────────

def detect_campaign_bursts(all_posts: list[dict]) -> list[dict]:
    """
    Find coordinated burst campaigns: 3+ posts with the same primary narrative,
    bot_score >= 0.4, all within a 72-hour window.
    Marks matching posts with campaign_burst=True.
    Returns list of burst event dicts for metadata.
    """
    WINDOW_HOURS = 72
    MIN_POSTS = 3
    BOT_THRESHOLD = 0.4

    # Candidate posts: has narratives, bot_score >= threshold, has a date
    candidates = [
        p for p in all_posts
        if p.get("narratives") and p.get("bot_score", 0) >= BOT_THRESHOLD and p.get("date")
    ]

    # Parse dates once
    dated = []
    for p in candidates:
        try:
            dt = datetime.fromisoformat(p["date"].replace("Z", "+00:00"))
            dated.append((dt, p))
        except Exception:
            pass

    dated.sort(key=lambda x: x[0])

    bursts = []
    all_narratives = set()
    for _, p in dated:
        for n in p.get("narratives", []):
            all_narratives.add(n)

    for narrative in all_narratives:
        # Posts for this narrative
        narr_posts = [(dt, p) for dt, p in dated if narrative in p.get("narratives", [])]
        if len(narr_posts) < MIN_POSTS:
            continue

        # Sliding window
        for i in range(len(narr_posts)):
            start_dt = narr_posts[i][0]
            end_dt = start_dt + timedelta(hours=WINDOW_HOURS)
            window = [(dt, p) for dt, p in narr_posts if start_dt <= dt <= end_dt]
            if len(window) >= MIN_POSTS:
                burst_ids = {p["id"] for _, p in window}
                # Check not already recorded
                already = any(
                    b["narrative"] == narrative and
                    b["start"] == start_dt.isoformat()
                    for b in bursts
                )
                if not already:
                    # Mark posts
                    for p in all_posts:
                        if p.get("id") in burst_ids:
                            p["campaign_burst"] = True
                    actual_end = max(dt for dt, _ in window)
                    hours_span = round((actual_end - start_dt).total_seconds() / 3600, 1)
                    bursts.append({
                        "narrative": narrative,
                        "count": len(window),
                        "start": start_dt.isoformat(),
                        "end": actual_end.isoformat(),
                        "hours_span": hours_span,
                    })
                break  # Move to next narrative once first burst window found

    if bursts:
        print(f"  Detected {len(bursts)} coordinated burst(s):")
        for b in bursts:
            print(f"    {b['narrative']}: {b['count']} posts over {b['hours_span']}h")
    return bursts


# ─── Metadata ─────────────────────────────────────────────────────────────────

def build_metadata(all_posts: list[dict]) -> dict:
    now = datetime.now(timezone.utc).isoformat()

    # Count by source
    by_source: dict[str, int] = {}
    by_narrative: dict[str, int] = {}
    by_platform: dict[str, int] = {}
    by_month: dict[str, int] = {}
    by_direction: dict[str, int] = {"pro": 0, "critical": 0, "neutral": 0}
    by_direction_by_month: dict[str, dict[str, int]] = {}

    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    posts_this_week = 0

    for p in all_posts:
        src = p.get("source", "unknown")
        by_source[src] = by_source.get(src, 0) + 1

        for n in p.get("narratives", []):
            by_narrative[n] = by_narrative.get(n, 0) + 1

        pf = p.get("platform_from", "generic")
        by_platform[pf] = by_platform.get(pf, 0) + 1

        date_str = p.get("date", "")
        if date_str:
            month = date_str[:7]
            by_month[month] = by_month.get(month, 0) + 1

        if date_str and date_str >= week_ago:
            posts_this_week += 1

        direction = p.get("direction", "neutral")
        by_direction[direction] = by_direction.get(direction, 0) + 1
        if date_str:
            month = date_str[:7]
            if month not in by_direction_by_month:
                by_direction_by_month[month] = {"pro": 0, "critical": 0, "neutral": 0}
            by_direction_by_month[month][direction] = by_direction_by_month[month].get(direction, 0) + 1

    high_bot = [p for p in all_posts if p.get("bot_score", 0) >= 0.6]

    return {
        "last_updated": now,
        "total_posts": len(all_posts),
        "posts_this_week": posts_this_week,
        "high_bot_suspicion_count": len(high_bot),
        "by_source": by_source,
        "by_narrative": dict(sorted(by_narrative.items(), key=lambda x: -x[1])),
        "by_platform_from": by_platform,
        "by_month": dict(sorted(by_month.items(), reverse=True)),
        "by_direction": by_direction,
        "by_direction_by_month": dict(sorted(by_direction_by_month.items(), reverse=True)),
        "detected_bursts": [],  # populated by detect_campaign_bursts()
    }


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=== Qobuz Nonsense Collector ===")
    print(f"Run time: {datetime.now(timezone.utc).isoformat()}")

    # Load existing data
    existing = load_existing()
    existing_ids = {p["id"] for p in existing}
    print(f"Existing posts: {len(existing)}")

    # Collect new posts from all sources
    new_posts = []
    reddit_creds = all([
        os.getenv("REDDIT_CLIENT_ID"),
        os.getenv("REDDIT_CLIENT_SECRET"),
        os.getenv("REDDIT_USERNAME"),
        os.getenv("REDDIT_PASSWORD"),
    ])
    if reddit_creds:
        new_posts += collect_reddit(existing_ids)          # authenticated PRAW
    else:
        print("Reddit OAuth credentials not set — using public API fallback.")
        new_posts += collect_reddit_public(existing_ids)   # no-auth fallback
    new_posts += collect_news(existing_ids)
    new_posts += collect_twitter(existing_ids)

    print(f"\nNew posts collected: {len(new_posts)}")

    # Merge with existing
    all_posts = existing + new_posts

    # Score bots on newly collected posts (pass full list for similarity checks)
    for post in new_posts:
        score, signals = score_account(post, all_posts)
        post["bot_score"] = score
        post["bot_signals"] = signals

    # Detect coordinated bursts (marks posts in-place)
    print("\nDetecting campaign bursts...")
    bursts = detect_campaign_bursts(all_posts)

    # Archive old posts and keep recent
    print("\nArchiving old posts...")
    recent_posts = archive_old_posts(all_posts)

    # Save recent posts
    save_posts(recent_posts)
    print(f"Saved {len(recent_posts)} recent posts to posts.json")

    # Build and save metadata (includes archive months in stats)
    metadata = build_metadata(all_posts)
    metadata["detected_bursts"] = bursts
    METADATA_FILE.write_text(json.dumps(metadata, indent=2, ensure_ascii=False))

    # Print summary
    print("\n=== Summary ===")
    print(f"Total tracked: {metadata['total_posts']}")
    print(f"This week: {metadata['posts_this_week']}")
    print(f"By source: {metadata['by_source']}")
    print(f"Top narratives: {list(metadata['by_narrative'].items())[:5]}")
    print("Done.")


if __name__ == "__main__":
    main()
