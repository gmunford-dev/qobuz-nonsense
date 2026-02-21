#!/usr/bin/env python3
"""
Qobuz Nonsense — Site Builder
Reads data/posts.json + data/metadata.json, injects into the HTML template,
outputs index_plain.html (which StaticCrypt then encrypts to index.html).
"""

import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
ARCHIVE_DIR = DATA_DIR / "archive"
TEMPLATE = ROOT / "templates" / "index.template.html"
OUTPUT = ROOT / "index_plain.html"


def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception as e:
            print(f"Warning: could not load {path}: {e}")
    return default


def get_archive_months() -> list[str]:
    """Return list of available archive month keys, sorted newest first."""
    months = []
    for f in ARCHIVE_DIR.glob("*.json"):
        if len(f.stem) == 7 and f.stem[4] == '-':
            months.append(f.stem)
    months.sort(reverse=True)
    # Limit to 6 most recent
    return months[:6]


def main():
    print("Building site...")

    posts = load_json(POSTS_FILE := DATA_DIR / "posts.json", [])
    metadata = load_json(DATA_DIR / "metadata.json", {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total_posts": len(posts),
        "posts_this_week": 0,
        "high_bot_suspicion_count": 0,
        "by_source": {},
        "by_narrative": {},
        "by_platform_from": {},
        "by_month": {},
    })

    archive_months = get_archive_months()

    template = TEMPLATE.read_text(encoding="utf-8")

    # Inject data as JS variables
    posts_json = json.dumps(posts, ensure_ascii=False, separators=(',', ':'))
    metadata_json = json.dumps(metadata, ensure_ascii=False, separators=(',', ':'))
    archive_json = json.dumps(archive_months, ensure_ascii=False)

    html = template.replace("__POSTS_DATA__", posts_json)
    html = html.replace("__METADATA__", metadata_json)
    html = html.replace("__ARCHIVE_MONTHS__", archive_json)

    OUTPUT.write_text(html, encoding="utf-8")
    print(f"Written: {OUTPUT}")
    print(f"  Posts embedded: {len(posts)}")
    print(f"  Archive months available: {archive_months}")
    print("Done. Run staticrypt to encrypt.")


if __name__ == "__main__":
    main()
