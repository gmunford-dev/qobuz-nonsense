"""
Bot suspicion scorer for Qobuz Nonsense.
Scores Reddit accounts 0.0 (clearly human) to 1.0 (likely bot/astroturf).
"""

import re
from datetime import datetime, timezone
from difflib import SequenceMatcher


def score_account(post: dict, all_posts: list[dict]) -> tuple[float, list[str]]:
    """
    Returns (score 0.0-1.0, list of signal names).
    Inputs:
      post: the post dict being scored
      all_posts: full list of collected posts (for similarity checks)
    """
    score = 0.0
    signals = []

    age_days = post.get("author_age_days")
    karma = post.get("author_karma")
    text = (post.get("text") or "") + " " + (post.get("title") or "")
    author = post.get("author", "")
    date_str = post.get("date")
    source = post.get("source", "")

    # Only apply Reddit-specific signals to Reddit posts
    if source == "reddit":
        # New account signal
        if age_days is not None:
            if age_days < 30:
                score += 0.35
                signals.append("very_new_account")
            elif age_days < 90:
                score += 0.2
                signals.append("new_account")

        # Low karma signal
        if karma is not None:
            if karma < 10:
                score += 0.25
                signals.append("very_low_karma")
            elif karma < 100:
                score += 0.15
                signals.append("low_karma")

        # New account + low karma combo amplifier
        if age_days is not None and karma is not None:
            if age_days < 90 and karma < 50:
                score += 0.1
                signals.append("new_and_low_karma")

    # Odd posting hour (2am–6am UTC — common for scheduled bots)
    if date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if 2 <= dt.hour <= 6:
                score += 0.08
                signals.append("odd_hour_post")
        except Exception:
            pass

    # Suspiciously high text similarity to other posts (copy-paste campaigns)
    if text.strip() and len(text) > 50:
        similar_count = 0
        for other in all_posts:
            if other.get("id") == post.get("id"):
                continue
            other_text = (other.get("text") or "") + " " + (other.get("title") or "")
            if not other_text.strip():
                continue
            ratio = SequenceMatcher(None, text[:300], other_text[:300]).ratio()
            if ratio > 0.75:
                similar_count += 1
        if similar_count >= 3:
            score += 0.35
            signals.append("copy_paste_campaign")
        elif similar_count >= 1:
            score += 0.15
            signals.append("similar_to_other_posts")

    # Username pattern signals (bot-like names)
    if author:
        if re.match(r'^[A-Z][a-z]+[A-Z][a-z]+\d{3,}$', author):
            score += 0.1
            signals.append("username_pattern")
        elif re.match(r'.*\d{6,}$', author):
            score += 0.08
            signals.append("numeric_username_suffix")

    # Cap at 1.0
    score = min(score, 1.0)
    return round(score, 3), signals
