"""
Translation module for Qobuz Nonsense.
Detects language of post text and translates non-English content to English
using Google Translate (via deep-translator, no API key required).
"""

from __future__ import annotations

import hashlib
import time

# ── Fast English detection heuristic ─────────────────────────────────────────

_EN_STOPWORDS = frozenset({
    "the", "is", "at", "which", "on", "a", "an", "and", "or", "but",
    "in", "with", "to", "for", "of", "it", "this", "that", "was", "are",
    "be", "have", "has", "had", "not", "you", "we", "they", "he", "she",
    "my", "your", "his", "her", "its", "our", "their", "from", "by",
    "as", "if", "so", "just", "about", "than", "more", "very", "can",
    "will", "do", "did", "been", "would", "could", "should", "all",
    "each", "every", "both", "few", "some", "any", "no", "other", "into",
})

# Minimum overlap to classify as English without calling the translation API
_MIN_EN_STOPWORD_HITS = 3


def _quick_is_english(text: str) -> bool:
    """Fast heuristic: if 3+ common English stopwords are present, likely English."""
    if not text:
        return True  # empty text defaults to English
    words = set(text.lower().split()[:60])
    return len(words & _EN_STOPWORDS) >= _MIN_EN_STOPWORD_HITS


# ── Translation cache (per-run, keyed by content hash) ──────────────────────

_cache: dict[str, tuple[str, str]] = {}  # hash -> (translated_text, detected_lang)


def _cache_key(text: str) -> str:
    return hashlib.md5(text.encode("utf-8", errors="ignore")).hexdigest()[:16]


# ── Core translation logic ───────────────────────────────────────────────────

# Lazy-loaded translator to avoid import cost when translation isn't needed
_translator = None
_translator_init_failed = False


def _get_translator():
    """Lazy-init GoogleTranslator from deep-translator."""
    global _translator, _translator_init_failed
    if _translator_init_failed:
        return None
    if _translator is not None:
        return _translator
    try:
        from deep_translator import GoogleTranslator
        _translator = GoogleTranslator(source="auto", target="en")
        return _translator
    except Exception as e:
        print(f"  [translate] Failed to init GoogleTranslator: {e}")
        _translator_init_failed = True
        return None


def _translate_text(text: str) -> tuple[str, str]:
    """Translate text to English. Returns (translated_text, detected_language).
    On failure returns (original_text, 'unknown').
    """
    if not text or not text.strip():
        return (text, "en")

    key = _cache_key(text)
    if key in _cache:
        return _cache[key]

    translator = _get_translator()
    if translator is None:
        return (text, "unknown")

    try:
        translated = translator.translate(text[:5000])  # Google has a ~5k char limit
        # deep-translator doesn't expose detected language directly,
        # so we detect it separately via a lightweight method
        lang = _detect_lang(text)
        result = (translated or text, lang)
        _cache[key] = result
        time.sleep(0.3)  # rate-limit: ~3 req/s keeps us well under Google's threshold
        return result
    except Exception as e:
        print(f"  [translate] Translation error: {e}")
        return (text, "unknown")


def _detect_lang(text: str) -> str:
    """Detect language code of text. Returns ISO 639-1 code or 'unknown'."""
    try:
        from deep_translator import single_detection
        lang = single_detection(text[:500], api_key="")  # uses free Google endpoint
        return lang if lang else "unknown"
    except Exception:
        # Fallback: heuristic detection based on common words
        return _heuristic_lang(text)


def _heuristic_lang(text: str) -> str:
    """Very rough language guess based on frequent word patterns."""
    lower = text.lower()

    # French markers
    fr_words = {"le", "la", "les", "des", "une", "est", "dans", "pour", "que",
                "qui", "pas", "sur", "avec", "son", "mais", "nous", "vous",
                "je", "tu", "il", "elle", "ce", "cette", "sont", "ont", "aux",
                "du", "au", "cette", "leur", "ces", "comme", "aussi", "etre",
                "avoir", "fait", "plus", "tres", "bien"}

    de_words = {"der", "die", "das", "und", "ist", "von", "mit", "den",
                "ein", "eine", "nicht", "auf", "sich", "auch", "nach",
                "wie", "bei", "oder", "nur", "noch", "aber", "kann",
                "ich", "wir", "sie", "es", "ihr", "sein", "haben", "wird"}

    es_words = {"el", "la", "los", "las", "de", "en", "que", "por", "con",
                "una", "para", "como", "pero", "del", "al", "son", "fue",
                "esta", "este", "tiene", "puede", "hay", "muy", "tambien",
                "yo", "tu", "su", "nos", "ellos", "todo", "otro", "mas"}

    pt_words = {"o", "a", "os", "as", "de", "em", "que", "para", "com",
                "uma", "por", "como", "mas", "dos", "das", "ao", "foi",
                "esta", "tem", "pode", "muito", "tambem", "eu", "seu",
                "nos", "eles", "tudo", "outro", "mais", "nao", "voce"}

    it_words = {"il", "la", "le", "di", "che", "per", "con", "una", "del",
                "della", "sono", "anche", "come", "questo", "quella", "hanno",
                "io", "noi", "loro", "suo", "suo", "molto", "tutto", "altro",
                "piu", "non", "dalla", "nella", "degli", "alle", "essere"}

    nl_words = {"de", "het", "een", "van", "en", "is", "dat", "op", "voor",
                "met", "niet", "zijn", "ook", "maar", "bij", "nog", "wordt",
                "ik", "je", "we", "ze", "hij", "dit", "kan", "meer", "wel",
                "naar", "aan", "om", "door", "dan", "alleen", "heel"}

    words = set(lower.split()[:50])

    scores = {
        "fr": len(words & fr_words),
        "de": len(words & de_words),
        "es": len(words & es_words),
        "pt": len(words & pt_words),
        "it": len(words & it_words),
        "nl": len(words & nl_words),
    }

    best_lang = max(scores, key=scores.get)
    if scores[best_lang] >= 3:
        return best_lang
    return "unknown"


# ── Public API ───────────────────────────────────────────────────────────────

def translate_post(post: dict) -> dict:
    """Detect language and translate a post's title/text to English if needed.

    Mutates the post dict in place:
    - Sets post["language"] to ISO 639-1 code ("en", "fr", "de", "es", etc.)
    - For non-English posts: saves originals in title_original/text_original,
      overwrites title/text with English translations
    - For English posts: just sets language="en", no other changes

    Returns the post dict for chaining. Never raises.
    """
    try:
        title = (post.get("title") or "").strip()
        text = (post.get("text") or "").strip()
        combined = f"{title} {text}".strip()

        # Fast path: if text looks English, skip translation entirely
        if _quick_is_english(combined):
            post["language"] = "en"
            return post

        # Detect + translate title
        if title:
            translated_title, lang = _translate_text(title)
            if lang != "en" and lang != "unknown" and translated_title != title:
                post["title_original"] = title
                post["title"] = translated_title
                post["language"] = lang
            elif lang == "en":
                post["language"] = "en"
                return post  # Was English after all
            else:
                post["language"] = lang
                return post
        else:
            # No title, try text
            if text:
                translated_text, lang = _translate_text(text)
                if lang != "en" and lang != "unknown" and translated_text != text:
                    post["text_original"] = text
                    post["text"] = translated_text
                    post["language"] = lang
                else:
                    post["language"] = lang
                return post
            else:
                post["language"] = "en"
                return post

        # Translate text body too (title was non-English)
        if text:
            translated_text, _ = _translate_text(text)
            if translated_text != text:
                post["text_original"] = text
                post["text"] = translated_text

    except Exception as e:
        print(f"  [translate] Unexpected error in translate_post: {e}")
        post.setdefault("language", "unknown")

    return post
