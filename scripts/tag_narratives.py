"""
Narrative tagger for Qobuz Nonsense.
Classifies posts/comments by the marketing narrative being used.
"""

NARRATIVE_RULES = {
    "royalties": [
        "pays artists", "royalties", "5x more", "five times", "fair pay",
        "artist pay", "artist payment", "pays musicians", "revenue share",
        "pays more", "better for artists", "artist compensation",
        "sustainable", "redistributed royalties", "streaming payouts",
        "per stream", "pays labels", "music economy"
    ],
    "quality": [
        "lossless", "hi-fi", "hifi", "high fidelity", "24-bit", "24bit",
        "flac", "wav", "sound quality", "audiophile", "high resolution",
        "hi-res", "hires", "studio quality", "cd quality", "master quality",
        "mqa", "320kbps", "high quality audio", "better audio", "superior sound",
        "better sound", "quality streaming", "no compression"
    ],
    "anti-ceo": [
        "daniel ek", "weapons", "drone", "700m", "investment", "military",
        "arms", "defense", "ek invested", "spotify ceo", "arms manufacturer",
        "embargo", "surveillance", "spotify founder", "war", "palantir",
        "anduril", "shield ai", "silicon valley", "tech billionaire"
    ],
    "boycott-israel": [
        "boycott", "israel", "bds", "apartheid", "palestinian", "genocide",
        "occupation", "zionist", "free palestine", "decolonize", "ethnic cleansing",
        "war crimes", "idf", "gaza", "west bank", "settler"
    ],
    "degoogle": [
        "degoogle", "de-google", "privacy", "big tech", "surveillance capitalism",
        "data collection", "tracking", "algorithmic", "algorithm", "independent",
        "no ads", "ad-free", "deamazon", "big streaming", "corporate streaming",
        "monopoly", "antitrust", "walled garden"
    ],
    "anti-spotify": [
        "quit spotify", "leave spotify", "death to spotify", "delete spotify",
        "cancel spotify", "drop spotify", "dump spotify", "ditch spotify",
        "boycott spotify", "fuck spotify", "f spotify", "hate spotify",
        "spotify is garbage", "spotify is trash", "spotify sucks",
        "moving away from spotify", "leaving spotify", "switched from spotify",
        "no longer use spotify", "unsubscribed from spotify"
    ],
    "pro-indie": [
        "independent artists", "indie artists", "diy", "direct pay", "small artists",
        "unsigned artists", "local artists", "emerging artists", "new artists",
        "artist first", "support artists", "music community", "grassroots",
        "authentic music", "curated music", "editorial team", "music experts",
        "music lovers", "music enthusiasts"
    ],
    "switch-recommendation": [
        "switch to qobuz", "switched to qobuz", "switching to qobuz",
        "try qobuz", "recommend qobuz", "moved to qobuz", "move to qobuz",
        "check out qobuz", "qobuz is better", "qobuz over spotify",
        "qobuz instead", "spotify to qobuz", "amazon to qobuz",
        "apple music to qobuz", "youtube to qobuz", "tidal to qobuz",
        "from spotify", "spotify alternative", "alternative to spotify"
    ],
}

# Which platform they're switching FROM
PLATFORM_FROM_RULES = {
    "spotify": [
        "spotify", "from spotify", "quit spotify", "leave spotify",
        "spotify premium", "spotify free", "spotify podcast"
    ],
    "amazon": [
        "amazon music", "amazon unlimited", "from amazon", "quit amazon",
        "leave amazon", "amazon prime music"
    ],
    "apple": [
        "apple music", "from apple", "quit apple music", "leave apple music",
        "itunes", "apple one"
    ],
    "youtube": [
        "youtube music", "youtube premium", "from youtube", "quit youtube music",
        "leave youtube music", "yt music", "ytm"
    ],
    "tidal": [
        "tidal", "from tidal", "quit tidal", "leave tidal", "jay-z streaming"
    ],
    "deezer": [
        "deezer", "from deezer", "quit deezer", "leave deezer"
    ],
}


def tag_narratives(text: str) -> list[str]:
    """Return list of narrative tags that apply to this text."""
    if not text:
        return []
    lower = text.lower()
    matched = []
    for narrative, keywords in NARRATIVE_RULES.items():
        if any(kw in lower for kw in keywords):
            matched.append(narrative)
    return matched


def detect_platform_from(text: str) -> str:
    """Detect which platform the user is switching FROM, or 'generic'."""
    if not text:
        return "generic"
    lower = text.lower()
    for platform, keywords in PLATFORM_FROM_RULES.items():
        if any(kw in lower for kw in keywords):
            return platform
    return "generic"
