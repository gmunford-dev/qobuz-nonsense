# Qobuz Nonsense

Competitive intelligence monitor tracking Reddit posts, news articles, and X/Twitter posts where users (or bots) recommend switching to Qobuz from Spotify, Amazon Music, Apple Music, and YouTube Music.

Auto-updates every 3 hours via GitHub Actions. Password-protected GitHub Pages site.

---

## Setup

### 1. Create the GitHub repo

Create a new **private** repo named `qobuz-nonsense`. Push this code to it.

### 2. Register a Reddit API app

1. Go to [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps)
2. Click **"Create App"** (or "Create another app")
3. Fill in:
   - **Name:** `QobuzNonsense`
   - **Type:** `script`
   - **Redirect URI:** `http://localhost:8080` (anything works for script apps)
4. Click **Create app**
5. Note your **client_id** (string under the app name) and **client_secret**

### 3. Add GitHub Secrets

In your repo: Settings → Secrets and variables → Actions → New repository secret

| Secret Name | Value |
|---|---|
| `REDDIT_CLIENT_ID` | Client ID from step 2 |
| `REDDIT_CLIENT_SECRET` | Client secret from step 2 |
| `REDDIT_USERNAME` | Your Reddit username |
| `REDDIT_PASSWORD` | Your Reddit password |
| `SITE_PASSWORD` | Your 5-digit access code (e.g. `90210`) |

### 4. Enable GitHub Pages

Settings → Pages → Source: **Deploy from a branch** → Branch: `main` / `/ (root)`

### 5. Trigger first run

Actions → "Qobuz Nonsense Monitor" → Run workflow

First run will do a year-long backfill search. Subsequent runs add new posts only.

---

## What It Tracks

### Sources
- **Reddit** — posts and comments across r/Music, r/audiophile, r/BoycottIsrael, r/degoogle, r/TIdaL, r/fantanoforever, r/spotify, and more
- **News** — articles via Google News RSS search
- **X / Twitter** — public posts via snscrape (no API key required)

### Narratives Detected
| Tag | What it flags |
|---|---|
| Switch Rec. | Direct "switch to Qobuz" recommendations |
| Anti-Spotify | "quit Spotify", "delete Spotify", etc. |
| Artist Pay | Royalties, 5x payout claims |
| Sound Quality | Lossless, hi-fi, FLAC, 24-bit language |
| Anti-CEO | Daniel Ek weapons investment angle |
| Boycott Israel | BDS/boycott framing |
| DeGoogle | Privacy/big tech framing |
| Pro-Indie | Independent artist support framing |

### Bot Suspicion Scoring
Each Reddit post is scored 0–100% based on:
- Account age (< 30 days, < 90 days)
- Karma levels
- Copy-paste similarity to other posts
- Odd posting hours (2–6am UTC)
- Username patterns

---

## Files

```
qobuz-nonsense/
├── .github/workflows/monitor.yml   # Runs every 3 hours
├── scripts/
│   ├── collect.py                  # Data collection (Reddit, News, X)
│   ├── tag_narratives.py           # Keyword narrative detection
│   ├── bot_score.py                # Bot suspicion scoring
│   └── build_site.py              # HTML generator
├── templates/
│   └── index.template.html         # Frontend template
├── data/
│   ├── posts.json                  # Recent posts (last 30 days)
│   ├── metadata.json               # Stats and summary
│   └── archive/
│       └── YYYY-MM.json            # Monthly archives (12 months)
├── index.html                      # StaticCrypt-encrypted output (auto-generated)
└── requirements.txt
```

---

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set env vars (or use a .env file)
export REDDIT_CLIENT_ID=your_id
export REDDIT_CLIENT_SECRET=your_secret
export REDDIT_USERNAME=your_username
export REDDIT_PASSWORD=your_password

# Run collection
python scripts/collect.py

# Build site
python scripts/build_site.py

# Encrypt (requires Node.js)
npx staticrypt@3 index_plain.html --password 90210 --short --remember 30

# Open index.html in browser
open index.html
```
