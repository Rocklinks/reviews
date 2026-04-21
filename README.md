# Sathya Agency — Google Maps Review Scraper

Scrapes recent Google Maps reviews (last 23 hours) for all 36 Sathya Agency branches,
4 times a day via GitHub Actions. Uses 3 browser automation methods as fallbacks.

---

## File Structure

```
├── branches.py              # Branch list, place IDs, AGM map
├── utils.py                 # Shared: date logic, dedup, JSON I/O, deletion check
├── scraper_playwright.py    # Method 1: Playwright (primary)
├── scraper_selenium.py      # Method 2: Selenium (fallback)
├── scraper_pyautogui.py     # Method 3: PyAutoGUI (last resort)
├── main.py                  # Orchestrator — tries methods in order
├── requirements.txt         # All dependencies
├── rev.json                 # Output: all scraped reviews
├── deleted.json             # Output: detected deleted reviews
├── .deletion_meta.json      # Internal: tracks last deletion-check date
└── .github/
    └── workflows/
        └── scrape.yml       # GitHub Actions: runs 4x daily
```

---

## Schedule (IST)

| Cron (UTC)    | IST Time | Date assigned      |
|---------------|----------|--------------------|
| 0:30 UTC      | 6:00 AM  | Today              |
| 6:30 UTC      | 12:00 PM | Today              |
| 12:30 UTC     | 6:00 PM  | Today              |
| 18:30 UTC     | 12:00 AM | **Previous day**   |

---

## rev.json format

```json
{
  "<review_id>": {
    "review_id":     "abc123def456",
    "branch_id":     5,
    "branch_name":   "Tirunelveli-1",
    "place_id":      "ChIJ2RU2NvQRBDsRq-Fw7IVwx7k",
    "agm":           "John",
    "author":        "Ramesh Kumar",
    "stars":         5,
    "relative_time": "2 hours ago",
    "text":          "Great service!",
    "date":          "2024-01-15",
    "scraped_at":    "2024-01-15T12:35:22.123456",
    "method":        "playwright"
  }
}
```

---

## Deletion Detection

- Every 5–10 days (randomised) a full scrape is compared against stored reviews.
- Reviews no longer visible are saved to `deleted.json` with a `detected_deleted_on` field.

---

## 3-Method Fallback Logic

```
main.py
  ├─ Try Playwright  → success? done.
  ├─ Try Selenium    → success? done.
  └─ Try PyAutoGUI   → success? done. else exit(1)
```

---

## Local Run

```bash
pip install -r requirements.txt
python -m playwright install chromium   # one-time
python main.py --hour 6                 # simulate 6 AM IST run
python main.py --method selenium        # force a specific method
```

---

## Notes on PyAutoGUI method

PyAutoGUI controls mouse/keyboard at the OS level. It requires:
- A real or virtual display (`Xvfb` on Linux)
- Brave browser installed and on PATH
- `xdotool` for window focusing (Linux)

GitHub Actions installs all of this automatically via `scrape.yml`.
