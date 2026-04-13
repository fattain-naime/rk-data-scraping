<div align="center">
  
# RK-Data-Scraping

**Enterprise-grade, state-aware e-commerce data mining bot**

[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20Windows%20%7C%20Termux-informational?style=flat-square)](https://github.com/)
[![Maintained](https://img.shields.io/badge/Maintained-Yes-brightgreen?style=flat-square)](https://github.com/)
[![License](https://img.shields.io/badge/License-apache2.0-yellow?style=flat-square)](LICENSE)
[![Author](https://img.shields.io/badge/Author-Fattain_Naime-black?style=flat-square&logo=github)](https://iamnaime.info.bd)

*Born from [lab\_0x4E](https://iamnaime.info.bd) — Security Research & Automation Laboratory*

</div>

---

## 📌 Overview

**RK-Data-Scraping** is a high-efficiency, crash-proof web scraping framework built for e-commerce intelligence gathering. It features intelligent delta updates, sitemap-driven discovery, WAF bypass capabilities, and incremental CSV output — designed to handle large-scale product catalogs reliably across repeated runs.

> ⚠️ **Scope:** Currently optimized for WordPress-based e-commerce sites. Parses WordPress-specific HTML class structures for product element extraction.

---

## ✨ Key Features

| Feature | Description |
|---|---|
| 🗺️ **Dynamic Sitemap Discovery** | Auto-parses `sitemap_index.xml` to discover all product sitemaps without manual configuration |
| ⚡ **Delta Updates** | Tracks `lastmod` timestamps — subsequent runs fetch *only* updated sitemaps, saving bandwidth and time |
| 💾 **State Management** | Persists scraping state (`Pending` / `Done`) in a local `config.json` — resumes exactly where it left off after a crash or interruption |
| 🛡️ **WAF & Anti-Bot Bypass** | Integrates `cloudscraper` to bypass Cloudflare and similar protections via genuine browser handshake impersonation |
| 🔍 **Deep DOM Parsing** | Extracts nested and custom-themed elements: breadcrumbs, market/reseller/wholesale prices, stock status, ratings |
| 📤 **Incremental CSV Output** | Streams extracted records directly to `Scraped_Products_Data.csv` in real-time — zero data loss on interruption |

---

## 🏗️ Architecture & Run Lifecycle

```
Run 1 (Initial)
└── Parse sitemap_index.xml
    └── Index all product URLs → config.json [Pending]
        └── Scrape each URL → Append to CSV → Mark [Done]

Run 2+ (Delta)
└── Parse sitemap_index.xml
    └── Compare lastmod timestamps
        └── Fetch only updated sitemaps
            └── Scrape new products → Append to CSV
```

**Example across multiple days:**
- **Day 1:** Full scan — indexes and scrapes all products found across all sitemaps.
- **Day 3:** Detects which sitemaps changed since the last run, fetches only newly added products, and appends them to the existing CSV.

---

## 🛠️ Prerequisites

- Python **3.8 or higher**
- Compatible with **Linux**, **Windows**, and **Termux (Android)**

---

## 📦 Installation

**1. Clone the repository:**
```bash
git clone https://github.com/yourusername/rk-data-scraping.git
cd rk-data-scraping
```

**2. Install dependencies:**
```bash
pip install -r requirements.txt
```

---

## ⚙️ Configuration

Before the first run, open `scrap.py` and update the two required variables at the top of the file:

```python
# ── Target Configuration ──────────────────────────────────────────────────────

# Base URL of the target WordPress e-commerce site
TARGET_BASE_URL = "https://example.com"

# Full logged-in session cookie string (required for restricted pricing tiers)
MY_COOKIES = "your_full_cookie_string_here"
```

> **Why cookies?** Reseller and wholesale price tiers are typically gated behind authentication. Providing a valid session cookie ensures the scraper can access restricted pricing logic.

---

## 🚀 Usage

```bash
python scrap.py
```

No arguments required. The script manages its own state and resumes automatically on subsequent runs.

---

## 📊 Output Data Schema

Output is written to **`Scraped_Products_Data.csv`** — UTF-8 encoded, Excel-compatible.

| Column | Description |
|---|---|
| `Product Name` | Full product title |
| `Category` | Deepest breadcrumb node |
| `Market Price` | Standard retail price |
| `Reseller Price` | Authenticated reseller tier price |
| `Wholesale Price` | Bulk pricing tier |
| `Est. Profit` | Calculated profit margin |
| `Stock Status` | Live inventory status |
| `Product Uploaded Date` | Publication date |
| `Review / Rating` | Aggregate customer rating |
| `Description` | Full product description |
| `Image URL` | Direct link to product image |
| `Product Link` | Canonical product page URL |

---

## ⚠️ Limitations

- Designed exclusively for **WordPress-based** e-commerce sites
- Depends on WordPress HTML class naming conventions for element targeting
- Non-WordPress or heavily customized themes may require selector adjustments in `scrap.py`

---

## 👨‍💻 Author

**Fattain Naime**
- 🌐 [iamnaime.info.bd](https://iamnaime.info.bd)
- 🔬 lab_0x4E — Security Research & Automation Laboratory

---

## 📄 Disclaimer

This tool is developed strictly for **educational and authorized research purposes**. The author assumes no liability for misuse, server overloads, rate limit violations, or breaches of any site's Terms of Service resulting from the execution of this script.

**Always obtain explicit permission before scraping any target application.**
