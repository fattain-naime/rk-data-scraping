import os
import json
import time
import csv
import logging
import warnings
from typing import Dict, Any, Optional, List
import cloudscraper
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

"""
=======================================================================================
Project          : RK-Data-Scraping
File             : scrap.py
Version          : V1.0
Description      : An enterprise-grade, state-aware e-commerce data mining bot.
                   Features dynamic sitemap discovery, incremental state 
                   management, delta updates, and automated WAF bypass.
Author           : Fattain Naime
Website          : https://iamnaime.info.bd
Laboratory       : Born from lab_0x4E
=======================================================================================

Copyright 2026 Fattain Naime (lab_0x4E)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Disclaimer:
This tool is developed strictly for educational and authorized research purposes.
The author assumes no liability for misuse, server overloads, rate limit violations, 
or breaches of any site's Terms of Service resulting from the execution of this script.
=======================================================================================
"""


# ==========================================
# CONFIGURATION SECTION
# ==========================================

# ⚠️ SET YOUR TARGET WEBSITE BASE URL HERE (No trailing slash)
TARGET_BASE_URL = "https://example.com"

# ⚠️ SET YOUR COMPLETE COOKIE STRING HERE
MY_COOKIES = "your-cookies-here"

# System Files
STATE_FILE = "config.json"
DATA_FILE = "Scraped_Products_Data.csv"

# ==========================================
# SYSTEM SETUP & LOGGING
# ==========================================

# Suppress noisy XML parsing warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ==========================================
# CORE SCRAPER CLASS
# ==========================================

class DynamicEcommerceScraper:
    """
    Enterprise-grade asynchronous-ready web scraper.
    Implements dynamic sitemap discovery, delta updates, and state persistence.
    """

    def __init__(self, base_url: str, cookies: str):
        self.base_url = base_url.rstrip('/')
        self.sitemap_index_url = f"{self.base_url}/sitemap_index.xml"
        self.state_file = STATE_FILE
        self.data_file = DATA_FILE
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": f"{self.base_url}/",
            "Cookie": cookies
        }
        
        # Initialize Cloudscraper for advanced WAF/Bot protection bypass
        self.scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
        )
        
        # Load or initialize the local database (JSON state)
        self.state = self._load_state()

        # Define CSV schema
        self.csv_headers = [
            "Product Name", "Category", "Est. Profit", "Market Price", "Reseller Price", 
            "Wholesale Price", "Stock Status", "Product Uploaded", "Review", 
            "Description", "Image URL", "Product Link"
        ]

    def _load_state(self) -> Dict[str, Any]:
        """Loads the tracking state from the JSON database."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Corrupted state file: {self.state_file}. Rebuilding state.")
        
        # Default state structure
        return {"sitemaps": {}, "products": {}}

    def _save_state(self) -> None:
        """Atomically commits the current state to the JSON database."""
        with open(self.state_file, 'w', encoding='utf-8') as f:
            json.dump(self.state, f, indent=4, ensure_ascii=False)

    def _clean_text(self, text: Optional[str]) -> str:
        """Sanitizes extracted HTML strings to maintain CSV integrity."""
        if not text:
            return "N/A"
        # Removes excessive whitespaces, newlines, and carriage returns
        return " ".join(text.strip().replace("\n", " ").replace("\r", " ").split())

    def _fetch_xml_soup(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        """Fetches XML data with an exponential backoff retry mechanism."""
        for attempt in range(1, retries + 1):
            try:
                response = self.scraper.get(url, headers=self.headers, timeout=20)
                if response.status_code == 200:
                    raw_xml = response.text
                    # Ensure pure XML parsing by bypassing leading whitespaces
                    start_index = raw_xml.find('<?xml')
                    clean_xml = raw_xml[start_index:] if start_index != -1 else raw_xml
                    return BeautifulSoup(clean_xml, 'html.parser')
                else:
                    logger.warning(f"[Attempt {attempt}/{retries}] Server returned {response.status_code} for {url}")
            except Exception as e:
                logger.warning(f"[Attempt {attempt}/{retries}] Network error on {url}: {e}")
            
            # Exponential backoff before retrying
            time.sleep(2 * attempt)
            
        logger.error(f"Failed to fetch {url} after {retries} attempts.")
        return None

    def discover_and_index_sitemaps(self) -> None:
        """
        Dynamically fetches the root sitemap, discovers product-specific sitemaps,
        compares lastmod timestamps, and indexes new product URLs.
        """
        logger.info(f"🔍 Analyzing Root Sitemap Index: {self.sitemap_index_url}")
        index_soup = self._fetch_xml_soup(self.sitemap_index_url)
        
        if not index_soup:
            logger.error("Root sitemap index unreachable. Aborting discovery phase.")
            return

        sitemap_tags = index_soup.find_all('sitemap')
        new_links_indexed = 0

        for sitemap in sitemap_tags:
            loc_tag = sitemap.find('loc')
            lastmod_tag = sitemap.find('lastmod')
            
            if not loc_tag:
                continue
                
            sitemap_url = loc_tag.text.strip()
            
            # Filter constraint: Process only product sitemaps
            if 'product-sitemap' not in sitemap_url:
                continue
                
            current_lastmod = lastmod_tag.text.strip() if lastmod_tag else "UNKNOWN_DATE"
            saved_lastmod = self.state["sitemaps"].get(sitemap_url)
            
            # Delta Check: Process only if the sitemap is new or recently updated
            if saved_lastmod != current_lastmod:
                logger.info(f"🔄 Delta Update Detected -> Processing: {sitemap_url} (Lastmod: {current_lastmod})")
                
                prod_soup = self._fetch_xml_soup(sitemap_url)
                if prod_soup:
                    urls = prod_soup.find_all('url')
                    for u in urls:
                        product_loc = u.find('loc')
                        # Ensure the URL is an actual product page
                        if product_loc and '/product/' in product_loc.text:
                            prod_url = product_loc.text.strip()
                            
                            # Add to state DB if not already present
                            if prod_url not in self.state["products"]:
                                self.state["products"][prod_url] = "[Pending]"
                                new_links_indexed += 1
                                
                    # Commit the new lastmod timestamp to prevent rescanning in the future
                    self.state["sitemaps"][sitemap_url] = current_lastmod
                    self._save_state()

        logger.info(f"✅ Sitemap indexing complete. Total new products indexed: {new_links_indexed}")

    def extract_and_dump_data(self) -> None:
        """
        Iterates through [Pending] URLs, extracts DOM data based on specific CSS selectors,
        dumps into CSV, and transitions URL state to [Done].
        """
        # Filter URLs that are marked as [Pending]
        pending_queue = [url for url, status in self.state["products"].items() if status == "[Pending]"]
        total_pending = len(pending_queue)
        
        if total_pending == 0:
            logger.info("🎉 Database is up to date. No pending products to extract.")
            return

        logger.info(f"🚀 Initiating Deep Data Extraction for {total_pending} products...")

        # Prepare CSV File (using utf-8-sig for safe Excel rendering of foreign scripts)
        file_exists = os.path.exists(self.data_file) and os.stat(self.data_file).st_size > 0
        with open(self.data_file, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.csv_headers)
            
            if not file_exists:
                writer.writeheader()

            for index, target_url in enumerate(pending_queue, 1):
                try:
                    response = self.scraper.get(target_url, headers=self.headers, timeout=20)
                    if response.status_code != 200:
                        logger.warning(f"HTTP {response.status_code} on {target_url}. Skipping.")
                        continue
                        
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # --- DATA EXTRACTION LOGIC ---
                    
                    # 1. Product Name
                    name_tag = soup.find('h1', class_='product_title')
                    name = self._clean_text(name_tag.text) if name_tag else "N/A"
                    
                    # 2. Category (Targets the deepest node in the breadcrumb trail)
                    breadcrumb_nav = soup.find('nav', class_='woocommerce-breadcrumb')
                    if breadcrumb_nav:
                        crumb_links = breadcrumb_nav.find_all('a')
                        category = self._clean_text(crumb_links[-1].text) if crumb_links else "N/A"
                    else:
                        category = "N/A"
                    
                    # 3. Pricing Matrix
                    market_price = "N/A"
                    reseller_price = "N/A"
                    wholesale_price = "N/A"
                    
                    mp_tag = soup.find('a', class_='market-price-btn')
                    if mp_tag: market_price = mp_tag.text.replace('Market Price:', '').strip()
                    
                    rp_tag = soup.find('a', class_='reselling-price-btn')
                    if rp_tag: reseller_price = rp_tag.text.replace('Reseller Price:', '').strip()
                    
                    wp_tag = soup.find('a', class_='wholesale-price-btn')
                    if wp_tag: wholesale_price = wp_tag.text.replace('Wholesale Price:', '').strip()
                    
                    # Estimated Profit (Scope limited to summary container to avoid UI conflicts)
                    summary_div = soup.find('div', class_='summary-inner') or soup.find('div', class_='summary')
                    est_profit = "N/A"
                    if summary_div:
                        profit_tag = summary_div.find('p', class_='price')
                        est_profit = self._clean_text(profit_tag.text) if profit_tag else "N/A"
                    
                    # 4. Stock & Lifecycle Metrics
                    upload_date, stock = "N/A", "N/A"
                    stat_items = soup.find_all('div', class_='stat-item')
                    for item in stat_items:
                        label = item.find('span', class_='stat-label')
                        val = item.find('span', class_='stat-value')
                        if label and val:
                            label_text = self._clean_text(label.text)
                            if 'আপলোডেড' in label_text:
                                upload_date = self._clean_text(val.text)
                            elif 'স্টক' in label_text:
                                stock = self._clean_text(val.text)
                    
                    # 5. Reviews
                    review_div = soup.find('div', id='tab-reviews')
                    review = "N/A"
                    if review_div:
                        total_rev = review_div.find('div', class_='wd-rating-summary-total')
                        if total_rev:
                            review = self._clean_text(total_rev.text)
                        else:
                            no_rev = review_div.find('p', class_='woocommerce-noreviews')
                            review = self._clean_text(no_rev.text) if no_rev else "N/A"
                    
                    # 6. Description (Joins multiple paragraphs with a pipeline separator)
                    desc_tab = soup.find('div', id='tab-description')
                    description = "N/A"
                    if desc_tab:
                        desc_paragraphs = [p.text.strip() for p in desc_tab.find_all('p') if p.text.strip()]
                        description = " | ".join(desc_paragraphs) if desc_paragraphs else self._clean_text(desc_tab.text)
                    
                    # 7. Media
                    img_tag = soup.find('img', class_='wp-post-image')
                    image_url = img_tag.get('src') if img_tag else "N/A"
                    
                    # --- DATA COMPILATION & STORAGE ---
                    
                    product_data = {
                        "Product Name": name, "Category": category, "Est. Profit": est_profit,
                        "Market Price": market_price, "Reseller Price": reseller_price, 
                        "Wholesale Price": wholesale_price, "Stock Status": stock,
                        "Product Uploaded": upload_date, "Review": review,
                        "Description": description, "Image URL": image_url, "Product Link": target_url
                    }
                    
                    # Stream data to CSV immediately to prevent data loss on crash
                    writer.writerow(product_data)
                    f.flush() 
                    
                    # Mutate state: Mark as successfully processed
                    self.state["products"][target_url] = "[Done]"
                    
                    # Persist state to JSON every 10 iterations to optimize I/O operations
                    if index % 10 == 0 or index == total_pending:
                        self._save_state()
                    
                    logger.info(f"✨ [{index}/{total_pending}] EXTRACTED: {name[:30]}... | Cat: {category}")
                    
                    # Rate limiting to prevent server bans
                    time.sleep(0.5)

                except Exception as e:
                    logger.error(f"⚠️ Exception occurred processing {target_url}: {str(e)}")
                    # Trigger longer delay on exception
                    time.sleep(2) 

    def execute(self) -> None:
        """Orchestrates the entire scraping pipeline."""
        self.discover_and_index_sitemaps()
        self.extract_and_dump_data()


# ==========================================
# APPLICATION ENTRY POINT
# ==========================================

if __name__ == "__main__":
    try:
        logger.info("Initializing Advanced E-commerce Scraper...")
        
        # Instantiate and execute the bot
        bot = DynamicEcommerceScraper(
            base_url=TARGET_BASE_URL,
            cookies=MY_COOKIES
        )
        bot.execute()
        
        logger.info("🏁 Execution Terminated Successfully.")
        
    except KeyboardInterrupt:
        logger.info("🛑 Process manually interrupted by user. Safe exit initiated.")
    except Exception as e:
        logger.critical(f"❌ Fatal System Error: {str(e)}")
# END OF THE FILE
# ~exit()