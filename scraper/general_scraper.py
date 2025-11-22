import time
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
import pandas as pd
import os
import urllib.parse
import logging

logger = logging.getLogger(__name__)

class SimplifiedGoogleMapsGeneralScraper:
    def __init__(self, headless=False):
        """Initialize the general scraper"""
        self.options = Options()
        if headless:
            self.options.add_argument("--headless")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--disable-gpu")  # Prevent GPU-related errors
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        self.options.add_argument("--window-size=1920,1080")
        self.options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
        self.options.add_experimental_option("prefs", {
            "profile.default_content_setting_values.geolocation": 1  # Allow geolocation
        })
        
    def scrape_general_comprehensive(self, location, custom_term, max_results=None):
        """General scraping for any custom term"""
        driver = webdriver.Chrome(options=self.options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        all_items = []
        all_urls = set()
        
        try:
            if 'near me' in location.lower():
                driver.execute_cdp_cmd('Emulation.setGeolocationOverride', {
                    "latitude": 13.0827,
                    "longitude": 80.2707,
                    "accuracy": 100
                })
            is_near_me = "near me" in location.lower()
            if is_near_me:
                search_terms = [
                    f"{custom_term} near me",
                    f"{custom_term} nearby",
                    f"best {custom_term} near me"
                ]
                print("🎯 Detected 'near me' search - will limit to ~15km radius")
            else:
                search_terms = [
                    f"{custom_term} {location}",
                    f"{custom_term} in {location}",
                    f"best {custom_term} {location}",
                    f"{custom_term} near {location}"
                ]
            
            for search_term in search_terms:
                try:
                    print(f"\n🔍 Searching: {search_term}")
                    driver.get("https://www.google.com/maps")
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.ID, "searchboxinput"))
                    )
                    search_box = driver.find_element(By.ID, "searchboxinput")
                    search_box.clear()
                    search_box.send_keys(search_term)
                    search_box.send_keys(Keys.ENTER)
                    time.sleep(5)  # Wait for initial results to load
                    
                    urls = self.enhanced_url_collection(driver, max_results)
                    new_urls = [url for url in urls if url not in all_urls]
                    all_urls.update(new_urls)
                    print(f"   Found {len(new_urls)} new item URLs")
                    
                    if max_results and len(all_urls) >= max_results:
                        break
                except Exception as e:
                    print(f"   Error with search term '{search_term}': {e}")
                    logger.error(f"Search term error: {str(e)}")
                    continue
            
            print(f"\n📊 Total unique item URLs collected: {len(all_urls)}")
            url_list = list(all_urls)[:max_results] if max_results else list(all_urls)
            
            for i, url in enumerate(url_list):
                try:
                    print(f"\n📍 Processing item {i+1}/{len(url_list)}: {url}")
                    driver.get(url)
                    time.sleep(4)
                    item_data = self.extract_complete_item_data(driver)
                    if item_data and item_data.get('name') and item_data['name'] != 'Results':
                        all_items.append(item_data)
                        print(f"   ✅ {item_data['name']}")
                except Exception as e:
                    print(f"   Error processing URL {url}: {e}")
                    logger.error(f"URL processing error: {str(e)}")
                    continue
        
        finally:
            driver.quit()
        
        return all_items

    def enhanced_url_collection(self, driver, target_count):
        """Enhanced URL collection with scrolling"""
        item_urls = []
        scroll_attempts = 0
        max_scroll_attempts = 15  # Increased for more thorough scrolling
        
        # Wait for results to load
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[role='main']"))
            )
        except TimeoutException:
            logger.error("Timeout waiting for main results panel")
            return item_urls
        
        while scroll_attempts < max_scroll_attempts and (not target_count or len(item_urls) < target_count):
            selectors = [
                "a[href*='/maps/place/']",
                "div[role='link'] a",
                ".hfpxzc"  # Common class for place links in Google Maps
            ]
            links_found = False
            
            for selector in selectors:
                try:
                    links = driver.find_elements(By.CSS_SELECTOR, selector)
                    for link in links:
                        try:
                            href = link.get_attribute('href')
                            if href and '/maps/place/' in href and href not in item_urls:
                                item_urls.append(href)
                                links_found = True
                        except:
                            continue
                except:
                    continue
            
            logger.info(f"Scroll attempt {scroll_attempts + 1}: Found {len(item_urls)} URLs so far")
            
            if not links_found:
                logger.warning(f"No links found in scroll attempt {scroll_attempts + 1}")
            
            try:
                results_panel = driver.find_element(By.CSS_SELECTOR, "[role='main']")
                driver.execute_script("arguments[0].scrollTop += 1000;", results_panel)  # Increased scroll distance
                time.sleep(3)  # Increased wait time for dynamic loading
            except:
                driver.execute_script("window.scrollBy(0, 1000);")
                time.sleep(3)
            
            scroll_attempts += 1
        
        return item_urls[:target_count]

    def extract_complete_item_data(self, driver):
        """Extract essential item data from Google Maps page"""
        item_data = {}
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            time.sleep(3)
            # Name
            name_selectors = ["h1.DUwDvf", "h1"]
            for selector in name_selectors:
                try:
                    name_element = driver.find_element(By.CSS_SELECTOR, selector)
                    name_text = name_element.text.strip()
                    if name_text and len(name_text) > 1:
                        item_data['name'] = name_text
                        break
                except:
                    continue
            # Address
            try:
                address = driver.find_element(By.CSS_SELECTOR, "[data-item-id='address']").text.strip()
                item_data['address'] = address
                encoded_address = urllib.parse.quote(address)
                item_data['directions_url'] = f"https://www.google.com/maps/dir/?api=1&destination={encoded_address}"
            except:
                item_data['address'] = ''
                item_data['directions_url'] = ''
            # Phone
            try:
                phone = driver.find_element(By.CSS_SELECTOR, "[data-item-id*='phone']").text.strip()
                item_data['phone'] = phone
            except:
                item_data['phone'] = ''
            # Website
            try:
                website = driver.find_element(By.CSS_SELECTOR, "[data-item-id='authority']").get_attribute('href')
                item_data['website'] = website
            except:
                item_data['website'] = ''
            # Rating and reviews
            try:
                rating = driver.find_element(By.CSS_SELECTOR, "div.F7nice span[aria-hidden]").text.strip()
                item_data['rating'] = rating
                reviews_count = driver.find_element(By.CSS_SELECTOR, "div.F7nice span:last-child").text.strip()
                item_data['reviews_count'] = reviews_count.replace('(', '').replace(')', '')
            except:
                item_data['rating'] = ''
                item_data['reviews_count'] = ''
            # Category
            try:
                category = driver.find_element(By.CSS_SELECTOR, "button.DkEaL").text.strip()
                item_data['category'] = category
            except:
                item_data['category'] = ''
        except Exception as e:
            print(f"   Error extracting data: {e}")
            logger.error(f"Data extraction error: {str(e)}")
        
        return item_data

    def save_simplified_csv(self, items, filename, base_dir=None):
        """Save data to CSV, optionally using base_dir for file path"""
        if not items:
            logger.info("No items to save to CSV")
            return 0
        
        # Construct the full file path
        if base_dir:
            filename = os.path.join(base_dir, filename)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(filename) or '.', exist_ok=True)
        
        item_data = [
            {
                'name': item.get('name', ''),
                'address': item.get('address', ''),
                'phone': item.get('phone', ''),
                'website': item.get('website', ''),
                'rating': item.get('rating', ''),
                'reviews_count': item.get('reviews_count', ''),
                'category': item.get('category', ''),
                'directions_url': item.get('directions_url', '')
            }
            for item in items
        ]
        df = pd.DataFrame(item_data)
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        logger.info(f"CSV saved to: {filename}")
        print(f"\n🔗 CSV FILE LOCATION: {filename}")
        return len(item_data)

def scrape_general(location, custom_term, max_results):
    scraper = SimplifiedGoogleMapsGeneralScraper(headless=True)
    return scraper.scrape_general_comprehensive(location, custom_term, max_results)