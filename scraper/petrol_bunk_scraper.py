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

class SimplifiedGoogleMapsPetrolBunkScraper:
    def __init__(self, headless=False):
        """Initialize the simplified petrol bunk scraper"""
        self.options = Options()
        if headless:
            self.options.add_argument("--headless")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        self.options.add_argument("--window-size=1920,1080")
        self.options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
    def scrape_petrol_bunks_comprehensive(self, location, max_results=None):
        """Simplified petrol bunk scraping focused on essential data only"""
        driver = webdriver.Chrome(options=self.options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        all_bunks = []
        all_urls = set()
        
        try:
            is_near_me = "near me" in location.lower()
            if is_near_me:
                search_terms = [
                    f"petrol bunk near me",
                    f"petrol pump near me",
                    f"petrol station near me",
                    f"gas station near me",
                    f"fuel station near me"
                ]
                print("🎯 Detected 'near me' search - will limit to ~15km radius")
            else:
                search_terms = [
                    f"petrol bunk {location}",
                    f"petrol pump {location}",
                    f"petrol station {location}",
                    f"gas station {location}",
                    f"fuel station {location}",
                    f"fuel pump {location}"
                ]
            
            for search_term in search_terms:
                try:
                    print(f"\n🔍 Searching: {search_term}")
                    driver.get("https://www.google.com/maps")
                    time.sleep(3)
                    search_box = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable((By.ID, "searchboxinput"))
                    )
                    search_box.clear()
                    search_box.send_keys(search_term)
                    search_box.send_keys(Keys.ENTER)
                    time.sleep(5)
                    
                    urls = self.enhanced_url_collection(driver, max_results)
                    new_urls = [url for url in urls if url not in all_urls]
                    all_urls.update(new_urls)
                    print(f"   Found {len(new_urls)} new bunk URLs")
                    
                    if max_results and len(all_urls) >= max_results:
                        break
                except Exception as e:
                    print(f"   Error with search term '{search_term}': {e}")
                    continue
            
            print(f"\n📊 Total unique bunk URLs collected: {len(all_urls)}")
            url_list = list(all_urls)[:max_results] if max_results else list(all_urls)
            
            for i, url in enumerate(url_list):
                try:
                    print(f"\n📍 Processing bunk {i+1}/{len(url_list)}...")
                    driver.get(url)
                    time.sleep(4)
                    bunk_data = self.extract_complete_bunk_data(driver)
                    if bunk_data and bunk_data.get('name') and bunk_data['name'] != 'Results':
                        bunk_data['category'] = 'Petrol Bunk'
                        all_bunks.append(bunk_data)
                        print(f"   ✅ {bunk_data['name']}")
                except Exception as e:
                    print(f"   Error processing URL {url}: {e}")
                    continue
        
        finally:
            driver.quit()
        
        return all_bunks

    def enhanced_url_collection(self, driver, target_count):
        """Enhanced URL collection with scrolling"""
        bunk_urls = []
        time.sleep(5)
        scroll_attempts = 0
        max_scroll_attempts = 10
        
        while scroll_attempts < max_scroll_attempts and (not target_count or len(bunk_urls) < target_count):
            links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/maps/place/']")
            for link in links:
                try:
                    href = link.get_attribute('href')
                    if href and href not in bunk_urls:
                        bunk_urls.append(href)
                except:
                    continue
            try:
                results_panel = driver.find_element(By.CSS_SELECTOR, "[role='main']")
                driver.execute_script("arguments[0].scrollTop += 500;", results_panel)
                time.sleep(2)
            except:
                driver.execute_script("window.scrollBy(0, 500);")
                time.sleep(2)
            scroll_attempts += 1
        
        return bunk_urls

    def extract_complete_bunk_data(self, driver):
        """Extract essential bunk data from Google Maps page"""
        bunk_data = {}
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
                        bunk_data['name'] = name_text
                        break
                except:
                    continue
            # Address
            try:
                address = driver.find_element(By.CSS_SELECTOR, "[data-item-id='address']").text.strip()
                bunk_data['address'] = address
                encoded_address = urllib.parse.quote(address)
                bunk_data['directions_url'] = f"https://www.google.com/maps/dir/?api=1&destination={encoded_address}"
            except:
                bunk_data['address'] = ''
                bunk_data['directions_url'] = ''
            # Phone
            try:
                phone = driver.find_element(By.CSS_SELECTOR, "[data-item-id*='phone']").text.strip()
                bunk_data['phone'] = phone
            except:
                bunk_data['phone'] = ''
            # Website
            try:
                website = driver.find_element(By.CSS_SELECTOR, "[data-item-id='authority']").get_attribute('href')
                bunk_data['website'] = website
            except:
                bunk_data['website'] = ''
            # Rating and reviews
            try:
                rating = driver.find_element(By.CSS_SELECTOR, "div.F7nice span[aria-hidden]").text.strip()
                bunk_data['rating'] = rating
                reviews_count = driver.find_element(By.CSS_SELECTOR, "div.F7nice span:last-child").text.strip()
                bunk_data['reviews_count'] = reviews_count.replace('(', '').replace(')', '')
            except:
                bunk_data['rating'] = ''
                bunk_data['reviews_count'] = ''
            # Category
            try:
                category = driver.find_element(By.CSS_SELECTOR, "button.DkEaL").text.strip()
                bunk_data['category'] = category
            except:
                bunk_data['category'] = 'Petrol Bunk'
        except Exception as e:
            print(f"   Error extracting petrol bunk data: {e}")
        
        return bunk_data

    def save_simplified_csv(self, bunks, filename):
        """Save petrol bunk data to CSV"""
        if not bunks:
            return 0
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        bunk_data = [
            {
                'name': bunk.get('name', ''),
                'address': bunk.get('address', ''),
                'phone': bunk.get('phone', ''),
                'website': bunk.get('website', ''),
                'rating': bunk.get('rating', ''),
                'reviews_count': bunk.get('reviews_count', ''),
                'category': bunk.get('category', 'Petrol Bunk'),
                'directions_url': bunk.get('directions_url', '')
            }
            for bunk in bunks
        ]
        df = pd.DataFrame(bunk_data)
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"\n🔗 CSV FILE LOCATION: {filename}")
        return len(bunk_data)

def scrape_petrol_bunk(location, max_results):
    scraper = SimplifiedGoogleMapsPetrolBunkScraper(headless=True)
    return scraper.scrape_petrol_bunks_comprehensive(location, max_results)