import time
import re
import csv
import os
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException, InvalidSessionIdException, NoSuchWindowException
from selenium.webdriver.common.keys import Keys
import pandas as pd
from django.core.cache import cache
import signal
import psutil
import atexit
import threading
import json

logger = logging.getLogger(__name__)

class GymScraper:
    # Class variable to store all active scrapers
    active_scrapers = {}

    def __init__(self, headless=False, job_id=None):
        self.options = Options()
        if headless:
            self.options.add_argument("--headless")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        self.options.add_argument("--window-size=1920,1080")
        self.options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
        self.options.add_experimental_option("prefs", {
            "profile.default_content_setting_values.geolocation": 1   
        })
        
        # Add a unique user data directory for this instance
        import tempfile
        self.user_data_dir = tempfile.mkdtemp()
        self.options.add_argument(f"--user-data-dir={self.user_data_dir}")
        
        self.job_id = job_id  # Store job_id for cancellation checks
        self.driver = None
        self.driver_pid = None
        self.is_cancelled = False
        self.chrome_start_time = time.time()
        
        # Register this instance globally
        if self.job_id:
            GymScraper.active_scrapers[self.job_id] = self

    def should_cancel(self):
        """Check if scraping should be cancelled"""
        if self.job_id:
            cancelled = cache.get(f"cancel_scraping_{self.job_id}", False)
            if cancelled:
                self.is_cancelled = True
            return cancelled
        return False

    def close_chrome_tab(self):
        """Close only the Chrome browser/driver for this specific job"""
        # Mark as cancelled
        self.is_cancelled = True
        
        # First, try to close the driver normally
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                # If driver closing failed, try to kill the process
                self.kill_chrome_process()

        # Clean up user data directory
        try:
            import shutil
            shutil.rmtree(self.user_data_dir, ignore_errors=True)
        except:
            pass

    def kill_chrome_process(self):
        """Kill Chrome process for this specific scraper"""
        try:
            # If we have the PID, kill it directly
            if self.driver_pid:
                try:
                    proc = psutil.Process(self.driver_pid)
                    proc.kill()
                    proc.wait(timeout=5)
                    return True
                except psutil.NoSuchProcess:
                    # Process already terminated
                    return True
                except psutil.TimeoutExpired:
                    # Force kill if timeout
                    proc.kill()
                    return True
                except Exception:
                    pass
            
            # If PID approach failed, try to find and kill by user data directory
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if 'chrome' in proc.info['name'].lower():
                        cmdline = ' '.join(proc.info['cmdline'])
                        if self.user_data_dir in cmdline:
                            proc.kill()
                            proc.wait(timeout=5)
                            return True
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
                    continue
            
            # If user data approach failed, try to find by startup time (within last 30 seconds)
            current_time = time.time()
            for proc in psutil.process_iter(['pid', 'name', 'create_time', 'cmdline']):
                try:
                    if 'chrome' in proc.info['name'].lower():
                        proc_start_time = proc.info['create_time']
                        if (current_time - proc_start_time < 30 and 
                            self.chrome_start_time - 5 <= proc_start_time <= self.chrome_start_time + 30):
                            cmdline = ' '.join(proc.info['cmdline'])
                            if '--user-data-dir' in cmdline:  # Likely our process
                                proc.kill()
                                proc.wait(timeout=5)
                                return True
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
                    continue
            
            return False
        except Exception as e:
            print(f"Error in kill_chrome_process: {e}")
            return False

    def scrape_gyms_comprehensive(self, location, gym_type="gym", max_results=25, job_id=None):
        """
        Comprehensive gym scraping with cancellation support.
        """
        driver = webdriver.Chrome(options=self.options)
        self.driver = driver
        self.driver_pid = self.driver.service.process.pid
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        all_gyms = []
        all_urls = set()
        
        try:
            search_terms = self.get_gym_search_terms(gym_type, location)
            
            for search_term in search_terms:
                if self.should_cancel():
                    print("Scraping cancelled by user - closing Chrome for this job")
                    self.close_chrome_tab()
                    return "CANCELLED"
                    
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
                    print(f"   Found {len(new_urls)} new gym URLs")
                    
                    if len(all_urls) >= max_results:
                        break
                        
                except Exception as e:
                    print(f"   Error with search term '{search_term}': {e}")
                    continue
            
            print(f"\n📊 Total unique gym URLs collected: {len(all_urls)}")
            url_list = list(all_urls)[:max_results]
            
            for i, url in enumerate(url_list):
                if self.should_cancel():
                    print("Scraping cancelled by user - closing Chrome for this job")
                    self.close_chrome_tab()
                    return "CANCELLED"
                    
                try:
                    print(f"\n🏋️ Processing gym {i+1}/{len(url_list)}...")
                    driver.get(url)
                    time.sleep(4)
                    
                    gym_data = self.extract_complete_gym_data(driver, url)
                    
                    if gym_data and gym_data.get('name') and gym_data['name'] != 'Results':
                        gym_data['gym_type'] = gym_type
                        all_gyms.append(gym_data)
                        print(f"   ✅ {gym_data['name']}")
                        if gym_data.get('phone'):
                            print(f"      📞 {gym_data['phone']}")
                        if gym_data.get('address'):
                            addr = gym_data['address'][:50] + "..." if len(gym_data['address']) > 50 else gym_data['address']
                            print(f"      📍 {addr}")
                        if gym_data.get('rating'):
                            print(f"      ⭐ {gym_data['rating']} ({gym_data.get('reviews_count', 'N/A')})")
                    else:
                        print(f"   ❌ Failed to extract valid data")
                except Exception as e:
                    print(f"   ❌ Error processing gym {i+1}: {e}")
                    continue
                    
            return all_gyms
            
        except (InvalidSessionIdException, NoSuchWindowException) as e:
            # Handle the case where the driver has been closed
            print(f"Driver session ended unexpectedly: {e}")
            return "CANCELLED"
        except Exception as e:
            print(f"Unexpected error during scraping: {e}")
            return []
        finally:
            # Remove this instance from global registry
            if self.job_id in GymScraper.active_scrapers:
                del GymScraper.active_scrapers[self.job_id]
            
            # Always close Chrome when done (unless already closed due to cancellation)
            if not self.is_cancelled:
                self.close_chrome_tab()

    def get_gym_search_terms(self, gym_type, location):
        base_terms = {
            'gym': [f"gym {location}", f"fitness center {location}", f"health club {location}"],
            'crossfit': [f"crossfit {location}", f"crossfit box {location}"],
            'yoga': [f"yoga studio {location}", f"yoga center {location}"],
            'pilates': [f"pilates studio {location}", f"pilates center {location}"],
            'martial_arts': [f"martial arts {location}", f"boxing gym {location}", f"karate {location}"],
            'swimming': [f"swimming pool {location}", f"aquatic center {location}"],
            'all_gyms': [
                f"gym {location}", f"fitness center {location}", f"crossfit {location}",
                f"yoga studio {location}", f"pilates {location}", f"martial arts {location}"
            ]
        }
        return base_terms.get(gym_type, [f"{gym_type} {location}"])

    def enhanced_url_collection(self, driver, target_count):
        gym_urls = []
        time.sleep(5)
        scroll_attempts = 0
        max_scroll_attempts = 30
        last_count = 0
        stagnant_scrolls = 0
        
        while scroll_attempts < max_scroll_attempts and len(gym_urls) < target_count:
            if self.should_cancel():
                return gym_urls  # Return what we have so far
                
            links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/maps/place/']")
            for link in links:
                if self.should_cancel():
                    return gym_urls  # Return what we have so far
                    
                try:
                    href = link.get_attribute('href')
                    if href and 'google.com/maps' in href and href not in gym_urls:
                        gym_urls.append(href)
                except:
                    continue
            
            current_count = len(gym_urls)
            if current_count == last_count:
                stagnant_scrolls += 1
                if stagnant_scrolls >= 3:
                    break
            else:
                stagnant_scrolls = 0
            last_count = current_count
            
            try:
                results_panel = driver.find_element(By.CSS_SELECTOR, "[role='feed']")
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", results_panel)
            except:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            scroll_attempts += 1
        
        return gym_urls[:target_count]

    def extract_complete_gym_data(self, driver, place_url):
        if self.should_cancel():
            return {}
            
        gym_data = {}
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
            time.sleep(2)

            if self.should_cancel():
                return {}

            # NAME
            name_selectors = ["h1.DUwDvf.lfPIob", "h1"]
            for selector in name_selectors:
                if self.should_cancel():
                    return {}
                try:
                    name = driver.find_element(By.CSS_SELECTOR, selector).text.strip()
                    if name and len(name) > 1 and name != "Results":
                        gym_data['name'] = name
                        break
                except:
                    continue
            if 'name' not in gym_data:
                gym_data['name'] = ''

            if self.should_cancel():
                return {}

            # ADDRESS
            try:
                address_btn = driver.find_element(By.CSS_SELECTOR, "button[data-item-id='address']")
                gym_data['address'] = address_btn.text.strip()
            except:
                gym_data['address'] = ''

            if self.should_cancel():
                return {}

            # PHONE NUMBER (FIXED)
            gym_data['phone'] = ''
            try:
                # Method 1: New Google Maps structure
                phone_btn = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//button[contains(@jsaction, 'pane.wfvdle')]"))
                )
                phone_divs = phone_btn.find_elements(By.TAG_NAME, "div")
                for div in phone_divs:
                    if self.should_cancel():
                        return {}
                    text = div.text.strip()
                    if re.match(r'^\+?\d[\d\s\-]{8,}\d$', text):
                        gym_data['phone'] = text
                        break

                # Method 2: Fallback - scan visible text
                if not gym_data['phone']:
                    phone_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'fontBodyMedium') or contains(@class, 'Io6YTe')]")
                    for elem in phone_elements:
                        if self.should_cancel():
                            return {}
                        text = elem.text.strip()
                        if re.match(r'^\+?\d[\d\s\-]{8,}\d$', text):
                            gym_data['phone'] = text
                            break

                # Method 3: Last resort - page source
                if not gym_data['phone']:
                    all_text = driver.page_source
                    phone_matches = re.findall(r'\+?\d[\d\s\-]{8,}\d', all_text)
                    if phone_matches:
                        for match in phone_matches:
                            if self.should_cancel():
                                return {}
                            cleaned = re.sub(r'[^\d\+]', '', match)
                            if 10 <= len(cleaned) <= 15:
                                gym_data['phone'] = match
                                break
            except Exception as e:
                print(f"   ⚠️ Phone extraction error: {e}")

            if self.should_cancel():
                return {}

            # WEBSITE
            try:
                website_btn = driver.find_element(By.CSS_SELECTOR, "a[data-item-id='authority']")
                gym_data['website'] = website_btn.get_attribute('href')
            except:
                gym_data['website'] = ''

            if self.should_cancel():
                return {}

            # RATING
            try:
                rating_span = driver.find_element(By.CSS_SELECTOR, "div.F7nice span[aria-hidden='true']")
                rating = rating_span.text.strip()
                if re.match(r'^\d\.\d$', rating):
                    gym_data['rating'] = rating
                else:
                    gym_data['rating'] = ''
            except:
                gym_data['rating'] = ''

            if self.should_cancel():
                return {}

            # REVIEWS COUNT
            try:
                reviews_span = driver.find_element(By.CSS_SELECTOR, "div.F7nice span:last-child")
                reviews_text = reviews_span.text.strip()
                reviews_num = re.sub(r'[^\d]', '', reviews_text)
                gym_data['reviews_count'] = reviews_num if reviews_num.isdigit() else ''
            except:
                gym_data['reviews_count'] = ''

            if self.should_cancel():
                return {}

            # CATEGORY
            try:
                category_btn = driver.find_element(By.CSS_SELECTOR, "button.DkEaL")
                gym_data['category'] = category_btn.text.strip()
            except:
                gym_data['category'] = 'Gym'

            if self.should_cancel():
                return {}

            # DIRECTIONS URL
            try:
                data_match = re.search(r'data=!4m7!3m6!1s0x[^:]+:0x[^!]+!8m2!3d([-\d.]+)!4d([-\d.]+)', place_url)
                if data_match:
                    lat, lng = data_match.groups()
                    gym_data['directions_url'] = f"{place_url}&dirflg=d&travelmode=driving&ll={lat}%2C{lng}"
                else:
                    gym_data['directions_url'] = f"{place_url}&dirflg=d"
            except:
                gym_data['directions_url'] = place_url

        except (TimeoutException, NoSuchElementException, StaleElementReferenceException, InvalidSessionIdException, NoSuchWindowException) as e:
            print(f"   ❌ Data extraction failed: {e}")
            return {}

        return gym_data if gym_data.get('name') else {}

    def save_gym_csv(self, gyms, filename, gym_type, base_dir='.'):
        if not gyms:
            return 0
        os.makedirs(base_dir, exist_ok=True)
        full_path = os.path.join(base_dir, filename)
        
        gym_data = [
            {
                'name': g.get('name', ''),
                'address': g.get('address', ''),
                'phone': g.get('phone', ''),
                'website': g.get('website', ''),
                'rating': g.get('rating', ''),
                'reviews_count': g.get('reviews_count', ''),
                'category': g.get('category', gym_type.capitalize()),
                'directions_url': g.get('directions_url', '')
            }
            for g in gyms
        ]
        df = pd.DataFrame(gym_data)
        df.to_csv(full_path, index=False, encoding="utf-8-sig")
        print(f"\n💾 Saved CSV: {full_path}")
        return len(gym_data)


# Standalone function (for non-Django use)
def scrape_gym_type(gym_type, location, max_results, job_id=None):
    scraper = GymScraper(headless=True, job_id=job_id)
    return scraper.scrape_gyms_comprehensive(location, gym_type, max_results, job_id)

def close_gym_scraper_by_job_id(job_id):
    """Close a specific gym scraper instance by job ID"""
    scraper_instance = GymScraper.active_scrapers.get(job_id)
    if scraper_instance:
        try:
            scraper_instance.close_chrome_tab()
            return True
        except Exception as e:
            print(f"Error closing gym scraper for job {job_id}: {e}")
            return False
    else:
        # If instance not found in registry, try to kill by process
        try:
            import psutil
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if 'chrome' in proc.info['name'].lower():
                        cmdline = ' '.join(proc.info['cmdline'])
                        if f'job_{job_id}' in cmdline or f'jobid_{job_id}' in cmdline:
                            proc.kill()
                            proc.wait(timeout=5)
                            print(f"Killed Chrome process by cmdline for job {job_id}")
                            return True
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
                    continue
        except Exception as e:
            print(f"Error in fallback process killing: {e}")
        
        return False