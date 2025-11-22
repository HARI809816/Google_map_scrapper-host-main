import time
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, InvalidSessionIdException, NoSuchWindowException
from selenium.webdriver.common.keys import Keys
import pandas as pd
import os
import urllib.parse
from django.core.cache import cache
import signal
import psutil
import atexit
import threading
import json

class SimplifiedGoogleMapsEbikeShowroomScraper:
    # Class variable to store all active scrapers
    active_scrapers = {}

    def __init__(self, headless=True, job_id=None):
        """Initialize the simplified e-bike showroom scraper with job_id for cancellation"""
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
            SimplifiedGoogleMapsEbikeShowroomScraper.active_scrapers[self.job_id] = self

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

    def scrape_showrooms_comprehensive(self, location, max_results=None):
        """Simplified e-bike showroom scraping focused on essential data only with cancellation support"""
        driver = webdriver.Chrome(options=self.options)
        self.driver = driver
        self.driver_pid = self.driver.service.process.pid
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        all_showrooms = []
        all_urls = set()
        
        try:
            is_near_me = "near me" in location.lower()
            if is_near_me:
                search_terms = [
                    f"electric bike showroom near me",
                    f"ebike dealer near me",
                    f"electric scooter showroom near me",
                    f"e bike store near me",
                    f"electric vehicle showroom near me"
                ]
                print("🎯 Detected 'near me' search - will limit to ~15km radius")
            else:
                search_terms = [
                    f"electric bike showroom {location}",
                    f"ebike dealer {location}",
                    f"electric scooter showroom {location}",
                    f"e bike store {location}",
                    f"electric vehicle dealer {location}",
                    f"electric motorcycle showroom {location}",
                    f"battery bike showroom {location}",
                    f"ev showroom {location}",
                    f"electric two wheeler dealer {location}",
                    f"ebike shop {location}"
                ]
            
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
                    
                    urls = self.enhanced_url_collection(driver, max_results, is_near_me)
                    new_urls = [url for url in urls if url not in all_urls]
                    all_urls.update(new_urls)
                    print(f"   Found {len(new_urls)} new showroom URLs")
                    
                    if max_results and len(all_urls) >= max_results:
                        break
                except Exception as e:
                    print(f"   Error with search term '{search_term}': {e}")
                    continue
            
            print(f"\n📊 Total unique showroom URLs collected: {len(all_urls)}")
            url_list = list(all_urls)[:max_results] if max_results else list(all_urls)
            
            for i, url in enumerate(url_list):
                if self.should_cancel():
                    print("Scraping cancelled by user - closing Chrome for this job")
                    self.close_chrome_tab()
                    return "CANCELLED"
                
                try:
                    print(f"\n📍 Processing showroom {i+1}/{len(url_list)}...")
                    driver.get(url)
                    time.sleep(4)
                    showroom_data = self.extract_complete_showroom_data(driver)
                    if showroom_data and showroom_data.get('name') and showroom_data['name'] != 'Results':
                        showroom_data['category'] = 'E-Bike Showroom'
                        all_showrooms.append(showroom_data)
                        print(f"   ✅ {showroom_data['name']}")
                except Exception as e:
                    print(f"   Error processing URL {url}: {e}")
                    continue
        
        except (InvalidSessionIdException, NoSuchWindowException) as e:
            # Handle the case where the driver has been closed
            print(f"Driver session ended unexpectedly: {e}")
            return "CANCELLED"
        except Exception as e:
            print(f"Unexpected error during scraping: {e}")
            return []
        
        finally:
            # Remove this instance from global registry
            if self.job_id in SimplifiedGoogleMapsEbikeShowroomScraper.active_scrapers:
                del SimplifiedGoogleMapsEbikeShowroomScraper.active_scrapers[self.job_id]
            
            # Always close Chrome when done (unless already closed due to cancellation)
            if not self.is_cancelled:
                self.close_chrome_tab()
        
        return all_showrooms

    def enhanced_url_collection(self, driver, target_count, is_near_me):
        """Enhanced URL collection with scrolling and progress tracking with cancellation support"""
        showroom_urls = []
        time.sleep(5)
        scroll_attempts = 0
        max_scroll_attempts = 10
        
        while scroll_attempts < max_scroll_attempts and (not target_count or len(showroom_urls) < target_count):
            if self.should_cancel():
                return showroom_urls  # Return what we have so far
            
            links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/maps/place/']")
            for link in links:
                if self.should_cancel():
                    return showroom_urls  # Return what we have so far
                
                try:
                    href = link.get_attribute('href')
                    if href and href not in showroom_urls:
                        showroom_urls.append(href)
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
        
        return showroom_urls

    def extract_complete_showroom_data(self, driver):
        """Extract essential showroom data from Google Maps page with cancellation support"""
        if self.should_cancel():
            return None
            
        showroom_data = {}
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            time.sleep(3)
            
            if self.should_cancel():
                return None
            
            # Name
            name_selectors = ["h1.DUwDvf", "h1"]
            for selector in name_selectors:
                if self.should_cancel():
                    return None
                try:
                    name_element = driver.find_element(By.CSS_SELECTOR, selector)
                    name_text = name_element.text.strip()
                    if name_text and len(name_text) > 1:
                        showroom_data['name'] = name_text
                        break
                except:
                    continue
            
            if self.should_cancel():
                return None
            
            # Address
            try:
                address = driver.find_element(By.CSS_SELECTOR, "[data-item-id='address']").text.strip()
                showroom_data['address'] = address
                encoded_address = urllib.parse.quote(address)
                showroom_data['directions_url'] = f"https://www.google.com/maps/dir/?api=1&destination={encoded_address}"
            except:
                showroom_data['address'] = ''
                showroom_data['directions_url'] = ''
            
            if self.should_cancel():
                return None
            
            # Phone
            try:
                phone = driver.find_element(By.CSS_SELECTOR, "[data-item-id*='phone']").text.strip()
                showroom_data['phone'] = phone
            except:
                showroom_data['phone'] = ''
            
            if self.should_cancel():
                return None
            
            # Website
            try:
                website = driver.find_element(By.CSS_SELECTOR, "[data-item-id='authority']").get_attribute('href')
                showroom_data['website'] = website
            except:
                showroom_data['website'] = ''
            
            if self.should_cancel():
                return None
            
            # Rating and reviews
            try:
                rating = driver.find_element(By.CSS_SELECTOR, "div.F7nice span[aria-hidden]").text.strip()
                showroom_data['rating'] = rating
                reviews_count = driver.find_element(By.CSS_SELECTOR, "div.F7nice span:last-child").text.strip()
                showroom_data['reviews_count'] = reviews_count.replace('(', '').replace(')', '')
            except:
                showroom_data['rating'] = ''
                showroom_data['reviews_count'] = ''
        except (TimeoutException, NoSuchElementException, InvalidSessionIdException, NoSuchWindowException) as e:
            print(f"   Error extracting showroom  {e}")
            return None
        
        return showroom_data

    def save_simplified_csv(self, showrooms, filename):
        """Save showroom data to CSV"""
        if not showrooms:
            return 0
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        showroom_data = [
            {
                'name': showroom.get('name', ''),
                'address': showroom.get('address', ''),
                'phone': showroom.get('phone', ''),
                'website': showroom.get('website', ''),
                'rating': showroom.get('rating', ''),
                'reviews_count': showroom.get('reviews_count', ''),
                'category': showroom.get('category', 'E-Bike Showroom'),
                'directions_url': showroom.get('directions_url', '')
            }
            for showroom in showrooms
        ]
        df = pd.DataFrame(showroom_data)
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"\n🔗 CSV FILE LOCATION: {filename}")
        return len(showroom_data)

    def to_dict(self, showrooms):
        """Convert showrooms list to a list of dictionaries for Django"""
        return [
            {
                'name': showroom.get('name', ''),
                'address': showroom.get('address', ''),
                'phone': showroom.get('phone', ''),
                'website': showroom.get('website', ''),
                'rating': showroom.get('rating', ''),
                'reviews_count': showroom.get('reviews_count', ''),
                'category': showroom.get('category', 'E-Bike Showroom'),
                'directions_url': showroom.get('directions_url', '')
            }
            for showroom in showrooms
        ]

def scrape_ebike(location, max_results, csv_filename="showrooms.csv", job_id=None):
    scraper = SimplifiedGoogleMapsEbikeShowroomScraper(headless=True, job_id=job_id)
    showrooms = scraper.scrape_showrooms_comprehensive(location, max_results)
    if showrooms != "CANCELLED":
        scraper.save_simplified_csv(showrooms, csv_filename)
        return scraper.to_dict(showrooms)
    else:
        return []

def close_ebike_scraper_by_job_id(job_id):
    """Close a specific ebike scraper instance by job ID"""
    scraper_instance = SimplifiedGoogleMapsEbikeShowroomScraper.active_scrapers.get(job_id)
    if scraper_instance:
        try:
            scraper_instance.close_chrome_tab()
            return True
        except Exception as e:
            print(f"Error closing ebike scraper for job {job_id}: {e}")
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