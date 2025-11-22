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

class SimplifiedGoogleMapsCollegeScraper:
    # Class variable to store all active scrapers
    active_scrapers = {}

    def __init__(self, headless=False, job_id=None):
        """Initialize the simplified college scraper with job_id for cancellation"""
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
            SimplifiedGoogleMapsCollegeScraper.active_scrapers[self.job_id] = self

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

    def scrape_colleges_comprehensive(self, location, max_results=None):
        """Simplified college scraping focused on essential data only with cancellation support"""
        driver = webdriver.Chrome(options=self.options)
        self.driver = driver
        self.driver_pid = self.driver.service.process.pid
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        all_colleges = []
        all_urls = set()
        
        try:
            is_near_me = "near me" in location.lower()
            if is_near_me:
                search_terms = [
                    f"engineering colleges near me",
                    f"arts and science colleges near me",
                ]
                print("🎯 Detected 'near me' search - will limit to ~15km radius")
            else:
                search_terms = [
                    f"engineering colleges {location}",
                    f"arts and science colleges {location}",
                    f"engineering college {location}",
                    f"arts science college {location}",
                    f"technical colleges {location}",
                    f"degree colleges {location}",
                    f"autonomous colleges {location}",
                    f"university colleges {location}",
                    f"polytechnic colleges {location}",
                    f"technology colleges {location}"
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
                    
                    urls = self.enhanced_url_collection(driver, max_results)
                    new_urls = [url for url in urls if url not in all_urls]
                    all_urls.update(new_urls)
                    print(f"   Found {len(new_urls)} new college URLs")
                    
                    if max_results and len(all_urls) >= max_results:
                        break
                except Exception as e:
                    print(f"   Error with search term '{search_term}': {e}")
                    continue
            
            print(f"\n📊 Total unique college URLs collected: {len(all_urls)}")
            url_list = list(all_urls)[:max_results] if max_results else list(all_urls)
            
            for i, url in enumerate(url_list):
                if self.should_cancel():
                    print("Scraping cancelled by user - closing Chrome for this job")
                    self.close_chrome_tab()
                    return "CANCELLED"
                
                try:
                    print(f"\n📍 Processing college {i+1}/{len(url_list)}...")
                    driver.get(url)
                    time.sleep(4)
                    college_data = self.extract_complete_college_data(driver)
                    if college_data and college_data.get('name') and college_data['name'] != 'Results':
                        college_data['category'] = 'College'
                        all_colleges.append(college_data)
                        print(f"   ✅ {college_data['name']}")
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
            if self.job_id in SimplifiedGoogleMapsCollegeScraper.active_scrapers:
                del SimplifiedGoogleMapsCollegeScraper.active_scrapers[self.job_id]
            
            # Always close Chrome when done (unless already closed due to cancellation)
            if not self.is_cancelled:
                self.close_chrome_tab()
        
        return all_colleges

    def enhanced_url_collection(self, driver, target_count):
        """Enhanced URL collection with scrolling and cancellation support"""
        college_urls = []
        time.sleep(5)
        scroll_attempts = 0
        max_scroll_attempts = 10
        
        while scroll_attempts < max_scroll_attempts and (not target_count or len(college_urls) < target_count):
            if self.should_cancel():
                return college_urls  # Return what we have so far
            
            links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/maps/place/']")
            for link in links:
                if self.should_cancel():
                    return college_urls  # Return what we have so far
                
                try:
                    href = link.get_attribute('href')
                    if href and href not in college_urls:
                        college_urls.append(href)
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
        
        return college_urls

    def extract_complete_college_data(self, driver):
        """Extract essential college data from Google Maps page with cancellation support"""
        if self.should_cancel():
            return None
            
        college_data = {}
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
                        college_data['name'] = name_text
                        break
                except:
                    continue
            
            if self.should_cancel():
                return None
            
            # Address
            try:
                address = driver.find_element(By.CSS_SELECTOR, "[data-item-id='address']").text.strip()
                college_data['address'] = address
                encoded_address = urllib.parse.quote(address)
                college_data['directions_url'] = f"https://www.google.com/maps/dir/?api=1&destination={encoded_address}"
            except:
                college_data['address'] = ''
                college_data['directions_url'] = ''
            
            if self.should_cancel():
                return None
            
            # Phone
            try:
                phone = driver.find_element(By.CSS_SELECTOR, "[data-item-id*='phone']").text.strip()
                college_data['phone'] = phone
            except:
                college_data['phone'] = ''
            
            if self.should_cancel():
                return None
            
            # Website
            try:
                website = driver.find_element(By.CSS_SELECTOR, "[data-item-id='authority']").get_attribute('href')
                college_data['website'] = website
            except:
                college_data['website'] = ''
            
            if self.should_cancel():
                return None
            
            # Rating and reviews
            try:
                rating = driver.find_element(By.CSS_SELECTOR, "div.F7nice span[aria-hidden]").text.strip()
                college_data['rating'] = rating
                reviews_count = driver.find_element(By.CSS_SELECTOR, "div.F7nice span:last-child").text.strip()
                college_data['reviews_count'] = reviews_count.replace('(', '').replace(')', '')
            except:
                college_data['rating'] = ''
                college_data['reviews_count'] = ''
            
            if self.should_cancel():
                return None
            
            # Category (e.g., College Type)
            try:
                category = driver.find_element(By.CSS_SELECTOR, "button.DkEaL").text.strip()
                college_data['category'] = category
            except:
                college_data['category'] = 'College'
        except (TimeoutException, NoSuchElementException, InvalidSessionIdException, NoSuchWindowException) as e:
            print(f"   Error extracting college  {e}")
            return None
        
        return college_data

    def save_simplified_csv(self, colleges, filename):
        """Save college data to CSV"""
        if not colleges:
            return 0
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        college_data = [
            {
                'name': college.get('name', ''),
                'address': college.get('address', ''),
                'phone': college.get('phone', ''),
                'website': college.get('website', ''),
                'rating': college.get('rating', ''),
                'reviews_count': college.get('reviews_count', ''),
                'category': college.get('category', 'College'),
                'directions_url': college.get('directions_url', '')
            }
            for college in colleges
        ]
        df = pd.DataFrame(college_data)
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"\n🔗 CSV FILE LOCATION: {filename}")
        return len(college_data)

def scrape_college(location, max_results, job_id=None):
    scraper = SimplifiedGoogleMapsCollegeScraper(headless=True, job_id=job_id)
    return scraper.scrape_colleges_comprehensive(location, max_results)

def close_college_scraper_by_job_id(job_id):
    """Close a specific college scraper instance by job ID"""
    scraper_instance = SimplifiedGoogleMapsCollegeScraper.active_scrapers.get(job_id)
    if scraper_instance:
        try:
            scraper_instance.close_chrome_tab()
            return True
        except Exception as e:
            print(f"Error closing college scraper for job {job_id}: {e}")
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