import time
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, InvalidSessionIdException, NoSuchWindowException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
import re
import os
import urllib.parse
import threading
from django.core.cache import cache
import signal
import psutil
import atexit
import json

class BusinessScraper:
    # Class variable to store all active scrapers
    active_scrapers = {}

    def __init__(self, headless=False, job_id=None):
        """Initialize the business scraper with job_id for cancellation"""
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
            BusinessScraper.active_scrapers[self.job_id] = self

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

    def scrape_businesses_comprehensive(self, location, business_type, max_results=25):
        """Comprehensive business scraping with cancellation support"""
        driver = webdriver.Chrome(options=self.options)
        self.driver = driver
        self.driver_pid = self.driver.service.process.pid
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        all_businesses = []
        all_urls = set()
        
        try:
            # Define search terms based on business type
            search_terms = self.get_search_terms(business_type, location)
            
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
                    
                    print(f"   Found {len(new_urls)} new business URLs")
                    
                    if len(all_urls) >= max_results:
                        break
                        
                except Exception as e:
                    print(f"   Error with search term '{search_term}': {e}")
                    continue
            
            print(f"\n📊 Total unique business URLs collected: {len(all_urls)}")
            url_list = list(all_urls)[:max_results]
            
            for i, url in enumerate(url_list):
                if self.should_cancel():
                    print("Scraping cancelled by user - closing Chrome for this job")
                    self.close_chrome_tab()
                    return "CANCELLED"
                    
                try:
                    print(f"\n📍 Processing business {i+1}/{len(url_list)}...")
                    driver.get(url)
                    time.sleep(4)
                    business_data = self.extract_complete_business_data(driver, url)
                    if business_data and business_data.get('name') and business_data['name'] != 'Results':
                        business_data['category'] = business_type.capitalize()
                        all_businesses.append(business_data)
                        print(f"   ✅ {business_data['name']}")
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
            if self.job_id in BusinessScraper.active_scrapers:
                del BusinessScraper.active_scrapers[self.job_id]
            
            # Always close Chrome when done (unless already closed due to cancellation)
            if not self.is_cancelled:
                self.close_chrome_tab()
        
        return all_businesses

    def get_search_terms(self, business_type, location):
        """Get search terms based on business type"""
        base_terms = {
            'startup': [
                f"startup companies {location}",
                f"tech startups {location}",
            ],
            'manufacturing': [
                f"manufacturing companies {location}",
                f"industrial units {location}",
            ],
            'consultant': [
                f"business consultants {location}",
                f"management consultants {location}",
            ]
        }
        
        return base_terms.get(business_type, [f"{business_type} {location}"])

    def enhanced_url_collection(self, driver, target_count):
        """Enhanced URL collection with scrolling and cancellation support"""
        business_urls = []
        time.sleep(5)
        
        scroll_attempts = 0
        max_scroll_attempts = 10
        
        while scroll_attempts < max_scroll_attempts and len(business_urls) < target_count and not self.should_cancel():
            links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/maps/place/']")
            
            for link in links:
                if self.should_cancel():
                    return business_urls  # Return what we have so far
                    
                try:
                    href = link.get_attribute('href')
                    if href and href not in business_urls:
                        business_urls.append(href)
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
        
        return business_urls
    
    def extract_complete_business_data(self, driver, place_url):
        """Extract business data from Google Maps page with cancellation support"""
        if self.should_cancel():
            return {}
            
        business_data = {}
        
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            time.sleep(3)
            
            if self.should_cancel():
                return {}
            
            # Extract name
            name_selectors = ["h1.DUwDvf", "h1"]
            for selector in name_selectors:
                if self.should_cancel():
                    return {}
                try:
                    name_element = driver.find_element(By.CSS_SELECTOR, selector)
                    name_text = name_element.text.strip()
                    if name_text and len(name_text) > 1:
                        business_data['name'] = name_text
                        break
                except:
                    continue
            
            if self.should_cancel():
                return {}
            
            # Address
            try:
                address = driver.find_element(By.CSS_SELECTOR, "[data-item-id='address']").text.strip()
                business_data['address'] = address
                encoded_address = urllib.parse.quote(address)
                business_data['directions_url'] = f"https://www.google.com/maps/dir/?api=1&destination={encoded_address}"
            except:
                business_data['address'] = ''
                business_data['directions_url'] = ''
            
            if self.should_cancel():
                return {}
            
            # Phone
            try:
                phone = driver.find_element(By.CSS_SELECTOR, "[data-item-id*='phone']").text.strip()
                business_data['phone'] = phone
            except:
                business_data['phone'] = ''
            
            if self.should_cancel():
                return {}
            
            # Website
            try:
                website = driver.find_element(By.CSS_SELECTOR, "[data-item-id='authority']").get_attribute('href')
                business_data['website'] = website
            except:
                business_data['website'] = ''
            
            if self.should_cancel():
                return {}
            
            # Rating and reviews
            try:
                rating = driver.find_element(By.CSS_SELECTOR, "div.F7nice span[aria-hidden]").text.strip()
                business_data['rating'] = rating
                reviews_count = driver.find_element(By.CSS_SELECTOR, "div.F7nice span:last-child").text.strip()
                business_data['reviews_count'] = reviews_count.replace('(', '').replace(')', '')
            except:
                business_data['rating'] = ''
                business_data['reviews_count'] = ''
            
            if self.should_cancel():
                return {}
            
            # Additional fields (email, hours, etc.)
            business_data['email'] = ''
            try:
                email_elements = driver.find_elements(By.CSS_SELECTOR, 'a[href^="mailto:"]')
                if email_elements:
                    href = email_elements[0].get_attribute('href')
                    business_data['email'] = href.replace('mailto:', '').strip()
                else:
                    page_text = driver.page_source
                    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                    email_matches = re.findall(email_pattern, page_text)
                    if email_matches:
                        business_data['email'] = email_matches[0]
            except:
                pass
            
            if self.should_cancel():
                return {}
            
            business_data['hours'] = ''
            try:
                hours_selectors = [
                    "button[data-item-id='oh'] .Io6YTe",
                    "button[aria-label*='Hours'] .fontBodyMedium",
                    ".t39EBf .G8aQO"
                ]
                for selector in hours_selectors:
                    if self.should_cancel():
                        return {}
                    try:
                        hours_element = driver.find_element(By.CSS_SELECTOR, selector)
                        hours_text = hours_element.text.strip()
                        if hours_text:
                            business_data['hours'] = hours_text
                            break
                    except:
                        continue
            except:
                pass
            
            if self.should_cancel():
                return {}
            
            category_selectors = ["button[jsaction*='pane.category'] .DkEaL", ".DkEaL", ".YhemCb"]
            for selector in category_selectors:
                if self.should_cancel():
                    return {}
                try:
                    category_element = driver.find_element(By.CSS_SELECTOR, selector)
                    category_text = category_element.text.strip()
                    if category_text and len(category_text) < 100:
                        business_data['category'] = category_text
                        break
                except:
                    continue
            if 'category' not in business_data:
                business_data['category'] = ''
            
            if self.should_cancel():
                return {}
            
            try:
                data_match = re.search(r'data=!4m7!3m6!1s0x[^:]+:0x[^!]+!8m2!3d([-\d.]+)!4d([-\d.]+)', place_url)
                if data_match:
                    lat, lng = data_match.groups()
                    business_data['directions_url'] = f"{place_url}&dirflg=d&travelmode=driving&ll={lat}%2C{lng}"
                else:
                    business_data['directions_url'] = f"{place_url}&dirflg=d"
            except:
                business_data['directions_url'] = place_url

        except (TimeoutException, NoSuchElementException, InvalidSessionIdException, NoSuchWindowException) as e:
            print(f"   Error extracting business  {e}")
            return {}

        return business_data if business_data.get('name') else {}

    def save_simplified_csv(self, businesses, filename, business_type=None, base_dir='.'):
        if not businesses:
            return 0
        os.makedirs(base_dir, exist_ok=True)
        full_path = os.path.join(base_dir, filename)
        
        business_data = [
            {
                'name': g.get('name', ''),
                'address': g.get('address', ''),
                'phone': g.get('phone', ''),
                'email': g.get('email', ''),
                'website': g.get('website', ''),
                'rating': g.get('rating', ''),
                'reviews_count': g.get('reviews_count', ''),
                'hours': g.get('hours', ''),
                'category': g.get('category', business_type.capitalize() if business_type else ''),
                'directions_url': g.get('directions_url', '')
            }
            for g in businesses
        ]
        df = pd.DataFrame(business_data)
        df.to_csv(full_path, index=False, encoding="utf-8-sig")
        print(f"\n💾 Saved CSV: {full_path}")
        return len(business_data)

    def save_business_csv(self, businesses, filename, business_type):
        """Wrapper for save_simplified_csv to match views.py call"""
        return self.save_simplified_csv(businesses, filename, business_type)

# Standalone function (for non-Django use)
def scrape_business_type(business_type, location, max_results, job_id=None):
    scraper = BusinessScraper(headless=True, job_id=job_id)
    return scraper.scrape_businesses_comprehensive(location, business_type, max_results)

def close_business_scraper_by_job_id(job_id):
    """Close a specific business scraper instance by job ID"""
    scraper_instance = BusinessScraper.active_scrapers.get(job_id)
    if scraper_instance:
        try:
            scraper_instance.close_chrome_tab()
            return True
        except Exception as e:
            print(f"Error closing business scraper for job {job_id}: {e}")
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