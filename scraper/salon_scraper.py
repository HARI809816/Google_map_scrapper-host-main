import time
import csv
import random
import logging
import re
from urllib.parse import urlparse, parse_qs
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, ElementClickInterceptedException, InvalidSessionIdException, NoSuchWindowException
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

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EnhancedGoogleMapsScraper:
    # Class variable to store all active scrapers
    active_scrapers = {}

    def __init__(self, headless=False, job_id=None):
        """Initialize the enhanced scraper with job_id for cancellation"""
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
        
        self.logger = logging.getLogger(__name__)
        self.job_id = job_id  # Store job_id for cancellation checks
        self.driver = None
        self.driver_pid = None
        self.is_cancelled = False
        self.chrome_start_time = time.time()
        
        # Register this instance globally
        if self.job_id:
            EnhancedGoogleMapsScraper.active_scrapers[self.job_id] = self

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
        self.logger.info(f"Attempting to close Chrome for job {self.job_id}")
        
        # Mark as cancelled
        self.is_cancelled = True
        
        # First, try to close the driver normally
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info(f"Chrome driver closed normally for job {self.job_id}")
                return True
            except Exception as e:
                self.logger.error(f"Error closing Chrome driver normally: {e}")
        
        # If driver closing failed, try to kill the process
        success = self.kill_chrome_process()
        if success:
            self.logger.info(f"Chrome process killed for job {self.job_id}")
        
        # Clean up user data directory
        try:
            import shutil
            shutil.rmtree(self.user_data_dir, ignore_errors=True)
        except:
            pass
        
        return success

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
                            self.logger.info(f"Killed Chrome process (PID: {proc.info['pid']}) for job {self.job_id}")
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
                                self.logger.info(f"Killed Chrome process (PID: {proc.info['pid']}) by time for job {self.job_id}")
                                return True
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
                    continue
            
            return False
        except Exception as e:
            self.logger.error(f"Error in kill_chrome_process: {e}")
            return False

    def scrape_salons_comprehensive(self, location, max_results=25):
        """Comprehensive salon scraping with multiple strategies and cancellation support"""
        # Initialize driver
        try:
            self.driver = webdriver.Chrome(options=self.options)
            self.driver_pid = self.driver.service.process.pid
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.logger.info(f"Chrome driver started with PID {self.driver_pid} for job {self.job_id}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome driver: {e}")
            return []
        
        all_salons = []
        all_urls = set()
        
        try:
            search_terms = [
                f"hair salons {location}",
                f"beauty salons {location}",
                f"unisex salons {location}",
                f"hair cutting {location}",
                f"barber shops {location}",
                f"beauty parlour {location}"
            ]
            
            for search_term in search_terms:
                if self.should_cancel():
                    self.logger.info("Scraping cancelled by user - closing Chrome for this job")
                    self.close_chrome_tab()
                    return "CANCELLED"
                
                try:
                    self.logger.info(f"Searching: {search_term}")
                    self.driver.get("https://www.google.com/maps")
                    time.sleep(random.uniform(2, 5))  # Random delay
                    
                    if self.should_cancel():
                        self.logger.info("Scraping cancelled by user - closing Chrome for this job")
                        self.close_chrome_tab()
                        return "CANCELLED"
                    
                    search_box = WebDriverWait(self.driver, 15).until(
                        EC.element_to_be_clickable((By.ID, "searchboxinput"))
                    )
                    search_box.clear()
                    search_box.send_keys(search_term)
                    search_box.send_keys(Keys.ENTER)
                    time.sleep(random.uniform(3, 6))
                    
                    if self.should_cancel():
                        self.logger.info("Scraping cancelled by user - closing Chrome for this job")
                        self.close_chrome_tab()
                        return "CANCELLED"
                    
                    urls = self.enhanced_url_collection(self.driver, max_results)
                    new_urls = [url for url in urls if url not in all_urls]
                    all_urls.update(new_urls)
                    self.logger.info(f"Found {len(new_urls)} new salon URLs")
                    
                    if len(all_urls) >= max_results:
                        break
                except (TimeoutException, NoSuchElementException, WebDriverException) as e:
                    self.logger.error(f"Error with search term '{search_term}': {e}")
                    continue
            
            self.logger.info(f"Total unique salon URLs collected: {len(all_urls)}")
            url_list = list(all_urls)[:max_results]
            
            for i, url in enumerate(url_list):
                if self.should_cancel():
                    self.logger.info("Scraping cancelled by user - closing Chrome for this job")
                    self.close_chrome_tab()
                    return "CANCELLED"
                
                try:
                    self.logger.info(f"Processing salon {i+1}/{len(url_list)}: {url}")
                    self.driver.get(url)
                    time.sleep(random.uniform(3, 6))
                    
                    # Check cancellation again before extracting data
                    if self.should_cancel():
                        self.logger.info("Scraping cancelled by user - closing Chrome for this job")
                        self.close_chrome_tab()
                        return "CANCELLED"
                    
                    salon_data = self.extract_complete_salon_data(self.driver)
                    if salon_data and salon_data.get('name') and salon_data['name'] != 'Results':
                        salon_data['category'] = 'Salon'
                        all_salons.append(salon_data)
                        self.logger.info(f"Successfully extracted: {salon_data['name']}")
                except (TimeoutException, WebDriverException) as e:
                    self.logger.error(f"Error processing URL {url}: {e}")
                    continue
        
        except (InvalidSessionIdException, NoSuchWindowException) as e:
            # Handle the case where the driver has been closed
            self.logger.warning(f"Driver session ended unexpectedly: {e}")
            return "CANCELLED"
        except Exception as e:
            self.logger.error(f"Unexpected error during scraping: {e}")
            return []
        
        finally:
            # Remove this instance from global registry
            if self.job_id in EnhancedGoogleMapsScraper.active_scrapers:
                del EnhancedGoogleMapsScraper.active_scrapers[self.job_id]
            
            # Always close Chrome when done (unless already closed due to cancellation)
            if not self.is_cancelled:
                self.close_chrome_tab()
        
        return all_salons

    def enhanced_url_collection(self, driver, target_count):
        """Enhanced URL collection with dynamic scrolling and cancellation support"""
        salon_urls = set()
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[role='main']"))
            )
            time.sleep(random.uniform(2, 5))
            results_panel = driver.find_element(By.CSS_SELECTOR, "[role='main']")
            last_height = driver.execute_script("return arguments[0].scrollHeight", results_panel)
            
            while len(salon_urls) < target_count:
                if self.should_cancel():
                    return list(salon_urls)  # Return what we have so far
                
                links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/maps/place/']")
                for link in links:
                    if self.should_cancel():
                        return list(salon_urls)
                    
                    try:
                        href = link.get_attribute('href')
                        if href and href not in salon_urls:
                            salon_urls.add(href)
                    except:
                        continue
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", results_panel)
                time.sleep(random.uniform(1, 3))
                
                if self.should_cancel():
                    return list(salon_urls)
                
                new_height = driver.execute_script("return arguments[0].scrollHeight", results_panel)
                if new_height == last_height:
                    break
                last_height = new_height
        except (NoSuchElementException, TimeoutException, InvalidSessionIdException, NoSuchWindowException) as e:
            self.logger.error(f"Error during URL collection: {e}")
            # If driver is no longer available, return what we have
            return list(salon_urls)
        
        return list(salon_urls)

    def extract_complete_salon_data(self, driver):
        """Extract essential salon data from Google Maps page with cancellation support"""
        if self.should_cancel():
            return None
            
        salon_data = {}
        try:
            # Wait for the main element to be present
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            time.sleep(random.uniform(2, 5))
            
            if self.should_cancel():
                return None
            
            # Name
            name_selectors = ["h1.DUwDvf", "h1", "div[role='main'] h1"]
            for selector in name_selectors:
                if self.should_cancel():
                    return None
                try:
                    name_element = driver.find_element(By.CSS_SELECTOR, selector)
                    name_text = name_element.text.strip()
                    if name_text and len(name_text) > 1:
                        salon_data['name'] = name_text
                        break
                except NoSuchElementException:
                    continue
            
            if self.should_cancel():
                return None
            
            # Address
            address_selectors = ["[data-item-id='address']", ".rogA2c"]
            for selector in address_selectors:
                if self.should_cancel():
                    return None
                try:
                    address = driver.find_element(By.CSS_SELECTOR, selector).text.strip()
                    salon_data['address'] = address
                    encoded_address = urllib.parse.quote(address)
                    salon_data['directions_url'] = f"https://www.google.com/maps/dir/?api=1&destination={encoded_address}"
                    break
                except NoSuchElementException:
                    salon_data['address'] = ''
                    salon_data['directions_url'] = ''
            
            if self.should_cancel():
                return None
            
            # Phone
            try:
                phone = driver.find_element(By.CSS_SELECTOR, "[data-item-id*='phone']").text.strip()
                salon_data['phone'] = phone
            except NoSuchElementException:
                salon_data['phone'] = ''
            
            if self.should_cancel():
                return None
            
            # Website
            try:
                website_element = driver.find_element(By.CSS_SELECTOR, "[data-item-id='authority']")
                aria_label = website_element.get_attribute('aria-label') or ''
                if 'book' in aria_label.lower() or 'reserve' in aria_label.lower() or 'appointment' in aria_label.lower():
                    self.logger.warning(f"Skipping booking link: {aria_label}")
                    salon_data['website'] = ''
                else:
                    href = website_element.get_attribute('href')
                    if 'google.com/url' in href:
                        parsed = urlparse(href)
                        query = parse_qs(parsed.query)
                        website = query.get('q', [''])[0]
                    else:
                        website = href
                    salon_data['website'] = website
                    self.logger.info(f"Found valid website: {website}")
            except NoSuchElementException:
                salon_data['website'] = ''
                self.logger.warning("No valid website found for this salon")
            
            if self.should_cancel():
                return None
            
            # Rating and reviews
            try:
                rating = driver.find_element(By.CSS_SELECTOR, "div.F7nice span[aria-hidden]").text.strip()
                salon_data['rating'] = rating
                reviews_count = driver.find_element(By.CSS_SELECTOR, "div.F7nice span:last-child").text.strip()
                salon_data['reviews_count'] = reviews_count.replace('(', '').replace(')', '')
            except NoSuchElementException:
                salon_data['rating'] = ''
                salon_data['reviews_count'] = ''
            
            if self.should_cancel():
                return None
            
            # Category
            try:
                category = driver.find_element(By.CSS_SELECTOR, "button.DkEaL").text.strip()
                salon_data['category'] = category
            except NoSuchElementException:
                salon_data['category'] = 'Salon'
        
        except (TimeoutException, WebDriverException, InvalidSessionIdException, NoSuchWindowException) as e:
            self.logger.error(f"Error extracting salon data: {e}")
            return None
        
        return salon_data

    def save_simplified_csv(self, salons, filename, base_dir='output'):
        """Save salon data to CSV in the specified base directory"""
        if not salons:
            self.logger.warning("No salon data to save")
            return 0
        # Ensure filename ends with .csv
        if not filename.endswith('.csv'):
            filename += '.csv'
        # Construct full path
        full_path = os.path.join(base_dir, filename)
        # Create directory if it doesn't exist
        try:
            os.makedirs(base_dir, exist_ok=True)
        except OSError as e:
            self.logger.error(f"Failed to create directory {base_dir}: {e}")
            return 0
        salon_data = [
            {
                'name': salon.get('name', ''),
                'address': salon.get('address', ''),
                'phone': salon.get('phone', ''),
                'website': salon.get('website', ''),
                'rating': salon.get('rating', ''),
                'reviews_count': salon.get('reviews_count', ''),
                'category': salon.get('category', 'Salon'),
                'directions_url': salon.get('directions_url', '')
            }
            for salon in salons
        ]
        try:
            df = pd.DataFrame(salon_data)
            df.to_csv(full_path, index=False, encoding="utf-8-sig")
            self.logger.info(f"CSV saved to: {full_path}")
            return len(salon_data)
        except Exception as e:
            self.logger.error(f"Failed to save CSV to {full_path}: {e}")
            return 0

def scrape_salon(location, max_results, job_id=None):
    scraper = EnhancedGoogleMapsScraper(headless=False, job_id=job_id)  # Set to False to see Chrome
    salons = scraper.scrape_salons_comprehensive(location, max_results)
    return salons 

def close_scraper_by_job_id(job_id):
    """Close a specific scraper instance by job ID"""
    scraper_instance = EnhancedGoogleMapsScraper.active_scrapers.get(job_id)
    if scraper_instance:
        try:
            success = scraper_instance.close_chrome_tab()
            return success
        except Exception as e:
            print(f"Error closing scraper for job {job_id}: {e}")
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