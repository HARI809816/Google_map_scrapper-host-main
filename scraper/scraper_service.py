import csv
import os
import time
import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from django.conf import settings
from .models import ScrapeJob, Gym

class DjangoGymScraper:
    def __init__(self, headless=True):
        """Initialize the gym scraper for Django"""
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
        
        # Setup Chrome driver with webdriver-manager
        service = Service(ChromeDriverManager().install())
        self.driver = None
        self.service = service
        
    def update_job_progress(self, job, progress, message=""):
        """Update job progress"""
        job.progress = progress
        if message:
            job.error_message = message
        job.save()
        
    def scrape_gyms_for_job(self, job_id):
        """Main scraping function for Django integration"""
        try:
            job = ScrapeJob.objects.get(id=job_id)
            job.status = 'running'
            job.save()
            
            self.driver = webdriver.Chrome(service=self.service, options=self.options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            all_gyms = []
            all_urls = set()
            
            # Get search terms
            if job.gym_type == 'all':
                gym_types = ['gym', 'crossfit', 'yoga', 'pilates', 'martial_arts', 'swimming']
            else:
                gym_types = [job.gym_type]
            
            total_types = len(gym_types)
            current_type = 0
            
            for gym_type in gym_types:
                current_type += 1
                self.update_job_progress(job, (current_type - 1) * 100 // total_types, f"Processing {gym_type}")
                
                search_terms = self.get_gym_search_terms(gym_type, job.location)
                
                for search_term in search_terms:
                    try:
                        self.driver.get("https://www.google.com/maps")
                        time.sleep(3)
                        
                        search_box = WebDriverWait(self.driver, 15).until(
                            EC.element_to_be_clickable((By.ID, "searchboxinput"))
                        )
                        search_box.clear()
                        search_box.send_keys(search_term)
                        search_box.send_keys(Keys.ENTER)
                        time.sleep(5)
                        
                        urls = self.enhanced_url_collection(self.driver, job.max_results)
                        new_urls = [url for url in urls if url not in all_urls]
                        all_urls.update(new_urls)
                        
                        if len(all_urls) >= job.max_results:
                            break
                            
                    except Exception as e:
                        continue
                
                # Extract data from URLs
                url_list = list(all_urls)[:job.max_results]
                
                for i, url in enumerate(url_list):
                    try:
                        progress = ((current_type - 1) + (i + 1) / len(url_list)) * 100 // total_types
                        self.update_job_progress(job, progress, f"Processing gym {i+1}/{len(url_list)} for {gym_type}")
                        
                        self.driver.get(url)
                        time.sleep(4)
                        
                        gym_data = self.extract_complete_gym_data(self.driver)
                        
                        if gym_data and gym_data.get('name') and gym_data['name'] != 'Results':
                            # Save to database
                            gym_obj = Gym.objects.create(
                                scrape_job=job,
                                name=gym_data.get('name', ''),
                                address=gym_data.get('address', ''),
                                phone=gym_data.get('phone', ''),
                                email=gym_data.get('email', ''),
                                website=gym_data.get('website', ''),
                                rating=gym_data.get('rating', ''),
                                reviews_count=gym_data.get('reviews_count', ''),
                                category=gym_data.get('category', ''),
                                gym_type=gym_type,
                                hours=gym_data.get('hours', ''),
                                description=gym_data.get('description', ''),
                                membership_fee=gym_data.get('membership_fee', ''),
                                facilities=gym_data.get('facilities', ''),
                                equipment=gym_data.get('equipment', ''),
                                classes_offered=gym_data.get('classes_offered', ''),
                                trainers_available=gym_data.get('trainers_available', ''),
                                parking_available=gym_data.get('parking_available', ''),
                                locker_rooms=gym_data.get('locker_rooms', ''),
                                shower_facilities=gym_data.get('shower_facilities', ''),
                                air_conditioning=gym_data.get('air_conditioning', ''),
                                accessibility=gym_data.get('accessibility', ''),
                                group_classes=gym_data.get('group_classes', '')
                            )
                            all_gyms.append(gym_obj)
                        
                    except Exception as e:
                        continue
            
            # Generate CSV file
            csv_filename = self.generate_csv_file(job, all_gyms)
            if csv_filename:
                job.csv_file.name = csv_filename
            
            job.total_found = len(all_gyms)
            job.status = 'completed'
            job.progress = 100
            job.save()
            
            return len(all_gyms)
            
        except Exception as e:
            job.status = 'failed'
            job.error_message = str(e)
            job.save()
            return 0
        finally:
            if self.driver:
                self.driver.quit()
    
    def generate_csv_file(self, job, gyms):
        """Generate CSV file from scraped data"""
        if not gyms:
            return None
            
        filename = f'{job.gym_type}_{job.location.replace(" ", "_").replace(",", "").lower()}_gyms.csv'
        file_path = os.path.join(settings.MEDIA_ROOT, 'csv_files', filename)
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Convert gym objects to dictionaries
        gym_data = []
        for gym in gyms:
            gym_dict = {
                'name': gym.name,
                'address': gym.address,
                'phone': gym.phone,
                'email': gym.email,
                'website': gym.website,
                'rating': gym.rating,
                'reviews_count': gym.reviews_count,
                'category': gym.category,
                'gym_type': gym.gym_type,
                'hours': gym.hours,
                'description': gym.description,
                'membership_fee': gym.membership_fee,
                'facilities': gym.facilities,
                'equipment': gym.equipment,
                'classes_offered': gym.classes_offered,
                'trainers_available': gym.trainers_available,
                'parking_available': gym.parking_available,
                'locker_rooms': gym.locker_rooms,
                'shower_facilities': gym.shower_facilities,
                'air_conditioning': gym.air_conditioning,
                'accessibility': gym.accessibility,
                'group_classes': gym.group_classes,
            }
            gym_data.append(gym_dict)
        
        # Create CSV
        df = pd.DataFrame(gym_data)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        
        return f'csv_files/{filename}'
    
    def get_gym_search_terms(self, gym_type, location):
        """Get search terms based on gym type"""
        base_terms = {
            'gym': [
                f"gym {location}",
                f"fitness center {location}",
                f"health club {location}",
            ],
            'crossfit': [
                f"crossfit gym {location}",
                f"crossfit box {location}",
            ],
            'yoga': [
                f"yoga studio {location}",
                f"yoga center {location}",
            ],
            'pilates': [
                f"pilates studio {location}",
            ],
            'martial_arts': [
                f"martial arts {location}",
                f"boxing gym {location}",
            ],
            'swimming': [
                f"swimming pool {location}",
                f"aquatic center {location}",
            ]
        }
        
        return base_terms.get(gym_type, [f"{gym_type} {location}"])
    
    def enhanced_url_collection(self, driver, target_count):
        """Enhanced URL collection with scrolling"""
        gym_urls = []
        time.sleep(5)
        
        scroll_attempts = 0
        max_scroll_attempts = 10
        
        while scroll_attempts < max_scroll_attempts and len(gym_urls) < target_count:
            links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/maps/place/']")
            
            for link in links:
                try:
                    href = link.get_attribute('href')
                    if href and href not in gym_urls:
                        gym_urls.append(href)
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
        
        return gym_urls
    
    def extract_complete_gym_data(self, driver):
        """Extract gym data from Google Maps page"""
        gym_data = {}
        
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            time.sleep(3)
            
            # Extract name
            name_selectors = ["h1.DUwDvf", "h1"]
            for selector in name_selectors:
                try:
                    name_element = driver.find_element(By.CSS_SELECTOR, selector)
                    name_text = name_element.text.strip()
                    if name_text and len(name_text) > 1:
                        gym_data['name'] = name_text
                        break
                except:
                    continue
            
            # Extract other details (simplified version)
            # You can add more detailed extraction here similar to the original code
            
        except Exception as e:
            pass
            
        return gym_data