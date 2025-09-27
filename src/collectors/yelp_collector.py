"""
Yelp Lead Collector
Scrapes business information from Yelp for lead generation
"""
import requests
import time
import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import json
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass
import re
from urllib.parse import urljoin, urlparse

@dataclass
class Lead:
    """Data class for lead information"""
    business_name: str = ""
    phone: str = ""
    address: str = ""
    website: str = ""
    category: str = ""
    rating: float = 0.0
    review_count: int = 0
    yelp_url: str = ""
    description: str = ""
    hours: str = ""
    price_range: str = ""
    contact_email: str = ""
    
    def to_dict(self) -> Dict:
        """Convert lead to dictionary"""
        return {
            'business_name': self.business_name,
            'phone': self.phone,
            'address': self.address,
            'website': self.website,
            'category': self.category,
            'rating': self.rating,
            'review_count': self.review_count,
            'yelp_url': self.yelp_url,
            'description': self.description,
            'hours': self.hours,
            'price_range': self.price_range,
            'contact_email': self.contact_email
        }

class YelpCollector:
    """Yelp business data collector"""
    
    def __init__(self, headless: bool = True, delay_range: tuple = (2, 5)):
        self.headless = headless
        self.delay_range = delay_range
        self.driver = None
        self.session = requests.Session()
        self.leads = []
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # User agents for rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]
    
    def setup_driver(self):
        """Setup Chrome WebDriver with options"""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument('--headless')
        
        # Additional options for stability
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument(f'--user-agent={random.choice(self.user_agents)}')
        
        # Disable images for faster loading
        prefs = {"profile.managed_default_content_settings.images": 2}
        chrome_options.add_experimental_option("prefs", prefs)
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver.implicitly_wait(10)
    
    def random_delay(self):
        """Add random delay between requests"""
        delay = random.uniform(*self.delay_range)
        time.sleep(delay)
    
    def search_businesses(self, query: str, location: str, max_results: int = 50) -> List[Lead]:
        """
        Search for businesses on Yelp
        
        Args:
            query: Business type or keyword (e.g., "restaurants", "dentist")
            location: City, state, or zip code
            max_results: Maximum number of results to collect
        
        Returns:
            List of Lead objects
        """
        self.leads = []
        
        try:
            if not self.driver:
                self.setup_driver()
            
            # Build search URL
            base_url = "https://www.yelp.com/search"
            search_url = f"{base_url}?find_desc={query}&find_loc={location}"
            
            self.logger.info(f"Starting search: {query} in {location}")
            self.driver.get(search_url)
            
            # Wait for results to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='serp-ia-card']"))
            )
            
            page_num = 1
            collected_count = 0
            
            while collected_count < max_results:
                self.logger.info(f"Scraping page {page_num}...")
                
                # Get business cards on current page
                business_cards = self.driver.find_elements(By.CSS_SELECTOR, "[data-testid='serp-ia-card']")
                
                if not business_cards:
                    self.logger.warning("No business cards found on this page")
                    break
                
                # Extract data from each business card
                for card in business_cards:
                    if collected_count >= max_results:
                        break
                    
                    try:
                        lead = self.extract_business_card_data(card)
                        if lead and lead.business_name:
                            self.leads.append(lead)
                            collected_count += 1
                            self.logger.info(f"Collected lead {collected_count}: {lead.business_name}")
                        
                        self.random_delay()
                    
                    except Exception as e:
                        self.logger.error(f"Error extracting business card data: {e}")
                        continue
                
                # Try to go to next page
                if collected_count < max_results:
                    if not self.go_to_next_page():
                        break
                    page_num += 1
                    self.random_delay()
            
            self.logger.info(f"Collection completed. Total leads: {len(self.leads)}")
            return self.leads
        
        except Exception as e:
            self.logger.error(f"Error during business search: {e}")
            return self.leads
        
        finally:
            if self.driver:
                self.driver.quit()
    
    def extract_business_card_data(self, card) -> Optional[Lead]:
        """Extract data from a business card element"""
        lead = Lead()
        
        try:
            # Business name
            name_element = card.find_element(By.CSS_SELECTOR, "[data-testid='business-name']")
            lead.business_name = name_element.text.strip()
            
            # Yelp URL
            link_element = card.find_element(By.CSS_SELECTOR, "a[href*='/biz/']")
            lead.yelp_url = urljoin("https://www.yelp.com", link_element.get_attribute("href"))
            
            # Rating
            try:
                rating_element = card.find_element(By.CSS_SELECTOR, "[role='img'][aria-label*='star']")
                rating_text = rating_element.get_attribute("aria-label")
                rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                if rating_match:
                    lead.rating = float(rating_match.group(1))
            except NoSuchElementException:
                pass
            
            # Review count
            try:
                review_element = card.find_element(By.CSS_SELECTOR, "[data-testid='review-count']")
                review_text = review_element.text
                review_match = re.search(r'(\d+)', review_text)
                if review_match:
                    lead.review_count = int(review_match.group(1))
            except NoSuchElementException:
                pass
            
            # Category
            try:
                category_elements = card.find_elements(By.CSS_SELECTOR, "[data-testid='business-categories'] a")
                if category_elements:
                    lead.category = category_elements[0].text.strip()
            except NoSuchElementException:
                pass
            
            # Price range
            try:
                price_element = card.find_element(By.CSS_SELECTOR, "[data-testid='price-range']")
                lead.price_range = price_element.text.strip()
            except NoSuchElementException:
                pass
            
            # Address
            try:
                address_element = card.find_element(By.CSS_SELECTOR, "[data-testid='business-address']")
                lead.address = address_element.text.strip()
            except NoSuchElementException:
                pass
            
            # Phone
            try:
                phone_element = card.find_element(By.CSS_SELECTOR, "[data-testid='business-phone']")
                lead.phone = phone_element.text.strip()
            except NoSuchElementException:
                pass
            
            return lead
        
        except Exception as e:
            self.logger.error(f"Error extracting business card data: {e}")
            return None
    
    def get_detailed_business_info(self, lead: Lead) -> Lead:
        """Get detailed information from business page"""
        if not lead.yelp_url:
            return lead
        
        try:
            self.driver.get(lead.yelp_url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1"))
            )
            
            # Website
            try:
                website_element = self.driver.find_element(By.CSS_SELECTOR, "a[href*='biz_redir']")
                lead.website = website_element.get_attribute("href")
            except NoSuchElementException:
                pass
            
            # Phone (if not already found)
            if not lead.phone:
                try:
                    phone_element = self.driver.find_element(By.CSS_SELECTOR, "[data-testid='business-phone-number']")
                    lead.phone = phone_element.text.strip()
                except NoSuchElementException:
                    pass
            
            # Hours
            try:
                hours_elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-testid='business-hours'] p")
                if hours_elements:
                    hours_text = []
                    for hour_elem in hours_elements:
                        hours_text.append(hour_elem.text.strip())
                    lead.hours = "; ".join(hours_text)
            except NoSuchElementException:
                pass
            
            # Description/About
            try:
                about_elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-testid='business-about'] p")
                if about_elements:
                    lead.description = about_elements[0].text.strip()
            except NoSuchElementException:
                pass
            
            self.random_delay()
            return lead
        
        except Exception as e:
            self.logger.error(f"Error getting detailed info for {lead.business_name}: {e}")
            return lead
    
    def go_to_next_page(self) -> bool:
        """Navigate to next page of results"""
        try:
            next_button = self.driver.find_element(By.CSS_SELECTOR, "a[aria-label='Next']")
            if next_button.is_enabled():
                next_button.click()
                # Wait for new page to load
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='serp-ia-card']"))
                )
                return True
        except (NoSuchElementException, TimeoutException):
            pass
        
        return False
    
    def save_to_csv(self, filename: str = "leads.csv"):
        """Save leads to CSV file"""
        if not self.leads:
            self.logger.warning("No leads to save")
            return
        
        df = pd.DataFrame([lead.to_dict() for lead in self.leads])
        df.to_csv(f"data/exports/{filename}", index=False)
        self.logger.info(f"Saved {len(self.leads)} leads to {filename}")
    
    def save_to_json(self, filename: str = "leads.json"):
        """Save leads to JSON file"""
        if not self.leads:
            self.logger.warning("No leads to save")
            return
        
        leads_data = [lead.to_dict() for lead in self.leads]
        with open(f"data/exports/{filename}", 'w') as f:
            json.dump(leads_data, f, indent=2)
        self.logger.info(f"Saved {len(self.leads)} leads to {filename}")
    
    def cleanup(self):
        """Cleanup resources"""
        if self.driver:
            self.driver.quit()

# Example usage function
def example_usage():
    """Example of how to use the YelpCollector"""
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Create collector
    collector = YelpCollector(headless=True)
    
    try:
        # Search for restaurants in New York
        leads = collector.search_businesses(
            query="restaurants",
            location="New York, NY",
            max_results=20
        )
        
        # Get detailed info for first 5 leads
        for i, lead in enumerate(leads[:5]):
            print(f"Getting detailed info for {lead.business_name}...")
            detailed_lead = collector.get_detailed_business_info(lead)
            leads[i] = detailed_lead
        
        # Save results
        collector.save_to_csv("restaurant_leads.csv")
        collector.save_to_json("restaurant_leads.json")
        
        print(f"Successfully collected {len(leads)} leads")
        
    finally:
        collector.cleanup()

if __name__ == "__main__":
    example_usage()
