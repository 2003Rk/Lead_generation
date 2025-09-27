#!/usr/bin/env python3
"""
GOOGLE MAPS LEAD SCRAPER - PRODUCTION READY
Web interface for scraping Google Maps business data and extracting emails
"""

from flask import Flask, render_template, request, jsonify, send_file
from flask_cors import CORS
import asyncio
import threading
import time
import json
import os
import logging
import re
import csv
from pathlib import Path
import uuid
from typing import List, Dict, Optional
from urllib.parse import quote_plus, urlparse

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import playwright
try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
except ImportError:
    print("Installing playwright...")
    import subprocess
    subprocess.run(["pip", "install", "playwright"], check=True)
    subprocess.run(["playwright", "install"], check=True)
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

app = Flask(__name__)
app.secret_key = 'google-maps-scraper-secret-key-2025'

# Configure CORS
CORS(app, origins=[
    "http://localhost:3000",
    "http://127.0.0.1:8080",
    "https://your-netlify-domain.netlify.app"  # Replace with your actual domain
])

# Global variables for managing scraping jobs
active_jobs = {}

class GoogleMapsScraper:
    """Scraper for Google Maps business data"""
    
    def __init__(self, headless=True):
        self.headless = headless
        self.browser = None
        self.context = None
        self.page = None
    
    async def setup(self):
        """Setup browser and context"""
        try:
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch(
                headless=self.headless,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--no-first-run',
                    '--no-zygote',
                    '--disable-gpu'
                ]
            )
            self.context = await self.browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            self.page = await self.context.new_page()
            await self.page.set_viewport_size({"width": 1366, "height": 768})
            logger.info("Browser setup completed successfully")
        except Exception as e:
            logger.error(f"Failed to setup browser: {str(e)}")
            raise Exception(f"Browser setup failed: {str(e)}")
    
    async def close(self):
        """Close browser"""
        if self.browser:
            await self.browser.close()
    
    async def scrape_google_maps(self, query: str, location: str, max_results: int = 20) -> List[Dict]:
        """
        Scrape business data from Google Maps
        
        Args:
            query: Business type to search for
            location: City and country (e.g., "New York, USA")
            max_results: Maximum number of results to return
        
        Returns:
            List of business dictionaries with details
        """
        businesses = []
        
        try:
            # Construct better search query for more specific results
            if "contractor" in query.lower() or "remodel" in query.lower():
                # For contractors, add specific terms to get better results
                search_query = f"{query} contractors near {location}"
            elif any(term in query.lower() for term in ["plumber", "electrician", "hvac", "roofing"]):
                # For specific trades
                search_query = f"{query} services in {location}"
            else:
                # Default format
                search_query = f"{query} businesses in {location}"
            
            encoded_query = quote_plus(search_query)
            maps_url = f"https://www.google.com/maps/search/{encoded_query}"
            
            logger.info(f"Navigating to Google Maps: {maps_url}")
            await self.page.goto(maps_url, timeout=60000)
            await self.page.wait_for_timeout(5000)  # Wait for results to load
            
            # Scroll to load more results
            scroll_attempts = 0
            loaded_results = 0
            
            while scroll_attempts < 10 and loaded_results < max_results:
                # Find all business cards
                business_cards = await self.page.query_selector_all('div[role="article"]')
                loaded_results = len(business_cards)
                
                if loaded_results >= max_results:
                    break
                
                # Scroll to the last result
                if business_cards:
                    await business_cards[-1].scroll_into_view_if_needed()
                    await self.page.wait_for_timeout(3000)
                
                scroll_attempts += 1
            
            # Extract business information
            # Updated selectors for current Google Maps layout (2025)
            selectors_to_try = [
                'div[role="feed"] > div > div[jsaction]',  # Main feed containers
                'div[role="feed"] div[data-result-index]',  # Indexed results
                'div[role="feed"] a[data-cid]',  # Business links with CID
                'div[aria-label*="Results"] > div',  # Results container
                'div[role="article"]',  # Article role (older selector)
                'div.Nv2PK.THOPZb.CpccDe',  # Specific class combination
                'a[href*="/maps/place/"]',  # Direct place links
                'div[jsaction*="mouseover"]',  # Interactive elements
                '.hfpxzc',  # Business card class
                'div.THOPZb',  # Card container
                '[data-result-index]',  # Any result index
                'div[aria-label][jsaction]'  # Interactive labeled divs
            ]
            
            business_cards = []
            for selector in selectors_to_try:
                cards = await self.page.query_selector_all(selector)
                # Filter out navigation/header elements
                filtered_cards = []
                for card in cards:
                    try:
                        # Check if element has substantial text content
                        text = await card.inner_text()
                        if text and len(text) > 20:  # Must have meaningful content
                            filtered_cards.append(card)
                    except:
                        continue
                
                if filtered_cards and len(filtered_cards) >= 3:  # Need at least 3 businesses
                    business_cards = filtered_cards
                    logger.info(f"Found {len(business_cards)} business cards using selector: {selector}")
                    break
            
            # If still no results, try alternative approach
            if not business_cards:
                logger.warning("No business cards found with standard selectors. Trying alternative approach...")
                
                # Wait longer for content to load
                await self.page.wait_for_timeout(8000)
                
                # Try to find the results feed/panel first
                feed_panel = await self.page.query_selector('div[role="feed"]')
                if feed_panel:
                    # Look for any div children with content
                    all_children = await feed_panel.query_selector_all('div')
                    potential_businesses = []
                    
                    for child in all_children:
                        try:
                            text = await child.inner_text()
                            # Check if it looks like a business listing
                            if (text and len(text) > 30 and 
                                any(indicator in text.lower() for indicator in 
                                    ['★', 'rating', 'review', 'phone', 'website', 'hour', '·', 'open', 'close'])):
                                potential_businesses.append(child)
                        except:
                            continue
                    
                    business_cards = potential_businesses[:max_results]
                    logger.info(f"Found {len(business_cards)} potential businesses using content analysis")
                
                # Final fallback: look for any clickable business links
                if not business_cards:
                    potential_businesses = await self.page.query_selector_all('a[href*="place/"]')
                    business_cards = potential_businesses[:max_results]
                    logger.info(f"Using {len(business_cards)} business links as final fallback")
            
            logger.info(f"Final count: {len(business_cards)} business cards")
            
            for i, card in enumerate(business_cards[:max_results]):
                try:
                    business_data = await self.extract_business_data(card)
                    if business_data:
                        businesses.append(business_data)
                        logger.info(f"Extracted business {i+1}: {business_data.get('name', 'Unknown')}")
                except Exception as e:
                    logger.error(f"Error extracting business {i+1}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error scraping Google Maps: {str(e)}")
        
        return businesses
    
    async def extract_business_data(self, card) -> Optional[Dict]:
        """Extract business data from a card element with improved selectors"""
        try:
            # First try to get data from the card itself without clicking
            card_text = await card.inner_text()
            
            # Try to extract name from card directly
            name = "Unknown"
            name_selectors = [
                'h3', 'h2', 'h1',
                '[role="button"] span',
                '.qBF1Pd', '.DUwDvf',
                'div[style*="font-weight"]',
                'span[style*="font-weight"]'
            ]
            
            for selector in name_selectors:
                try:
                    name_elem = await card.query_selector(selector)
                    if name_elem:
                        name_text = await name_elem.inner_text()
                        if name_text and len(name_text.strip()) > 2 and len(name_text) < 100:
                            name = name_text.strip()
                            break
                except:
                    continue
            
            # If name not found in card, try clicking to get details
            if name == "Unknown":
                try:
                    await card.click()
                    await self.page.wait_for_timeout(3000)
                    
                    # Try to get name from details panel
                    detail_name_selectors = [
                        'h1.DUwDvf', 'h1', '.x3AX1-LfntMc-header-title-title',
                        '[data-attrid="title"]', '.qBF1Pd.fontHeadlineSmall'
                    ]
                    
                    for selector in detail_name_selectors:
                        try:
                            name_elem = await self.page.query_selector(selector)
                            if name_elem:
                                name_text = await name_elem.inner_text()
                                if name_text and len(name_text.strip()) > 2:
                                    name = name_text.strip()
                                    break
                        except:
                            continue
                except Exception as e:
                    logger.warning(f"Could not click on card: {str(e)}")
            
            # Extract other information from details panel or card
            address = ""
            website = ""
            phone = ""
            rating = ""
            
            # Try to get address
            address_selectors = [
                'button[data-item-id="address"]',
                '[data-item-id="address"]',
                '.Io6YTe', '.rogA2c .fontBodyMedium',
                '[aria-label*="Address"]',
                'span[jstcache*="address"]'
            ]
            
            for selector in address_selectors:
                try:
                    addr_elem = await self.page.query_selector(selector)
                    if not addr_elem:
                        addr_elem = await card.query_selector(selector)
                    if addr_elem:
                        addr_text = await addr_elem.inner_text()
                        if addr_text and ',' in addr_text:  # Likely an address
                            address = addr_text.strip()
                            break
                except:
                    continue
            
            # Try to get website
            website_selectors = [
                'a[data-item-id="authority"]',
                'a[href*="://"][href*="."]',
                '.CsEnBe a', '.lcr4fd a',
                'a[href]:not([href*="google.com"]):not([href*="maps"])'
            ]
            
            for selector in website_selectors:
                try:
                    website_elems = await self.page.query_selector_all(selector)
                    if not website_elems:
                        website_elems = await card.query_selector_all(selector)
                    
                    for elem in website_elems:
                        href = await elem.get_attribute('href')
                        if (href and 'http' in href and 
                            'google.com' not in href and 
                            'maps.google.com' not in href and
                            'goo.gl' not in href):
                            website = href
                            break
                    if website:
                        break
                except:
                    continue
            
            # Try to get phone
            phone_selectors = [
                'button[data-item-id="phone"]',
                '[data-item-id="phone"]',
                '.rogA2c .fontBodyMedium',
                '[aria-label*="Phone"]',
                'span[dir="ltr"]',
                'button[data-value*="+"]'
            ]
            
            for selector in phone_selectors:
                try:
                    phone_elem = await self.page.query_selector(selector)
                    if not phone_elem:
                        phone_elem = await card.query_selector(selector)
                    if phone_elem:
                        phone_text = await phone_elem.inner_text()
                        # Check if it looks like a phone number
                        if phone_text and any(char.isdigit() for char in phone_text):
                            phone = phone_text.strip()
                            break
                except:
                    continue
            
            # Try to get rating
            rating_selectors = [
                '.MW4etd', '.fontDisplayLarge',
                '[aria-label*="stars"]', '.ceNzKf',
                'span[aria-label*="star"]'
            ]
            
            for selector in rating_selectors:
                try:
                    rating_elem = await self.page.query_selector(selector)
                    if not rating_elem:
                        rating_elem = await card.query_selector(selector)
                    if rating_elem:
                        rating_text = await rating_elem.inner_text()
                        # Check if it looks like a rating
                        if rating_text and ('.' in rating_text or '★' in rating_text):
                            rating = rating_text.strip()
                            break
                except:
                    continue
            
            # Close details panel if opened
            try:
                close_button = await self.page.query_selector('button[aria-label="Close"]')
                if close_button:
                    await close_button.click()
                    await self.page.wait_for_timeout(500)
            except:
                pass
            
            # Only return data if we have at least a name that looks valid
            if name and name != "Unknown" and len(name) > 2:
                business_data = {
                    "name": name,
                    "address": address,
                    "website": website,
                    "phone": phone,
                    "rating": rating,
                    "source": "Google Maps"
                }
                
                # Add any additional info found in card text
                if not address and card_text:
                    lines = card_text.split('\n')
                    for line in lines:
                        if ',' in line and any(word in line.lower() for word in ['st', 'ave', 'rd', 'dr', 'blvd']):
                            business_data["address"] = line.strip()
                            break
                
                return business_data
            else:
                logger.warning(f"Could not extract valid business name from card. Got: {name}")
                return None
                
        except Exception as e:
            logger.error(f"Error extracting business data: {str(e)}")
            return None
    
    async def extract_emails_from_websites(self, businesses: List[Dict], max_concurrent: int = 3) -> List[Dict]:
        """
        Visit business websites and extract email addresses
        
        Args:
            businesses: List of business dictionaries
            max_concurrent: Maximum number of concurrent website visits
        
        Returns:
            List of businesses with extracted emails
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def extract_email(business):
            async with semaphore:
                email = await self.extract_email_from_website(business.get('website', ''))
                business['email'] = email
                business['email_scraped'] = bool(email)
                return business
        
        tasks = [extract_email(business) for business in businesses if business.get('website')]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error extracting email for business {i}: {str(result)}")
                results[i] = businesses[i]  # Keep original business data
        
        return results
    
    async def extract_email_from_website(self, url: str) -> str:
        """
        Extract email addresses from a website
        
        Args:
            url: Website URL to scrape
        
        Returns:
            Extracted email address or empty string
        """
        if not url:
            return ""
        
        try:
            # Ensure URL has scheme
            if not url.startswith('http'):
                url = 'https://' + url
            
            logger.info(f"Extracting email from: {url}")
            
            # Create a new page for this request
            page = await self.context.new_page()
            await page.goto(url, timeout=30000)
            
            # Get page content
            content = await page.content()
            
            # Close the page
            await page.close()
            
            # Extract emails using regex
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            emails = re.findall(email_pattern, content)
            
            # Filter out common non-personal emails
            filtered_emails = []
            for email in emails:
                if not any(domain in email for domain in ['noreply', 'no-reply', 'email', 'info', 'contact', 'hello', 'support']):
                    filtered_emails.append(email)
            
            # Return the first valid email found
            return filtered_emails[0] if filtered_emails else ""
            
        except Exception as e:
            logger.error(f"Error extracting email from {url}: {str(e)}")
            return ""

class GoogleMapsScrapingJob:
    """Job class for Google Maps scraping"""
    
    def __init__(self, job_id: str, business_type: str, location: str, max_results: int = 20):
        self.job_id = job_id
        self.business_type = business_type
        self.location = location
        self.max_results = max_results
        self.status = "pending"
        self.progress = 0
        self.current_step = ""
        self.detailed_status = ""
        self.results = []
        self.total_found = 0
        self.error = None
        self.created_at = time.time()
        self.started_at = None
        self.files = {}
    
    async def execute(self):
        """Execute the scraping job"""
        try:
            self.status = "running"
            self.started_at = time.time()
            self.progress = 5
            self.current_step = "🚀 Initializing"
            self.detailed_status = "Starting Google Maps scraping process..."
            
            # Initialize scraper
            scraper = GoogleMapsScraper(headless=True)
            await scraper.setup()
            
            self.progress = 20
            self.current_step = "🗺️ Searching Google Maps"
            self.detailed_status = f"Searching for '{self.business_type}' in '{self.location}'"
            
            # Scrape Google Maps
            businesses = await scraper.scrape_google_maps(
                self.business_type, self.location, self.max_results
            )
            
            self.progress = 60
            self.current_step = f"📊 Found {len(businesses)} businesses"
            self.detailed_status = "Processing business information..."
            
            self.progress = 70
            self.current_step = "🌐 Visiting websites"
            self.detailed_status = "Extracting email addresses from business websites..."
            
            # Extract emails from websites
            businesses_with_emails = await scraper.extract_emails_from_websites(businesses)
            
            self.progress = 90
            self.current_step = "💾 Preparing results"
            self.detailed_status = "Generating downloadable files..."
            
            # Store results
            self.results = businesses_with_emails
            self.total_found = len(self.results)
            
            # Generate downloadable files
            await self._generate_files()
            
            # Close scraper
            await scraper.close()
            
            self.progress = 100
            self.current_step = "🎉 Complete!"
            self.detailed_status = f"Successfully scraped {self.total_found} businesses with emails!"
            self.status = "completed"
            
            logger.info(f"Job {self.job_id} completed successfully with {self.total_found} results")
            
        except Exception as e:
            self.status = "failed"
            self.error = str(e)
            self.current_step = "❌ Error"
            self.detailed_status = f"Process failed: {str(e)}"
            logger.error(f"Job {self.job_id} failed: {str(e)}")
    
    async def _generate_files(self):
        """Generate downloadable files from results"""
        # Ensure exports directory exists
        Path("data/exports").mkdir(parents=True, exist_ok=True)
        
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        base_filename = f"google_maps_leads_{self.job_id}_{timestamp}"
        
        # Generate CSV file
        csv_path = f"data/exports/{base_filename}.csv"
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            if self.results:
                fieldnames = list(self.results[0].keys())
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.results)
        
        # Generate JSON file
        json_path = f"data/exports/{base_filename}.json"
        with open(json_path, 'w', encoding='utf-8') as jsonfile:
            data = {
                'job_id': self.job_id,
                'business_type': self.business_type,
                'location': self.location,
                'generated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_leads': len(self.results),
                'leads': self.results
            }
            json.dump(data, jsonfile, indent=2, ensure_ascii=False)
        
        # Store file paths
        self.files = {
            'csv': csv_path,
            'json': json_path
        }

def run_async_job(job):
    """Run async job in a separate thread"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(job.execute())
    loop.close()

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/start-scraping', methods=['POST'])
@app.route('/api/start-google-maps-scraping', methods=['POST'])
def start_scraping():
    """Start a new Google Maps scraping job"""
    
    data = request.get_json()
    
    # Validate input - accept both old and new parameter names
    business_type = data.get('business_type', data.get('search_query', '')).strip()
    location = data.get('location', '').strip()
    max_results = int(data.get('max_results', 20))
    
    if not business_type or not location:
        return jsonify({'error': 'Business type and location are required'}), 400
    
    if max_results < 1 or max_results > 50:
        return jsonify({'error': 'Max results must be between 1 and 50'}), 400
    
    # Create job
    job_id = str(uuid.uuid4())
    job = GoogleMapsScrapingJob(job_id, business_type, location, max_results)
    
    active_jobs[job_id] = job
    
    # Start job in background thread
    thread = threading.Thread(target=run_async_job, args=(job,))
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'job_id': job_id,
        'status': 'started',
        'message': 'Google Maps scraping started successfully'
    })

@app.route('/api/job-status/<job_id>')
def job_status(job_id):
    """Get status of a scraping job"""
    
    if job_id not in active_jobs:
        return jsonify({'error': 'Job not found'}), 404
    
    job = active_jobs[job_id]
    
    response = {
        'job_id': job_id,
        'status': job.status,
        'progress': job.progress,
        'current_step': job.current_step,
        'detailed_status': job.detailed_status,
        'business_type': job.business_type,
        'location': job.location,
        'max_results': job.max_results,
        'started_at': job.started_at,
        'leads_count': len(job.results),
        'error': job.error
    }
    
    if job.status == 'completed':
        response['leads'] = job.results
        response['total_found'] = job.total_found
    
    return jsonify(response)

@app.route('/api/download/<job_id>/<file_type>')
def download_file(job_id, file_type):
    """Download generated files"""
    
    if job_id not in active_jobs:
        return jsonify({'error': 'Job not found'}), 404
    
    job = active_jobs[job_id]
    
    if job.status != 'completed' or file_type not in job.files:
        return jsonify({'error': 'File not available'}), 404
    
    file_path = job.files[file_type]
    
    if not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 404
    
    return send_file(file_path, as_attachment=True)

@app.route('/api/recent-jobs')
def recent_jobs():
    """Get list of recent jobs"""
    
    jobs_list = []
    for job_id, job in active_jobs.items():
        jobs_list.append({
            'job_id': job_id,
            'business_type': job.business_type,
            'location': job.location,
            'status': job.status,
            'progress': job.progress,
            'leads_count': len(job.results),
            'started_at': job.started_at
        })
    
    # Sort by start time (newest first)
    jobs_list.sort(key=lambda x: x.get('started_at', 0) or 0, reverse=True)
    
    return jsonify({'jobs': jobs_list[:10]})  # Return last 10 jobs

@app.route('/api/delete-job/<job_id>', methods=['DELETE'])
def delete_job(job_id):
    """Delete a job and its files"""
    
    if job_id not in active_jobs:
        return jsonify({'error': 'Job not found'}), 404
    
    job = active_jobs[job_id]
    
    # Delete files if they exist
    for file_path in job.files.values():
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass
    
    # Remove from active jobs
    del active_jobs[job_id]
    
    return jsonify({'message': 'Job deleted successfully'})

if __name__ == '__main__':
    # Ensure data directory exists
    Path("data/exports").mkdir(parents=True, exist_ok=True)
    
    # Production configuration
    port = int(os.environ.get('PORT', 8080))
    debug = os.environ.get('FLASK_ENV') != 'production'
    
    print("🚀 GOOGLE MAPS LEAD SCRAPER STARTING...")
    print(f"📱 Server running on port: {port}")
    print(f"🔧 Debug mode: {debug}")
    print("✅ Complete web interface for Google Maps lead generation")
    
    app.run(debug=debug, host='0.0.0.0', port=port)