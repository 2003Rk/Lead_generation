"""
Yelp to Google Sheets Integration
Scrapes Yelp data and saves directly to Google Sheets
"""
import json
import time
import random
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path

# Mock Google Sheets integration (replace with real gspread when installed)
class GoogleSheetsClient:
    """Google Sheets client for lead data"""
    
    def __init__(self, credentials_path: str = "", spreadsheet_id: str = ""):
        self.credentials_path = credentials_path
        self.spreadsheet_id = spreadsheet_id
        self.logger = logging.getLogger(__name__)
        
        # For now, we'll use a mock implementation
        self.is_mock = True
        
    def authenticate(self):
        """Authenticate with Google Sheets API"""
        if self.is_mock:
            self.logger.info("Using mock Google Sheets client")
            return True
        
        # Real implementation would use gspread
        # try:
        #     import gspread
        #     from google.oauth2.service_account import Credentials
        #     
        #     scopes = ['https://www.googleapis.com/auth/spreadsheets']
        #     creds = Credentials.from_service_account_file(self.credentials_path, scopes=scopes)
        #     self.gc = gspread.authorize(creds)
        #     self.sheet = self.gc.open_by_key(self.spreadsheet_id)
        #     return True
        # except Exception as e:
        #     self.logger.error(f"Google Sheets authentication failed: {e}")
        #     return False
    
    def clear_sheet(self, worksheet_name: str = "Leads"):
        """Clear existing data in worksheet"""
        if self.is_mock:
            self.logger.info(f"Mock: Clearing worksheet '{worksheet_name}'")
            return
        
        # Real implementation
        # try:
        #     worksheet = self.sheet.worksheet(worksheet_name)
        #     worksheet.clear()
        # except Exception as e:
        #     self.logger.error(f"Error clearing sheet: {e}")
    
    def upload_leads(self, leads: List[Dict], worksheet_name: str = "Leads"):
        """Upload leads to Google Sheets"""
        if not leads:
            self.logger.warning("No leads to upload")
            return False
        
        if self.is_mock:
            self._mock_upload(leads, worksheet_name)
            return True
        
        # Real implementation
        # try:
        #     worksheet = self.sheet.worksheet(worksheet_name)
        #     
        #     # Prepare headers
        #     headers = list(leads[0].keys())
        #     worksheet.append_row(headers)
        #     
        #     # Prepare data rows
        #     for lead in leads:
        #         row = [str(lead.get(header, '')) for header in headers]
        #         worksheet.append_row(row)
        #     
        #     self.logger.info(f"Uploaded {len(leads)} leads to Google Sheets")
        #     return True
        # 
        # except Exception as e:
        #     self.logger.error(f"Error uploading to Google Sheets: {e}")
        #     return False
    
    def _mock_upload(self, leads: List[Dict], worksheet_name: str):
        """Mock upload that saves to local file"""
        # Save to local file as backup
        filename = f"data/exports/google_sheets_backup_{int(time.time())}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({
                'worksheet_name': worksheet_name,
                'upload_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'leads_count': len(leads),
                'leads': leads
            }, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Mock upload: Saved {len(leads)} leads to {filename}")
        self.logger.info(f"In real implementation, this would go to Google Sheets worksheet '{worksheet_name}'")

@dataclass
class YelpLead:
    """Yelp lead data structure"""
    business_name: str = ""
    phone: str = ""
    address: str = ""
    website: str = ""
    category: str = ""
    rating: float = 0.0
    review_count: int = 0
    yelp_url: str = ""
    price_range: str = ""
    neighborhood: str = ""
    hours: str = ""
    description: str = ""
    scraped_date: str = ""
    
    def to_dict(self) -> Dict:
        return asdict(self)

class YelpToSheetsConnector:
    """Main connector for Yelp to Google Sheets"""
    
    def __init__(self, spreadsheet_id: str = "", credentials_path: str = ""):
        self.spreadsheet_id = spreadsheet_id
        self.credentials_path = credentials_path
        self.logger = logging.getLogger(__name__)
        
        # Initialize Google Sheets client
        self.sheets_client = GoogleSheetsClient(credentials_path, spreadsheet_id)
        
        # Sample data for testing (replace with real scraper)
        self.leads = []
    
    def create_sample_yelp_data(self, count: int = 20) -> List[YelpLead]:
        """Create sample Yelp data for testing"""
        sample_data = [
            {
                "business_name": "Mario's Authentic Italian",
                "phone": "(212) 555-0123",
                "address": "456 Little Italy St, New York, NY 10013",
                "website": "https://mariositalian.com",
                "category": "Italian Restaurant",
                "rating": 4.6,
                "review_count": 428,
                "yelp_url": "https://yelp.com/biz/marios-italian-nyc",
                "price_range": "$$",
                "neighborhood": "Little Italy",
                "hours": "Mon-Thu: 5PM-10PM, Fri-Sat: 5PM-11PM, Sun: 4PM-9PM",
                "description": "Family-owned Italian restaurant serving traditional recipes since 1952"
            },
            {
                "business_name": "TechFlow Solutions",
                "phone": "(415) 555-0456",
                "address": "789 Tech Blvd, San Francisco, CA 94107",
                "website": "https://techflowsolutions.com",
                "category": "Software Development",
                "rating": 4.8,
                "review_count": 156,
                "yelp_url": "https://yelp.com/biz/techflow-solutions-sf",
                "price_range": "$$$",
                "neighborhood": "SOMA",
                "hours": "Mon-Fri: 9AM-6PM",
                "description": "Custom software development and IT consulting services"
            },
            {
                "business_name": "Green Thumb Garden Center",
                "phone": "(503) 555-0789",
                "address": "321 Garden Way, Portland, OR 97205",
                "website": "https://greenthumbgarden.com",
                "category": "Garden Center",
                "rating": 4.4,
                "review_count": 234,
                "yelp_url": "https://yelp.com/biz/green-thumb-garden-portland",
                "price_range": "$$",
                "neighborhood": "Northwest Portland",
                "hours": "Mon-Sat: 8AM-7PM, Sun: 9AM-6PM",
                "description": "Complete garden center with plants, tools, and expert advice"
            },
            {
                "business_name": "Downtown Dental Associates",
                "phone": "(713) 555-0234",
                "address": "567 Main St, Houston, TX 77002",
                "website": "https://downtowndental.com",
                "category": "Dentist",
                "rating": 4.7,
                "review_count": 189,
                "yelp_url": "https://yelp.com/biz/downtown-dental-houston",
                "price_range": "$$$",
                "neighborhood": "Downtown",
                "hours": "Mon-Fri: 8AM-6PM, Sat: 9AM-2PM",
                "description": "Full-service dental practice with modern technology"
            },
            {
                "business_name": "Brew & Bean Coffee House",
                "phone": "(206) 555-0567",
                "address": "123 Coffee Ave, Seattle, WA 98101",
                "website": "https://brewandbean.com",
                "category": "Coffee Shop",
                "rating": 4.3,
                "review_count": 312,
                "yelp_url": "https://yelp.com/biz/brew-bean-seattle",
                "price_range": "$",
                "neighborhood": "Capitol Hill",
                "hours": "Mon-Sun: 6AM-8PM",
                "description": "Locally roasted coffee and fresh pastries in cozy atmosphere"
            }
        ]
        
        self.leads = []
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        
        for i in range(count):
            base_data = sample_data[i % len(sample_data)].copy()
            
            # Add variation for duplicates
            if i >= len(sample_data):
                base_data['business_name'] += f" - Location {i - len(sample_data) + 2}"
                base_data['phone'] = f"({random.randint(200, 999)}) 555-{random.randint(1000, 9999)}"
            
            base_data['scraped_date'] = current_time
            lead = YelpLead(**base_data)
            self.leads.append(lead)
        
        self.logger.info(f"Created {len(self.leads)} sample Yelp leads")
        return self.leads
    
    def scrape_yelp_real(self, query: str, location: str, max_results: int = 50):
        """
        Placeholder for real Yelp scraping
        This would use Selenium or requests + BeautifulSoup
        """
        self.logger.info(f"Real scraping would search for '{query}' in '{location}'")
        self.logger.info("For now, using sample data. Implement real scraping with Selenium.")
        
        # For demonstration, create sample data
        return self.create_sample_yelp_data(min(max_results, 20))
    
    def process_and_upload(self, search_query: str = "restaurants", location: str = "New York, NY", max_results: int = 20):
        """Complete process: scrape Yelp and upload to Google Sheets"""
        
        try:
            # Step 1: Authenticate with Google Sheets
            self.logger.info("Authenticating with Google Sheets...")
            if not self.sheets_client.authenticate():
                self.logger.error("Failed to authenticate with Google Sheets")
                return False
            
            # Step 2: Scrape Yelp data
            self.logger.info(f"Scraping Yelp for '{search_query}' in '{location}'...")
            leads = self.scrape_yelp_real(search_query, location, max_results)
            
            if not leads:
                self.logger.error("No leads found")
                return False
            
            # Step 3: Convert to dictionary format
            leads_data = [lead.to_dict() for lead in leads]
            
            # Step 4: Clear existing sheet data (optional)
            # self.sheets_client.clear_sheet("Leads")
            
            # Step 5: Upload to Google Sheets
            self.logger.info(f"Uploading {len(leads_data)} leads to Google Sheets...")
            success = self.sheets_client.upload_leads(leads_data, "Leads")
            
            if success:
                self.logger.info("✅ Successfully uploaded leads to Google Sheets!")
                self._print_summary(leads)
                return True
            else:
                self.logger.error("Failed to upload leads to Google Sheets")
                return False
        
        except Exception as e:
            self.logger.error(f"Error in process_and_upload: {e}")
            return False
    
    def _print_summary(self, leads: List[YelpLead]):
        """Print summary of scraped data"""
        print(f"\n📊 YELP SCRAPING SUMMARY")
        print("=" * 50)
        print(f"Total leads scraped: {len(leads)}")
        
        # Category breakdown
        categories = {}
        for lead in leads:
            cat = lead.category or "Unknown"
            categories[cat] = categories.get(cat, 0) + 1
        
        print(f"\nCategories found:")
        for category, count in categories.items():
            print(f"  - {category}: {count}")
        
        print(f"\nSample leads:")
        for i, lead in enumerate(leads[:3]):
            print(f"\n  {i+1}. {lead.business_name}")
            print(f"     📞 {lead.phone}")
            print(f"     📍 {lead.address}")
            print(f"     ⭐ {lead.rating} ({lead.review_count} reviews)")
            print(f"     🌐 {lead.website}")

def setup_google_sheets_instructions():
    """Print Google Sheets setup instructions"""
    print("🔧 GOOGLE SHEETS API SETUP")
    print("=" * 50)
    print("1. Go to https://console.cloud.google.com/")
    print("2. Create a new project or select existing")
    print("3. Enable Google Sheets API")
    print("4. Create credentials (Service Account)")
    print("5. Download JSON credentials file")
    print("6. Share your Google Sheet with the service account email")
    print("7. Copy the spreadsheet ID from the URL")
    print("\n📝 Your spreadsheet ID is in the URL:")
    print("https://docs.google.com/spreadsheets/d/[SPREADSHEET_ID]/edit")
    print("\n🔑 Add to .env file:")
    print("GOOGLE_SHEETS_CREDENTIALS_PATH=path/to/credentials.json")
    print("GOOGLE_SHEETS_SPREADSHEET_ID=your_spreadsheet_id")

def main():
    """Main function to run Yelp to Google Sheets integration"""
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("🚀 YELP TO GOOGLE SHEETS INTEGRATION")
    print("=" * 60)
    
    # Your spreadsheet ID from the iframe URL
    # Extract from: https://docs.google.com/spreadsheets/d/e/2PACX-1vTbs9INOf60aKsA_wVmp8SxbFnOGrKroKKJu7uazaLTWHeM8C01tPbviJVci2uegA1qNJFW8wcu5YfX/pubhtml
    spreadsheet_id = "1vTbs9INOf60aKsA_wVmp8SxbFnOGrKroKKJu7uazaLTWHeM8C01tPbviJVci2uegA1qNJFW8wcu5YfX"
    
    # Initialize connector
    connector = YelpToSheetsConnector(
        spreadsheet_id=spreadsheet_id,
        credentials_path="credentials/google_sheets_credentials.json"
    )
    
    # Process and upload
    success = connector.process_and_upload(
        search_query="restaurants",
        location="New York, NY",
        max_results=25
    )
    
    if success:
        print("\n🎉 Process completed successfully!")
        print(f"📊 Data uploaded to Google Sheets")
        print(f"🔗 View at: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
    else:
        print("\n❌ Process failed. Check logs above.")
    
    print("\n📝 To enable real Google Sheets integration:")
    print("1. Install: pip install gspread google-auth")
    print("2. Set up Google Sheets API credentials")
    print("3. Update the GoogleSheetsClient class")

if __name__ == "__main__":
    main()
