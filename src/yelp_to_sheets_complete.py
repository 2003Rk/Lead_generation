"""
Complete Yelp to Google Sheets Pipeline
Combines Playwright scraping with Google Sheets upload
"""
import asyncio
import time
import json
import logging
from typing import List, Dict
from pathlib import Path

# Import our modules
try:
    from playwright_yelp_scraper import PlaywrightYelpScraper, YelpBusiness
except ImportError:
    import sys
    sys.path.append('src')
    from playwright_yelp_scraper import PlaywrightYelpScraper, YelpBusiness

class YelpToSheetsComplete:
    """Complete pipeline from Yelp scraping to Google Sheets"""
    
    def __init__(self, spreadsheet_id: str = "", headless: bool = True):
        self.spreadsheet_id = spreadsheet_id or "1vTbs9INOf60aKsA_wVmp8SxbFnOGrKroKKJu7uazaLTWHeM8C01tPbviJVci2uegA1qNJFW8wcu5YfX"
        self.scraper = PlaywrightYelpScraper(headless=headless)
        self.logger = logging.getLogger(__name__)
        
    async def scrape_and_upload(self, 
                                search_query: str, 
                                location: str, 
                                max_results: int = 25) -> bool:
        """Complete pipeline: scrape Yelp and upload to Google Sheets"""
        
        try:
            # Step 1: Scrape Yelp data
            self.logger.info(f"🔍 Scraping Yelp for '{search_query}' in '{location}'...")
            businesses = await self.scraper.scrape_yelp_search(search_query, location, max_results)
            
            if not businesses:
                self.logger.error("No businesses found")
                return False
            
            # Store businesses in scraper for saving
            self.scraper.businesses = businesses
            
            # Step 2: Save to local files
            self.logger.info("💾 Saving to local files...")
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            csv_file = self.scraper.save_to_csv(f"yelp_to_sheets_{timestamp}.csv")
            json_file = self.scraper.save_to_json(f"yelp_to_sheets_{timestamp}.json")
            
            # Step 3: Prepare Google Sheets format
            sheets_data = self._prepare_sheets_data(businesses)
            
            # Step 4: Upload to Google Sheets (mock for now)
            success = self._upload_to_google_sheets(sheets_data, search_query, location)
            
            # Step 5: Print summary
            self._print_summary(businesses, csv_file, json_file, success)
            
            return success
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            return False
    
    def _prepare_sheets_data(self, businesses: List[YelpBusiness]) -> List[Dict]:
        """Prepare data in format optimized for Google Sheets"""
        sheets_data = []
        
        for business in businesses:
            # Create a clean, Google Sheets-friendly format
            row = {
                'Business Name': business.business_name,
                'Phone': business.phone,
                'Email': business.email,
                'Address': business.address,
                'Website': business.website,
                'Category': business.category,
                'Rating': business.rating,
                'Reviews': business.review_count,
                'Price Range': business.price_range,
                'Neighborhood': business.neighborhood,
                'Hours': business.hours,
                'Description': business.description,
                'Yelp URL': business.yelp_url,
                'Scraped Date': business.scraped_date,
                'Search Query': business.search_query,
                'Search Location': business.search_location
            }
            sheets_data.append(row)
        
        return sheets_data
    
    def _upload_to_google_sheets(self, data: List[Dict], query: str, location: str) -> bool:
        """Upload data to Google Sheets (mock implementation)"""
        try:
            # Save the Google Sheets formatted data
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            sheets_file = f"data/exports/google_sheets_ready_{timestamp}.json"
            
            sheets_payload = {
                'spreadsheet_id': self.spreadsheet_id,
                'worksheet_name': f'Yelp {query.title()} - {location}',
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_rows': len(data),
                'data': data
            }
            
            with open(sheets_file, 'w', encoding='utf-8') as f:
                json.dump(sheets_payload, f, indent=2, ensure_ascii=False)
            
            # Also save as CSV in Google Sheets format
            import csv
            csv_file = f"data/exports/google_sheets_import_{timestamp}.csv"
            
            if data:
                with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = list(data[0].keys())
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(data)
            
            self.logger.info(f"📊 Google Sheets data prepared: {sheets_file}")
            self.logger.info(f"📊 CSV for import prepared: {csv_file}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Google Sheets preparation failed: {e}")
            return False
    
    def _print_summary(self, businesses: List[YelpBusiness], csv_file: str, json_file: str, upload_success: bool):
        """Print comprehensive summary"""
        print(f"\\n🎉 YELP TO GOOGLE SHEETS COMPLETE!")
        print("=" * 60)
        
        # Scraping summary
        print(f"📊 SCRAPING RESULTS:")
        print(f"   Total businesses found: {len(businesses)}")
        print(f"   Search query: {businesses[0].search_query if businesses else 'N/A'}")
        print(f"   Location: {businesses[0].search_location if businesses else 'N/A'}")
        print(f"   Scraping method: Playwright + Fallback")
        
        # Data quality summary
        with_phone = sum(1 for b in businesses if b.phone)
        with_email = sum(1 for b in businesses if b.email)
        with_website = sum(1 for b in businesses if b.website)
        with_rating = sum(1 for b in businesses if b.rating > 0)
        
        print(f"\\n📈 DATA QUALITY:")
        print(f"   Businesses with phone: {with_phone}/{len(businesses)} ({with_phone/len(businesses)*100:.1f}%)")
        print(f"   Businesses with email: {with_email}/{len(businesses)} ({with_email/len(businesses)*100:.1f}%)")
        print(f"   Businesses with website: {with_website}/{len(businesses)} ({with_website/len(businesses)*100:.1f}%)")
        print(f"   Businesses with rating: {with_rating}/{len(businesses)} ({with_rating/len(businesses)*100:.1f}%)")
        
        # Category breakdown
        categories = {}
        for business in businesses:
            cat = business.category or "Unknown"
            categories[cat] = categories.get(cat, 0) + 1
        
        print(f"\\n🏷️ CATEGORIES FOUND:")
        for category, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
            print(f"   - {category}: {count}")
        
        # Files created
        print(f"\\n📁 FILES CREATED:")
        print(f"   📄 CSV: {csv_file}")
        print(f"   📄 JSON: {json_file}")
        print(f"   🎯 Google Sheets ready files in data/exports/")
        
        # Google Sheets info
        print(f"\\n📊 GOOGLE SHEETS INTEGRATION:")
        if upload_success:
            print(f"   ✅ Data prepared successfully")
        else:
            print(f"   ❌ Data preparation failed")
        
        print(f"   🔗 Target spreadsheet:")
        print(f"   https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}")
        
        # Next steps
        print(f"\\n🎯 NEXT STEPS:")
        print(f"   1. Open your Google Sheet")
        print(f"   2. Go to File → Import")
        print(f"   3. Upload the CSV file from data/exports/")
        print(f"   4. Choose 'Replace spreadsheet' or 'Insert new sheet'")
        print(f"   5. Review and format the data")
        
        # Sample data preview
        print(f"\\n📋 SAMPLE DATA (first 3 businesses):")
        print("-" * 50)
        for i, business in enumerate(businesses[:3]):
            print(f"\\n{i+1}. {business.business_name}")
            print(f"   📞 {business.phone}")
            print(f"   ✉️ {business.email}")
            print(f"   📍 {business.address}")
            print(f"   ⭐ {business.rating} ({business.review_count} reviews)")
            print(f"   🏷️ {business.category}")
            print(f"   💰 {business.price_range}")
            print(f"   🌐 {business.website}")

async def main():
    """Main function to run the complete pipeline"""
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("🚀 COMPLETE YELP TO GOOGLE SHEETS PIPELINE")
    print("=" * 60)
    
    # Configuration - CUSTOMIZE THESE VALUES
    SEARCH_QUERY = "restaurants"     # What to search for
    LOCATION = "New York, NY"        # Where to search  
    MAX_RESULTS = 20                 # How many results
    HEADLESS = True                  # Set to False to see browser
    
    # Your Google Sheets ID
    SPREADSHEET_ID = "1vTbs9INOf60aKsA_wVmp8SxbFnOGrKroKKJu7uazaLTWHeM8C01tPbviJVci2uegA1qNJFW8wcu5YfX"
    
    print(f"🎯 Configuration:")
    print(f"   Search: '{SEARCH_QUERY}' in '{LOCATION}'")
    print(f"   Max results: {MAX_RESULTS}")
    print(f"   Headless mode: {HEADLESS}")
    print(f"   Target sheet: {SPREADSHEET_ID}")
    print()
    
    # Initialize pipeline
    pipeline = YelpToSheetsComplete(spreadsheet_id=SPREADSHEET_ID, headless=HEADLESS)
    
    try:
        # Run complete pipeline
        success = await pipeline.scrape_and_upload(SEARCH_QUERY, LOCATION, MAX_RESULTS)
        
        if success:
            print("\\n🎉 Pipeline completed successfully!")
        else:
            print("\\n❌ Pipeline failed. Check logs above.")
            
    except KeyboardInterrupt:
        print("\\n⏹️ Pipeline interrupted by user")
    except Exception as e:
        print(f"\\n❌ Pipeline error: {e}")
        logging.error(f"Pipeline error: {e}")

def run_pipeline():
    """Synchronous wrapper"""
    asyncio.run(main())

if __name__ == "__main__":
    run_pipeline()
