
import gspread
from google.oauth2.service_account import Credentials
import json
import logging
from typing import List, Dict

class RealGoogleSheetsClient:
    """Real Google Sheets API client"""
    
    def __init__(self, credentials_path: str, spreadsheet_id: str):
        self.credentials_path = credentials_path
        self.spreadsheet_id = spreadsheet_id
        self.logger = logging.getLogger(__name__)
        self.gc = None
        self.spreadsheet = None
    
    def authenticate(self):
        """Authenticate with Google Sheets API"""
        try:
            # Define the scope
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            # Load credentials
            creds = Credentials.from_service_account_file(
                self.credentials_path, 
                scopes=scopes
            )
            
            # Authorize the client
            self.gc = gspread.authorize(creds)
            
            # Open the spreadsheet
            self.spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
            
            self.logger.info("✅ Successfully authenticated with Google Sheets")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Authentication failed: {e}")
            return False
    
    def create_or_get_worksheet(self, worksheet_name: str = "Yelp Leads"):
        """Create or get worksheet"""
        try:
            # Try to get existing worksheet
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            self.logger.info(f"Found existing worksheet: {worksheet_name}")
            return worksheet
            
        except gspread.WorksheetNotFound:
            # Create new worksheet
            worksheet = self.spreadsheet.add_worksheet(
                title=worksheet_name, 
                rows=1000, 
                cols=20
            )
            self.logger.info(f"Created new worksheet: {worksheet_name}")
            return worksheet
    
    def clear_worksheet(self, worksheet_name: str = "Yelp Leads"):
        """Clear all data in worksheet"""
        try:
            worksheet = self.create_or_get_worksheet(worksheet_name)
            worksheet.clear()
            self.logger.info(f"Cleared worksheet: {worksheet_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error clearing worksheet: {e}")
            return False
    
    def upload_leads(self, leads_data: List[Dict], worksheet_name: str = "Yelp Leads"):
        """Upload leads to Google Sheets"""
        try:
            if not leads_data:
                self.logger.warning("No data to upload")
                return False
            
            # Get or create worksheet
            worksheet = self.create_or_get_worksheet(worksheet_name)
            
            # Clear existing data
            worksheet.clear()
            
            # Prepare headers
            headers = list(leads_data[0].keys())
            
            # Prepare all data (headers + rows)
            all_data = [headers]
            for lead in leads_data:
                row = [str(lead.get(header, '')) for header in headers]
                all_data.append(row)
            
            # Upload all data at once (more efficient)
            worksheet.update('A1', all_data)
            
            # Format headers
            worksheet.format('A1:Z1', {
                'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9},
                'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}}
            })
            
            # Auto-resize columns
            worksheet.columns_auto_resize(0, len(headers))
            
            self.logger.info(f"✅ Uploaded {len(leads_data)} leads to Google Sheets")
            self.logger.info(f"🔗 View at: https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Upload failed: {e}")
            return False
    
    def append_leads(self, leads_data: List[Dict], worksheet_name: str = "Yelp Leads"):
        """Append leads to existing data"""
        try:
            worksheet = self.create_or_get_worksheet(worksheet_name)
            
            # Get existing data to determine next row
            existing_data = worksheet.get_all_values()
            next_row = len(existing_data) + 1
            
            # If sheet is empty, add headers first
            if not existing_data:
                headers = list(leads_data[0].keys())
                worksheet.append_row(headers)
                next_row = 2
            
            # Append each lead
            for lead in leads_data:
                headers = list(leads_data[0].keys())
                row = [str(lead.get(header, '')) for header in headers]
                worksheet.append_row(row)
            
            self.logger.info(f"✅ Appended {len(leads_data)} leads to Google Sheets")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Append failed: {e}")
            return False

# Example usage
def example_real_integration():
    """Example of real Google Sheets integration"""
    
    # Your configuration
    CREDENTIALS_PATH = "credentials/google_sheets_credentials.json"
    SPREADSHEET_ID = "your_spreadsheet_id_here"
    
    # Sample data
    sample_leads = [
        {
            "business_name": "Tony's Pizza",
            "phone": "(555) 123-4567",
            "address": "123 Main St, New York, NY",
            "category": "Restaurant",
            "rating": 4.5,
            "website": "https://tonyspizza.com"
        },
        {
            "business_name": "Smith Law Firm",
            "phone": "(555) 987-6543",
            "address": "456 Oak Ave, New York, NY",
            "category": "Legal Services",
            "rating": 4.8,
            "website": "https://smithlaw.com"
        }
    ]
    
    # Initialize client
    client = RealGoogleSheetsClient(CREDENTIALS_PATH, SPREADSHEET_ID)
    
    # Authenticate
    if client.authenticate():
        # Upload data
        success = client.upload_leads(sample_leads, "Yelp Scraping Results")
        
        if success:
            print("🎉 Successfully uploaded to Google Sheets!")
        else:
            print("❌ Upload failed")
    else:
        print("❌ Authentication failed")

if __name__ == "__main__":
    example_real_integration()
