"""
Storage Module for Lead Data
Handles CSV, Excel, Google Sheets, and Database storage
"""
import json
import csv
import sqlite3
import logging
from typing import List, Dict, Optional
from pathlib import Path
import time
from datetime import datetime

class CSVStorage:
    """CSV file storage handler"""
    
    def __init__(self, base_path: str = "data/exports"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def save_leads(self, leads: List[Dict], filename: str = None) -> str:
        """Save leads to CSV file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"leads_{timestamp}.csv"
        
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        filepath = self.base_path / filename
        
        if not leads:
            self.logger.warning("No leads to save")
            return str(filepath)
        
        # Get all possible field names
        fieldnames = set()
        for lead in leads:
            fieldnames.update(lead.keys())
        fieldnames = sorted(list(fieldnames))
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(leads)
        
        self.logger.info(f"Saved {len(leads)} leads to {filepath}")
        return str(filepath)
    
    def load_leads(self, filename: str) -> List[Dict]:
        """Load leads from CSV file"""
        filepath = self.base_path / filename
        
        if not filepath.exists():
            self.logger.error(f"File not found: {filepath}")
            return []
        
        leads = []
        with open(filepath, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                leads.append(dict(row))
        
        self.logger.info(f"Loaded {len(leads)} leads from {filepath}")
        return leads

class ExcelStorage:
    """Excel file storage handler (simplified version without openpyxl)"""
    
    def __init__(self, base_path: str = "data/exports"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def save_leads(self, leads: List[Dict], filename: str = None) -> str:
        """Save leads to Excel-compatible CSV (for now)"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"leads_{timestamp}.xlsx.csv"
        
        # For now, save as CSV that can be opened in Excel
        csv_storage = CSVStorage(self.base_path)
        filepath = csv_storage.save_leads(leads, filename)
        
        self.logger.info(f"Saved {len(leads)} leads to Excel-compatible format: {filepath}")
        return filepath

class DatabaseStorage:
    """SQLite database storage handler"""
    
    def __init__(self, db_path: str = "data/leads.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self.init_database()
    
    def init_database(self):
        """Initialize database tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create leads table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    business_name TEXT,
                    phone TEXT,
                    address TEXT,
                    website TEXT,
                    category TEXT,
                    rating REAL,
                    review_count INTEGER,
                    yelp_url TEXT,
                    description TEXT,
                    hours TEXT,
                    price_range TEXT,
                    contact_email TEXT,
                    owner_email TEXT,
                    employee_count INTEGER,
                    annual_revenue TEXT,
                    linkedin_url TEXT,
                    facebook_url TEXT,
                    twitter_url TEXT,
                    domain TEXT,
                    industry TEXT,
                    founded_year INTEGER,
                    company_type TEXT,
                    confidence_score REAL,
                    enrichment_source TEXT,
                    enrichment_date TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create campaigns table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS campaigns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    status TEXT DEFAULT 'draft',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create campaign_leads table (many-to-many)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS campaign_leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id INTEGER,
                    lead_id INTEGER,
                    status TEXT DEFAULT 'pending',
                    sent_at TIMESTAMP,
                    opened_at TIMESTAMP,
                    replied_at TIMESTAMP,
                    FOREIGN KEY (campaign_id) REFERENCES campaigns (id),
                    FOREIGN KEY (lead_id) REFERENCES leads (id)
                )
            ''')
            
            conn.commit()
    
    def save_leads(self, leads: List[Dict], update_existing: bool = True) -> int:
        """Save leads to database"""
        if not leads:
            return 0
        
        saved_count = 0
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for lead in leads:
                try:
                    # Check if lead exists (by business name and address)
                    cursor.execute('''
                        SELECT id FROM leads 
                        WHERE business_name = ? AND address = ?
                    ''', (lead.get('business_name', ''), lead.get('address', '')))
                    
                    existing = cursor.fetchone()
                    
                    if existing and update_existing:
                        # Update existing lead
                        lead_id = existing[0]
                        self._update_lead(cursor, lead_id, lead)
                        saved_count += 1
                    elif not existing:
                        # Insert new lead
                        self._insert_lead(cursor, lead)
                        saved_count += 1
                
                except Exception as e:
                    self.logger.error(f"Error saving lead {lead.get('business_name', 'Unknown')}: {e}")
            
            conn.commit()
        
        self.logger.info(f"Saved {saved_count} leads to database")
        return saved_count
    
    def _insert_lead(self, cursor, lead: Dict):
        """Insert a new lead"""
        columns = [
            'business_name', 'phone', 'address', 'website', 'category',
            'rating', 'review_count', 'yelp_url', 'description', 'hours',
            'price_range', 'contact_email', 'owner_email', 'employee_count',
            'annual_revenue', 'linkedin_url', 'facebook_url', 'twitter_url',
            'domain', 'industry', 'founded_year', 'company_type',
            'confidence_score', 'enrichment_source', 'enrichment_date'
        ]
        
        values = [lead.get(col, None) for col in columns]
        placeholders = ', '.join(['?' for _ in columns])
        
        cursor.execute(f'''
            INSERT INTO leads ({', '.join(columns)})
            VALUES ({placeholders})
        ''', values)
    
    def _update_lead(self, cursor, lead_id: int, lead: Dict):
        """Update an existing lead"""
        cursor.execute('''
            UPDATE leads SET
                business_name = ?, phone = ?, address = ?, website = ?,
                category = ?, rating = ?, review_count = ?, yelp_url = ?,
                description = ?, hours = ?, price_range = ?, contact_email = ?,
                owner_email = ?, employee_count = ?, annual_revenue = ?,
                linkedin_url = ?, facebook_url = ?, twitter_url = ?, domain = ?,
                industry = ?, founded_year = ?, company_type = ?,
                confidence_score = ?, enrichment_source = ?, enrichment_date = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            lead.get('business_name'), lead.get('phone'), lead.get('address'),
            lead.get('website'), lead.get('category'), lead.get('rating'),
            lead.get('review_count'), lead.get('yelp_url'), lead.get('description'),
            lead.get('hours'), lead.get('price_range'), lead.get('contact_email'),
            lead.get('owner_email'), lead.get('employee_count'), lead.get('annual_revenue'),
            lead.get('linkedin_url'), lead.get('facebook_url'), lead.get('twitter_url'),
            lead.get('domain'), lead.get('industry'), lead.get('founded_year'),
            lead.get('company_type'), lead.get('confidence_score'),
            lead.get('enrichment_source'), lead.get('enrichment_date'), lead_id
        ))
    
    def load_leads(self, limit: int = None, category: str = None) -> List[Dict]:
        """Load leads from database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            cursor = conn.cursor()
            
            query = "SELECT * FROM leads"
            params = []
            
            if category:
                query += " WHERE category = ?"
                params.append(category)
            
            query += " ORDER BY created_at DESC"
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            leads = [dict(row) for row in rows]
            self.logger.info(f"Loaded {len(leads)} leads from database")
            return leads
    
    def get_lead_stats(self) -> Dict:
        """Get lead statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Total leads
            cursor.execute("SELECT COUNT(*) FROM leads")
            total_leads = cursor.fetchone()[0]
            
            # Leads with emails
            cursor.execute("SELECT COUNT(*) FROM leads WHERE contact_email IS NOT NULL AND contact_email != ''")
            leads_with_email = cursor.fetchone()[0]
            
            # Leads by category
            cursor.execute("SELECT category, COUNT(*) FROM leads GROUP BY category ORDER BY COUNT(*) DESC")
            categories = dict(cursor.fetchall())
            
            return {
                'total_leads': total_leads,
                'leads_with_email': leads_with_email,
                'categories': categories
            }

class GoogleSheetsStorage:
    """Google Sheets storage handler (mock implementation)"""
    
    def __init__(self, credentials_path: str = None, spreadsheet_id: str = None):
        self.credentials_path = credentials_path
        self.spreadsheet_id = spreadsheet_id
        self.logger = logging.getLogger(__name__)
    
    def save_leads(self, leads: List[Dict], sheet_name: str = "Leads") -> bool:
        """Mock save to Google Sheets"""
        self.logger.info(f"Mock: Would save {len(leads)} leads to Google Sheets")
        self.logger.info(f"Sheet: {sheet_name}, Spreadsheet ID: {self.spreadsheet_id}")
        
        # For now, save to CSV as backup
        csv_storage = CSVStorage()
        backup_file = f"google_sheets_backup_{sheet_name.lower()}.csv"
        csv_storage.save_leads(leads, backup_file)
        
        return True
    
    def load_leads(self, sheet_name: str = "Leads") -> List[Dict]:
        """Mock load from Google Sheets"""
        self.logger.info(f"Mock: Would load leads from Google Sheets: {sheet_name}")
        return []

class StorageManager:
    """Main storage manager class"""
    
    def __init__(self):
        self.csv_storage = CSVStorage()
        self.excel_storage = ExcelStorage()
        self.db_storage = DatabaseStorage()
        self.sheets_storage = GoogleSheetsStorage()
        self.logger = logging.getLogger(__name__)
    
    def save_leads(self, leads: List[Dict], storage_types: List[str] = None) -> Dict[str, str]:
        """Save leads to multiple storage types"""
        if storage_types is None:
            storage_types = ['csv', 'database']
        
        results = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for storage_type in storage_types:
            try:
                if storage_type == 'csv':
                    filename = f"leads_{timestamp}.csv"
                    filepath = self.csv_storage.save_leads(leads, filename)
                    results['csv'] = filepath
                
                elif storage_type == 'excel':
                    filename = f"leads_{timestamp}.xlsx"
                    filepath = self.excel_storage.save_leads(leads, filename)
                    results['excel'] = filepath
                
                elif storage_type == 'database':
                    count = self.db_storage.save_leads(leads)
                    results['database'] = f"Saved {count} leads to database"
                
                elif storage_type == 'google_sheets':
                    success = self.sheets_storage.save_leads(leads)
                    results['google_sheets'] = "Success" if success else "Failed"
                
                else:
                    self.logger.warning(f"Unknown storage type: {storage_type}")
            
            except Exception as e:
                self.logger.error(f"Error saving to {storage_type}: {e}")
                results[storage_type] = f"Error: {e}"
        
        return results
    
    def load_leads(self, source: str, **kwargs) -> List[Dict]:
        """Load leads from specified source"""
        try:
            if source == 'csv':
                filename = kwargs.get('filename')
                if not filename:
                    raise ValueError("CSV filename required")
                return self.csv_storage.load_leads(filename)
            
            elif source == 'database':
                return self.db_storage.load_leads(**kwargs)
            
            elif source == 'google_sheets':
                return self.sheets_storage.load_leads(**kwargs)
            
            else:
                raise ValueError(f"Unknown source: {source}")
        
        except Exception as e:
            self.logger.error(f"Error loading from {source}: {e}")
            return []
    
    def get_storage_stats(self) -> Dict:
        """Get statistics from all storage types"""
        stats = {}
        
        try:
            # Database stats
            stats['database'] = self.db_storage.get_lead_stats()
        except Exception as e:
            stats['database'] = {'error': str(e)}
        
        try:
            # File system stats
            csv_files = list(Path("data/exports").glob("*.csv"))
            stats['csv_files'] = len(csv_files)
        except Exception as e:
            stats['csv_files'] = 0
        
        return stats

def test_storage():
    """Test all storage functionality"""
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    # Load sample data
    try:
        with open("data/exports/milestone3_enriched_leads.json", 'r') as f:
            test_leads = json.load(f)
    except FileNotFoundError:
        # Create sample data if enriched leads don't exist
        test_leads = [
            {
                "business_name": "Test Company 1",
                "phone": "(555) 123-4567",
                "address": "123 Test St, Test City, TS 12345",
                "website": "https://test1.com",
                "category": "Technology",
                "contact_email": "info@test1.com"
            },
            {
                "business_name": "Test Company 2", 
                "phone": "(555) 987-6543",
                "address": "456 Test Ave, Test City, TS 12345",
                "website": "https://test2.com",
                "category": "Consulting",
                "contact_email": "contact@test2.com"
            }
        ]
    
    print(f"Testing storage with {len(test_leads)} leads...")
    
    # Test storage manager
    manager = StorageManager()
    
    # Save to multiple formats
    results = manager.save_leads(test_leads, ['csv', 'database'])
    
    print("\n📊 STORAGE RESULTS:")
    for storage_type, result in results.items():
        print(f"  {storage_type}: {result}")
    
    # Test loading
    print("\n📖 LOADING TESTS:")
    
    # Load from database
    db_leads = manager.load_leads('database', limit=5)
    print(f"Loaded {len(db_leads)} leads from database")
    
    # Get stats
    stats = manager.get_storage_stats()
    print(f"\n📈 STORAGE STATS:")
    print(f"Database: {stats.get('database', {})}")
    print(f"CSV files: {stats.get('csv_files', 0)}")
    
    print("\n✅ Milestone 4 storage test completed!")

if __name__ == "__main__":
    test_storage()
