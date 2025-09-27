"""
Configuration module for Lead Automation Tool
Handles environment variables and application settings
"""
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

class Config:
    """Base configuration class"""
    
    # Application settings
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'
    
    # API Keys
    HUNTER_API_KEY = os.getenv('HUNTER_API_KEY')
    CLEARBIT_API_KEY = os.getenv('CLEARBIT_API_KEY')
    SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
    
    # Email Configuration
    GMAIL_EMAIL = os.getenv('GMAIL_EMAIL')
    GMAIL_PASSWORD = os.getenv('GMAIL_PASSWORD')
    
    # Google Sheets
    GOOGLE_SHEETS_CREDENTIALS_PATH = os.getenv('GOOGLE_SHEETS_CREDENTIALS_PATH', 'credentials/google_sheets_credentials.json')
    GOOGLE_SHEETS_SPREADSHEET_ID = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')
    
    # Database
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///leads.db')
    
    # Redis
    REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    
    # Flask
    FLASK_ENV = os.getenv('FLASK_ENV', 'development')
    FLASK_PORT = int(os.getenv('FLASK_PORT', 5000))
    
    # Scraping settings
    SCRAPING_DELAY = int(os.getenv('SCRAPING_DELAY', 2))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
    USER_AGENT = os.getenv('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    # Email campaign settings
    EMAIL_RATE_LIMIT = int(os.getenv('EMAIL_RATE_LIMIT', 10))
    EMAIL_BATCH_SIZE = int(os.getenv('EMAIL_BATCH_SIZE', 50))
    CAMPAIGN_DELAY_MINUTES = int(os.getenv('CAMPAIGN_DELAY_MINUTES', 5))
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'logs/app.log')
    
    @classmethod
    def validate_config(cls):
        """Validate critical configuration values"""
        required_vars = []
        
        if not cls.HUNTER_API_KEY:
            required_vars.append('HUNTER_API_KEY')
        
        if not cls.GMAIL_EMAIL or not cls.GMAIL_PASSWORD:
            required_vars.append('GMAIL_EMAIL and GMAIL_PASSWORD')
            
        if required_vars:
            print(f"Warning: Missing required environment variables: {', '.join(required_vars)}")
            print("Some features may not work properly.")
        
        return len(required_vars) == 0

# Create directories if they don't exist
def ensure_directories():
    """Create necessary directories"""
    directories = [
        'logs',
        'data',
        'data/exports',
        'data/imports',
        'credentials',
        'templates'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)

# Initialize
ensure_directories()
config = Config()
