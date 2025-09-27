"""
Utility functions for the Lead Automation Tool
"""
import logging
import time
import random
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional
import validators

# Setup logging
def setup_logging(log_level: str = "INFO", log_file: str = "logs/app.log"):
    """Setup logging configuration"""
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

# Retry decorator
def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Retry decorator with exponential backoff"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay
            
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts == max_attempts:
                        raise e
                    
                    logging.warning(f"Attempt {attempts} failed: {e}. Retrying in {current_delay}s...")
                    time.sleep(current_delay)
                    current_delay *= backoff
            
        return wrapper
    return decorator

# Rate limiting
def rate_limit(calls_per_second: float = 1.0):
    """Rate limiting decorator"""
    def decorator(func: Callable) -> Callable:
        last_called = [0.0]
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_called[0]
            min_interval = 1.0 / calls_per_second
            
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            
            last_called[0] = time.time()
            return func(*args, **kwargs)
        
        return wrapper
    return decorator

# Data validation
def validate_email(email: str) -> bool:
    """Validate email address format"""
    return validators.email(email) is True

def validate_url(url: str) -> bool:
    """Validate URL format"""
    return validators.url(url) is True

def validate_phone(phone: str) -> bool:
    """Basic phone number validation"""
    # Remove common separators
    cleaned = ''.join(filter(str.isdigit, phone))
    # Check if it's between 10-15 digits (international format)
    return 10 <= len(cleaned) <= 15

# Data cleaning
def clean_text(text: str) -> str:
    """Clean and normalize text data"""
    if not text:
        return ""
    
    # Remove extra whitespace
    text = ' '.join(text.split())
    
    # Remove special characters that might cause issues
    text = text.replace('\n', ' ').replace('\r', '').replace('\t', ' ')
    
    return text.strip()

def clean_phone(phone: str) -> str:
    """Clean and format phone number"""
    if not phone:
        return ""
    
    # Keep only digits and common separators
    cleaned = ''.join(c for c in phone if c.isdigit() or c in '+-(). ')
    return cleaned.strip()

def clean_email(email: str) -> str:
    """Clean and normalize email address"""
    if not email:
        return ""
    
    return email.lower().strip()

# Progress tracking
class ProgressTracker:
    """Simple progress tracker for long-running operations"""
    
    def __init__(self, total: int, description: str = "Processing"):
        self.total = total
        self.current = 0
        self.description = description
        self.start_time = time.time()
    
    def update(self, increment: int = 1):
        """Update progress"""
        self.current += increment
        self._print_progress()
    
    def _print_progress(self):
        """Print progress bar"""
        if self.total == 0:
            return
            
        percentage = (self.current / self.total) * 100
        elapsed = time.time() - self.start_time
        
        if self.current > 0:
            eta = (elapsed / self.current) * (self.total - self.current)
            eta_str = f" ETA: {int(eta)}s"
        else:
            eta_str = ""
        
        bar_length = 30
        filled = int(bar_length * percentage / 100)
        bar = '█' * filled + '░' * (bar_length - filled)
        
        print(f"\r{self.description}: [{bar}] {percentage:.1f}% ({self.current}/{self.total}){eta_str}", end='')
        
        if self.current >= self.total:
            print()  # New line when complete

# File utilities
def ensure_file_extension(filename: str, extension: str) -> str:
    """Ensure filename has the correct extension"""
    if not filename.endswith(f'.{extension}'):
        return f"{filename}.{extension}"
    return filename

def safe_filename(filename: str) -> str:
    """Create a safe filename by removing invalid characters"""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    return filename

# Random delays for web scraping
def random_delay(min_delay: float = 1.0, max_delay: float = 3.0):
    """Add random delay to avoid detection"""
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)
