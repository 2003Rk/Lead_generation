"""
Simple lead collector without heavy dependencies
For testing basic functionality before installing all packages
"""
import json
import csv
import time
import random
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
import logging

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
        return asdict(self)

class SimpleCollector:
    """Simple lead collector for testing"""
    
    def __init__(self):
        self.leads = []
        self.logger = logging.getLogger(__name__)
    
    def create_sample_leads(self, count: int = 10) -> List[Lead]:
        """Create sample leads for testing"""
        sample_businesses = [
            {
                "business_name": "Tony's Pizza Palace",
                "phone": "(555) 123-4567",
                "address": "123 Main St, New York, NY 10001",
                "website": "https://tonyspizza.com",
                "category": "Italian Restaurant",
                "rating": 4.5,
                "review_count": 234,
                "description": "Authentic Italian pizza since 1985",
                "hours": "Mon-Sun: 11AM-11PM",
                "price_range": "$$"
            },
            {
                "business_name": "Smith & Associates Law Firm",
                "phone": "(555) 987-6543",
                "address": "456 Oak Ave, New York, NY 10002",
                "website": "https://smithlaw.com",
                "category": "Law Firm",
                "rating": 4.8,
                "review_count": 89,
                "description": "Personal injury and business law specialists",
                "hours": "Mon-Fri: 9AM-6PM",
                "price_range": "$$$"
            },
            {
                "business_name": "Green Dental Care",
                "phone": "(555) 456-7890",
                "address": "789 Pine St, New York, NY 10003",
                "website": "https://greendentalcare.com",
                "category": "Dentist",
                "rating": 4.7,
                "review_count": 156,
                "description": "Family dentistry with a gentle touch",
                "hours": "Mon-Sat: 8AM-7PM",
                "price_range": "$$"
            },
            {
                "business_name": "Auto Fix Pro",
                "phone": "(555) 321-0987",
                "address": "321 Elm St, New York, NY 10004",
                "website": "https://autofixpro.com",
                "category": "Auto Repair",
                "rating": 4.3,
                "review_count": 203,
                "description": "Complete automotive repair and maintenance",
                "hours": "Mon-Fri: 7AM-6PM, Sat: 8AM-4PM",
                "price_range": "$$"
            },
            {
                "business_name": "Fitness First Gym",
                "phone": "(555) 654-3210",
                "address": "654 Maple Ave, New York, NY 10005",
                "website": "https://fitnessfirstgym.com",
                "category": "Gym",
                "rating": 4.2,
                "review_count": 312,
                "description": "State-of-the-art fitness equipment and classes",
                "hours": "Mon-Sun: 5AM-11PM",
                "price_range": "$$"
            }
        ]
        
        self.leads = []
        for i in range(min(count, len(sample_businesses) * 2)):
            business = sample_businesses[i % len(sample_businesses)]
            lead = Lead(**business)
            # Add some variation for duplicates
            if i >= len(sample_businesses):
                lead.business_name += f" - Branch {i - len(sample_businesses) + 1}"
                lead.phone = f"(555) {random.randint(100, 999)}-{random.randint(1000, 9999)}"
            
            self.leads.append(lead)
        
        self.logger.info(f"Created {len(self.leads)} sample leads")
        return self.leads
    
    def save_to_csv(self, filename: str = "sample_leads.csv"):
        """Save leads to CSV file"""
        if not self.leads:
            self.logger.warning("No leads to save")
            return
        
        with open(f"data/exports/{filename}", 'w', newline='', encoding='utf-8') as csvfile:
            if self.leads:
                fieldnames = self.leads[0].to_dict().keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for lead in self.leads:
                    writer.writerow(lead.to_dict())
        
        self.logger.info(f"Saved {len(self.leads)} leads to {filename}")
    
    def save_to_json(self, filename: str = "sample_leads.json"):
        """Save leads to JSON file"""
        if not self.leads:
            self.logger.warning("No leads to save")
            return
        
        leads_data = [lead.to_dict() for lead in self.leads]
        with open(f"data/exports/{filename}", 'w', encoding='utf-8') as f:
            json.dump(leads_data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Saved {len(self.leads)} leads to {filename}")
    
    def print_leads_summary(self):
        """Print summary of collected leads"""
        if not self.leads:
            print("No leads collected")
            return
        
        print(f"\n📊 LEADS SUMMARY")
        print(f"{'='*50}")
        print(f"Total leads: {len(self.leads)}")
        
        # Category breakdown
        categories = {}
        for lead in self.leads:
            cat = lead.category or "Unknown"
            categories[cat] = categories.get(cat, 0) + 1
        
        print(f"\nCategories:")
        for category, count in categories.items():
            print(f"  - {category}: {count}")
        
        print(f"\nSample leads:")
        for i, lead in enumerate(self.leads[:3]):
            print(f"  {i+1}. {lead.business_name}")
            print(f"     📞 {lead.phone}")
            print(f"     📍 {lead.address}")
            print(f"     ⭐ {lead.rating} ({lead.review_count} reviews)")
            print()

def test_collector():
    """Test the simple collector"""
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    # Create collector
    collector = SimpleCollector()
    
    # Create sample leads
    leads = collector.create_sample_leads(10)
    
    # Print summary
    collector.print_leads_summary()
    
    # Save to files
    collector.save_to_csv("milestone2_test_leads.csv")
    collector.save_to_json("milestone2_test_leads.json")
    
    print("✅ Milestone 2 test completed successfully!")
    print("📁 Files saved to data/exports/")

if __name__ == "__main__":
    test_collector()
