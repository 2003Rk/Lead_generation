"""
Lead Enrichment Module
Enriches lead data using Hunter.io and Clearbit APIs
"""
import json
import time
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
import re
from urllib.parse import urlparse

# Simple HTTP client for API calls (no requests dependency needed for basic version)
import urllib.request
import urllib.parse
import urllib.error

@dataclass
class EnrichedLead:
    """Enhanced lead with enrichment data"""
    # Original lead data
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
    
    # Enriched data
    contact_email: str = ""
    owner_email: str = ""
    employee_count: int = 0
    annual_revenue: str = ""
    linkedin_url: str = ""
    facebook_url: str = ""
    twitter_url: str = ""
    domain: str = ""
    industry: str = ""
    founded_year: int = 0
    company_type: str = ""
    technologies: List[str] = None
    additional_emails: List[str] = None
    confidence_score: float = 0.0
    enrichment_source: str = ""
    enrichment_date: str = ""
    
    def __post_init__(self):
        if self.technologies is None:
            self.technologies = []
        if self.additional_emails is None:
            self.additional_emails = []
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)

class HunterAPIClient:
    """Hunter.io API client for email finding"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.hunter.io/v2"
        self.logger = logging.getLogger(__name__)
    
    def find_emails(self, domain: str) -> Dict:
        """Find emails for a domain using Hunter.io"""
        if not self.api_key:
            self.logger.warning("Hunter API key not provided")
            return {}
        
        endpoint = f"{self.base_url}/domain-search"
        params = {
            'domain': domain,
            'api_key': self.api_key,
            'limit': 10
        }
        
        try:
            # Build URL
            url = f"{endpoint}?{urllib.parse.urlencode(params)}"
            
            # Make request
            response = urllib.request.urlopen(url, timeout=30)
            data = json.loads(response.read().decode())
            
            if response.status == 200 and 'data' in data:
                return data['data']
            else:
                self.logger.error(f"Hunter API error: {data}")
                return {}
        
        except Exception as e:
            self.logger.error(f"Error calling Hunter API: {e}")
            return {}
    
    def verify_email(self, email: str) -> Dict:
        """Verify email using Hunter.io"""
        if not self.api_key:
            return {}
        
        endpoint = f"{self.base_url}/email-verifier"
        params = {
            'email': email,
            'api_key': self.api_key
        }
        
        try:
            url = f"{endpoint}?{urllib.parse.urlencode(params)}"
            response = urllib.request.urlopen(url, timeout=30)
            data = json.loads(response.read().decode())
            
            if response.status == 200 and 'data' in data:
                return data['data']
            else:
                return {}
        
        except Exception as e:
            self.logger.error(f"Error verifying email with Hunter: {e}")
            return {}

class ClearbitAPIClient:
    """Clearbit API client for company enrichment"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://company-stream.clearbit.com/v2/companies/find"
        self.logger = logging.getLogger(__name__)
    
    def enrich_company(self, domain: str) -> Dict:
        """Enrich company data using Clearbit"""
        if not self.api_key:
            self.logger.warning("Clearbit API key not provided")
            return {}
        
        params = {
            'domain': domain
        }
        
        try:
            url = f"{self.base_url}?{urllib.parse.urlencode(params)}"
            
            # Create request with auth header
            request = urllib.request.Request(url)
            request.add_header('Authorization', f'Bearer {self.api_key}')
            
            response = urllib.request.urlopen(request, timeout=30)
            data = json.loads(response.read().decode())
            
            return data
        
        except Exception as e:
            self.logger.error(f"Error calling Clearbit API: {e}")
            return {}

class LeadEnricher:
    """Main lead enrichment class"""
    
    def __init__(self, hunter_api_key: str = "", clearbit_api_key: str = ""):
        self.hunter_client = HunterAPIClient(hunter_api_key) if hunter_api_key else None
        self.clearbit_client = ClearbitAPIClient(clearbit_api_key) if clearbit_api_key else None
        self.logger = logging.getLogger(__name__)
    
    def extract_domain(self, website: str) -> str:
        """Extract domain from website URL"""
        if not website:
            return ""
        
        # Add protocol if missing
        if not website.startswith(('http://', 'https://')):
            website = 'https://' + website
        
        try:
            parsed = urlparse(website)
            domain = parsed.netloc.lower()
            # Remove www prefix
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return ""
    
    def enrich_single_lead(self, lead: Dict) -> EnrichedLead:
        """Enrich a single lead with additional data"""
        # Convert dict to EnrichedLead if needed
        if isinstance(lead, dict):
            enriched = EnrichedLead(**{k: v for k, v in lead.items() 
                                     if k in EnrichedLead.__dataclass_fields__})
        else:
            enriched = lead
        
        # Extract domain from website
        if enriched.website:
            enriched.domain = self.extract_domain(enriched.website)
        
        # Try to find emails using Hunter.io
        if enriched.domain and self.hunter_client:
            hunter_data = self.hunter_client.find_emails(enriched.domain)
            self._process_hunter_data(enriched, hunter_data)
        
        # Try to enrich company data using Clearbit
        if enriched.domain and self.clearbit_client:
            clearbit_data = self.clearbit_client.enrich_company(enriched.domain)
            self._process_clearbit_data(enriched, clearbit_data)
        
        # Set enrichment metadata
        enriched.enrichment_date = time.strftime("%Y-%m-%d %H:%M:%S")
        
        return enriched
    
    def _process_hunter_data(self, lead: EnrichedLead, hunter_data: Dict):
        """Process Hunter.io API response"""
        if not hunter_data:
            return
        
        try:
            # Extract emails
            emails = hunter_data.get('emails', [])
            if emails:
                # Find best email (usually first one)
                best_email = emails[0]
                lead.contact_email = best_email.get('value', '')
                
                # Store additional emails
                lead.additional_emails = [email.get('value', '') for email in emails[1:5]]
                
                # Set confidence score
                lead.confidence_score = best_email.get('confidence', 0) / 100.0
            
            # Company info from Hunter
            organization = hunter_data.get('organization', '')
            if organization and not lead.business_name:
                lead.business_name = organization
            
            lead.enrichment_source += "Hunter.io; "
            self.logger.info(f"Enriched {lead.business_name} with Hunter data")
        
        except Exception as e:
            self.logger.error(f"Error processing Hunter data: {e}")
    
    def _process_clearbit_data(self, lead: EnrichedLead, clearbit_data: Dict):
        """Process Clearbit API response"""
        if not clearbit_data:
            return
        
        try:
            # Company name
            name = clearbit_data.get('name', '')
            if name and not lead.business_name:
                lead.business_name = name
            
            # Employee count
            metrics = clearbit_data.get('metrics', {})
            if metrics:
                lead.employee_count = metrics.get('employees', 0)
                annual_revenue = metrics.get('annualRevenue')
                if annual_revenue:
                    lead.annual_revenue = f"${annual_revenue:,}"
            
            # Industry and category
            category = clearbit_data.get('category', {})
            if category:
                industry = category.get('industry', '')
                if industry:
                    lead.industry = industry
            
            # Founded year
            founded_year = clearbit_data.get('foundedYear')
            if founded_year:
                lead.founded_year = founded_year
            
            # Company type
            company_type = clearbit_data.get('type')
            if company_type:
                lead.company_type = company_type
            
            # Social media URLs
            lead.linkedin_url = clearbit_data.get('linkedin', {}).get('handle', '')
            lead.twitter_url = clearbit_data.get('twitter', {}).get('handle', '')
            lead.facebook_url = clearbit_data.get('facebook', {}).get('handle', '')
            
            # Technologies
            tech = clearbit_data.get('tech', [])
            if tech:
                lead.technologies = [t.get('name', '') for t in tech[:10]]
            
            lead.enrichment_source += "Clearbit; "
            self.logger.info(f"Enriched {lead.business_name} with Clearbit data")
        
        except Exception as e:
            self.logger.error(f"Error processing Clearbit data: {e}")
    
    def enrich_leads_batch(self, leads: List[Dict], delay: float = 1.0) -> List[EnrichedLead]:
        """Enrich multiple leads with rate limiting"""
        enriched_leads = []
        
        for i, lead in enumerate(leads):
            self.logger.info(f"Enriching lead {i+1}/{len(leads)}: {lead.get('business_name', 'Unknown')}")
            
            try:
                enriched = self.enrich_single_lead(lead)
                enriched_leads.append(enriched)
                
                # Rate limiting
                if delay > 0 and i < len(leads) - 1:
                    time.sleep(delay)
            
            except Exception as e:
                self.logger.error(f"Error enriching lead {i+1}: {e}")
                # Add original lead even if enrichment fails
                if isinstance(lead, dict):
                    enriched = EnrichedLead(**{k: v for k, v in lead.items() 
                                             if k in EnrichedLead.__dataclass_fields__})
                    enriched_leads.append(enriched)
        
        return enriched_leads
    
    def save_enriched_leads(self, leads: List[EnrichedLead], filename: str = "enriched_leads.json"):
        """Save enriched leads to file"""
        data = [lead.to_dict() for lead in leads]
        
        with open(f"data/exports/{filename}", 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Saved {len(leads)} enriched leads to {filename}")

class MockEnricher:
    """Mock enricher for testing without API keys"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def enrich_single_lead(self, lead: Dict) -> EnrichedLead:
        """Mock enrichment with sample data"""
        # Convert dict to EnrichedLead
        if isinstance(lead, dict):
            enriched = EnrichedLead(**{k: v for k, v in lead.items() 
                                     if k in EnrichedLead.__dataclass_fields__})
        else:
            enriched = lead
        
        # Add mock enrichment data
        if enriched.website:
            # Extract domain
            enriched.domain = self._extract_domain_simple(enriched.website)
            
            # Mock email
            domain_name = enriched.domain.split('.')[0] if enriched.domain else "company"
            enriched.contact_email = f"info@{enriched.domain}" if enriched.domain else f"contact@{domain_name}.com"
            
            # Mock additional data
            enriched.employee_count = 25
            enriched.annual_revenue = "$1,500,000"
            enriched.industry = enriched.category if enriched.category else "Business Services"
            enriched.confidence_score = 0.85
            enriched.enrichment_source = "Mock Data"
            enriched.enrichment_date = time.strftime("%Y-%m-%d %H:%M:%S")
        
        return enriched
    
    def _extract_domain_simple(self, website: str) -> str:
        """Simple domain extraction"""
        if not website:
            return ""
        
        # Remove protocol
        domain = website.replace('https://', '').replace('http://', '')
        # Remove path
        domain = domain.split('/')[0]
        # Remove www
        if domain.startswith('www.'):
            domain = domain[4:]
        
        return domain.lower()
    
    def enrich_leads_batch(self, leads: List[Dict], delay: float = 0.5) -> List[EnrichedLead]:
        """Mock batch enrichment"""
        enriched_leads = []
        
        for i, lead in enumerate(leads):
            self.logger.info(f"Mock enriching lead {i+1}/{len(leads)}: {lead.get('business_name', 'Unknown')}")
            
            enriched = self.enrich_single_lead(lead)
            enriched_leads.append(enriched)
            
            if delay > 0:
                time.sleep(delay)
        
        return enriched_leads
    
    def save_enriched_leads(self, leads: List[EnrichedLead], filename: str = "enriched_leads.json"):
        """Save enriched leads to file"""
        data = [lead.to_dict() for lead in leads]
        
        with open(f"data/exports/{filename}", 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Saved {len(leads)} enriched leads to {filename}")

def load_leads_from_json(filename: str) -> List[Dict]:
    """Load leads from JSON file"""
    try:
        with open(f"data/exports/{filename}", 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File {filename} not found in data/exports/")
        return []
    except Exception as e:
        print(f"Error loading leads: {e}")
        return []

def example_usage():
    """Example of how to use the enrichment module"""
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    # Load sample leads
    leads = load_leads_from_json("milestone2_test_leads.json")
    
    if not leads:
        print("No leads found. Run Milestone 2 first.")
        return
    
    print(f"Loaded {len(leads)} leads for enrichment")
    
    # Use mock enricher (replace with real enricher when you have API keys)
    enricher = MockEnricher()
    
    # Enrich leads
    enriched_leads = enricher.enrich_leads_batch(leads[:5])  # Enrich first 5
    
    # Save results
    enricher.save_enriched_leads(enriched_leads, "milestone3_enriched_leads.json")
    
    # Print summary
    print(f"\n📊 ENRICHMENT SUMMARY")
    print("=" * 50)
    print(f"Original leads: {len(leads)}")
    print(f"Enriched leads: {len(enriched_leads)}")
    
    print(f"\nSample enriched data:")
    for lead in enriched_leads[:2]:
        print(f"\n🏢 {lead.business_name}")
        print(f"   📧 Email: {lead.contact_email}")
        print(f"   🌐 Domain: {lead.domain}")
        print(f"   👥 Employees: {lead.employee_count}")
        print(f"   💰 Revenue: {lead.annual_revenue}")
        print(f"   🏭 Industry: {lead.industry}")
        print(f"   📈 Confidence: {lead.confidence_score:.2f}")

if __name__ == "__main__":
    example_usage()
