"""
API Setup Guide for Lead Enrichment
Instructions for setting up Hunter.io and Clearbit APIs
"""

def print_hunter_setup():
    """Print Hunter.io setup instructions"""
    print("🔧 HUNTER.IO API SETUP")
    print("=" * 50)
    print("1. Go to https://hunter.io/")
    print("2. Sign up for a free account")
    print("3. Free plan includes 25 searches/month")
    print("4. Go to API section in your dashboard")
    print("5. Copy your API key")
    print("6. Add to .env file: HUNTER_API_KEY=your_key_here")
    print("\n📝 API Endpoints used:")
    print("   - Domain Search: Find emails for a domain")
    print("   - Email Verifier: Verify email addresses")
    print("\n💰 Pricing:")
    print("   - Free: 25 requests/month")
    print("   - Starter: $49/month (1,000 requests)")
    print("   - Growth: $99/month (5,000 requests)")

def print_clearbit_setup():
    """Print Clearbit setup instructions"""
    print("\n🔧 CLEARBIT API SETUP")
    print("=" * 50)
    print("1. Go to https://clearbit.com/")
    print("2. Sign up for an account")
    print("3. Go to Dashboard > API Keys")
    print("4. Copy your Secret API key")
    print("5. Add to .env file: CLEARBIT_API_KEY=your_key_here")
    print("\n📝 API Endpoints used:")
    print("   - Company API: Enrich company data")
    print("   - Person API: Find person information")
    print("\n💰 Pricing:")
    print("   - Free: 50 requests/month")
    print("   - Risk: $99/month (1,000 requests)")
    print("   - Growth: $299/month (5,000 requests)")

def print_alternative_apis():
    """Print alternative API options"""
    print("\n🔄 ALTERNATIVE APIs")
    print("=" * 50)
    print("If Hunter.io/Clearbit are too expensive, consider:")
    print("\n📧 Email Finding:")
    print("   - Snov.io: Cheaper alternative to Hunter")
    print("   - Apollo.io: Has a free tier")
    print("   - Findymail: Good accuracy")
    print("\n🏢 Company Data:")
    print("   - FullContact: Company enrichment")
    print("   - Pipl: Person and company data")
    print("   - ZoomInfo: B2B database (enterprise)")

def print_email_finding_tips():
    """Print email finding tips"""
    print("\n💡 EMAIL FINDING TIPS")
    print("=" * 50)
    print("1. Common email patterns:")
    print("   - firstname@company.com")
    print("   - firstname.lastname@company.com")
    print("   - first.last@company.com")
    print("   - f.lastname@company.com")
    print("\n2. Generic emails to try:")
    print("   - info@company.com")
    print("   - contact@company.com")
    print("   - hello@company.com")
    print("   - sales@company.com")
    print("\n3. Use LinkedIn to find names:")
    print("   - Search for company employees")
    print("   - Look for decision makers")
    print("   - Find contact information")

def print_setup_checklist():
    """Print setup checklist"""
    print("\n✅ SETUP CHECKLIST")
    print("=" * 50)
    print("□ Hunter.io account created")
    print("□ Hunter API key added to .env")
    print("□ Clearbit account created")
    print("□ Clearbit API key added to .env")
    print("□ Test API connections")
    print("□ Set up rate limiting")
    print("□ Configure error handling")
    print("□ Plan for API cost management")

def main():
    """Print all setup instructions"""
    print("🚀 MILESTONE 3: API SETUP GUIDE")
    print("=" * 60)
    
    print_hunter_setup()
    print_clearbit_setup()
    print_alternative_apis()
    print_email_finding_tips()
    print_setup_checklist()
    
    print("\n🎯 NEXT STEPS:")
    print("1. Set up at least one API (Hunter.io recommended)")
    print("2. Update your .env file with API keys")
    print("3. Run: python src/enrichment/enricher.py")
    print("4. Check enriched data in data/exports/")

if __name__ == "__main__":
    main()
