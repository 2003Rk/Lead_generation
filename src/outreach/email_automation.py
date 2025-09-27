"""
Email Outreach Automation Module
Handles SMTP/SendGrid email campaigns with personalization
"""
import smtplib
import json
import time
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import re
from pathlib import Path

@dataclass
class EmailTemplate:
    """Email template data structure"""
    name: str
    subject: str
    body_text: str
    body_html: str = ""
    variables: List[str] = None
    
    def __post_init__(self):
        if self.variables is None:
            self.variables = []
        
        # Auto-detect variables from template
        self.variables = self._extract_variables()
    
    def _extract_variables(self) -> List[str]:
        """Extract variables from template (format: {{variable_name}})"""
        variables = set()
        text_vars = re.findall(r'\{\{(\w+)\}\}', self.body_text)
        variables.update(text_vars)
        
        if self.body_html:
            html_vars = re.findall(r'\{\{(\w+)\}\}', self.body_html)
            variables.update(html_vars)
        
        subject_vars = re.findall(r'\{\{(\w+)\}\}', self.subject)
        variables.update(subject_vars)
        
        return sorted(list(variables))

@dataclass 
class EmailCampaign:
    """Email campaign data structure"""
    name: str
    template: EmailTemplate
    leads: List[Dict]
    sender_email: str
    sender_name: str = ""
    schedule_time: Optional[datetime] = None
    status: str = "draft"  # draft, scheduled, running, completed, paused
    sent_count: int = 0
    delivery_rate: int = 10  # emails per hour
    
    def get_next_send_time(self) -> float:
        """Calculate delay until next email"""
        return 3600 / self.delivery_rate  # seconds between emails

class SMTPEmailSender:
    """SMTP email sender for Gmail/other providers"""
    
    def __init__(self, smtp_server: str, smtp_port: int, email: str, password: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.email = email
        self.password = password
        self.logger = logging.getLogger(__name__)
    
    def send_email(self, to_email: str, subject: str, body_text: str, 
                   body_html: str = "", sender_name: str = "") -> bool:
        """Send a single email via SMTP"""
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = f"{sender_name} <{self.email}>" if sender_name else self.email
            msg['To'] = to_email
            msg['Subject'] = subject
            
            # Add text version
            text_part = MIMEText(body_text, 'plain')
            msg.attach(text_part)
            
            # Add HTML version if provided
            if body_html:
                html_part = MIMEText(body_html, 'html')
                msg.attach(html_part)
            
            # Connect and send
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.email, self.password)
                server.send_message(msg)
            
            self.logger.info(f"Email sent successfully to {to_email}")
            return True
        
        except Exception as e:
            self.logger.error(f"Failed to send email to {to_email}: {e}")
            return False

class MockEmailSender:
    """Mock email sender for testing without actual email sending"""
    
    def __init__(self, email: str = "test@example.com"):
        self.email = email
        self.sent_emails = []
        self.logger = logging.getLogger(__name__)
    
    def send_email(self, to_email: str, subject: str, body_text: str,
                   body_html: str = "", sender_name: str = "") -> bool:
        """Mock send email (just log and store)"""
        email_data = {
            'to': to_email,
            'from': f"{sender_name} <{self.email}>" if sender_name else self.email,
            'subject': subject,
            'body_text': body_text,
            'body_html': body_html,
            'sent_at': datetime.now().isoformat()
        }
        
        self.sent_emails.append(email_data)
        self.logger.info(f"📧 MOCK EMAIL sent to {to_email}: {subject}")
        return True
    
    def get_sent_emails(self) -> List[Dict]:
        """Get list of sent emails"""
        return self.sent_emails
    
    def save_sent_emails(self, filename: str = "sent_emails_log.json"):
        """Save sent emails to file"""
        with open(f"data/exports/{filename}", 'w') as f:
            json.dump(self.sent_emails, f, indent=2)
        self.logger.info(f"Saved {len(self.sent_emails)} sent emails to {filename}")

class EmailPersonalizer:
    """Email personalization engine"""
    
    @staticmethod
    def personalize_template(template: EmailTemplate, lead: Dict) -> Tuple[str, str, str]:
        """Personalize template with lead data"""
        # Create personalization mapping
        personalization = {
            'business_name': lead.get('business_name', 'Your Business'),
            'first_name': EmailPersonalizer._extract_first_name(lead),
            'contact_email': lead.get('contact_email', ''),
            'phone': lead.get('phone', ''),
            'address': lead.get('address', ''),
            'category': lead.get('category', 'Business'),
            'website': lead.get('website', ''),
            'industry': lead.get('industry', lead.get('category', 'your industry')),
            'city': EmailPersonalizer._extract_city(lead),
            'rating': str(lead.get('rating', '')),
            'employee_count': str(lead.get('employee_count', ''))
        }
        
        # Add default values for missing variables
        for var in template.variables:
            if var not in personalization:
                personalization[var] = f"[{var}]"
        
        # Personalize subject
        subject = template.subject
        for var, value in personalization.items():
            subject = subject.replace(f"{{{{{var}}}}}", str(value))
        
        # Personalize body text
        body_text = template.body_text
        for var, value in personalization.items():
            body_text = body_text.replace(f"{{{{{var}}}}}", str(value))
        
        # Personalize body HTML
        body_html = template.body_html
        if body_html:
            for var, value in personalization.items():
                body_html = body_html.replace(f"{{{{{var}}}}}", str(value))
        
        return subject, body_text, body_html
    
    @staticmethod
    def _extract_first_name(lead: Dict) -> str:
        """Extract first name from business name or contact"""
        # Try to get from business name (for small businesses)
        business_name = lead.get('business_name', '')
        
        # Common patterns for owner names in business names
        if ' & ' in business_name or "'s " in business_name:
            # "John's Auto Repair" -> "John"
            if "'s " in business_name:
                return business_name.split("'s ")[0]
            # "Smith & Associates" -> "Smith" 
            if ' & ' in business_name:
                return business_name.split(' & ')[0]
        
        # If no clear name pattern, use "there" as fallback
        return "there"
    
    @staticmethod
    def _extract_city(lead: Dict) -> str:
        """Extract city from address"""
        address = lead.get('address', '')
        if not address:
            return "your area"
        
        # Try to extract city (format: "123 Main St, City, State ZIP")
        parts = address.split(', ')
        if len(parts) >= 2:
            return parts[-2]  # Second to last part is usually city
        
        return "your area"

class CampaignManager:
    """Email campaign management"""
    
    def __init__(self, email_sender):
        self.email_sender = email_sender
        self.campaigns = []
        self.logger = logging.getLogger(__name__)
    
    def create_campaign(self, name: str, template: EmailTemplate, leads: List[Dict],
                       sender_email: str, sender_name: str = "", delivery_rate: int = 10) -> EmailCampaign:
        """Create a new email campaign"""
        campaign = EmailCampaign(
            name=name,
            template=template,
            leads=leads,
            sender_email=sender_email,
            sender_name=sender_name,
            delivery_rate=delivery_rate
        )
        
        self.campaigns.append(campaign)
        self.logger.info(f"Created campaign '{name}' with {len(leads)} leads")
        return campaign
    
    def run_campaign(self, campaign: EmailCampaign, dry_run: bool = False) -> Dict:
        """Run an email campaign"""
        if campaign.status == "running":
            self.logger.warning(f"Campaign '{campaign.name}' is already running")
            return {"status": "error", "message": "Campaign already running"}
        
        campaign.status = "running"
        sent_count = 0
        failed_count = 0
        
        self.logger.info(f"Starting campaign '{campaign.name}' - {len(campaign.leads)} emails to send")
        
        for i, lead in enumerate(campaign.leads):
            try:
                # Personalize email
                subject, body_text, body_html = EmailPersonalizer.personalize_template(
                    campaign.template, lead
                )
                
                to_email = lead.get('contact_email')
                if not to_email:
                    self.logger.warning(f"No email for lead: {lead.get('business_name', 'Unknown')}")
                    failed_count += 1
                    continue
                
                if dry_run:
                    self.logger.info(f"DRY RUN: Would send to {to_email}: {subject}")
                    sent_count += 1
                else:
                    # Send email
                    success = self.email_sender.send_email(
                        to_email=to_email,
                        subject=subject,
                        body_text=body_text,
                        body_html=body_html,
                        sender_name=campaign.sender_name
                    )
                    
                    if success:
                        sent_count += 1
                    else:
                        failed_count += 1
                
                # Rate limiting
                if i < len(campaign.leads) - 1:  # Don't wait after last email
                    delay = campaign.get_next_send_time()
                    time.sleep(delay)
            
            except Exception as e:
                self.logger.error(f"Error sending email to {lead.get('contact_email', 'unknown')}: {e}")
                failed_count += 1
        
        campaign.sent_count = sent_count
        campaign.status = "completed"
        
        result = {
            "status": "completed",
            "sent": sent_count,
            "failed": failed_count,
            "total": len(campaign.leads)
        }
        
        self.logger.info(f"Campaign '{campaign.name}' completed: {sent_count} sent, {failed_count} failed")
        return result

class TemplateManager:
    """Email template management"""
    
    def __init__(self, templates_dir: str = "templates"):
        self.templates_dir = Path(templates_dir)
        self.templates_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def create_template(self, name: str, subject: str, body_text: str, body_html: str = "") -> EmailTemplate:
        """Create and save an email template"""
        template = EmailTemplate(
            name=name,
            subject=subject,
            body_text=body_text,
            body_html=body_html
        )
        
        self.save_template(template)
        return template
    
    def save_template(self, template: EmailTemplate):
        """Save template to file"""
        template_data = {
            'name': template.name,
            'subject': template.subject,
            'body_text': template.body_text,
            'body_html': template.body_html,
            'variables': template.variables
        }
        
        filename = self.templates_dir / f"{template.name.lower().replace(' ', '_')}.json"
        with open(filename, 'w') as f:
            json.dump(template_data, f, indent=2)
        
        self.logger.info(f"Saved template '{template.name}' to {filename}")
    
    def load_template(self, name: str) -> Optional[EmailTemplate]:
        """Load template from file"""
        filename = self.templates_dir / f"{name.lower().replace(' ', '_')}.json"
        
        if not filename.exists():
            self.logger.error(f"Template file not found: {filename}")
            return None
        
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
            
            return EmailTemplate(
                name=data['name'],
                subject=data['subject'],
                body_text=data['body_text'],
                body_html=data.get('body_html', '')
            )
        
        except Exception as e:
            self.logger.error(f"Error loading template: {e}")
            return None
    
    def list_templates(self) -> List[str]:
        """List available templates"""
        templates = []
        for file in self.templates_dir.glob("*.json"):
            templates.append(file.stem.replace('_', ' ').title())
        return templates

def create_sample_templates():
    """Create sample email templates"""
    template_manager = TemplateManager()
    
    # Template 1: Introduction
    intro_template = template_manager.create_template(
        name="Business Introduction",
        subject="Quick question about {{business_name}}",
        body_text="""Hi {{first_name}},

I came across {{business_name}} and was impressed by your {{rating}}-star rating in {{city}}!

I help {{category}} businesses like yours increase their online visibility and attract more customers.

Would you be interested in a quick 15-minute call to discuss how we could help {{business_name}} grow?

Best regards,
[Your Name]
[Your Company]
[Your Phone]""",
        body_html="""<p>Hi {{first_name}},</p>

<p>I came across <strong>{{business_name}}</strong> and was impressed by your {{rating}}-star rating in {{city}}!</p>

<p>I help {{category}} businesses like yours increase their online visibility and attract more customers.</p>

<p>Would you be interested in a quick 15-minute call to discuss how we could help {{business_name}} grow?</p>

<p>Best regards,<br>
[Your Name]<br>
[Your Company]<br>
[Your Phone]</p>"""
    )
    
    # Template 2: Service Offer
    service_template = template_manager.create_template(
        name="Service Offering",
        subject="Helping {{category}} businesses in {{city}}",
        body_text="""Hello {{first_name}},

I noticed {{business_name}} has been serving customers in {{city}}. That's fantastic!

We specialize in helping {{category}} businesses like yours:
- Increase online reviews and ratings
- Improve local search visibility
- Generate more qualified leads
- Automate customer follow-up

Many of our {{category}} clients see 30-50% more leads within 90 days.

Would you like to see how this could work for {{business_name}}?

I'm happy to provide a free 15-minute consultation.

Best,
[Your Name]""",
        body_html="""<p>Hello {{first_name}},</p>

<p>I noticed <strong>{{business_name}}</strong> has been serving customers in {{city}}. That's fantastic!</p>

<p>We specialize in helping {{category}} businesses like yours:</p>
<ul>
<li>Increase online reviews and ratings</li>
<li>Improve local search visibility</li>
<li>Generate more qualified leads</li>
<li>Automate customer follow-up</li>
</ul>

<p>Many of our {{category}} clients see 30-50% more leads within 90 days.</p>

<p>Would you like to see how this could work for <strong>{{business_name}}</strong>?</p>

<p>I'm happy to provide a free 15-minute consultation.</p>

<p>Best,<br>[Your Name]</p>"""
    )
    
    return [intro_template, service_template]

def test_outreach():
    """Test outreach functionality"""
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("🧪 Testing Milestone 5: Email Outreach")
    print("=" * 50)
    
    # Create sample templates
    templates = create_sample_templates()
    print(f"✅ Created {len(templates)} email templates")
    
    # Load test leads (from previous milestones)
    try:
        with open("data/exports/milestone3_enriched_leads.json", 'r') as f:
            test_leads = json.load(f)[:3]  # Use first 3 leads
    except FileNotFoundError:
        test_leads = [
            {
                "business_name": "Tony's Pizza Palace",
                "contact_email": "info@tonyspizza.com",
                "category": "Italian Restaurant",
                "rating": 4.5,
                "address": "123 Main St, New York, NY 10001"
            }
        ]
    
    print(f"✅ Loaded {len(test_leads)} test leads")
    
    # Create mock email sender
    email_sender = MockEmailSender("your_email@company.com")
    
    # Create campaign manager
    campaign_manager = CampaignManager(email_sender)
    
    # Create and run campaign
    campaign = campaign_manager.create_campaign(
        name="Test Campaign",
        template=templates[0],  # Use first template
        leads=test_leads,
        sender_email="your_email@company.com",
        sender_name="Your Name",
        delivery_rate=60  # 1 email per minute for testing
    )
    
    print(f"✅ Created campaign: {campaign.name}")
    
    # Run campaign (dry run first)
    print("\n🔍 Running dry run...")
    dry_result = campaign_manager.run_campaign(campaign, dry_run=True)
    print(f"Dry run result: {dry_result}")
    
    # Run actual campaign (mock)
    print("\n📧 Running actual campaign (mock)...")
    result = campaign_manager.run_campaign(campaign, dry_run=False)
    print(f"Campaign result: {result}")
    
    # Save sent emails log
    email_sender.save_sent_emails("milestone5_campaign_log.json")
    
    # Show sample email
    if email_sender.sent_emails:
        sample_email = email_sender.sent_emails[0]
        print(f"\n📬 SAMPLE EMAIL:")
        print(f"To: {sample_email['to']}")
        print(f"Subject: {sample_email['subject']}")
        print(f"Body Preview: {sample_email['body_text'][:200]}...")
    
    print(f"\n✅ Milestone 5 outreach test completed!")
    print(f"📊 Results: {result['sent']} sent, {result['failed']} failed")

if __name__ == "__main__":
    test_outreach()
