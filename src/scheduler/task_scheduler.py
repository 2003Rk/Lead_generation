"""
Campaign Scheduler Module
Handles scheduling and automation of lead generation and outreach campaigns
"""
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, asdict
from pathlib import Path
import threading
from enum import Enum

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class ScheduledTask:
    """Scheduled task data structure"""
    id: str
    name: str
    task_type: str  # 'lead_collection', 'enrichment', 'outreach', 'data_export'
    schedule_time: datetime
    repeat_interval: Optional[int] = None  # minutes
    max_repeats: Optional[int] = None
    config: Dict = None
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = None
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0
    results: Dict = None
    
    def __post_init__(self):
        if self.config is None:
            self.config = {}
        if self.results is None:
            self.results = {}
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.next_run is None:
            self.next_run = self.schedule_time

class TaskScheduler:
    """Main task scheduler class"""
    
    def __init__(self, data_file: str = "data/scheduler_tasks.json"):
        self.data_file = Path(data_file)
        self.data_file.parent.mkdir(parents=True, exist_ok=True)
        
        self.tasks: Dict[str, ScheduledTask] = {}
        self.running = False
        self.scheduler_thread = None
        self.task_handlers = {}
        
        self.logger = logging.getLogger(__name__)
        
        # Load existing tasks
        self.load_tasks()
        
        # Register default task handlers
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """Register default task handlers"""
        self.task_handlers = {
            'lead_collection': self._handle_lead_collection,
            'enrichment': self._handle_enrichment,
            'outreach': self._handle_outreach,
            'data_export': self._handle_data_export,
            'cleanup': self._handle_cleanup
        }
    
    def schedule_task(self, name: str, task_type: str, schedule_time: datetime,
                     config: Dict = None, repeat_interval: int = None,
                     max_repeats: int = None) -> str:
        """Schedule a new task"""
        task_id = f"{task_type}_{int(time.time())}"
        
        task = ScheduledTask(
            id=task_id,
            name=name,
            task_type=task_type,
            schedule_time=schedule_time,
            repeat_interval=repeat_interval,
            max_repeats=max_repeats,
            config=config or {}
        )
        
        self.tasks[task_id] = task
        self.save_tasks()
        
        self.logger.info(f"Scheduled task '{name}' ({task_id}) for {schedule_time}")
        return task_id
    
    def start_scheduler(self):
        """Start the task scheduler"""
        if self.running:
            self.logger.warning("Scheduler is already running")
            return
        
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        self.logger.info("Task scheduler started")
    
    def stop_scheduler(self):
        """Stop the task scheduler"""
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        
        self.logger.info("Task scheduler stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop"""
        while self.running:
            try:
                current_time = datetime.now()
                
                # Check for tasks to run
                for task in self.tasks.values():
                    if self._should_run_task(task, current_time):
                        self._run_task(task)
                
                # Sleep for 30 seconds before next check
                time.sleep(30)
            
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
                time.sleep(60)  # Wait longer on error
    
    def _should_run_task(self, task: ScheduledTask, current_time: datetime) -> bool:
        """Check if a task should run now"""
        if task.status == TaskStatus.RUNNING:
            return False
        
        if task.status == TaskStatus.CANCELLED:
            return False
        
        if task.next_run and current_time >= task.next_run:
            # Check if we've exceeded max repeats
            if task.max_repeats and task.run_count >= task.max_repeats:
                task.status = TaskStatus.COMPLETED
                return False
            
            return True
        
        return False
    
    def _run_task(self, task: ScheduledTask):
        """Execute a scheduled task"""
        self.logger.info(f"Running task: {task.name} ({task.id})")
        
        task.status = TaskStatus.RUNNING
        task.last_run = datetime.now()
        task.run_count += 1
        
        try:
            # Get task handler
            handler = self.task_handlers.get(task.task_type)
            if not handler:
                raise ValueError(f"No handler for task type: {task.task_type}")
            
            # Run the task
            result = handler(task)
            task.results = result
            task.status = TaskStatus.COMPLETED
            
            self.logger.info(f"Task completed successfully: {task.name}")
        
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.results = {"error": str(e)}
            self.logger.error(f"Task failed: {task.name} - {e}")
        
        # Schedule next run if repeating
        if task.repeat_interval and (not task.max_repeats or task.run_count < task.max_repeats):
            task.next_run = datetime.now() + timedelta(minutes=task.repeat_interval)
            task.status = TaskStatus.PENDING
            self.logger.info(f"Next run scheduled for {task.next_run}")
        
        self.save_tasks()
    
    def _handle_lead_collection(self, task: ScheduledTask) -> Dict:
        """Handle lead collection task"""
        self.logger.info("Running lead collection task")
        
        # Mock lead collection
        config = task.config
        query = config.get('query', 'restaurants')
        location = config.get('location', 'New York, NY')
        max_results = config.get('max_results', 20)
        
        # Simulate lead collection
        time.sleep(2)  # Simulate work
        
        result = {
            'leads_collected': max_results,
            'query': query,
            'location': location,
            'source': 'yelp',
            'timestamp': datetime.now().isoformat()
        }
        
        return result
    
    def _handle_enrichment(self, task: ScheduledTask) -> Dict:
        """Handle lead enrichment task"""
        self.logger.info("Running lead enrichment task")
        
        config = task.config
        source_file = config.get('source_file', 'latest_leads.json')
        
        # Simulate enrichment
        time.sleep(3)  # Simulate work
        
        result = {
            'leads_enriched': 15,
            'source_file': source_file,
            'apis_used': ['hunter.io', 'clearbit'],
            'timestamp': datetime.now().isoformat()
        }
        
        return result
    
    def _handle_outreach(self, task: ScheduledTask) -> Dict:
        """Handle email outreach task"""
        self.logger.info("Running outreach campaign")
        
        config = task.config
        campaign_name = config.get('campaign_name', 'Automated Campaign')
        template = config.get('template', 'default')
        max_emails = config.get('max_emails', 10)
        
        # Simulate outreach
        time.sleep(1)  # Simulate work
        
        result = {
            'emails_sent': max_emails,
            'campaign_name': campaign_name,
            'template': template,
            'delivery_rate': '95%',
            'timestamp': datetime.now().isoformat()
        }
        
        return result
    
    def _handle_data_export(self, task: ScheduledTask) -> Dict:
        """Handle data export task"""
        self.logger.info("Running data export task")
        
        config = task.config
        export_format = config.get('format', 'csv')
        include_enrichment = config.get('include_enrichment', True)
        
        # Simulate export
        time.sleep(1)  # Simulate work
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"leads_export_{timestamp}.{export_format}"
        
        result = {
            'export_file': filename,
            'format': export_format,
            'records_exported': 50,
            'include_enrichment': include_enrichment,
            'timestamp': datetime.now().isoformat()
        }
        
        return result
    
    def _handle_cleanup(self, task: ScheduledTask) -> Dict:
        """Handle cleanup task"""
        self.logger.info("Running cleanup task")
        
        config = task.config
        days_old = config.get('days_old', 30)
        
        # Simulate cleanup
        time.sleep(1)  # Simulate work
        
        result = {
            'files_cleaned': 5,
            'days_old': days_old,
            'space_freed': '100MB',
            'timestamp': datetime.now().isoformat()
        }
        
        return result
    
    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """Get status of a specific task"""
        task = self.tasks.get(task_id)
        if not task:
            return None
        
        return {
            'id': task.id,
            'name': task.name,
            'task_type': task.task_type,
            'status': task.status.value,
            'last_run': task.last_run.isoformat() if task.last_run else None,
            'next_run': task.next_run.isoformat() if task.next_run else None,
            'run_count': task.run_count,
            'results': task.results
        }
    
    def list_tasks(self, status_filter: str = None) -> List[Dict]:
        """List all tasks or filtered by status"""
        tasks = []
        
        for task in self.tasks.values():
            if status_filter and task.status.value != status_filter:
                continue
            
            tasks.append({
                'id': task.id,
                'name': task.name,
                'task_type': task.task_type,
                'status': task.status.value,
                'schedule_time': task.schedule_time.isoformat(),
                'last_run': task.last_run.isoformat() if task.last_run else None,
                'next_run': task.next_run.isoformat() if task.next_run else None,
                'run_count': task.run_count
            })
        
        return sorted(tasks, key=lambda x: x['schedule_time'])
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a scheduled task"""
        task = self.tasks.get(task_id)
        if not task:
            return False
        
        task.status = TaskStatus.CANCELLED
        self.save_tasks()
        
        self.logger.info(f"Cancelled task: {task.name} ({task_id})")
        return True
    
    def save_tasks(self):
        """Save tasks to file"""
        tasks_data = {}
        
        for task_id, task in self.tasks.items():
            task_dict = asdict(task)
            # Convert datetime objects to ISO strings
            for key, value in task_dict.items():
                if isinstance(value, datetime):
                    task_dict[key] = value.isoformat()
                elif isinstance(value, TaskStatus):
                    task_dict[key] = value.value
            
            tasks_data[task_id] = task_dict
        
        with open(self.data_file, 'w') as f:
            json.dump(tasks_data, f, indent=2)
    
    def load_tasks(self):
        """Load tasks from file"""
        if not self.data_file.exists():
            return
        
        try:
            with open(self.data_file, 'r') as f:
                tasks_data = json.load(f)
            
            for task_id, task_dict in tasks_data.items():
                # Convert ISO strings back to datetime objects
                for key, value in task_dict.items():
                    if key.endswith('_time') or key.endswith('_run') or key.endswith('_at'):
                        if value:
                            task_dict[key] = datetime.fromisoformat(value)
                    elif key == 'status':
                        task_dict[key] = TaskStatus(value)
                
                task = ScheduledTask(**task_dict)
                self.tasks[task_id] = task
            
            self.logger.info(f"Loaded {len(self.tasks)} tasks from {self.data_file}")
        
        except Exception as e:
            self.logger.error(f"Error loading tasks: {e}")

class CampaignAutomation:
    """High-level campaign automation"""
    
    def __init__(self, scheduler: TaskScheduler):
        self.scheduler = scheduler
        self.logger = logging.getLogger(__name__)
    
    def create_weekly_campaign(self, name: str, day_of_week: int, hour: int,
                              lead_config: Dict, outreach_config: Dict) -> List[str]:
        """Create a weekly automated campaign"""
        task_ids = []
        
        # Calculate next occurrence of the specified day/hour
        now = datetime.now()
        days_ahead = day_of_week - now.weekday()
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        
        next_run = now + timedelta(days=days_ahead)
        next_run = next_run.replace(hour=hour, minute=0, second=0, microsecond=0)
        
        # Schedule lead collection
        collection_task_id = self.scheduler.schedule_task(
            name=f"{name} - Lead Collection",
            task_type="lead_collection",
            schedule_time=next_run,
            config=lead_config,
            repeat_interval=7 * 24 * 60,  # Weekly (in minutes)
            max_repeats=52  # Run for 1 year
        )
        task_ids.append(collection_task_id)
        
        # Schedule enrichment (2 hours after collection)
        enrichment_time = next_run + timedelta(hours=2)
        enrichment_task_id = self.scheduler.schedule_task(
            name=f"{name} - Enrichment",
            task_type="enrichment",
            schedule_time=enrichment_time,
            config={'source_file': 'latest_leads.json'},
            repeat_interval=7 * 24 * 60,  # Weekly
            max_repeats=52
        )
        task_ids.append(enrichment_task_id)
        
        # Schedule outreach (4 hours after collection)
        outreach_time = next_run + timedelta(hours=4)
        outreach_task_id = self.scheduler.schedule_task(
            name=f"{name} - Outreach",
            task_type="outreach",
            schedule_time=outreach_time,
            config=outreach_config,
            repeat_interval=7 * 24 * 60,  # Weekly
            max_repeats=52
        )
        task_ids.append(outreach_task_id)
        
        self.logger.info(f"Created weekly campaign '{name}' with {len(task_ids)} tasks")
        return task_ids
    
    def create_daily_followup(self, name: str, hour: int, template: str) -> str:
        """Create daily follow-up campaign"""
        # Start tomorrow at specified hour
        tomorrow = datetime.now() + timedelta(days=1)
        start_time = tomorrow.replace(hour=hour, minute=0, second=0, microsecond=0)
        
        task_id = self.scheduler.schedule_task(
            name=f"{name} - Daily Follow-up",
            task_type="outreach",
            schedule_time=start_time,
            config={
                'campaign_name': name,
                'template': template,
                'max_emails': 5
            },
            repeat_interval=24 * 60,  # Daily (in minutes)
            max_repeats=30  # Run for 30 days
        )
        
        self.logger.info(f"Created daily follow-up '{name}' starting {start_time}")
        return task_id

def test_scheduler():
    """Test scheduler functionality"""
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("🧪 Testing Milestone 6: Task Scheduler")
    print("=" * 50)
    
    # Create scheduler
    scheduler = TaskScheduler()
    
    # Schedule some test tasks
    now = datetime.now()
    
    # Task 1: Lead collection in 5 seconds
    task1_id = scheduler.schedule_task(
        name="Test Lead Collection",
        task_type="lead_collection",
        schedule_time=now + timedelta(seconds=5),
        config={
            'query': 'restaurants',
            'location': 'San Francisco, CA',
            'max_results': 25
        }
    )
    
    # Task 2: Enrichment in 10 seconds
    task2_id = scheduler.schedule_task(
        name="Test Enrichment",
        task_type="enrichment",
        schedule_time=now + timedelta(seconds=10),
        config={'source_file': 'test_leads.json'}
    )
    
    # Task 3: Outreach in 15 seconds
    task3_id = scheduler.schedule_task(
        name="Test Outreach",
        task_type="outreach",
        schedule_time=now + timedelta(seconds=15),
        config={
            'campaign_name': 'Test Campaign',
            'template': 'business_introduction',
            'max_emails': 5
        }
    )
    
    print(f"✅ Scheduled 3 test tasks")
    
    # List tasks
    tasks = scheduler.list_tasks()
    print(f"\n📋 SCHEDULED TASKS:")
    for task in tasks:
        print(f"  - {task['name']}: {task['status']} (next: {task['next_run']})")
    
    # Start scheduler
    print(f"\n🚀 Starting scheduler...")
    scheduler.start_scheduler()
    
    # Wait for tasks to complete
    print("⏳ Waiting for tasks to run...")
    time.sleep(20)
    
    # Check results
    print(f"\n📊 TASK RESULTS:")
    for task_id in [task1_id, task2_id, task3_id]:
        status = scheduler.get_task_status(task_id)
        if status:
            print(f"  {status['name']}: {status['status']}")
            if status['results']:
                print(f"    Result: {status['results']}")
    
    # Test campaign automation
    print(f"\n🤖 Testing Campaign Automation:")
    automation = CampaignAutomation(scheduler)
    
    # Create a weekly campaign starting next Monday at 9 AM
    weekly_tasks = automation.create_weekly_campaign(
        name="Weekly Restaurant Outreach",
        day_of_week=0,  # Monday
        hour=9,
        lead_config={
            'query': 'restaurants',
            'location': 'Los Angeles, CA',
            'max_results': 50
        },
        outreach_config={
            'campaign_name': 'Restaurant Weekly',
            'template': 'service_offering',
            'max_emails': 20
        }
    )
    
    print(f"✅ Created weekly campaign with {len(weekly_tasks)} tasks")
    
    # Stop scheduler
    scheduler.stop_scheduler()
    
    # Final task list
    final_tasks = scheduler.list_tasks()
    print(f"\n📋 FINAL TASK LIST ({len(final_tasks)} tasks):")
    for task in final_tasks[-3:]:  # Show last 3 tasks
        print(f"  - {task['name']}: {task['status']}")
    
    print(f"\n✅ Milestone 6 scheduler test completed!")

if __name__ == "__main__":
    test_scheduler()
