"""
Test cases for Milestone 2: Lead Collection
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import json
import csv
from pathlib import Path

def test_simple_collector():
    """Test the simple collector functionality"""
    try:
        from src.collectors.simple_collector import SimpleCollector, Lead
        
        # Test Lead class
        lead = Lead(
            business_name="Test Business",
            phone="(555) 123-4567",
            address="123 Test St",
            category="Test Category"
        )
        
        assert lead.business_name == "Test Business"
        assert lead.to_dict()['business_name'] == "Test Business"
        print("✅ Lead class test passed")
        
        # Test SimpleCollector
        collector = SimpleCollector()
        leads = collector.create_sample_leads(5)
        
        assert len(leads) == 5
        assert all(isinstance(lead, Lead) for lead in leads)
        assert leads[0].business_name != ""
        print("✅ SimpleCollector test passed")
        
        return True
    
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_data_export():
    """Test data export functionality"""
    try:
        from src.collectors.simple_collector import SimpleCollector
        
        # Ensure export directory exists
        Path("data/exports").mkdir(parents=True, exist_ok=True)
        
        collector = SimpleCollector()
        collector.create_sample_leads(3)
        
        # Test CSV export
        collector.save_to_csv("test_export.csv")
        csv_file = Path("data/exports/test_export.csv")
        assert csv_file.exists()
        
        # Verify CSV content
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 3
            assert 'business_name' in rows[0]
        
        print("✅ CSV export test passed")
        
        # Test JSON export
        collector.save_to_json("test_export.json")
        json_file = Path("data/exports/test_export.json")
        assert json_file.exists()
        
        # Verify JSON content
        with open(json_file, 'r') as f:
            data = json.load(f)
            assert len(data) == 3
            assert 'business_name' in data[0]
        
        print("✅ JSON export test passed")
        
        return True
    
    except Exception as e:
        print(f"❌ Export test failed: {e}")
        return False

def test_milestone2_integration():
    """Test complete Milestone 2 functionality"""
    try:
        from src.collectors.simple_collector import test_collector
        
        # Run the full test
        test_collector()
        
        # Verify files were created
        csv_file = Path("data/exports/milestone2_test_leads.csv")
        json_file = Path("data/exports/milestone2_test_leads.json")
        
        assert csv_file.exists(), "CSV file not created"
        assert json_file.exists(), "JSON file not created"
        
        print("✅ Milestone 2 integration test passed")
        return True
    
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        return False

def main():
    """Run all Milestone 2 tests"""
    print("🧪 Testing Milestone 2: Lead Collection")
    print("=" * 50)
    
    tests = [
        ("Simple Collector", test_simple_collector),
        ("Data Export", test_data_export),
        ("Integration", test_milestone2_integration)
    ]
    
    passed = 0
    for test_name, test_func in tests:
        print(f"\n🔍 Running {test_name} test...")
        if test_func():
            passed += 1
        else:
            print(f"❌ {test_name} test failed")
    
    print(f"\n📊 Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("🎉 All Milestone 2 tests passed!")
        print("\n📝 Next steps for Milestone 2:")
        print("1. Install Selenium: pip install selenium webdriver-manager")
        print("2. Run full Yelp scraper: python src/collectors/yelp_collector.py")
        print("3. Review collected data in data/exports/")
    else:
        print("❌ Some tests failed. Please check the errors above.")

if __name__ == "__main__":
    main()
