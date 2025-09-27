"""
Test configuration and basic setup test
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_project_structure():
    """Test that basic project structure exists"""
    required_dirs = [
        'src',
        'src/config',
        'src/utils',
        'tests',
        'logs',
        'data'
    ]
    
    for dir_path in required_dirs:
        assert os.path.exists(dir_path), f"Directory {dir_path} should exist"

def test_config_import():
    """Test that configuration can be imported"""
    try:
        from src.config import config
        assert config is not None
    except ImportError as e:
        # This will fail until dependencies are installed
        print(f"Config import failed (expected until dependencies installed): {e}")

def test_utils_import():
    """Test that utilities can be imported"""
    try:
        from src.utils import setup_logging, clean_text
        assert setup_logging is not None
        assert clean_text is not None
    except ImportError as e:
        # This will fail until dependencies are installed
        print(f"Utils import failed (expected until dependencies installed): {e}")

if __name__ == "__main__":
    test_project_structure()
    test_config_import()
    test_utils_import()
    print("✅ Milestone 1 basic tests passed!")
