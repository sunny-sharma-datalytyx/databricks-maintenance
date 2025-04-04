"""
Tests for the LibraryManager class.
"""

import unittest
from unittest.mock import patch, MagicMock
import json
import os
import sys

# Add parent directory to path to import module under test
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from databricks_maintenance.library_manager import LibraryManager

class TestLibraryManager(unittest.TestCase):
    """Tests for the LibraryManager class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.api_client = MagicMock()
        self.cache = MagicMock()
        self.library_manager = LibraryManager(self.api_client, self.cache)
    
    def test_get_installed_libraries(self):
        """Test getting installed libraries for a cluster."""
        # Set up API client to return sample data
        self.api_client.get_libraries_status.return_value = {
            "library_statuses": [
                {
                    "library": {"pypi": {"package": "numpy", "repo": "pypi"}},
                    "status": "INSTALLED",
                    "library_details": {"pypi": {"version": "1.21.0"}}
                },
                {
                    "library": {"pypi": {"package": "pandas", "repo": "pypi"}},
                    "status": "INSTALLED",
                    "library_details": {"pypi": {"version": "1.2.0"}}
                }
            ]
        }
        
        # Call method
        libraries = self.library_manager.get_installed_libraries("cluster123")
        
        # Verify
        self.api_client.get_libraries_status.assert_called_once_with("cluster123")
        self.assertEqual(len(libraries), 2)
        self.assertEqual(libraries[0]["library"]["pypi"]["package"], "numpy")
        self.assertEqual(libraries[1]["library"]["pypi"]["package"], "pandas")
    
    @patch('requests.get')
    def test_check_pypi_package_updates(self, mock_get):
        """Test checking for PyPI package updates."""
        # Set up cache to return None (cache miss)
        self.cache.get.return_value = None
        
        # Set up mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "info": {
                "version": "1.22.4"
            }
        }
        mock_get.return_value = mock_response
        
        # Call method
        update_info = self.library_manager.check_pypi_package_updates("numpy", "1.21.0")
        
        # Verify
        self.cache.get.assert_called_once_with("pypi_numpy")
        mock_get.assert_called_once_with("https://pypi.org/pypi/numpy/json", timeout=10)
        self.cache.set.assert_called_once_with("pypi_numpy", {"latest_version": "1.22.4"})
        
        # Should detect that an update is available
        self.assertTrue(update_info["update_available"])
        self.assertEqual(update_info["current_version"], "1.21.0")
        self.assertEqual(update_info["latest_version"], "1.22.4")
    
    @patch('concurrent.futures.ThreadPoolExecutor')
    def test_check_library_versions(self, mock_executor):
        """Test checking for outdated libraries on a cluster."""
        # Set up manager to return sample installed libraries
        self.library_manager.get_installed_libraries = MagicMock(return_value=[
            {
                "library": {"pypi": {"package": "numpy", "repo": "pypi==1.19.0"}},
                "status": "INSTALLED"
            },
            {
                "library": {"pypi": {"package": "requests", "repo": "pypi==2.25.0"}},
                "status": "INSTALLED"
            },
            {
                "library": {"pypi": {"package": "pandas", "repo": "pypi==1.3.4"}},
                "status": "INSTALLED"
            }
        ])
        
        # Set up manager to return update info for specific packages
        def check_pypi_side_effect(package, version):
            if package == "numpy":
                return {
                    "current_version": "1.19.0",
                    "latest_version": "1.22.4",
                    "update_available": True
                }
            elif package == "requests":
                return {
                    "current_version": "2.25.0",
                    "latest_version": "2.28.1",
                    "update_available": True
                }
            else:
                return None  # No update for pandas
        
        self.library_manager.check_pypi_package_updates = MagicMock(side_effect=check_pypi_side_effect)
        
        # Set up executor to run the functions directly
        mock_executor.return_value.__enter__.return_value.map = lambda func, args: [func(arg) for arg in args]
        
        # Call method
        outdated_libraries = self.library_manager.check_library_versions("cluster123")
        
        # Verify
        self.library_manager.get_installed_libraries.assert_called_once_with("cluster123")
        
        # Should find 2 outdated libraries (numpy and requests)
        self.assertEqual(len(outdated_libraries), 2)
        
        # numpy should be flagged as high severity (security critical)
        self.assertEqual(outdated_libraries[0]["library_name"], "numpy")
        self.assertEqual(outdated_libraries[0]["severity"], "high")
        
        # requests should also be flagged (security critical)
        self.assertEqual(outdated_libraries[1]["library_name"], "requests")


if __name__ == '__main__':
    unittest.main()


