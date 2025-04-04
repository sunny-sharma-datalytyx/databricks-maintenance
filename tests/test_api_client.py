"""
Tests for the DatabricksApiClient class.
"""

import unittest
from unittest.mock import patch, MagicMock
import json
import os
import sys
import requests

# Add parent directory to path to import module under test
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from databricks_maintenance.api_client import DatabricksApiClient
from databricks_maintenance.cache import CacheManager

class TestDatabricksApiClient(unittest.TestCase):
    """Tests for the DatabricksApiClient class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.cache = MagicMock(spec=CacheManager)
        self.client = DatabricksApiClient("https://test-workspace.cloud.databricks.com", "dummy-token", self.cache)
    
    def test_init(self):
        """Test initialization of the client."""
        self.assertEqual(self.client.workspace_url, "https://test-workspace.cloud.databricks.com")
        self.assertEqual(self.client.token, "dummy-token")
        self.assertEqual(self.client.headers["Authorization"], "Bearer dummy-token")
        self.assertEqual(self.client.headers["Content-Type"], "application/json")
    
    @patch('requests.get')
    def test_make_api_request_get(self, mock_get):
        """Test making a GET API request."""
        # Set up mock
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"key": "value"}'
        mock_get.return_value = mock_response
        
        # Call method
        result = self.client.make_api_request("get", "2.0/endpoint", error_message="Test error")
        
        # Verify
        mock_get.assert_called_once_with(
            "https://test-workspace.cloud.databricks.com/api/2.0/endpoint",
            headers=self.client.headers,
            json=None,
            timeout=30
        )
        self.assertEqual(result, {"key": "value"})
    
    @patch('requests.post')
    def test_make_api_request_post(self, mock_post):
        """Test making a POST API request."""
        # Set up mock
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"success": true}'
        mock_post.return_value = mock_response
        
        # Call method
        data = {"param": "value"}
        result = self.client.make_api_request("post", "2.0/endpoint", data=data)
        
        # Verify
        mock_post.assert_called_once_with(
            "https://test-workspace.cloud.databricks.com/api/2.0/endpoint",
            headers=self.client.headers,
            json=data,
            timeout=30
        )
        self.assertEqual(result, {"success": True})
    
    @patch('requests.get')
    def test_make_api_request_error(self, mock_get):
        """Test handling of API errors."""
        # Set up mock for error response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = '{"error": "Not Found"}'
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        # Call method and check exception
        with self.assertRaises(requests.exceptions.HTTPError):
            self.client.make_api_request("get", "2.0/endpoint", retry_count=1)
        
        # Verify
        mock_get.assert_called_once()
        mock_response.raise_for_status.assert_called_once()
    
    @patch('requests.get')
    def test_make_api_request_retry(self, mock_get):
        """Test retry logic for API requests."""
        # Set up mocks for failed and successful responses
        fail_response = MagicMock()
        fail_response.status_code = 500
        fail_response.text = '{"error": "Internal Server Error"}'
        
        success_response = MagicMock()
        success_response.status_code = 200
        success_response.text = '{"success": true}'
        
        # First call fails, second succeeds
        mock_get.side_effect = [fail_response, success_response]
        
        # Call method
        with patch('time.sleep') as mock_sleep:  # Prevent actual sleep
            result = self.client.make_api_request("get", "2.0/endpoint", retry_count=2, retry_delay=1)
        
        # Verify
        self.assertEqual(mock_get.call_count, 2)
        mock_sleep.assert_called_once_with(1)  # Should sleep once after first failure
        self.assertEqual(result, {"success": True})
    
    @patch('requests.get')
    def test_get_cluster_list(self, mock_get):
        """Test getting the cluster list."""
        # Set up mock
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"clusters": [{"cluster_id": "123", "cluster_name": "Test Cluster"}]}'
        mock_get.return_value = mock_response
        
        # Set up cache to return None (cache miss)
        self.cache.get.return_value = None
        
        # Call method
        clusters = self.client.get_cluster_list()
        
        # Verify
        self.cache.get.assert_called_once_with("clusters_list")
        mock_get.assert_called_once()
        self.cache.set.assert_called_once_with("clusters_list", [{"cluster_id": "123", "cluster_name": "Test Cluster"}])
        self.assertEqual(clusters, [{"cluster_id": "123", "cluster_name": "Test Cluster"}])
    
    @patch('requests.get')
    def test_get_cluster_list_cached(self, mock_get):
        """Test getting the cluster list from cache."""
        # Set up cache to return data (cache hit)
        cached_clusters = [{"cluster_id": "456", "cluster_name": "Cached Cluster"}]
        self.cache.get.return_value = cached_clusters
        
        # Call method
        clusters = self.client.get_cluster_list()
        
        # Verify
        self.cache.get.assert_called_once_with("clusters_list")
        mock_get.assert_not_called()  # API call should not be made
        self.assertEqual(clusters, cached_clusters)


if __name__ == '__main__':
    unittest.main()


