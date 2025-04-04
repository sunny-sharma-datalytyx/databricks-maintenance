"""
Tests for the RuntimeManager class.
"""

import unittest
from unittest.mock import patch, MagicMock
import json
import os
import sys
import datetime
from bs4 import BeautifulSoup

# Add parent directory to path to import module under test
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from databricks_maintenance.runtime_manager import RuntimeManager

class TestRuntimeManager(unittest.TestCase):
    """Tests for the RuntimeManager class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.api_client = MagicMock()
        self.cache = MagicMock()
        self.runtime_manager = RuntimeManager(self.api_client, self.cache)
    
    def test_get_available_runtime_versions(self):
        """Test getting available runtime versions."""
        # Set up cache to return None (cache miss)
        self.cache.get.return_value = None
        
        # Set up API client to return sample data
        self.api_client.get_spark_versions.return_value = {
            "versions": [
                {"key": "7.3.x-scala2.12", "name": "7.3 LTS (Scala 2.12)"},
                {"key": "8.4.x-scala2.12", "name": "8.4 (Scala 2.12)"},
                {"key": "9.1.x-scala2.12", "name": "9.1 (Scala 2.12)"}
            ]
        }
        
        # Call method
        versions = self.runtime_manager.get_available_runtime_versions()
        
        # Verify
        self.cache.get.assert_called_once_with("runtime_versions")
        self.api_client.get_spark_versions.assert_called_once()
        self.cache.set.assert_called_once()  # Should cache the result
        
        # Check that versions are properly extracted and sorted
        self.assertEqual(len(versions), 3)
        self.assertEqual(versions[0]["version"], "7.3")
        self.assertEqual(versions[1]["version"], "8.4")
        self.assertEqual(versions[2]["version"], "9.1")
    
    @patch('requests.get')
    def test_fetch_deprecation_dates_from_docs(self, mock_get):
        """Test fetching deprecation dates from documentation."""
        # Set up cache to return None (cache miss)
        self.cache.get.return_value = None
        
        # Set up mock for available versions
        self.runtime_manager.get_available_runtime_versions = MagicMock(return_value=[
            {"key": "8.4.x-scala2.12", "name": "8.4 (Scala 2.12)", "version": "8.4"},
            {"key": "9.1.x-scala2.12", "name": "9.1 (Scala 2.12)", "version": "9.1"}
        ])
        
        # Create a simple HTML page with a table containing deprecation info
        html_content = """
        <html>
            <body>
                <table>
                    <tr>
                        <th>Runtime Version</th>
                        <th>End of Life Date</th>
                    </tr>
                    <tr>
                        <td>7.3 LTS</td>
                        <td>January 15, 2023</td>
                    </tr>
                    <tr>
                        <td>8.0</td>
                        <td>2022-12-31</td>
                    </tr>
                </table>
                <p>Runtime version 6.4 is deprecated as of June 30, 2022.</p>
            </body>
        </html>
        """
        
        # Set up mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html_content
        mock_get.return_value = mock_response
        
        # Call method
        deprecation_dates = self.runtime_manager.fetch_deprecation_dates_from_docs()
        
        # Verify
        self.cache.get.assert_called_once_with("deprecation_dates")
        mock_get.assert_called()  # Should make HTTP requests
        self.cache.set.assert_called_once_with("deprecation_dates", deprecation_dates)
        
        # Check that deprecation dates are properly extracted
        self.assertIn("7.3", deprecation_dates)
        self.assertEqual(deprecation_dates["7.3"]["deprecation_date"], "2023-01-15")
        
        self.assertIn("8.0", deprecation_dates)
        self.assertEqual(deprecation_dates["8.0"]["deprecation_date"], "2022-12-31")
        
        # Should also have inferred that 6.4 is deprecated
        self.assertIn("6.4", deprecation_dates)
    
    def test_get_deprecated_runtime_clusters(self):
        """Test identifying clusters with deprecated runtimes."""
        # Set up manager to return sample deprecation dates
        self.runtime_manager.fetch_deprecation_dates_from_docs = MagicMock(return_value={
            "7.3": {
                "version": "7.3",
                "deprecation_date": "2023-01-15",
                "source": "docs"
            },
            "8.0": {
                "version": "8.0",
                "deprecation_date": (datetime.datetime.now() + datetime.timedelta(days=30)).strftime("%Y-%m-%d"),
                "source": "docs"
            }
        })
        
        # Set up manager to return sample available runtimes
        self.runtime_manager.get_available_runtime_versions = MagicMock(return_value=[
            {"key": "8.4.x-scala2.12", "name": "8.4 (Scala 2.12)", "version": "8.4"},
            {"key": "9.1.x-scala2.12", "name": "9.1 (Scala 2.12)", "version": "9.1"}
        ])
        
        # Set up API client to return sample clusters
        self.api_client.get_cluster_list.return_value = [
            {
                "cluster_id": "cluster1",
                "cluster_name": "Prod Cluster",
                "spark_version": "7.3.x-scala2.12"
            },
            {
                "cluster_id": "cluster2",
                "cluster_name": "Dev Cluster",
                "spark_version": "8.0.x-scala2.12"
            },
            {
                "cluster_id": "cluster3",
                "cluster_name": "New Cluster",
                "spark_version": "9.1.x-scala2.12"
            }
        ]
        
        # Call method
        deprecated_clusters = self.runtime_manager.get_deprecated_runtime_clusters()
        
        # Verify
        self.assertEqual(len(deprecated_clusters), 2)  # Should find 2 deprecated clusters
        
        # First cluster should be marked as DEPRECATED (past date)
        self.assertEqual(deprecated_clusters[0]["cluster_id"], "cluster1")
        self.assertEqual(deprecated_clusters[0]["status"], "DEPRECATED")
        
        # Second cluster should be marked as SOON_DEPRECATED (future date)
        self.assertEqual(deprecated_clusters[1]["cluster_id"], "cluster2")
        self.assertEqual(deprecated_clusters[1]["status"], "SOON_DEPRECATED")
    
    def test_get_current_lts_runtimes(self):
        """Test identifying current LTS runtime versions."""
        # Set up manager to return sample available runtimes
        self.runtime_manager.get_available_runtime_versions = MagicMock(return_value=[
            {"key": "7.3.x-scala2.12", "name": "7.3 LTS (Scala 2.12)", "version": "7.3"},
            {"key": "8.4.x-scala2.12", "name": "8.4 (Scala 2.12)", "version": "8.4"},
            {"key": "9.1.x-scala2.12", "name": "9.1 LTS (Scala 2.12)", "version": "9.1"}
        ])
        
        # Call method
        lts_runtimes = self.runtime_manager.get_current_lts_runtimes()
        
        # Verify
        self.assertEqual(len(lts_runtimes), 2)  # Should find 2 LTS runtimes
        self.assertEqual(lts_runtimes[0]["version"], "9.1")  # Should be sorted with highest version first
        self.assertEqual(lts_runtimes[1]["version"], "7.3")
    
    def test_recommend_runtime_upgrades(self):
        """Test recommending runtime upgrades for clusters."""
        # Set up manager to return sample LTS runtimes
        self.runtime_manager.get_current_lts_runtimes = MagicMock(return_value=[
            {"key": "9.1.x-scala2.12", "name": "9.1 LTS (Scala 2.12)", "version": "9.1"}
        ])
        
        # Set up manager to return sample available runtimes
        self.runtime_manager.get_available_runtime_versions = MagicMock(return_value=[
            {"key": "8.4.x-scala2.12", "name": "8.4 (Scala 2.12)", "version": "8.4"},
            {"key": "9.1.x-scala2.12", "name": "9.1 LTS (Scala 2.12)", "version": "9.1"},
            {"key": "10.4.x-cpu-ml-scala2.12", "name": "10.4 ML (Scala 2.12)", "version": "10.4"}
        ])
        
        # Sample clusters to upgrade
        clusters = [
            {
                "cluster_id": "cluster1",
                "cluster_name": "Prod Cluster",
                "current_runtime": "7.3.x-scala2.12"
            },
            {
                "cluster_id": "cluster2",
                "cluster_name": "ML Dev Cluster",
                "current_runtime": "8.0.x-cpu-ml-scala2.12"
            }
        ]
        
        # Call method
        recommendations = self.runtime_manager.recommend_runtime_upgrades(clusters)
        
        # Verify
        self.assertEqual(len(recommendations), 2)  # Should have recommendations for both clusters
        
        # Production cluster should get LTS runtime recommendation
        self.assertEqual(recommendations["cluster1"]["runtime_name"], "9.1 LTS (Scala 2.12)")
        
        # ML cluster should get ML runtime recommendation
        self.assertEqual(recommendations["cluster2"]["runtime_name"], "10.4 ML (Scala 2.12)")


if __name__ == '__main__':
    unittest.main()


