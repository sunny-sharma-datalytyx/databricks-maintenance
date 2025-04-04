"""
Databricks Maintenance Toolkit - A toolkit to automate maintenance tasks for Databricks environments.
"""

from databricks_maintenance.api_client import DatabricksApiClient
from databricks_maintenance.runtime_manager import RuntimeManager
from databricks_maintenance.library_manager import LibraryManager
from databricks_maintenance.cache import CacheManager

__version__ = "0.1.0"

class DatabricksMaintenanceManager:
    """
    A class to automate maintenance tasks for a Databricks environment.
    
    This is the main entry point for the toolkit, which provides access to various
    maintenance functionality through specialized managers.
    """
    
    def __init__(self, workspace_url: str, token: str, cache_ttl: int = 60):
        """
        Initialize the Databricks maintenance manager.
        
        Args:
            workspace_url: The URL of your Databricks workspace
            token: Your Databricks personal access token
            cache_ttl: Time to live for cached data in seconds (default: 24 hours)
        """
        self.cache = CacheManager(cache_ttl=cache_ttl)
        self.api_client = DatabricksApiClient(workspace_url, token, self.cache)
        self.runtime_manager = RuntimeManager(self.api_client, self.cache)
        self.library_manager = LibraryManager(self.api_client, self.cache)
    
    def get_cluster_list(self):
        """Get a list of all clusters in the workspace."""
        return self.api_client.get_cluster_list()
    
    def get_available_runtime_versions(self):
        """Get a list of all Databricks runtime versions available for cluster creation."""
        return self.runtime_manager.get_available_runtime_versions()
    
    def fetch_deprecation_dates_from_docs(self):
        """Scrape Databricks documentation to extract runtime version deprecation dates."""
        return self.runtime_manager.fetch_deprecation_dates_from_docs()
    
    def get_deprecated_runtime_clusters(self, deprecated_date_threshold=None):
        """Identify clusters running deprecated or soon-to-be deprecated runtimes."""
        return self.runtime_manager.get_deprecated_runtime_clusters(deprecated_date_threshold)
    
    def get_current_lts_runtimes(self):
        """Identify the current Long Term Support (LTS) runtime versions."""
        return self.runtime_manager.get_current_lts_runtimes()
    
    def recommend_runtime_upgrades(self, clusters):
        """For each cluster with a deprecated runtime, recommend an upgrade target."""
        return self.runtime_manager.recommend_runtime_upgrades(clusters)
    
    def get_installed_libraries(self, cluster_id):
        """Get a list of libraries installed on a given cluster."""
        return self.library_manager.get_installed_libraries(cluster_id)
    
    def check_pypi_package_updates(self, package_name, current_version):
        """Check if a PyPI package has a newer version available."""
        return self.library_manager.check_pypi_package_updates(package_name, current_version)
    
    def check_library_versions(self, cluster_id):
        """Check for outdated or vulnerable libraries on a cluster."""
        return self.library_manager.check_library_versions(cluster_id)
    
    def analyze_cluster_utilization(self, days_back=30):
        """Analyze cluster utilization to identify cost optimization opportunities."""
        # This would be implemented in a separate manager class in a full implementation
        pass
 