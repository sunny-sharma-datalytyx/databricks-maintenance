"""
API client for interacting with the Databricks API.
"""

import requests
import json
import logging
import time
from typing import Dict, List, Optional, Any

logger = logging.getLogger("databricks-maintenance.api_client")

class DatabricksApiClient:
    """Client for making authenticated requests to the Databricks API."""
    
    def __init__(self, workspace_url: str, token: str, cache_manager):
        """
        Initialize the Databricks API client.
        
        Args:
            workspace_url: The URL of your Databricks workspace
            token: Your Databricks personal access token
            cache_manager: Instance of CacheManager for caching API responses
        """
        self.workspace_url = workspace_url.rstrip('/')
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.cache = cache_manager
    
    def make_api_request(self, method: str, endpoint: str, data: Optional[Dict] = None, 
                         error_message: str = "API request failed", 
                         retry_count: int = 3, retry_delay: int = 2) -> Dict:
        """
        Make an API request to Databricks with retry logic.
        
        Args:
            method: HTTP method (get, post, put, delete)
            endpoint: API endpoint
            data: Request payload
            error_message: Custom error message
            retry_count: Number of retries on failure
            retry_delay: Delay between retries in seconds
            
        Returns:
            JSON response
        """
        url = f"{self.workspace_url}/api/{endpoint}"
        
        for attempt in range(retry_count):
            try:
                if method.lower() == "get":
                    response = requests.get(url, headers=self.headers, json=data, timeout=30)
                elif method.lower() == "post":
                    response = requests.post(url, headers=self.headers, json=data, timeout=30)
                elif method.lower() == "put":
                    response = requests.put(url, headers=self.headers, json=data, timeout=30)
                elif method.lower() == "delete":
                    response = requests.delete(url, headers=self.headers, json=data, timeout=30)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                if response.status_code >= 400:
                    logger.warning(f"{error_message}: {response.status_code}, {response.text}")
                    if attempt < retry_count - 1:
                        sleep_time = retry_delay * (2 ** attempt)  # Exponential backoff
                        logger.info(f"Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
                        continue
                    response.raise_for_status()
                    
                return response.json() if response.text else {}
                
            except Exception as e:
                if attempt < retry_count - 1:
                    sleep_time = retry_delay * (2 ** attempt)
                    logger.warning(f"Request failed with {str(e)}. Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"{error_message} after {retry_count} attempts: {str(e)}")
                    raise
        
        return {}  # This should never be reached due to the raise above, but keeping for type safety
    
    def get_cluster_list(self) -> List[Dict]:
        """Get a list of all clusters in the workspace."""
        cache_key = "clusters_list"
        cached_data = self.cache.get(cache_key)
        
        if cached_data:
            return cached_data
            
        response = self.make_api_request("get", "2.0/clusters/list", 
                                        error_message="Failed to retrieve clusters")
        clusters = response.get("clusters", [])
        
        self.cache.set(cache_key, clusters)
        return clusters
    
    def get_libraries_status(self, cluster_id: str) -> Dict:
        """Get the status of libraries installed on a specific cluster."""
        return self.make_api_request("get", f"2.0/libraries/cluster-status?cluster_id={cluster_id}",
                                    error_message=f"Failed to retrieve libraries for cluster {cluster_id}")

    def get_spark_versions(self) -> Dict:
        """Get available Spark versions for cluster creation."""
        cache_key = "spark_versions"
        cached_data = self.cache.get(cache_key)
        
        if cached_data:
            return cached_data
            
        response = self.make_api_request("get", "2.0/clusters/spark-versions", 
                                        error_message="Failed to retrieve runtime versions")
        
        self.cache.set(cache_key, response)
        return response
 