"""
Library management for Databricks clusters.
"""

import logging
import requests
import concurrent.futures
from typing import Dict, List, Optional, Any

logger = logging.getLogger("databricks-maintenance.library_manager")

class LibraryManager:
    """Manages libraries installed on Databricks clusters, checking for updates and vulnerabilities."""
    
    def __init__(self, api_client, cache_manager):
        """
        Initialize the library manager.
        
        Args:
            api_client: Instance of DatabricksApiClient for making API requests
            cache_manager: Instance of CacheManager for caching results
        """
        self.api_client = api_client
        self.cache = cache_manager
    
    def get_installed_libraries(self, cluster_id: str) -> List[Dict]:
        """Get a list of libraries installed on a given cluster."""
        response = self.api_client.get_libraries_status(cluster_id)
        return response.get("library_statuses", [])
    
    def check_pypi_package_updates(self, package_name: str, current_version: str) -> Optional[Dict]:
        """
        Check if a PyPI package has a newer version available.
        
        Args:
            package_name: Name of the PyPI package
            current_version: Current installed version
            
        Returns:
            Update information if newer version is available, None otherwise
        """
        cache_key = f"pypi_{package_name}"
        cached_data = self.cache.get(cache_key)
        
        if cached_data:
            latest_version = cached_data.get("latest_version")
        else:
            try:
                response = requests.get(f"https://pypi.org/pypi/{package_name}/json", timeout=10)
                if response.status_code == 200:
                    package_data = response.json()
                    latest_version = package_data["info"]["version"]
                    self.cache.set(cache_key, {"latest_version": latest_version})
                else:
                    logger.warning(f"Failed to fetch PyPI info for {package_name}: {response.status_code}")
                    return None
            except Exception as e:
                logger.warning(f"Error checking PyPI for {package_name}: {str(e)}")
                return None
        
        # Compare versions
        try:
            from packaging import version
            if version.parse(latest_version) > version.parse(current_version):
                return {
                    "current_version": current_version,
                    "latest_version": latest_version,
                    "update_available": True
                }
        except Exception as e:
            logger.warning(f"Error comparing versions for {package_name}: {str(e)}")
            
        return None
    
    def check_library_versions(self, cluster_id: str) -> List[Dict]:
        """
        Check for outdated or vulnerable libraries on a cluster.
        
        Args:
            cluster_id: ID of the cluster to check
            
        Returns:
            List of libraries that need updates
        """
        installed_libraries = self.get_installed_libraries(cluster_id)
        outdated_libraries = []
        
        # Libraries to check more carefully (known to have security issues in older versions)
        security_critical_libs = {
            "numpy": "1.22.0",  # Example minimum safe version
            "pandas": "1.3.0",
            "requests": "2.27.0",
            "cryptography": "36.0.0",
            "pillow": "9.0.0",
            "tensorflow": "2.8.0",
            "torch": "1.10.0",
            "sqlalchemy": "1.4.0",
            "urllib3": "1.26.5",
            "pyjwt": "2.0.0"
        }
        
        # Process libraries in parallel for faster checks
        def process_library(lib_status):
            library = lib_status.get("library", {})
            lib_result = None
            
            # Check PyPI libraries
            if "pypi" in library:
                pypi_lib = library["pypi"]
                package_name = pypi_lib.get("package", "")
                
                # Extract version - handle different formats
                if "==" in pypi_lib.get("repo", ""):
                    package_version = pypi_lib.get("repo", "").split("==")[-1]
                else:
                    package_version = lib_status.get("library_details", {}).get("pypi", {}).get("version", "unknown")
                
                if package_version == "unknown":
                    return None
                
                # Check if this is a security-critical library
                if package_name in security_critical_libs:
                    min_safe_version = security_critical_libs[package_name]
                    try:
                        from packaging import version
                        if version.parse(package_version) < version.parse(min_safe_version):
                            return {
                                "library_name": package_name,
                                "type": "pypi",
                                "current_version": package_version,
                                "recommended_version": "latest",
                                "reason": f"Security vulnerabilities in versions before {min_safe_version}",
                                "severity": "high"
                            }
                    except Exception:
                        pass
                
                # Check for updates from PyPI
                update_info = self.check_pypi_package_updates(package_name, package_version)
                if update_info and update_info.get("update_available"):
                    return {
                        "library_name": package_name,
                        "type": "pypi",
                        "current_version": package_version,
                        "recommended_version": update_info.get("latest_version"),
                        "reason": "Newer version available",
                        "severity": "medium" if package_name in security_critical_libs else "low"
                    }
            
            # Similar checks could be implemented for Maven, CRAN, etc.
            return None
        
        # Process libraries in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(process_library, installed_libraries))
            
        # Filter out None results and sort by severity
        outdated_libraries = [lib for lib in results if lib is not None]
        outdated_libraries.sort(key=lambda x: {"high": 0, "medium": 1, "low": 2}.get(x.get("severity")))
        
        return outdated_libraries
 


 