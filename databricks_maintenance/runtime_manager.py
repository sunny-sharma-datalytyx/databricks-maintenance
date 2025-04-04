"""
Runtime version management for Databricks clusters.
"""

import re
import datetime
import logging
import requests
from typing import Dict, List, Optional, Any
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup

logger = logging.getLogger("databricks-maintenance.runtime_manager")

class RuntimeManager:
    """Manages Databricks runtime versions, deprecation dates, and upgrade recommendations."""
    
    def __init__(self, api_client, cache_manager):
        """
        Initialize the runtime manager.
        
        Args:
            api_client: Instance of DatabricksApiClient for making API requests
            cache_manager: Instance of CacheManager for caching results
        """
        self.api_client = api_client
        self.cache = cache_manager
    
    def get_available_runtime_versions(self) -> List[Dict]:
        """
        Get a list of all Databricks runtime versions available for cluster creation.
        
        Returns:
            List of runtime version details
        """
        cache_key = "runtime_versions"
        cached_data = self.cache.get(cache_key)
        
        if cached_data:
            return cached_data
            
        response = self.api_client.get_spark_versions()
        
        versions = []
        for version in response.get("versions", []):
            # Extract the actual version number from the name
            match = re.search(r'(\d+\.\d+)(\.x)?', version.get("name", ""))
            if match:
                version_number = match.group(1)
                versions.append({
                    "key": version.get("key"),
                    "name": version.get("name"),
                    "version": version_number,
                    "is_lts": "LTS" in version.get("name", "")
                })
        
        # Sort versions by numeric value
        versions.sort(key=lambda x: [int(part) for part in x["version"].split(".")])
        
        self.cache.set(cache_key, versions)
        return versions
    
    def fetch_deprecation_dates_from_docs(self) -> Dict[str, Dict[str, Any]]:
        """
        Scrape Databricks documentation to extract runtime version deprecation dates.
        
        This function scrapes the Databricks release notes and documentation pages
        to extract information about end-of-life dates for Databricks Runtime versions.
        
        Returns:
            Dictionary mapping runtime versions to their deprecation details
        """
        cache_key = "deprecation_dates"
        cached_data = self.cache.get(cache_key)
        
        if cached_data:
            return cached_data
        
        logger.info("Fetching Databricks runtime deprecation dates from documentation...")
        
        # Hard-coded EOL dates for known runtime versions
        # This ensures we have accurate data even if web scraping fails
        known_eol_dates = {
            "9.1": {
                "version": "9.1",
                "deprecation_date": "2024-12-19",
                "source": "hardcoded",
                "note": "DBR 9.1 LTS end of support date: December 19, 2024"
            },
            "10.4": {
                "version": "10.4",
                "deprecation_date": "2025-06-30",
                "source": "hardcoded",
                "note": "DBR 10.4 LTS end of support date: June 30, 2025"
            },
            "7.3": {
                "version": "7.3",
                "deprecation_date": "2022-12-31",
                "source": "hardcoded",
                "note": "DBR 7.3 LTS end of support date: December 31, 2022"
            },
            "8.4": {
                "version": "8.4",
                "deprecation_date": "2023-09-30",
                "source": "hardcoded",
                "note": "DBR 8.4 LTS end of support date: September 30, 2023"
            },
            "11.3": {
                "version": "11.3",
                "deprecation_date": "2025-12-31", 
                "source": "hardcoded",
                "note": "DBR 11.3 LTS end of support date: December 31, 2025"
            }
        }
        
        # URLs to check for deprecation information
        urls = [
            "https://docs.databricks.com/en/release-notes/runtime/releases.html",
            "https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime",
            "https://docs.databricks.com/gcp/en/release-notes/runtime"
        ]
        
        deprecation_dates = {}
        
        # Start with our known EOL dates
        deprecation_dates.update(known_eol_dates)
        
        for url in urls:
            try:
                response = requests.get(url, timeout=30, verify=False)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for tables that might contain deprecation information
                tables = soup.find_all('table')
                
                for table in tables:
                    headers = [header.text.strip().lower() for header in table.find_all('th')]
                    
                    # Check if this table has version and EOL/deprecation columns
                    version_col = None
                    eol_col = None
                    
                    for i, header in enumerate(headers):
                        if any(term in header for term in ['version', 'runtime', 'dbr']):
                            version_col = i
                        if any(term in header for term in ['eol', 'end of life', 'deprecation', 'support end', 'end of support', 'End-of-support date']):
                            eol_col = i
                    
                    if version_col is not None and eol_col is not None:
                        rows = table.find_all('tr')[1:]  # Skip header row
                        
                        for row in rows:
                            cells = row.find_all(['td', 'th'])
                            if len(cells) > max(version_col, eol_col):
                                version_text = cells[version_col].text.strip()
                                eol_text = cells[eol_col].text.strip()
                                
                                # Extract version number using regex
                                version_match = re.search(r'(\d+\.\d+)', version_text)
                                if version_match:
                                    version = version_match.group(1)
                                    
                                    # Parse date if possible
                                    date_match = re.search(r'(\w+ \d+, \d{4}|\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}|\w+ \d{4})', eol_text)
                                    
                                    if date_match:
                                        date_str = date_match.group(1)
                                        try:
                                            # Try different date formats
                                            if re.match(r'\w+ \d+, \d{4}', date_str):
                                                deprecation_date = datetime.datetime.strptime(date_str, '%B %d, %Y').strftime('%Y-%m-%d')
                                            elif re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                                                deprecation_date = date_str
                                            elif re.match(r'\d{2}/\d{2}/\d{4}', date_str):
                                                deprecation_date = datetime.datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
                                            elif re.match(r'\w+ \d{4}', date_str):
                                                # Handle month and year format (e.g., "December 2024")
                                                deprecation_date = datetime.datetime.strptime(f"{date_str} 1", '%B %Y %d').strftime('%Y-%m-%d')
                                            else:
                                                logger.warning(f"Unrecognized date format: {date_str}")
                                                continue
                                                
                                            if version not in deprecation_dates:
                                                deprecation_dates[version] = {
                                                    "version": version,
                                                    "deprecation_date": deprecation_date,
                                                    "source": url,
                                                    "note": f"Found in table: {version_text} - {eol_text}"
                                                }
                                            
                                        except ValueError as e:
                                            logger.warning(f"Failed to parse date {date_str}: {str(e)}")
                                    elif "deprecated" in eol_text.lower():
                                        # No specific date but marked as deprecated
                                        deprecation_dates[version] = {
                                            "version": version,
                                            "deprecation_date": datetime.datetime.now().strftime('%Y-%m-%d'),
                                            "source": url,
                                            "note": "Marked as deprecated without specific date"
                                        }
                
                # Also look for text mentions of deprecations
                paragraphs = soup.find_all(['p', 'li', 'div', 'span'])
                for p in paragraphs:
                    text = p.text.strip()
                    if any(word in text.lower() for term in ['deprecate', 'eol', 'end of life', 'end of support', 'no longer supported'] for word in term.split()):
                        # Try to extract version numbers and dates
                        version_matches = re.findall(r'(\d+\.\d+)(?:\s+LTS)?', text)
                        date_matches = re.findall(r'(\w+ \d+,? \d{4}|\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}|\w+ \d{4})', text)
                        
                        if version_matches and date_matches:
                            version = version_matches[0]
                            date_str = date_matches[0]
                            
                            try:
                                # Try different date formats
                                if re.match(r'\w+ \d+,? \d{4}', date_str):
                                    date_str = date_str.replace(',', '') # Remove comma if present
                                    deprecation_date = datetime.datetime.strptime(date_str, '%B %d %Y').strftime('%Y-%m-%d')
                                elif re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                                    deprecation_date = date_str
                                elif re.match(r'\d{2}/\d{2}/\d{4}', date_str):
                                    deprecation_date = datetime.datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
                                elif re.match(r'\w+ \d{4}', date_str):
                                    # Handle month and year format (e.g., "December 2024")
                                    deprecation_date = datetime.datetime.strptime(f"{date_str} 1", '%B %Y %d').strftime('%Y-%m-%d')
                                else:
                                    continue
                                    
                                if version not in deprecation_dates:
                                    deprecation_dates[version] = {
                                        "version": version,
                                        "deprecation_date": deprecation_date,
                                        "source": url,
                                        "note": text[:200]  # First 200 chars of the text
                                    }
                                
                            except ValueError:
                                pass
                
            except Exception as e:
                logger.warning(f"Error scraping {url}: {str(e)}")
        
        logger.info(f"Found deprecation dates for {len(deprecation_dates)} runtime versions")
        
        # Add inferenced deprecation dates for older versions
        available_versions = self.get_available_runtime_versions()
        available_version_numbers = set(v["version"] for v in available_versions)
        
        # Apply heuristic: older versions not in available versions are likely deprecated
        for major in range(1, 13):  # Cover reasonable range of major versions
            for minor in range(0, 15):  # Cover reasonable range of minor versions
                version = f"{major}.{minor}"
                if version not in deprecation_dates and version not in available_version_numbers:
                    # If it's an old version and not available, it's probably deprecated
                    if major < 9 or (major == 9 and minor < 1):  # Adjust threshold as needed
                        deprecation_dates[version] = {
                            "version": version,
                            "deprecation_date": datetime.datetime.now().strftime('%Y-%m-%d'),
                            "source": "inference",
                            "note": "Inferred deprecation (version not available for creation)"
                        }
        
        # Cache the results
        self.cache.set(cache_key, deprecation_dates)
        return deprecation_dates
    
    def get_deprecated_runtime_clusters(self, deprecated_date_threshold: Optional[datetime.datetime] = None) -> List[Dict]:
        """
        Identify clusters running deprecated or soon-to-be deprecated runtimes.
        
        Args:
            deprecated_date_threshold: Date threshold for considering a runtime as approaching deprecation
                                      (default: 3 months from now)
        
        Returns:
            List of clusters with deprecated or soon-to-be deprecated runtimes
        """
        if deprecated_date_threshold is None:
            deprecated_date_threshold = datetime.datetime.now() + relativedelta(months=3)
            
        # Fetch actual runtime deprecation information from Databricks docs
        runtime_deprecation_info = self.fetch_deprecation_dates_from_docs()
        
        # Get list of available runtimes (these are not deprecated)
        available_runtimes = self.get_available_runtime_versions()
        available_runtime_versions = set(rt["version"] for rt in available_runtimes)
        
        clusters = self.api_client.get_cluster_list()
        at_risk_clusters = []
        
        for cluster in clusters:
            spark_version = cluster.get("spark_version", "")
            cluster_name = cluster.get("cluster_name", "Unknown")
            cluster_id = cluster.get("cluster_id", "")
            
            # Extract version number from the spark_version string
            version_match = re.search(r'(\d+\.\d+)', spark_version)
            if not version_match:
                continue
                
            version = version_match.group(1)
            
            # Check if this runtime version has deprecation info
            if version in runtime_deprecation_info:
                deprecation_info = runtime_deprecation_info[version]
                deprecation_date_str = deprecation_info.get("deprecation_date")
                
                if deprecation_date_str:
                    try:
                        deprecation_date = datetime.datetime.strptime(deprecation_date_str, "%Y-%m-%d")
                        
                        if deprecation_date <= datetime.datetime.now():
                            status = "DEPRECATED"
                            note = deprecation_info.get("note", "This runtime version is deprecated.")
                        elif deprecation_date <= deprecated_date_threshold:
                            status = "SOON_DEPRECATED"
                            days_until = (deprecation_date - datetime.datetime.now()).days
                            note = f"This runtime will be deprecated in {days_until} days."
                        else:
                            status = "SUPPORTED"
                            note = None
                            
                        if status in ["DEPRECATED", "SOON_DEPRECATED"]:
                            at_risk_clusters.append({
                                "cluster_id": cluster_id,
                                "cluster_name": cluster_name,
                                "current_runtime": spark_version,
                                "status": status,
                                "deprecation_date": deprecation_date_str,
                                "note": note,
                                "source": deprecation_info.get("source", "Unknown")
                            })
                    except ValueError as e:
                        logger.warning(f"Invalid date format in deprecation info: {deprecation_date_str}, {str(e)}")
            
            # If no explicit deprecation info found, check if it's available in runtime list
            elif version not in available_runtime_versions:
                # If not available for new cluster creation, it's likely deprecated
                at_risk_clusters.append({
                    "cluster_id": cluster_id,
                    "cluster_name": cluster_name,
                    "current_runtime": spark_version,
                    "status": "DEPRECATED",
                    "deprecation_date": "Unknown",
                    "note": "This runtime is no longer available for new cluster creation",
                    "source": "availability_check"
                })
        
        return at_risk_clusters
    
    def get_current_lts_runtimes(self) -> List[Dict]:
        """
        Identify the current Long Term Support (LTS) runtime versions.
        
        Returns:
            List of LTS runtime versions
        """
        all_runtimes = self.get_available_runtime_versions()
        lts_runtimes = []
        
        for runtime in all_runtimes:
            if "LTS" in runtime.get("name", ""):
                lts_runtimes.append(runtime)
        
        # Sort by version, highest first
        lts_runtimes.sort(key=lambda x: [int(part) for part in x["version"].split(".")], reverse=True)
        return lts_runtimes
    
    def recommend_runtime_upgrades(self, clusters: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """
        For each cluster with a deprecated runtime, recommend an upgrade target.
        
        Args:
            clusters: List of clusters to evaluate
            
        Returns:
            Dictionary mapping cluster IDs to recommended runtime versions and details
        """
        recommendations = {}
        
        # Get the latest LTS and latest standard runtime versions
        lts_runtimes = self.get_current_lts_runtimes()
        latest_lts = lts_runtimes[0] if lts_runtimes else None
        
        all_runtimes = self.get_available_runtime_versions()
        latest_regular = all_runtimes[-1] if all_runtimes else None
        
        # Get available ML runtimes
        ml_runtimes = [rt for rt in all_runtimes if "ML" in rt.get("name", "")]
        latest_ml = ml_runtimes[-1] if ml_runtimes else None
        ml_lts_runtimes = [rt for rt in ml_runtimes if "LTS" in rt.get("name", "")]
        latest_ml_lts = ml_lts_runtimes[-1] if ml_lts_runtimes else latest_ml
        
        # Get available Genomics runtimes (if any)
        genomics_runtimes = [rt for rt in all_runtimes if "Genomics" in rt.get("name", "")]
        latest_genomics = genomics_runtimes[-1] if genomics_runtimes else None
        
        # Get photon runtimes
        photon_runtimes = [rt for rt in all_runtimes if "Photon" in rt.get("name", "")]
        latest_photon = photon_runtimes[-1] if photon_runtimes else None
        
        for cluster in clusters:
            cluster_id = cluster["cluster_id"]
            current_runtime = cluster["current_runtime"]
            cluster_name = cluster.get("cluster_name", "").lower()
            
            # Determine if this is a specialized runtime
            is_ml = "ml" in current_runtime.lower()
            is_genomics = "genomics" in current_runtime.lower()
            is_photon = "photon" in current_runtime.lower()
            is_lts = "lts" in current_runtime.lower()
            
            # Decide on production vs. development classification
            is_production = any(term in cluster_name for term in ["prod", "production", "prd", "live"])
            
            # Make recommendations based on cluster purpose and current runtime type
            if is_production:
                if is_ml and latest_ml_lts:
                    recommendation = {
                        "runtime_key": latest_ml_lts["key"],
                        "runtime_name": latest_ml_lts["name"],
                        "rationale": "Latest ML LTS runtime recommended for production ML workloads"
                    }
                elif is_genomics and latest_genomics:
                    recommendation = {
                        "runtime_key": latest_genomics["key"],
                        "runtime_name": latest_genomics["name"],
                        "rationale": "Latest Genomics runtime recommended for production genomics workloads"
                    }
                elif is_photon and latest_photon:
                    recommendation = {
                        "runtime_key": latest_photon["key"],
                        "runtime_name": latest_photon["name"],
                        "rationale": "Latest Photon runtime recommended for production SQL workloads"
                    }
                elif latest_lts:
                    recommendation = {
                        "runtime_key": latest_lts["key"],
                        "runtime_name": latest_lts["name"],
                        "rationale": "Latest LTS runtime recommended for stable production workloads"
                    }
                else:
                    recommendation = {
                        "runtime_key": latest_regular["key"],
                        "runtime_name": latest_regular["name"],
                        "rationale": "Latest runtime recommended (no LTS version available)"
                    }
            else:
                # For development clusters, recommend latest of the appropriate type
                if is_ml and latest_ml:
                    recommendation = {
                        "runtime_key": latest_ml["key"],
                        "runtime_name": latest_ml["name"],
                        "rationale": "Latest ML runtime recommended for ML development workloads"
                    }
                elif is_genomics and latest_genomics:
                    recommendation = {
                        "runtime_key": latest_genomics["key"],
                        "runtime_name": latest_genomics["name"],
                        "rationale": "Latest Genomics runtime recommended for genomics development"
                    }
                elif is_photon and latest_photon:
                    recommendation = {
                        "runtime_key": latest_photon["key"],
                        "runtime_name": latest_photon["name"],
                        "rationale": "Latest Photon runtime recommended for SQL development"
                    }
                else:
                    recommendation = {
                        "runtime_key": latest_regular["key"],
                        "runtime_name": latest_regular["name"],
                        "rationale": "Latest runtime recommended for development workloads"
                    }
            
            recommendations[cluster_id] = recommendation
        
        return recommendations
         


         