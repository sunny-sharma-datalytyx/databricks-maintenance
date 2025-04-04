"""
Cache management for the Databricks Maintenance Toolkit.
"""

import os
import json
import time
import logging
from typing import Optional, Any

logger = logging.getLogger("databricks-maintenance.cache")

class CacheManager:
    """Manages caching of API responses and other data to reduce API calls."""
    
    def __init__(self, cache_ttl: int = 60, cache_dir: Optional[str] = None):
        """
        Initialize the cache manager.
        
        Args:
            cache_ttl: Time to live for cached data in seconds (default: 24 hours)
            cache_dir: Directory to store cache files, defaults to .cache in the current dir
        """
        self.cache_ttl = cache_ttl
        
        if cache_dir is None:
            self.cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".cache")
        else:
            self.cache_dir = cache_dir
            
        # Create cache directory if it doesn't exist
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            
        logger.debug(f"Cache initialized at {self.cache_dir} with TTL of {cache_ttl} seconds")
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get data from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached data or None if expired/not found
        """
        cache_file = os.path.join(self.cache_dir, f"{key}.json")
        
        if not os.path.exists(cache_file):
            return None
            
        file_age = time.time() - os.path.getmtime(cache_file)
        if file_age > self.cache_ttl:
            logger.debug(f"Cache expired for {key}")
            return None
            
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                logger.debug(f"Retrieved {key} from cache")
                return data
        except Exception as e:
            logger.warning(f"Error reading cache file {cache_file}: {str(e)}")
            return None
            
    def set(self, key: str, data: Any) -> None:
        """
        Save data to cache.
        
        Args:
            key: Cache key
            data: Data to cache
        """
        cache_file = os.path.join(self.cache_dir, f"{key}.json")
        
        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f)
                logger.debug(f"Cached {key}")
        except Exception as e:
            logger.warning(f"Error writing to cache file {cache_file}: {str(e)}")
    
    def invalidate(self, key: str) -> bool:
        """
        Invalidate a specific cache entry.
        
        Args:
            key: Cache key to invalidate
            
        Returns:
            True if entry was removed, False otherwise
        """
        cache_file = os.path.join(self.cache_dir, f"{key}.json")
        
        if os.path.exists(cache_file):
            try:
                os.remove(cache_file)
                logger.debug(f"Invalidated cache for {key}")
                return True
            except Exception as e:
                logger.warning(f"Error removing cache file {cache_file}: {str(e)}")
        
        return False
    
    def clear(self) -> None:
        """Clear all cached data."""
        for file_name in os.listdir(self.cache_dir):
            if file_name.endswith(".json"):
                try:
                    os.remove(os.path.join(self.cache_dir, file_name))
                except Exception as e:
                    logger.warning(f"Error removing cache file {file_name}: {str(e)}")
        
        logger.info(f"Cleared all cache entries")
 