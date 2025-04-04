"""
Configuration handling for Databricks Maintenance Toolkit.
"""

import os
import yaml
import logging
from typing import Dict, Optional, Any

logger = logging.getLogger("databricks-maintenance.config")

class Config:
    """Configuration handler for the toolkit."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize with optional path to configuration file.
        
        Args:
            config_path: Path to configuration file (defaults to ~/.databricks-maintenance.yml)
        """
        if config_path is None:
            self.config_path = os.path.expanduser("~/.databricks-maintenance.yml")
        else:
            self.config_path = config_path
            
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """
        Load configuration from file or environment variables.
        
        Returns:
            Configuration dictionary
        """
        config = {}
        
        # Try to load from config file
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    config = yaml.safe_load(f)
                logger.debug(f"Loaded configuration from {self.config_path}")
            except Exception as e:
                logger.warning(f"Failed to load config from {self.config_path}: {str(e)}")
        
        # Fall back to environment variables if no workspaces defined
        if not config or 'workspaces' not in config:
            workspace_url = os.environ.get('DATABRICKS_HOST')
            token = os.environ.get('DATABRICKS_TOKEN')
            
            if workspace_url and token:
                config = {
                    'workspaces': {
                        'default': {
                            'url': workspace_url,
                            'token': token
                        }
                    }
                }
                logger.debug("Loaded configuration from environment variables")
        
        # Set default cache settings if not present
        if 'cache' not in config:
            config['cache'] = {
                'ttl': 60,  # 24 hours - 86400
                'directory': os.path.expanduser("~/.databricks-cache")
            }
        
        return config
    
    def get_workspace_config(self, workspace_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get configuration for a specific workspace.
        
        Args:
            workspace_name: Name of the workspace, uses first available if None
            
        Returns:
            Dictionary with workspace configuration
        """
        workspaces = self.config.get('workspaces', {})
        
        if not workspaces:
            logger.error("No workspaces found in configuration")
            return {}
        
        # Use first workspace if none specified
        if workspace_name is None:
            workspace_name = next(iter(workspaces))
        
        if workspace_name not in workspaces:
            logger.error(f"Workspace '{workspace_name}' not found in configuration")
            return {}
        
        workspace_config = workspaces[workspace_name]
        
        # Resolve environment variables in token
        token = workspace_config.get('token', '')
        if token and isinstance(token, str) and token.startswith('${') and token.endswith('}'):
            env_var = token[2:-1]
            workspace_config['token'] = os.environ.get(env_var, '')
        
        return workspace_config
    
    def get_cache_config(self) -> Dict[str, Any]:
        """
        Get cache configuration.
        
        Returns:
            Dictionary with cache configuration
        """
        return self.config.get('cache', {
            'ttl': 60,  # 24 hours - 86400
            'directory': os.path.expanduser("~/.databricks-cache")
        })
    
    def get_workspaces(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all configured workspaces.
        
        Returns:
            Dictionary of workspace configurations
        """
        return self.config.get('workspaces', {})
 

 