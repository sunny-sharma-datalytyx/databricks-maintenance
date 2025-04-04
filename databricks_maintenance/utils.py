"""
Utility functions for the Databricks Maintenance Toolkit.
"""

import re
import datetime
from typing import Dict, List, Optional, Any, Tuple

def parse_version(version_string: str) -> Tuple[int, ...]:
    """
    Parse a version string into comparable components.
    
    Args:
        version_string: Version string (e.g., "10.4 LTS" or "9.1.x-scala2.12")
        
    Returns:
        Tuple of integers representing the version components
    """
    # Extract the version number using regex
    match = re.search(r'(\d+)\.(\d+)', version_string)
    
    if not match:
        return (0, 0)
    
    major = int(match.group(1))
    minor = int(match.group(2))
    
    return (major, minor)

def format_size(size_bytes: int) -> str:
    """
    Format a byte size into a human-readable string.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string (e.g., "1.23 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024 or unit == 'TB':
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0

def parse_date(date_str: str) -> Optional[datetime.datetime]:
    """
    Parse a date string in various formats.
    
    Args:
        date_str: Date string
        
    Returns:
        Datetime object or None if parsing fails
    """
    formats = [
        '%Y-%m-%d',
        '%B %d, %Y',
        '%m/%d/%Y',
        '%d/%m/%Y',
        '%Y/%m/%d'
    ]
    
    for fmt in formats:
        try:
            return datetime.datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None

def cluster_type_from_name(cluster_name: str) -> str:
    """
    Infer the cluster type/purpose from its name.
    
    Args:
        cluster_name: Name of the cluster
        
    Returns:
        Inferred type (production, development, test, etc.)
    """
    name_lower = cluster_name.lower()
    
    if any(term in name_lower for term in ['prod', 'production', 'prd']):
        return 'production'
    elif any(term in name_lower for term in ['dev', 'development']):
        return 'development'
    elif any(term in name_lower for term in ['test', 'testing', 'qa']):
        return 'testing'
    elif any(term in name_lower for term in ['stag', 'staging']):
        return 'staging'
    elif 'demo' in name_lower:
        return 'demo'
    else:
        return 'unknown'

def get_severity_color(severity: str) -> str:
    """
    Map severity level to a color for display.
    
    Args:
        severity: Severity level (high, medium, low)
        
    Returns:
        Color code for the severity level
    """
    severity_colors = {
        'high': '#ff6b6b',  # Red
        'medium': '#ffd166',  # Yellow
        'low': '#4dabf5'  # Blue
    }
    
    return severity_colors.get(severity.lower(), '#cccccc')

 

  