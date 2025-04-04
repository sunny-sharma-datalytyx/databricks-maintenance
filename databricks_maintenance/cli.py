"""
Command-line interface for the Databricks Maintenance Toolkit.
"""

import os
import yaml
import logging
import click
import json
import datetime
import pandas as pd
from tabulate import tabulate
from typing import Dict, List, Optional

from databricks_maintenance import DatabricksMaintenanceManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("databricks_maintenance.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("databricks-maintenance.cli")

def load_config():
    """Load configuration from file or environment variables."""
    config_path = os.path.expanduser("~/.databricks-maintenance.yml")
    config = {}
    
    # Try to load from config file
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.debug(f"Loaded configuration from {config_path}")
        except Exception as e:
            logger.warning(f"Failed to load config from {config_path}: {str(e)}")
    
    # Fall back to environment variables
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
    
    return config

def get_workspace_manager(workspace_name=None):
    """Get a DatabricksMaintenanceManager for the specified workspace."""
    config = load_config()
    
    if not config or 'workspaces' not in config:
        logger.error("No configuration found. Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables or create a config file.")
        return None
    
    workspaces = config.get('workspaces', {})
    
    if not workspace_name:
        # Use the first workspace if none specified
        workspace_name = next(iter(workspaces))
    
    if workspace_name not in workspaces:
        logger.error(f"Workspace '{workspace_name}' not found in configuration")
        return None
    
    workspace_config = workspaces[workspace_name]
    url = workspace_config.get('url')
    token = workspace_config.get('token')
    
    # Handle environment variable references in token
    if token and token.startswith('${') and token.endswith('}'):
        env_var = token[2:-1]
        token = os.environ.get(env_var)
    
    if not url or not token:
        logger.error(f"Missing URL or token for workspace '{workspace_name}'")
        return None
    
    # Get cache settings if available
    cache_ttl = config.get('cache', {}).get('ttl', 60)
    
    return DatabricksMaintenanceManager(url, token, cache_ttl)

@click.group()
def cli():
    """Databricks Maintenance Toolkit - Automate maintenance tasks for Databricks environments."""
    pass

@cli.command('check-runtimes')
@click.option('--workspace', '-w', help='Workspace name from config')
@click.option('--months', '-m', default=3, help='Threshold for soon-to-be deprecated in months')
@click.option('--output', '-o', help='Output file (JSON)')
def check_runtimes(workspace, months, output):
    """Check for clusters running deprecated or soon-to-be deprecated runtimes."""
    manager = get_workspace_manager(workspace)
    if not manager:
        return
    
    # Calculate threshold date
    threshold = datetime.datetime.now() + datetime.timedelta(days=30*months)
    
    # Get clusters with deprecated runtimes
    deprecated_clusters = manager.get_deprecated_runtime_clusters(threshold)
    
    if not deprecated_clusters:
        click.echo("No clusters found with deprecated or soon-to-be deprecated runtimes.")
        return
    
    # Get upgrade recommendations
    recommendations = manager.recommend_runtime_upgrades(deprecated_clusters)
    
    # Prepare output data
    results = []
    for cluster in deprecated_clusters:
        cluster_id = cluster['cluster_id']
        rec = recommendations.get(cluster_id, {})
        
        results.append({
            'cluster_name': cluster['cluster_name'],
            'cluster_id': cluster_id,
            'current_runtime': cluster['current_runtime'],
            'status': cluster['status'],
            'deprecation_date': cluster['deprecation_date'],
            'recommended_runtime': rec.get('runtime_name', 'Unknown'),
            'rationale': rec.get('rationale', '')
        })
    
    # Display as table
    df = pd.DataFrame(results)
    click.echo(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
    
    # Write to file if requested
    if output:
        with open(output, 'w') as f:
            json.dump(results, f, indent=2)
        click.echo(f"Results written to {output}")

@cli.command('check-libraries')
@click.option('--workspace', '-w', help='Workspace name from config')
@click.option('--cluster-id', '-c', required=True, help='Cluster ID to check')
@click.option('--output', '-o', help='Output file (JSON)')
def check_libraries(workspace, cluster_id, output):
    """Check for outdated or vulnerable libraries on a cluster."""
    manager = get_workspace_manager(workspace)
    if not manager:
        return
    
    # Get outdated libraries
    outdated_libraries = manager.check_library_versions(cluster_id)
    
    if not outdated_libraries:
        click.echo("No outdated or vulnerable libraries found.")
        return
    
    # Display as table
    df = pd.DataFrame(outdated_libraries)
    click.echo(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
    
    # Write to file if requested
    if output:
        with open(output, 'w') as f:
            json.dump(outdated_libraries, f, indent=2)
        click.echo(f"Results written to {output}")

@cli.command('generate-report')
@click.option('--workspace', '-w', help='Workspace name from config')
@click.option('--output', '-o', required=True, help='Output file (HTML)')
def generate_report(workspace, output):
    """Generate a comprehensive maintenance report."""
    manager = get_workspace_manager(workspace)
    if not manager:
        return
    
    # Get clusters
    all_clusters = manager.get_cluster_list()
    
    # Get deprecated runtimes
    deprecated_clusters = manager.get_deprecated_runtime_clusters()
    
    # Get recommendations
    recommendations = manager.recommend_runtime_upgrades(deprecated_clusters)
    
    # Check libraries for each cluster (limited to first 5 for performance)
    library_issues = {}
    for cluster in all_clusters[:5]:  # Limit to 5 clusters for demo
        cluster_id = cluster['cluster_id']
        library_issues[cluster_id] = manager.check_library_versions(cluster_id)
    
    # Generate HTML report
    html = f"""
    <html>
    <head>
        <title>Mphasis Datalytyx - Databricks Maintenance Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1, h2, h3 {{ color: #0077b6; }}
            table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .high {{ background-color: #ffcccc; }}
            .medium {{ background-color: #fff2cc; }}
            .low {{ background-color: #e6f3ff; }}
            .DEPRECATED {{ background-color: #ffcccc; }}
            .SOON_DEPRECATED {{ background-color: #fff2cc; }}
        </style>
    </head>
    <body>
        <h1>Mphasis Datalytyx - Databricks Maintenance Report</h1>
        <p>Generated on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <h2>Runtime Version Status</h2>
        <p>Found {len(deprecated_clusters)} clusters with deprecated or soon-to-be deprecated runtimes.</p>
        
        <table>
            <tr>
                <th>Cluster Name</th>
                <th>Current Runtime</th>
                <th>Status</th>
                <th>Deprecation Date</th>
                <th>Recommended Runtime</th>
                <th>Rationale</th>
            </tr>
    """
    
    for cluster in deprecated_clusters:
        cluster_id = cluster['cluster_id']
        rec = recommendations.get(cluster_id, {})
        
        html += f"""
            <tr class="{cluster['status']}">
                <td>{cluster['cluster_name']}</td>
                <td>{cluster['current_runtime']}</td>
                <td>{cluster['status']}</td>
                <td>{cluster['deprecation_date']}</td>
                <td>{rec.get('runtime_name', 'Unknown')}</td>
                <td>{rec.get('rationale', '')}</td>
            </tr>
        """
    
    html += """
        </table>
        
        <h2>Library Status</h2>
    """
    
    for cluster_id, issues in library_issues.items():
        cluster_name = next((c['cluster_name'] for c in all_clusters if c['cluster_id'] == cluster_id), "Unknown")
        
        html += f"""
        <h3>Cluster: {cluster_name}</h3>
        <p>Found {len(issues)} libraries that need attention.</p>
        """
        
        if not issues:
            html += "<p>No issues found with libraries on this cluster.</p>"
            continue
        
        html += """
        <table>
            <tr>
                <th>Library</th>
                <th>Current Version</th>
                <th>Recommended Version</th>
                <th>Reason</th>
                <th>Severity</th>
            </tr>
        """
        
        for lib in issues:
            html += f"""
            <tr class="{lib.get('severity', 'low')}">
                <td>{lib.get('library_name', 'Unknown')}</td>
                <td>{lib.get('current_version', 'Unknown')}</td>
                <td>{lib.get('recommended_version', 'Latest')}</td>
                <td>{lib.get('reason', 'Update recommended')}</td>
                <td>{lib.get('severity', 'low').upper()}</td>
            </tr>
            """
        
        html += "</table>"
    
    html += """
    </body>
    </html>
    """
    
    # Write to file
    with open(output, 'w') as f:
        f.write(html)
    
    click.echo(f"Report generated at {output}")

def main():
    """Entry point for the CLI."""
    cli()

if __name__ == "__main__":
    main()
 

 