# Databricks Maintenance Toolkit

A comprehensive toolkit to automate maintenance tasks for Databricks environments.

## Features

- **Runtime Version Management**: Identify clusters running deprecated or soon-to-be deprecated runtimes
- **Upgrade Recommendations**: Get intelligent recommendations for runtime upgrades based on cluster purpose
- **Library Maintenance**: Check for outdated or potentially vulnerable libraries on clusters
- **Cluster Utilization Analysis**: Identify optimization opportunities for cost savings  [Future Development!]

## Installation

```bash
pip install databricks-maintenance-toolkit
```

## Quick Start

```python
from databricks_maintenance import DatabricksMaintenanceManager

# Initialize with your workspace URL and token
manager = DatabricksMaintenanceManager(
    workspace_url="https://your-workspace.cloud.databricks.com",
    token="your-personal-access-token"
)

# Find clusters with deprecated runtimes
deprecated_clusters = manager.get_deprecated_runtime_clusters()
print(f"Found {len(deprecated_clusters)} clusters with deprecated runtimes")

# Get upgrade recommendations
if deprecated_clusters:
    recommendations = manager.recommend_runtime_upgrades(deprecated_clusters)
    for cluster_id, rec in recommendations.items():
        print(f"Cluster {cluster_id}: Recommend upgrading to {rec['runtime_name']}")
```

## CLI Usage

The toolkit also comes with a command-line interface:

```bash
# Set up your credentials
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-personal-access-token

# Check for deprecated runtimes
databricks-maintenance check-runtimes

# Generate a maintenance report
databricks-maintenance generate-report --output report.html
```

## Configuration

Create a `.databricks-maintenance.yml` file in your home directory:

```yaml
workspaces:
  dev:
    url: https://dev-workspace.cloud.databricks.com
    token: ${DATABRICKS_DEV_TOKEN}  # Use environment variable
  prod:
    url: https://prod-workspace.cloud.databricks.com
    token: ${DATABRICKS_PROD_TOKEN}

cache:
  ttl: 86400  # Cache TTL in seconds (24 hours)
  directory: ~/.databricks-cache
```

## How to Use This Code

You can:

1. Install the package with ```pip install -e .``` in the project directory
1. Setup your environment variables using ```export DATABRICKS_HOST=https://your-host-name``` and ```export DATABRICKS_TOKEN=your-persona-access-token``` (Windows users use SET instead of EXPORT) **NB: Probably better to use databricks-maintenance.yml file for multiple workspaces such as dev, prod, test, etc and adjust your env vars accordingly.**
1. Configure multiple workspaces in a ```.databricks-maintenance.yml``` file
1. Run commands like ```databricks-maintenance check-runtimes```
1. Generate reports with ```databricks-maintenance generate-report```


## Contributing

Contributions are welcome and needed to make this toolkit more functional! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
