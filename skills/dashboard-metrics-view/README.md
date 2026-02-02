# Dashboard Metrics View Skill

Extract metrics from Databricks dashboards and generate Unity Catalog Metric Views.

## Overview

This Agent Skill automatically extracts metric definitions (dimensions and measures) from existing Databricks dashboards and transforms them into **Metric Views**—reusable, governed assets registered in Unity Catalog.

For more information on Metric Views, see [Unity Catalog metric views](https://docs.databricks.com/aws/en/metric-views/).

## How It Works

The skill uses a **4-step workflow**:

| Step | Action |
|------|--------|
| 1. Fetch Dashboard | Retrieve dashboard JSON via Databricks SDK |
| 2. Extract Fields | Parse widget queries and custom calculations |
| 3. LLM Analysis | Claude Opus 4.5 classifies dimensions vs. measures |
| 4. Generate SQL | Format YAML and wrap in SQL DDL |

## Files

| File | Description |
|------|-------------|
| `SKILL.md` | Agent Skill configuration for Databricks Assistant |
| `extract_dashboard_metrics_uc_function.sql` | UC function for SQL Warehouse/SQL Editor |
| `scripts/extract_dashboard_metrics.py` | Python script for cluster/notebook compute |

## Usage

### Option 1: UC Function (SQL Warehouse / SQL Editor)

```sql
-- Deploy the UC function first (run extract_dashboard_metrics_uc_function.sql)

-- Then call it
SELECT extract_dashboard_metrics(
    '<DASHBOARD_ID>',
    'my_catalog.my_schema',
    secret('scope', 'token')
) as result;
```

### Option 2: Python Script (Cluster / Notebook)

```python
from scripts.extract_dashboard_metrics import extract_dashboard_metrics

result = extract_dashboard_metrics(
    dashboard_id="<DASHBOARD_ID>",
    target_catalog_schema="my_catalog.my_schema",
    pat_token="dapi..."
)

# Result contains execution_steps with SQL to run
for step in result['execution_steps']:
    print(f"Step {step['step']}: {step['description']}")
    print(step['sql'])
```

## Output

The skill returns execution steps in order:

```json
{
  "execution_steps": [
    {
      "step": 1,
      "type": "create_source_view",
      "description": "Create source view for joined dataset: Customer Orders",
      "sql": "CREATE OR REPLACE VIEW ..."
    },
    {
      "step": 2,
      "type": "create_metrics_view",
      "description": "Create metrics view for: Customer Orders",
      "sql": "CREATE OR REPLACE VIEW ... WITH METRICS ..."
    }
  ]
}
```

**Important:** Execute the SQL statements manually in order. Source views must be created before their dependent metric views.

## Requirements

- Databricks workspace with Unity Catalog enabled
- Personal Access Token (PAT) for API authentication
- Access to Claude Opus 4.5 via Databricks Foundation Model API
- `SELECT` privileges on source tables
- `CREATE TABLE` privilege in target schema

## Configuration

Update `DATABRICKS_HOST` in both implementations to match your workspace:

```python
DATABRICKS_HOST = "https://your-workspace.cloud.databricks.com"
```

## License

See repository root for license information.
