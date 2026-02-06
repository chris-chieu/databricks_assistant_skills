# Dashboard Metrics View Skill

Extract metrics from Databricks dashboards and generate Unity Catalog Metric Views.

## Overview

This Agent Skill automatically extracts metric definitions (dimensions and measures) from existing Databricks dashboards and transforms them into **Metric Views**—reusable, governed assets registered in Unity Catalog.

It handles single-table sources, multi-table joins (star and snowflake schemas via native YAML joins), and SQL window functions (mapped to metric view window measures with trailing/cumulative ranges).

For more information on Metric Views, see [Unity Catalog metric views](https://docs.databricks.com/aws/en/metric-views/).

## How It Works

The skill uses a **5-step workflow**:

| Step | Action |
|------|--------|
| 1. Fetch Dashboard | Retrieve dashboard JSON via Databricks SDK |
| 2. Extract Fields | Parse widget queries and custom calculations |
| 3. LLM Analysis | Claude Opus 4.5 classifies dimensions vs. measures, identifies joins and window functions |
| 4. Validate & Fix | Post-processing layer auto-fixes structural issues (see below) |
| 5. Generate SQL | Format YAML and wrap in SQL DDL |

### Validation Layer (Step 4)

After LLM analysis, a post-processing validation layer catches and corrects common issues:

| Fix | Issue | Method |
|-----|-------|--------|
| 1. Join restructuring | Flat joins that should be nested (snowflake schema) | Programmatic |
| 2. Field references | Nested join fields missing full chain dot notation | Programmatic |
| 3. Missing window measures | Orphaned `MEASURE(x)` references without base measure | Targeted LLM call |
| 4. Missing semiadditive | Window measures missing the required `semiadditive` property | Targeted LLM call |

## Files

| File | Description |
|------|-------------|
| `SKILL.md` | Agent Skill configuration for Databricks Assistant |
| `extract_dashboard_metrics_uc_function.sql` | UC function for SQL Warehouse / SQL Editor |
| `scripts/extract_dashboard_metrics.py` | Python script for cluster / notebook compute |

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

The skill generates Metrics View SQL with native joins and window measures:

```yaml
version: 1.1
source: catalog.schema.events
joins:
  - name: campaigns
    source: catalog.schema.campaigns
    'on': source.campaign_id = campaigns.campaign_id
  - name: contacts
    source: catalog.schema.contacts
    'on': source.contact_id = contacts.contact_id
    joins:
      - name: prospects
        source: catalog.schema.prospects
        'on': contacts.prospect_id = prospects.prospect_id
dimensions:
  - name: campaign_name
    expr: campaigns.campaign_name
  - name: prospect_country
    expr: contacts.prospects.country
measures:
  - name: total_events
    expr: COUNT(event_id)
  - name: clicks_t7d
    expr: SUM(unique_clicks)
    window:
      - order: date
        range: trailing 7 day
        semiadditive: last
```

The output JSON also includes a `validation_fixes` array showing what was auto-corrected:

```json
{
  "validation_fixes": [
    {"type": "restructured_nested_join", "join": "prospects", "nested_under": "contacts"},
    {"type": "fixed_nested_ref", "field": "country", "before": "prospects.country", "after": "contacts.prospects.country"},
    {"type": "repaired_missing_measures", "dataset": "metrics_daily", "added": ["clicks_t28d"]},
    {"type": "added_semiadditive", "measure": "clicks_t28d", "value": "last"}
  ]
}
```

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
