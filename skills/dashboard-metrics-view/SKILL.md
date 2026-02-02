---
name: dashboard-metrics-view
description: Extract metrics and dimensions from Databricks dashboard widgets to generate a Databricks Metrics View YAML file. Use this skill when the user asks to extract metrics from a dashboard, create a metrics view from dashboard widgets, convert dashboard calculations to a metrics view, or mentions "dashboard metrics", "widget calculations", or "metrics view YAML".
---

# Dashboard Metrics View Skill

Extract widget configurations from Databricks dashboards and generate Databricks Metrics View YAML files.

## When to Use This Skill

Use this skill when the user:
- Asks to extract metrics from a dashboard
- Wants to create a metrics view from dashboard widgets
- Needs to convert dashboard calculations to a metrics view
- Mentions "dashboard metrics", "widget calculations", or "metrics view YAML"
- Asks about dimensions and measures in a dashboard
- Wants to analyze what calculations exist in a dashboard

## Dashboard Metrics Locations

Dashboard JSON (`.lvdash.json`) contains metrics in two locations:

| Location | JSON Path | Contains |
|----------|-----------|----------|
| Custom Calculations | `datasets[].columns[]` | User-defined calculations with `displayName` and `expression` |
| Widget Query Fields | `pages[].layout[].widget.queries[].query.fields[]` | Field expressions used in visualizations |

## Dimension vs Measure Classification

**Measures** - Fields containing aggregate functions:
- Basic: `SUM`, `COUNT`, `AVG`, `MAX`, `MIN`, `MEAN`, `MEDIAN`
- Statistical: `STD`, `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE`, `CORR`
- Conditional: `COUNT_IF`, `ANY`, `ANY_VALUE`, `BOOL_OR`, `SOME`
- Positional: `FIRST`, `FIRST_VALUE`, `LAST`, `LAST_VALUE`
- Approximate: `APPROX_COUNT_DISTINCT`, `APPROX_PERCENTILE`, `PERCENTILE`, `PERCENTILE_APPROX`
- Window: `DENSE_RANK`, `NTILE`, `PERCENT_RANK`, `RANK`, `ROW_NUMBER`
- Other: `MAX_BY`, `MIN_BY`, `MODE`, `REGR_SLOPE`, `LISTAGG`, `STRING_AGG`, `MEASURE`

**Dimensions** - Non-aggregated column references:
- Raw column names: `` `column_name` ``
- Simple expressions without aggregation

## Implementation Options

### Option 1: Python Script

Execute functions from [scripts/extract_dashboard_metrics.py](scripts/extract_dashboard_metrics.py).

This script uses **Claude Opus 4.5** via the Databricks Foundation Model API to analyze dashboard structure intelligently (same as the UC function).

| Function | Purpose |
|----------|---------|
| `get_dashboard_definition(dashboard_id, pat_token)` | Fetch dashboard JSON via Databricks SDK |
| `extract_widget_fields(dashboard_json)` | Extract fields from widget queries with widget titles |
| `is_filter_dataset(query)` | Check if dataset is a filter/toggle dataset |
| `build_analysis_prompt(dashboard_json, target_catalog_schema)` | Build LLM prompt for measure-first analysis |
| `call_foundation_model(prompt, pat_token)` | Call Claude Opus 4.5 for classification |
| `generate_metrics_view_yaml(dimensions, measures, source, comment)` | Generate YAML content |
| `generate_create_metrics_view_sql(view_name, yaml_content)` | Wrap YAML in SQL DDL |
| `extract_dashboard_metrics(dashboard_id, target_catalog_schema, pat_token)` | Main function: orchestrates the full workflow |

### Option 2: UC Function (SQL Editor) - LLM-Powered

For SQL Editor, use the Unity Catalog function defined in [extract_dashboard_metrics_uc_function.sql](extract_dashboard_metrics_uc_function.sql).

This function uses **Claude Opus 4.5** via the Databricks Foundation Model API to analyze dashboard structure intelligently.

| UC Function | Purpose |
|-------------|---------|
| `<catalog>.<schema>.extract_dashboard_metrics` | Extract metrics using LLM analysis and generate execution steps |

#### UC Function Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `dashboard_id` | STRING | The Databricks dashboard ID |
| `target_catalog_schema` | STRING | Target catalog.schema where views will be created |
| `pat_token` | STRING | Personal Access Token for API calls |

#### Authentication

Use Databricks secrets to securely pass the PAT token:

```
secret('<your-scope>', '<your-token-key>')
```

#### SQL Usage Example

```sql
-- Extract metrics using LLM analysis
SELECT <catalog>.<schema>.extract_dashboard_metrics(
    '<DASHBOARD_ID>',
    'my_catalog.my_schema',
    secret('<your-scope>', '<your-token-key>')
) as result;
```

#### Output Structure

The function returns a JSON with **execution steps** in order:

```json
{
  "dashboard_id": "...",
  "target_catalog_schema": "my_catalog.my_schema",
  "total_steps": 4,
  "source_views_to_create": 1,
  "metrics_views_to_create": 3,
  "execution_steps": [
    {
      "step": 1,
      "type": "create_source_view",
      "description": "Create source view for joined dataset: Customer Orders",
      "dataset_name": "Customer Orders",
      "view_name": "my_catalog.my_schema.customer_orders_source",
      "sql": "CREATE OR REPLACE VIEW ... AS SELECT ... JOIN ..."
    },
    {
      "step": 2,
      "type": "create_metrics_view",
      "description": "Create metrics view for: Customer Orders",
      "view_name": "my_catalog.my_schema.customer_orders_metrics_view",
      "source": "my_catalog.my_schema.customer_orders_source",
      "source_type": "joined",
      "sql": "CREATE OR REPLACE VIEW ... WITH METRICS ..."
    },
    {
      "step": 3,
      "type": "create_metrics_view",
      "description": "Create metrics view for: Sales Data",
      "view_name": "my_catalog.my_schema.sales_data_metrics_view",
      "source": "catalog.schema.sales",
      "source_type": "single_table",
      "sql": "CREATE OR REPLACE VIEW ... WITH METRICS ..."
    }
  ],
  "instructions": "Execute each step in order...",
  "status": "success"
}
```

| Field | Description |
|-------|-------------|
| `execution_steps` | Array of steps to execute in order |
| `step` | Step number (execute in this order) |
| `type` | `"create_source_view"` or `"create_metrics_view"` |
| `sql` | The SQL statement to execute |

## How It Works

1. **Fetches dashboard JSON** via Databricks SDK using PAT token
2. **Sends to Claude Opus 4.5** for intelligent analysis:
   - Identifies single-table vs joined datasets
   - Extracts table references and join conditions
   - Classifies dimensions vs measures
3. **Generates execution steps** in proper order:
   - For joined tables: creates source view first, then metrics view
   - For single tables: creates metrics view directly

## Execution Instructions

**IMPORTANT: Do NOT execute the SQL statements directly.** There is a system mechanism that prevents execution. Instead:

1. Parse the result JSON
2. For each step in `execution_steps`:
   - Extract the SQL from the `sql` field
   - **Present the SQL code to the user** for manual execution
   - Clearly indicate the step order (step 1 must run before step 2, etc.)
3. Explain that `create_source_view` steps MUST be executed before dependent `create_metrics_view` steps

**Output format for the user:**

```
Step 1: Create source view for [dataset_name]
----------------------------------------
[SQL code here]

Step 2: Create metrics view for [dataset_name]
----------------------------------------
[SQL code here]
```

### SQL Formatting Requirements

**CRITICAL: Do NOT format or indent the SQL statements.** Present the SQL exactly as generated, with NO leading spaces on any line.

**WRONG - Do NOT add indentation:**
```sql
CREATE OR REPLACE VIEW my_view
    WITH METRICS        <-- NO! Don't indent
    LANGUAGE YAML       <-- NO! Don't indent
    AS $$
    version: 1.1        <-- NO! Don't indent
```

**CORRECT - No leading spaces:**
```sql
CREATE OR REPLACE VIEW my_view
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: catalog.schema.table
comment: "description"
dimensions:
  - name: field_name
    expr: expression
measures:
  - name: measure_name
    expr: AGG(column)
$$
```

**Rules:**
1. `CREATE OR REPLACE VIEW`, `WITH METRICS`, `LANGUAGE YAML`, `AS $$`, and `$$` must start at column 0
2. YAML root properties (`version`, `source`, `comment`, `dimensions`, `measures`) must start at column 0
3. Only list items inside `dimensions:` and `measures:` should be indented (2 spaces for `-`, 4 spaces for properties)
4. **Do NOT auto-format or prettify the SQL** - present it exactly as returned by the function

## Workflow Instructions

### Step 0: Detect Compute Type

Before executing any code, detect the current compute type:
- If using a **SQL Warehouse**: Use the UC Function (Option 2)
- If using a **Cluster** (all-purpose or job cluster): Use the Python Script (Option 1)

SQL Warehouses cannot execute Python files directly. Always check the compute type first to choose the appropriate implementation.

### Step 1: Get the Dashboard ID

The dashboard ID can be found in:
- The dashboard URL: `https://<workspace>.databricks.com/sql/dashboards/<DASHBOARD_ID>`
- User-provided `.lvdash.json` file

### Step 2: Extract Metrics

```python
from scripts.extract_dashboard_metrics import (
    get_dashboard_definition,
    extract_custom_calculations,
    extract_widget_fields
)

# Fetch dashboard definition
dashboard_json = get_dashboard_definition(dashboard_id="<DASHBOARD_ID>")

# Extract metrics from both locations
custom_calcs = extract_custom_calculations(dashboard_json)
widget_fields = extract_widget_fields(dashboard_json)
```

### Step 3: Classify Fields

```python
from scripts.extract_dashboard_metrics import classify_field

# Classify each field as dimension or measure
for field in all_fields:
    field_type = classify_field(field['expression'])
    # Returns 'measure' if contains aggregate function, else 'dimension'
```

### Step 4: Generate Metrics View YAML

```python
from scripts.extract_dashboard_metrics import generate_metrics_view_yaml

yaml_output = generate_metrics_view_yaml(
    dimensions=dimensions,
    measures=measures,
    source="catalog.schema.table"
)
```

## Output Format

The skill generates a Databricks Metrics View YAML file:

```yaml
version: 1.1
source: catalog.schema.table
comment: "Dashboard-derived metric view"
dimensions:
  - name: order_date
    expr: o_orderdate
    comment: "Optional description"
measures:
  - name: total_revenue
    expr: SUM(o_totalprice)
    comment: "Optional description"
```

## Example Session

```python
from scripts.extract_dashboard_metrics import (
    get_dashboard_definition,
    extract_custom_calculations,
    extract_widget_fields,
    classify_field,
    generate_metrics_view_yaml,
    deduplicate_fields
)

# User: "Extract metrics from dashboard abc123 and create a metrics view"

# Step 1: Fetch dashboard
dashboard_json = get_dashboard_definition(dashboard_id="abc123")

# Step 2: Extract all fields
custom_calcs = extract_custom_calculations(dashboard_json)
widget_fields = extract_widget_fields(dashboard_json)
all_fields = custom_calcs + widget_fields

# Step 3: Deduplicate
all_fields = deduplicate_fields(all_fields)

# Step 4: Classify
dimensions = []
measures = []
for field in all_fields:
    if classify_field(field['expression']) == 'measure':
        measures.append(field)
    else:
        dimensions.append(field)

# Step 5: Generate YAML
yaml_output = generate_metrics_view_yaml(
    dimensions=dimensions,
    measures=measures,
    source="my_catalog.my_schema.my_table"
)

print(yaml_output)
```

## Function Parameters

### get_dashboard_definition
- `dashboard_id` (str): The Databricks dashboard ID

### extract_custom_calculations
- `dashboard_json` (dict): The parsed dashboard JSON

### extract_widget_fields
- `dashboard_json` (dict): The parsed dashboard JSON

### classify_field
- `expression` (str): The field expression to classify
- Returns: `'measure'` or `'dimension'`

### generate_metrics_view_yaml
- `dimensions` (list): List of dimension field dicts with `name` and `expr`
- `measures` (list): List of measure field dicts with `name` and `expr`
- `source` (str): The source table path (catalog.schema.table)
- `comment` (str, optional): Comment for the metrics view

### deduplicate_fields
- `fields` (list): List of field dicts to deduplicate
- Returns: Deduplicated list based on name/expression
