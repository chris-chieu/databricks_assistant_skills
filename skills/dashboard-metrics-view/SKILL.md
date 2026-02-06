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
| `build_analysis_prompt(dashboard_json, target_catalog_schema)` | Build LLM prompt for measure-first analysis with consolidation |
| `call_foundation_model(prompt, pat_token)` | Call Claude Opus 4.5 for classification |
| `consolidate_datasets(datasets_analysis)` | Merge datasets sharing the same primary source table |
| `build_join_chain_map(joins, parent_chain)` | Walk joins tree to build join_name -> full_chain_path map |
| `validate_join_structure(joins)` | Detect and restructure flat joins that should be nested |
| `fix_nested_join_references(dimensions, measures, chain_map)` | Fix field expr to use full chain dot notation for nested joins |
| `validate_measure_references(measures)` | Find orphaned MEASURE(x) references with no base measure |
| `build_repair_prompt(missing_refs, all_sql)` | Build focused LLM prompt to generate missing base window measures |
| `build_semiadditive_prompt(measures_missing_semiadditive, all_sql)` | Build focused LLM prompt to determine semiadditive values for window measures |
| `validate_and_fix_analysis(consolidated, dashboard_json, pat_token)` | Post-processing validation layer: fixes joins, references, missing measures, and semiadditive |
| `generate_metrics_view_yaml(dimensions, measures, source, comment, joins)` | Generate YAML content (with optional joins and window measures) |
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

The function returns a JSON with **execution steps** in order. Datasets sharing the same primary source table are **consolidated** into a single metrics view with all dimensions and measures merged.

```json
{
  "dashboard_id": "...",
  "target_catalog_schema": "my_catalog.my_schema",
  "total_steps": 3,
  "source_views_to_create": 1,
  "metrics_views_to_create": 2,
  "execution_steps": [
    {
      "step": 1,
      "type": "create_metrics_view",
      "description": "Create metrics view for: Orders (with native joins)",
      "dataset_name": "Orders",
      "view_name": "my_catalog.my_schema.orders_metrics_view",
      "source": "catalog.schema.orders",
      "source_type": "joined_native",
      "sql": "CREATE OR REPLACE VIEW ... WITH METRICS ... (includes joins in YAML)"
    },
    {
      "step": 2,
      "type": "create_source_view",
      "description": "Create source view for joined dataset: Complex Report",
      "dataset_name": "Complex Report",
      "view_name": "my_catalog.my_schema.complex_report_source",
      "sql": "CREATE OR REPLACE VIEW ... AS SELECT ... (CTE/complex query)"
    },
    {
      "step": 3,
      "type": "create_metrics_view",
      "description": "Create metrics view for: Complex Report",
      "view_name": "my_catalog.my_schema.complex_report_metrics_view",
      "source": "my_catalog.my_schema.complex_report_source",
      "source_type": "joined",
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
| `source_type` | `"single_table"`, `"joined_native"` (uses YAML joins), or `"joined"` (uses source view) |
| `sql` | The SQL statement to execute |

## How It Works

1. **Fetches dashboard JSON** via Databricks SDK using PAT token
2. **Sends to Claude Opus 4.5** for intelligent analysis:
   - Identifies single-table vs joined datasets
   - Extracts table references and join conditions
   - Classifies dimensions vs measures
3. **Consolidates datasets** sharing the same primary source table:
   - Merges dimensions and measures (deduplicated by expression)
   - Combines joins from different datasets into a single joins list
   - Ensures one metrics view per source table (no duplicates)
4. **Validates and fixes** the LLM output (post-processing validation layer):
   - Restructures flat joins that should be nested (snowflake schema)
   - Fixes field references for nested joins to use full chain dot notation
   - Detects orphaned `MEASURE(x)` references and generates missing base window measures via a targeted LLM repair call
5. **Generates execution steps** in proper order:
   - For simple joins (star/snowflake schema): creates metrics view with native YAML joins (no intermediate source view needed)
   - For complex queries (CTEs, subqueries, UNIONs): creates source view first, then metrics view
   - For single tables: creates metrics view directly

## Validation Layer

After LLM analysis and dataset consolidation, a post-processing validation layer automatically detects and fixes common issues. This runs between consolidation and SQL generation.

### Fix 1: Nested Join Restructuring (Programmatic)

When the LLM produces flat joins where a join's `on` clause references a sibling join (instead of `source`), the validator auto-restructures by nesting the child join under its parent.

**Before (flat, invalid):**
```yaml
joins:
  - name: contacts
    source: catalog.schema.contacts
    'on': source.contact_id = contacts.contact_id
  - name: prospects
    source: catalog.schema.prospects
    'on': contacts.prospect_id = prospects.prospect_id
```

**After (nested, valid):**
```yaml
joins:
  - name: contacts
    source: catalog.schema.contacts
    'on': source.contact_id = contacts.contact_id
    joins:
      - name: prospects
        source: catalog.schema.prospects
        'on': contacts.prospect_id = prospects.prospect_id
```

### Fix 2: Chained Dot Notation for Nested Joins (Programmatic)

When joins are nested, field references must use the full chain path. The validator auto-rewrites expressions.

| Before (invalid) | After (valid) |
|---|---|
| `prospects.country` | `contacts.prospects.country` |
| `SUM(prospects.employees)` | `SUM(contacts.prospects.employees)` |

### Fix 3: Missing Base Window Measures (LLM Repair Call)

When derived measures reference `MEASURE(x)` but the base measure `x` is not defined, the validator makes a targeted LLM repair call to generate only the missing base measures from the original SQL.

**Example:** If `ctr_t28d` references `MEASURE(clicks_t28d)` and `MEASURE(delivered_t28d)`, but neither base measure exists, the repair call generates them from the original SQL's `OVER` clauses.

### Fix 4: Missing `semiadditive` on Window Measures (LLM Call)

Databricks requires the `semiadditive` property on every window measure (`"first"` or `"last"`). When the LLM omits it, the validator makes a targeted LLM call to determine the correct value based on the measure semantics and the original SQL.

- `"last"` — use the last value in the window (typical for trailing sums, running totals, end-of-period snapshots)
- `"first"` — use the first value in the window (typical for opening balances, start-of-period values)

If the LLM call fails, defaults to `"last"` as a safe fallback since it's the most common value.

### Validation Output

The output JSON includes a `validation_fixes` array showing what was auto-fixed:

```json
{
  "validation_fixes": [
    {"type": "restructured_nested_join", "join": "prospects", "nested_under": "contacts"},
    {"type": "fixed_nested_ref", "field": "country", "before": "prospects.country", "after": "contacts.prospects.country"},
    {"type": "repaired_missing_measures", "dataset": "metrics_daily", "added": ["clicks_t28d", "delivered_t28d"]},
    {"type": "added_semiadditive", "measure": "clicks_t28d", "value": "last"}
  ]
}
```

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
2. YAML root properties (`version`, `source`, `comment`, `joins`, `dimensions`, `measures`) must start at column 0
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

## Dataset Consolidation

When multiple dashboard datasets reference the same primary source table, they are automatically **consolidated** into a single metrics view:

| Scenario | Before (without consolidation) | After (with consolidation) |
|----------|-------------------------------|---------------------------|
| Dataset A uses `orders` with `SUM(revenue)`, Dataset B uses `orders` with `COUNT(order_id)` | 2 separate metrics views for `orders` | 1 metrics view with both measures |
| Dataset A joins `orders` + `customers`, Dataset B joins `orders` + `shipping` | 2 metrics views with 2 source views | 1 metrics view with `orders` joining both `customers` and `shipping` |

This consolidation happens in two places:
1. **LLM prompt** — instructs the LLM to group by primary source table
2. **`consolidate_datasets()` function** — deterministic post-processing that merges any remaining duplicates

## Output Format

The skill generates a Databricks Metrics View YAML file.

**Single table (no joins):**

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

**With native joins (consolidated from multiple datasets):**

```yaml
version: 1.1
source: catalog.schema.orders
comment: "Metrics view for orders (with joins)"
joins:
  - name: customers
    source: catalog.schema.customers
    'on': source.customer_id = customers.id
  - name: shipping
    source: catalog.schema.shipping
    'on': source.order_id = shipping.order_id
dimensions:
  - name: customer_name
    expr: customers.name
  - name: shipping_status
    expr: shipping.status
measures:
  - name: total_revenue
    expr: SUM(o_totalprice)
  - name: order_count
    expr: COUNT(o_orderkey)
```

**With window measures (rolling/trailing calculations):**

```yaml
version: 1.1
source: catalog.schema.metrics_daily
comment: "Metrics view with rolling windows"
dimensions:
  - name: date
    expr: date
measures:
  - name: total_clicks
    expr: SUM(unique_clicks)
  - name: clicks_t7d
    expr: SUM(unique_clicks)
    window:
      - order: date
        range: trailing 7 day
        semiadditive: last
  - name: delivered_t7d
    expr: SUM(total_delivered)
    window:
      - order: date
        range: trailing 7 day
        semiadditive: last
  - name: ctr_t7d
    expr: MEASURE(clicks_t7d) / MEASURE(delivered_t7d)
```

## Joins in Metric Views

Metric views support native joins in the YAML definition, allowing you to join a fact table (`source`) with dimension tables directly — without needing to create an intermediate source view. This follows star schema and snowflake schema patterns.

Reference: https://docs.databricks.com/aws/en/metric-views/data-modeling/joins

### Star Schema Joins

Join the source (fact table) to one or more dimension tables using `LEFT OUTER JOIN`. Define the join condition with either an `on` clause or a `using` clause:

- **`on` clause**: A boolean expression defining the join condition.
- **`using` clause**: A list of columns with the same name in both tables.

```yaml
version: 1.1
source: catalog.schema.fact_table

joins:
  # Using an on clause (boolean expression)
  - name: dimension_table_1
    source: catalog.schema.dimension_table_1
    'on': source.dimension_table_1_fk = dimension_table_1.pk

  # Using a using clause (shared column names)
  - name: dimension_table_2
    source: catalog.schema.dimension_table_2
    using:
      - dimension_table_2_key_a
      - dimension_table_2_key_b

dimensions:
  # Reference joined columns with dot notation
  - name: dim1_key
    expr: dimension_table_1.pk

measures:
  - name: count_dim1_keys
    expr: COUNT(dimension_table_1.pk)
```

The `source` namespace references columns from the metric view's source (fact table), while the join `name` references columns from the joined table. If no prefix is provided in an `on` clause, the reference defaults to the join table.

### Snowflake Schema Joins (Nested / Multi-Hop)

Snowflake schemas extend star schemas by normalizing dimension tables into subdimensions. Nested joins are defined by adding a `joins` block inside a join definition.

**Note:** Snowflake joins require Databricks Runtime 17.1 and above.

```yaml
version: 1.1
source: samples.tpch.orders

joins:
  - name: customer
    source: samples.tpch.customer
    'on': source.o_custkey = customer.c_custkey
    joins:
      - name: nation
        source: samples.tpch.nation
        'on': customer.c_nationkey = nation.n_nationkey
        joins:
          - name: region
            source: samples.tpch.region
            'on': nation.n_regionkey = region.r_regionkey

dimensions:
  - name: clerk
    expr: o_clerk
  - name: customer_name
    expr: customer.c_name
  - name: nation_name
    expr: customer.nation.n_name

measures:
  - name: total_orders
    expr: COUNT(o_orderkey)
```

### Joins — Important Caveats

| Caveat | Details |
|--------|---------|
| **YAML key quoting** | YAML 1.1 parsers interpret unquoted `on`, `off`, `yes`, `no` as booleans. Always wrap the `on` key in quotes: `'on': source.fk = dim.pk` |
| **MAP columns** | Joined tables cannot include `MAP` type columns. Unpack them first using `EXPLODE` |
| **Join cardinality** | Joins follow a many-to-one relationship. In many-to-many cases, the first matching row is selected |
| **Dot notation** | Reference joined columns using `join_name.column` (e.g., `customer.c_name`). For nested joins, chain the names (e.g., `customer.nation.n_name`) |

### When to Use Joins vs. Source Views

| Approach | When to Use |
|----------|-------------|
| **Native joins in YAML** | When the dashboard joins a fact table to dimension tables in a star or snowflake schema pattern. This is the preferred approach as it keeps everything in one metric view. |
| **Intermediate source view** | When the dashboard query has complex logic (CTEs, subqueries, UNIONs) that cannot be expressed as simple joins in the YAML definition. |

## Window Measures in Metric Views

Window measures enable defining measures with windowed, cumulative, or semiadditive aggregations directly in the YAML definition. This means SQL window functions (`OVER`, `RANGE BETWEEN`, etc.) can be mapped to native metric view window measures instead of requiring pre-computation.

Reference: https://docs.databricks.com/aws/en/metric-views/data-modeling/window-measures

**Note:** This feature is Experimental.

### Window Measure Properties

Each window measure includes a `window` block with the following properties:

| Property | Description | Values |
|----------|-------------|--------|
| `order` | The dimension that orders the window | Dimension name (e.g., `date`) |
| `range` | Defines the extent of the window | `current`, `cumulative`, `trailing N unit`, `leading N unit`, `all` |
| `semiadditive` | How to summarize when order dimension is not in GROUP BY | `first` or `last` |

### Mapping SQL Window Functions to Window Measures

| SQL Pattern | Window Measure `range` |
|-------------|----------------------|
| `RANGE BETWEEN 6 PRECEDING AND CURRENT ROW` (7 days) | `trailing 7 day` |
| `RANGE BETWEEN 27 PRECEDING AND CURRENT ROW` (28 days) | `trailing 28 day` |
| `RANGE BETWEEN 90 PRECEDING AND CURRENT ROW` (91 days) | `trailing 91 day` |
| Running total (no upper bound) | `cumulative` |
| Current row only | `current` |

### Trailing / Rolling Window Example

Calculate a 7-day rolling sum:

```yaml
measures:
  - name: clicks_t7d
    expr: SUM(unique_clicks)
    window:
      - order: date
        range: trailing 7 day
        semiadditive: last
```

### Period-Over-Period Example

Calculate day-over-day growth using `MEASURE()` references:

```yaml
measures:
  - name: previous_day_sales
    expr: SUM(total_sales)
    window:
      - order: date
        range: trailing 1 day
        semiadditive: last
  - name: current_day_sales
    expr: SUM(total_sales)
    window:
      - order: date
        range: current
        semiadditive: last
  - name: day_over_day_growth
    expr: (MEASURE(current_day_sales) - MEASURE(previous_day_sales)) / MEASURE(previous_day_sales) * 100
```

### Cumulative (Running Total) Example

```yaml
measures:
  - name: running_total_sales
    expr: SUM(total_sales)
    window:
      - order: date
        range: cumulative
        semiadditive: last
```

### Full Example: Marketing Metrics with Rolling Windows

This example shows how a dashboard query with window functions maps to a complete metrics view:

**Original SQL query:**
```sql
SELECT date, unique_clicks, total_delivered,
  SUM(unique_clicks) OVER (ORDER BY date RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS clicks_t7d,
  SUM(total_delivered) OVER (ORDER BY date RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS delivered_t7d,
  unique_clicks / total_delivered AS ctr
FROM catalog.schema.metrics_daily
```

**Equivalent metrics view YAML:**
```yaml
version: 1.1
source: catalog.schema.metrics_daily
comment: "Marketing metrics with rolling windows"
dimensions:
  - name: date
    expr: date
measures:
  - name: total_unique_clicks
    expr: SUM(unique_clicks)
  - name: total_delivered
    expr: SUM(total_delivered)
  - name: clicks_t7d
    expr: SUM(unique_clicks)
    window:
      - order: date
        range: trailing 7 day
        semiadditive: last
  - name: delivered_t7d
    expr: SUM(total_delivered)
    window:
      - order: date
        range: trailing 7 day
        semiadditive: last
  - name: ctr_t7d
    expr: MEASURE(clicks_t7d) / MEASURE(delivered_t7d)
```

### Using `MEASURE()` for Derived Measures

The `MEASURE()` function references other measures defined in the same metric view. It is used to create **derived measures** — measures whose expressions depend on other measures (e.g., ratios between two window measures).

**Rules:**
- `MEASURE()` is ONLY used in derived measures, NEVER in base window measures
- Every `MEASURE(x)` reference MUST have a corresponding base measure named `x` defined in the same metrics view
- Never create a `MEASURE()` reference without first defining the base measure it points to

**Correct pattern:**

```yaml
measures:
  # Step 1: Define base window measures (no MEASURE() here)
  - name: clicks_t7d
    expr: SUM(unique_clicks)
    window:
      - order: date
        range: trailing 7 day
        semiadditive: last
  - name: delivered_t7d
    expr: SUM(total_delivered)
    window:
      - order: date
        range: trailing 7 day
        semiadditive: last

  # Step 2: Define derived measure referencing the base measures
  - name: ctr_t7d
    expr: MEASURE(clicks_t7d) / NULLIF(MEASURE(delivered_t7d), 0)
```

**Wrong pattern (missing base measures):**

```yaml
measures:
  # ERROR: clicks_t7d and delivered_t7d are referenced but never defined!
  - name: ctr_t7d
    expr: MEASURE(clicks_t7d) / NULLIF(MEASURE(delivered_t7d), 0)
```

### Window Measures — Important Notes

- Window measures are an **Experimental** feature in Databricks
- The `MEASURE()` function is used to reference other measures in derived calculations — both inside the YAML and when querying a metric view in SQL
- Multiple `window` entries can be combined on a single measure (e.g., cumulative within a year — see Period to Date in the docs)
- `semiadditive: last` is the most common choice — it uses the last value in the window when the order dimension is not in the query's GROUP BY

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

### consolidate_datasets
- `datasets_analysis` (list): List of dataset analysis dicts from LLM response
- Returns: Consolidated list where each primary source table appears only once
- Merges dimensions, measures, and joins; deduplicates by expression

### build_join_chain_map
- `joins` (list): List of join dicts (may contain nested `joins`)
- `parent_chain` (str, optional): Chain path of the parent join (used internally for recursion)
- Returns: Dict mapping join name to full chain path (e.g., `{'contacts': 'contacts', 'prospects': 'contacts.prospects'}`)

### validate_join_structure
- `joins` (list): List of top-level join dicts
- Returns: Tuple of (restructured joins, list of fixes applied)
- Auto-nests joins whose `on` clause references a sibling join

### fix_nested_join_references
- `dimensions` (list): List of dimension dicts
- `measures` (list): List of measure dicts
- `chain_map` (dict): Map of join name to full chain path (from `build_join_chain_map`)
- Returns: Tuple of (fixed dimensions, fixed measures, list of fixes applied)

### validate_measure_references
- `measures` (list): List of measure dicts
- Returns: Dict mapping missing base measure name to the derived measure that references it

### build_repair_prompt
- `missing_refs` (dict): Dict of missing measure name to its referencing derived measure
- `all_sql` (str): All original SQL queries concatenated
- Returns: Focused LLM prompt string

### build_semiadditive_prompt
- `measures_missing_semiadditive` (list): List of measure dicts with window but no semiadditive
- `all_sql` (str): All original SQL queries concatenated
- Returns: Focused LLM prompt string for determining semiadditive values

### validate_and_fix_analysis
- `consolidated` (list): List of consolidated dataset analysis dicts
- `dashboard_json` (dict): The original dashboard JSON definition
- `pat_token` (str): Personal Access Token for LLM API calls
- Returns: Tuple of (fixed consolidated datasets, list of all fixes applied)

### generate_metrics_view_yaml
- `dimensions` (list): List of dimension field dicts with `name` and `expr`
- `measures` (list): List of measure field dicts with `name`, `expr`, and optional `window` (list of dicts with `order`, `range`, `semiadditive`)
- `source` (str): The source table path (catalog.schema.table)
- `comment` (str, optional): Comment for the metrics view
- `joins` (list, optional): List of join dicts with `name`, `source`, `on`/`using`, and optional nested `joins`

### deduplicate_fields
- `fields` (list): List of field dicts to deduplicate
- Returns: Deduplicated list based on name/expression
