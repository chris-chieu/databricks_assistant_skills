-- Unity Catalog Python Function for Dashboard Metrics Extraction
-- Uses LLM (Claude Opus 4.5) to analyze dashboard structure and generate metrics views
-- Handles both single-table and joined-table sources with execution order

CREATE OR REPLACE FUNCTION <catalog>.<schema>.extract_dashboard_metrics(
    dashboard_id STRING,
    target_catalog_schema STRING,
    pat_token STRING
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Extract metrics from a Databricks dashboard using LLM analysis and generate multiple CREATE METRIC VIEW SQL statements with execution order.'
AS $$
import json
import re
import requests
from databricks.sdk import WorkspaceClient

DATABRICKS_HOST = "https://<your-workspace>.cloud.databricks.com"
LLM_MODEL = "databricks-claude-opus-4-5"


def call_foundation_model(prompt, pat_token):
    """Call Databricks Foundation Model API with Claude Opus 4.5."""
    url = f"{DATABRICKS_HOST}/serving-endpoints/{LLM_MODEL}/invocations"
    
    headers = {
        "Authorization": f"Bearer {pat_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 32000
    }
    
    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    
    result = response.json()
    # Extract content from response
    if "choices" in result and len(result["choices"]) > 0:
        return result["choices"][0]["message"]["content"]
    return result.get("content", str(result))


def is_filter_dataset(query):
    """Check if a dataset is a filter/toggle dataset (uses explode(array(...)))."""
    if not query:
        return False
    query_lower = query.lower()
    # Filter datasets typically use explode(array(...)) to create dropdown options
    return 'explode(array(' in query_lower or 'explode (array(' in query_lower


def extract_widget_fields(dashboard_json):
    """Extract fields used in widget visualizations, including widget titles."""
    widget_fields = []
    for page in dashboard_json.get('pages', []):
        for layout_item in page.get('layout', []):
            widget = layout_item.get('widget', {})
            widget_title = widget.get('name', '')  # Widget title for measure naming
            for query in widget.get('queries', []):
                query_obj = query.get('query', {})
                dataset_name = query_obj.get('datasetName', '')
                for field in query_obj.get('fields', []):
                    widget_fields.append({
                        'dataset': dataset_name,
                        'widget_title': widget_title,  # Include widget title
                        'name': field.get('name', ''),
                        'expression': field.get('expression', '')
                    })
    return widget_fields


def build_analysis_prompt(dashboard_json, target_catalog_schema):
    """Build the prompt for LLM to analyze dashboard structure."""
    
    # Extract datasets info for the prompt, filtering out filter/toggle datasets
    datasets_info = []
    skipped_count = 0
    for dataset in dashboard_json.get('datasets', []):
        query = ''.join(dataset.get('queryLines', []))
        
        # Skip filter/toggle datasets that use explode(array(...))
        if is_filter_dataset(query):
            skipped_count += 1
            continue
        
        ds_info = {
            'name': dataset.get('name', ''),
            'displayName': dataset.get('displayName', ''),
            'queryLines': query,
            'columns': dataset.get('columns', [])
        }
        datasets_info.append(ds_info)
    
    # Extract widget fields to identify what's actually being used
    widget_fields = extract_widget_fields(dashboard_json)
    
    prompt = f"""Analyze this Databricks dashboard using a MEASURE-FIRST approach.

## Widget Fields (what's actually visualized)

These are the fields used in dashboard widgets. Each field includes:
- `widget_title`: The title of the widget (use this as the measure name when available)
- `expression`: The field expression (look for aggregate functions to identify measures)

```json
{json.dumps(widget_fields, indent=2)}
```

## Dashboard Datasets

Note: {len(datasets_info)} data datasets found ({skipped_count} filter/toggle datasets were pre-filtered).

```json
{json.dumps(datasets_info, indent=2)}
```

## Task - MEASURE-FIRST Approach

1. **FIRST: Scan dataset queryLines for window functions** - Before anything else, examine each dataset's `queryLines` SQL for `OVER (ORDER BY ... RANGE BETWEEN ...)` clauses. For each one, create a base window measure. This is the HIGHEST PRIORITY step. Do NOT treat window function aliases (e.g., `clicks_t7d`) as pre-existing columns on the source table — they are computed by the query and must be recreated as window measures.
2. **Identify measures from widgets** - Look at widget fields for aggregate functions (SUM, COUNT, AVG, etc.)
3. **Identify measures from custom calculations** - Check datasets[].columns[] for aggregations
4. **Only include datasets that have at least 1 measure** - Skip datasets with only dimensions
5. **Determine source tables** - Single table or joined tables
6. **For simple joins (star/snowflake schema)** - Use native metric view joins syntax instead of source_query
7. **For complex queries (CTEs, subqueries, UNIONs)** - Preserve the full SQL query as source_query

## Consolidation Rules (CRITICAL)

**You MUST consolidate datasets that share the same primary source table into a SINGLE entry.**

- **Same source table**: If multiple datasets query the same primary table (e.g., `catalog.schema.orders`), merge them into ONE entry with all dimensions and measures combined.
- **Overlapping joins**: If dataset A joins `orders` with `customers`, and dataset B joins `orders` with `shipping`, produce ONE entry with `orders` as source and both `customers` and `shipping` in the joins array.
- **Deduplication**: When merging, deduplicate dimensions and measures by their `expr`. If the same expression appears with different names, keep the most descriptive name.
- **Primary source table**: The primary source table is the main fact table (typically the FROM table). Use it as the grouping key.

## Output Format

Return a JSON object with this exact structure:

```json
{{
  "datasets_analysis": [
    {{
      "dataset_name": "display name (use primary table name if consolidated)",
      "source_type": "single_table" or "joined",
      "primary_table": "catalog.schema.fact_table",
      "tables": ["catalog.schema.table1", "catalog.schema.table2"],
      "joins": [
        {{
          "name": "dimension_alias",
          "source": "catalog.schema.dimension_table",
          "on": "source.fk_column = dimension_alias.pk_column"
        }}
      ],
      "source_query": "full SQL query ONLY for complex joins that cannot be expressed as simple joins (CTEs, subqueries, UNIONs, window functions). null otherwise.",
      "dimensions": [
        {{"name": "field_name", "expr": "expression", "description": "optional"}}
      ],
      "measures": [
        {{"name": "field_name", "expr": "SUM(column)", "description": "optional"}},
        {{"name": "window_measure_name", "expr": "SUM(column)", "description": "optional", "window": [{{"order": "date_dimension", "range": "trailing 7 day", "semiadditive": "last"}}]}}
      ]
    }}
  ]
}}
```

### Window Measures (for SQL window functions)

When a dataset query uses SQL window functions like `SUM(...) OVER (ORDER BY date RANGE BETWEEN N PRECEDING AND CURRENT ROW)`, map them to **window measures** instead of ignoring them or requiring pre-computation.

**Mapping SQL window functions to metric view window measures:**

| SQL Pattern | Window Measure `range` |
|-------------|----------------------|
| `RANGE BETWEEN 6 PRECEDING AND CURRENT ROW` (7 days) | `trailing 7 day` |
| `RANGE BETWEEN 27 PRECEDING AND CURRENT ROW` (28 days) | `trailing 28 day` |
| `RANGE BETWEEN 90 PRECEDING AND CURRENT ROW` (91 days) | `trailing 91 day` |
| Running total (cumulative) | `cumulative` |
| Current row only | `current` |

**Window measure format in the measures array:**
```json
{{
  "name": "clicks_t7d",
  "expr": "SUM(unique_clicks)",
  "description": "7-day trailing sum of unique clicks",
  "window": [
    {{
      "order": "date",
      "range": "trailing 7 day",
      "semiadditive": "last"
    }}
  ]
}}
```

**Step-by-step process for window functions:**

1. **For each SQL window function** like `SUM(col) OVER (ORDER BY date RANGE BETWEEN N PRECEDING AND CURRENT ROW) AS alias`:
   - Create a **base window measure** with `"name": "alias"`, `"expr": "SUM(col)"`, and `"window": [...]`
   - The `expr` is JUST the aggregate function (e.g., `SUM(col)`), NOT the full OVER clause
   - The OVER clause is translated into the `window` property

2. **For derived ratios** that reference window measures (e.g., `clicks_t7d / delivered_t7d`):
   - Create a separate measure using `MEASURE()` to reference the base measures
   - Example: `"expr": "MEASURE(clicks_t7d) / NULLIF(MEASURE(delivered_t7d), 0)"`
   - **CRITICAL**: Every `MEASURE(x)` reference MUST have a corresponding base measure named `x` defined in the same measures array. If you reference `MEASURE(clicks_t7d)`, there MUST be a measure with `"name": "clicks_t7d"` defined above it.

3. **Do NOT skip base window measures** - If the SQL has `SUM(unique_clicks) OVER (...) AS clicks_t7d`, you MUST create a measure named `clicks_t7d`. Never create only the derived ratio without the base measures it references.

**Complete example - SQL with window functions:**

SQL input:
```sql
SUM(unique_clicks) OVER (ORDER BY date RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS clicks_t7d,
SUM(total_delivered) OVER (ORDER BY date RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS delivered_t7d,
clicks_t7d / delivered_t7d AS ctr_t7d
```

Required output (ALL THREE measures):
```json
[
  {{
    "name": "clicks_t7d",
    "expr": "SUM(unique_clicks)",
    "description": "7-day trailing sum of unique clicks",
    "window": [{{"order": "date", "range": "trailing 7 day", "semiadditive": "last"}}]
  }},
  {{
    "name": "delivered_t7d",
    "expr": "SUM(total_delivered)",
    "description": "7-day trailing sum of total delivered",
    "window": [{{"order": "date", "range": "trailing 7 day", "semiadditive": "last"}}]
  }},
  {{
    "name": "ctr_t7d",
    "expr": "MEASURE(clicks_t7d) / NULLIF(MEASURE(delivered_t7d), 0)",
    "description": "7-day trailing click-through rate"
  }}
]
```

**Rules for window measures:**
- `order`: The dimension name that orders the window (typically a date/time dimension)
- `range`: One of `trailing N unit`, `leading N unit`, `cumulative`, `current`, or `all`
- `semiadditive`: How to summarize when the order dimension is not in GROUP BY. Use `first` or `last`
- Only add `window` property when the original SQL uses window functions (OVER clause). Regular aggregate measures should NOT have a `window` property
- `MEASURE()` is ONLY used in derived measures that reference other measures, NEVER in base window measures

### When to use `joins` vs `source_query`:
- **Use `joins`** (preferred): For simple star/snowflake schema joins (fact table + dimension tables via foreign keys). Set `source_query` to null.
- **Use `source_query`**: ONLY for complex queries with CTEs, subqueries, UNIONs, or window functions that span joined tables. Set `joins` to null or empty.

### Join format rules:
- `name`: An alias for the joined table (used to reference its columns in dimensions/measures via dot notation, e.g., `customer.c_name`)
- `source`: The fully qualified table name (catalog.schema.table)
- `on`: Join condition as `source.fk = alias.pk` (use `source` to reference the primary table, use the join `name` to reference the joined table)
- For nested joins (snowflake schema), add a `joins` array inside a join entry
- Dimensions referencing joined columns must use dot notation: `join_name.column_name`

## Critical Rules

- **SCAN queryLines FOR WINDOW FUNCTIONS FIRST** - This is the #1 priority rule. For each dataset, scan the `queryLines` SQL for `OVER (ORDER BY ... RANGE BETWEEN ...)` patterns. When found, you MUST create base window measures with the `window` property. Do NOT ignore them. Do NOT treat the computed aliases (like `clicks_t7d`, `delivered_t7d`) as pre-existing columns on the source table. The dataset query defines the computation — extract each `AGG(col) OVER (...) AS alias` into a base window measure with the appropriate `window` property. Then create derived measures for any ratios that reference those aliases using `MEASURE()`.
- **CREATE A WINDOW MEASURE FOR EVERY OVER CLAUSE** - You MUST create a separate base window measure for EVERY `OVER` clause in the SQL. If the dataset query has 6 window functions, you MUST produce 6 base window measures. Do NOT stop after the first one. Scan the entire query exhaustively.
- **ONLY include datasets that have at least 1 measure** - A metrics view without measures is useless
- **CONSOLIDATE datasets sharing the same primary source table** - Never produce two entries with the same primary table
- **Remove SET statements** - Ignore any `SET ansi_mode = true`, `SET timezone = ...`, or similar SET statements from queries. They are session configuration and should NOT be included in source_query
- **USE EXACT COLUMN NAMES ONLY** - For dimensions and measures:
  - For single_table: Use ONLY column names that exist in the source table
  - For joined sources with `joins`: Use dot notation for joined table columns (e.g., `customer.c_name`), plain column names for source table columns
  - For joined sources with `source_query`: Use ONLY column names from the SELECT clause of the source_query
  - Do NOT invent, modify, or append suffixes to column names
  - The `expr` field must reference columns exactly as they appear in the source
  - Example: If the SELECT has `usage_usd`, use `usage_usd` NOT `usage_usd_dynamic`
- **NO BACKTICKS in expr** - Do NOT use backticks (\`) around column names in the expr field. Use plain column names:
  - CORRECT: `"expr": "time_key"` or `"expr": "SUM(usage_usd)"`
  - WRONG: `"expr": "\`time_key\`"` or `"expr": "SUM(\`usage_usd\`)"`
- **MEASURE expr MUST contain an aggregate function (SUM, COUNT, AVG, MAX, MIN, COUNT_IF, etc.)** - Every measure expression MUST have an aggregate function:
  - CORRECT: `"expr": "SUM(in_progress_tickets)"`, `"expr": "COUNT(ticket_id)"`, `"expr": "AVG(revenue)"`
  - WRONG: `"expr": "in_progress_tickets"` (missing aggregate function - will cause GROUP BY error)
  - Even if the column name suggests it's already aggregated (like `count_tickets`), wrap it in SUM(): `"expr": "SUM(count_tickets)"`
- Dimensions are fields WITHOUT aggregate functions (raw columns, date fields, categories)
- Include try_divide() and similar helper functions as measures (they contain aggregates)
- For single_table: source is the table name directly
- For joined with `joins`: source is the primary fact table, dimension tables are in `joins`
- For joined with `source_query`: source will be a view created from source_query
- **WINDOW FUNCTIONS** - When the SQL query contains `OVER (ORDER BY ... RANGE BETWEEN ...)`, convert them to window measures with `window` property. Do NOT ignore window functions or require pre-computation.
- **MEASURE() REFERENCES** - For derived measures that reference other measures (e.g., `clicks_t7d / delivered_t7d`), use `MEASURE()` function: `"expr": "MEASURE(clicks_t7d) / NULLIF(MEASURE(delivered_t7d), 0)"`. **CRITICAL: Every MEASURE(x) reference MUST have a corresponding base measure named x defined in the same measures array. Never create a MEASURE() reference without first defining the base measure it points to.**
- **MEASURE NAMING**: Use the `widget_title` as the measure name when available (normalized to snake_case). If no widget_title exists, fall back to pattern: `aggregation_field` (e.g., `sum_revenue`, `count_orders`, `avg_price`)
- Normalize all names to snake_case (for the `name` field, but keep `expr` as exact column references)

## Target Catalog/Schema

All views will be created in: {target_catalog_schema}

Return ONLY the JSON object, no additional text."""

    return prompt


def _render_joins_yaml(joins, indent=0):
    """Recursively render joins into YAML lines."""
    lines = []
    prefix = ' ' * indent
    for join in joins:
        join_name = join.get('name', '')
        join_source = join.get('source', '')
        join_on = join.get('on', '')
        join_using = join.get('using', [])
        nested_joins = join.get('joins', [])
        
        lines.append(f"{prefix}  - name: {join_name}")
        lines.append(f"{prefix}    source: {join_source}")
        if join_on:
            lines.append(f"{prefix}    'on': {join_on}")
        elif join_using:
            lines.append(f"{prefix}    using:")
            for col in join_using:
                lines.append(f"{prefix}      - {col}")
        
        if nested_joins:
            lines.append(f"{prefix}    joins:")
            lines.extend(_render_joins_yaml(nested_joins, indent + 4))
    
    return lines


def generate_metrics_view_yaml(dimensions, measures, source, comment, joins=None):
    """Generate the YAML content for a Databricks Metrics View."""
    lines = [
        "version: 1.1",
        f"source: {source}",
        f'comment: "{comment}"',
    ]
    if joins:
        lines.append("joins:")
        lines.extend(_render_joins_yaml(joins))
    if dimensions:
        lines.append("dimensions:")
        for dim in dimensions:
            name = dim.get('name', '').lower().replace(' ', '_')
            expr = dim.get('expr', dim.get('expression', ''))
            desc = dim.get('description', '')
            lines.append(f"  - name: {name}")
            lines.append(f"    expr: {expr}")
            if desc:
                lines.append(f'    comment: "{desc}"')
    if measures:
        lines.append("measures:")
        for measure in measures:
            name = measure.get('name', '').lower().replace(' ', '_')
            expr = measure.get('expr', measure.get('expression', ''))
            desc = measure.get('description', '')
            window = measure.get('window')
            lines.append(f"  - name: {name}")
            lines.append(f"    expr: {expr}")
            if desc:
                lines.append(f'    comment: "{desc}"')
            if window:
                lines.append("    window:")
                for w in window:
                    order = w.get('order', '')
                    w_range = w.get('range', '')
                    semiadditive = w.get('semiadditive', '')
                    lines.append(f"      - order: {order}")
                    lines.append(f"        range: {w_range}")
                    if semiadditive:
                        lines.append(f"        semiadditive: {semiadditive}")
    return '\n'.join(lines)


def normalize_yaml_indentation(yaml_content):
    """Normalize YAML indentation to ensure root-level properties start at column 0."""
    lines = yaml_content.split('\n')
    if not lines:
        return yaml_content
    
    # Find minimum indentation of non-empty lines
    min_indent = float('inf')
    for line in lines:
        if line.strip():  # Skip empty lines
            indent = len(line) - len(line.lstrip())
            min_indent = min(min_indent, indent)
    
    if min_indent == float('inf') or min_indent == 0:
        return yaml_content.strip()
    
    # Remove the minimum indentation from all lines
    normalized_lines = []
    for line in lines:
        if line.strip():
            normalized_lines.append(line[min_indent:] if len(line) >= min_indent else line)
        else:
            normalized_lines.append('')
    
    return '\n'.join(normalized_lines).strip()


def generate_create_metrics_view_sql(view_name, yaml_content):
    """Generate the CREATE METRIC VIEW SQL statement."""
    delimiter = "$" + "$"
    # Normalize YAML indentation to ensure consistent formatting
    clean_yaml = normalize_yaml_indentation(yaml_content)
    return f"""CREATE OR REPLACE VIEW {view_name}
WITH METRICS
LANGUAGE YAML
AS {delimiter}
{clean_yaml}
{delimiter}"""


def normalize_name(name):
    """Normalize a name to be SQL-safe."""
    import re
    normalized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    normalized = re.sub(r'_+', '_', normalized)
    normalized = normalized.strip('_').lower()
    return normalized


def consolidate_datasets(datasets_analysis):
    """Consolidate datasets sharing the same primary source table into a single entry."""
    groups = {}
    for ds in datasets_analysis:
        primary = ds.get('primary_table', '')
        if not primary:
            tables = ds.get('tables', [])
            primary = tables[0] if tables else ''
        
        if not primary:
            primary = ds.get('dataset_name', f'unknown_{len(groups)}')
        
        primary_lower = primary.lower()
        if primary_lower not in groups:
            groups[primary_lower] = {
                'dataset_name': ds.get('dataset_name', ''),
                'primary_table': primary,
                'tables': set(),
                'joins': [],
                'source_query': None,
                'dimensions': [],
                'measures': [],
                'source_type': ds.get('source_type', 'single_table'),
                '_seen_dim_exprs': set(),
                '_seen_measure_exprs': set(),
                '_seen_join_sources': set(),
            }
        
        group = groups[primary_lower]
        
        for t in ds.get('tables', []):
            group['tables'].add(t)
        
        if ds.get('source_type') == 'joined':
            group['source_type'] = 'joined'
        
        for join in ds.get('joins', []) or []:
            join_source = join.get('source', '').lower()
            if join_source and join_source not in group['_seen_join_sources']:
                group['_seen_join_sources'].add(join_source)
                group['joins'].append(join)
        
        if ds.get('source_query') and not group['source_query']:
            group['source_query'] = ds['source_query']
        
        for dim in ds.get('dimensions', []):
            expr_key = dim.get('expr', '').lower().strip()
            if expr_key and expr_key not in group['_seen_dim_exprs']:
                group['_seen_dim_exprs'].add(expr_key)
                group['dimensions'].append(dim)
        
        for measure in ds.get('measures', []):
            expr_key = measure.get('expr', '').lower().strip()
            if expr_key and expr_key not in group['_seen_measure_exprs']:
                group['_seen_measure_exprs'].add(expr_key)
                group['measures'].append(measure)
    
    consolidated = []
    for group in groups.values():
        entry = {
            'dataset_name': group['dataset_name'],
            'source_type': group['source_type'],
            'primary_table': group['primary_table'],
            'tables': sorted(group['tables']),
            'joins': group['joins'] if group['joins'] else None,
            'source_query': group['source_query'],
            'dimensions': group['dimensions'],
            'measures': group['measures'],
        }
        consolidated.append(entry)
    
    return consolidated


def build_join_chain_map(joins, parent_chain=""):
    """Walk the joins tree and build a map of join_name -> full_chain_path.
    
    For example, if 'prospects' is nested under 'contacts':
      - 'contacts' -> 'contacts'
      - 'prospects' -> 'contacts.prospects'
    """
    chain_map = {}
    for j in (joins or []):
        name = j.get('name', '')
        chain = f"{parent_chain}.{name}" if parent_chain else name
        chain_map[name] = chain
        nested = j.get('joins', []) or []
        if nested:
            chain_map.update(build_join_chain_map(nested, chain))
    return chain_map


def validate_join_structure(joins):
    """Detect flat joins whose 'on' clause references a sibling join (should be nested).
    
    Auto-restructures by moving the child join under its parent.
    Example: if 'prospects' has on='contacts.prospect_id = prospects.prospect_id',
    it references sibling 'contacts' and should be nested under it.
    """
    if not joins:
        return joins, []
    
    fixes = []
    top_level_names = {j.get('name', '') for j in joins}
    
    # Find joins whose 'on' references a sibling (not 'source')
    to_nest = []
    for i, j in enumerate(joins):
        on_clause = j.get('on', '')
        if not on_clause:
            continue
        for sibling in top_level_names:
            if sibling == j.get('name', ''):
                continue
            # Check if 'on' references this sibling join
            if re.search(rf'(?<!\w){re.escape(sibling)}\.', on_clause):
                to_nest.append((i, sibling))
                fixes.append({
                    'type': 'restructured_nested_join',
                    'join': j.get('name', ''),
                    'nested_under': sibling
                })
                break
    
    if not to_nest:
        return joins, fixes
    
    # Restructure: remove children from top level and nest under parents
    children_indices = {idx for idx, _ in to_nest}
    new_joins = [j for i, j in enumerate(joins) if i not in children_indices]
    
    for child_idx, parent_name in to_nest:
        child_join = joins[child_idx]
        for j in new_joins:
            if j.get('name', '') == parent_name:
                if 'joins' not in j or j['joins'] is None:
                    j['joins'] = []
                j['joins'].append(child_join)
                break
    
    return new_joins, fixes


def fix_nested_join_references(dimensions, measures, chain_map):
    """Fix dimension/measure expr to use full chain notation for nested joins.
    
    For example, if 'prospects' is nested under 'contacts':
      - 'prospects.country' -> 'contacts.prospects.country'
      - 'SUM(prospects.employees)' -> 'SUM(contacts.prospects.employees)'
    """
    nested_joins = {name: chain for name, chain in chain_map.items() if '.' in chain}
    
    if not nested_joins:
        return dimensions, measures, []
    
    fixes = []
    
    for field_list in [dimensions, measures]:
        for field in field_list:
            expr = field.get('expr', '')
            original_expr = expr
            
            for nested_name, full_chain in nested_joins.items():
                # Match 'nested_name.col' but NOT if already preceded by parent chain
                # Negative lookbehind: don't match if preceded by word char or dot
                pattern = rf'(?<![\w.]){re.escape(nested_name)}\.'
                replacement = f'{full_chain}.'
                if re.search(pattern, expr):
                    expr = re.sub(pattern, replacement, expr)
            
            if expr != original_expr:
                fixes.append({
                    'type': 'fixed_nested_ref',
                    'field': field.get('name', ''),
                    'before': original_expr,
                    'after': expr
                })
                field['expr'] = expr
    
    return dimensions, measures, fixes


def validate_measure_references(measures):
    """Find MEASURE(x) references that have no corresponding base measure defined."""
    measure_names = {m['name'] for m in measures}
    missing = {}
    for m in measures:
        for ref in re.findall(r'MEASURE\((\w+)\)', m.get('expr', '')):
            if ref not in measure_names:
                missing[ref] = m['name']
    return missing


def build_repair_prompt(missing_refs, all_sql):
    """Build a focused LLM prompt to generate only the missing base window measures."""
    missing_list = '\n'.join(
        f'- {name} (referenced by derived measure: {derived})'
        for name, derived in missing_refs.items()
    )
    return f"""These base window measures are referenced by MEASURE() but were not defined.
Generate ONLY the missing base window measures.

Missing measures needed:
{missing_list}

Original SQL queries (find the OVER clauses that define these aliases):
```sql
{all_sql}
```

For each missing measure, return:
- "name": the alias name
- "expr": JUST the aggregate function (e.g., "SUM(column)"), NO OVER clause
- "description": brief description
- "window": [{{"order": "date_column", "range": "trailing N day", "semiadditive": "last"}}]

Mapping for RANGE BETWEEN:
- 6 PRECEDING AND CURRENT ROW = trailing 7 day
- 27 PRECEDING AND CURRENT ROW = trailing 28 day
- 90 PRECEDING AND CURRENT ROW = trailing 91 day

Return ONLY a JSON array of the missing measures, nothing else."""


def build_semiadditive_prompt(measures_missing_semiadditive, all_sql):
    """Build a focused prompt to determine semiadditive values for window measures."""
    measures_list = '\n'.join(
        f'- {m["name"]} (expr: {m["expr"]}, range: {m["window"][0].get("range", "")})'
        for m in measures_missing_semiadditive
    )
    return f"""For each window measure below, determine the correct semiadditive value.

semiadditive controls how the measure is summarized when the order dimension
is NOT in the GROUP BY:
- "last": use the last value in the window (typical for running totals, trailing sums, end-of-period snapshots)
- "first": use the first value in the window (typical for opening balances, start-of-period values)

Measures needing semiadditive:
{measures_list}

Original SQL for context:
```sql
{all_sql}
```

Return a JSON object mapping measure name to semiadditive value ("first" or "last").
Example: {{"clicks_t28d": "last", "opening_balance": "first"}}

Return ONLY the JSON object, nothing else."""


def validate_and_fix_analysis(consolidated, dashboard_json, pat_token):
    """Post-processing validation layer. Runs after LLM analysis and consolidation.
    
    Programmatic fixes (instant, free):
      1. Restructure flat joins that should be nested
      2. Fix nested join field references (chained dot notation)
    
    LLM repair calls (only when needed):
      3. Generate missing base window measures for orphaned MEASURE() references
      4. Determine semiadditive values for window measures missing this required property
    """
    # Collect all non-filter dataset SQL queries for potential repair prompts
    all_queries = []
    for dataset in dashboard_json.get('datasets', []):
        query = ''.join(dataset.get('queryLines', []))
        if query and not is_filter_dataset(query):
            all_queries.append(query)
    all_sql = '\n\n'.join(all_queries)
    
    all_fixes = []
    
    for ds in consolidated:
        joins = ds.get('joins', []) or []
        dimensions = ds.get('dimensions', [])
        measures = ds.get('measures', [])
        
        # Fix 1: Restructure flat joins that should be nested
        if joins:
            joins, join_fixes = validate_join_structure(joins)
            if join_fixes:
                ds['joins'] = joins
                all_fixes.extend(join_fixes)
        
        # Fix 2: Fix nested join field references (chained dot notation)
        if joins:
            chain_map = build_join_chain_map(joins)
            dimensions, measures, ref_fixes = fix_nested_join_references(
                dimensions, measures, chain_map
            )
            ds['dimensions'] = dimensions
            ds['measures'] = measures
            if ref_fixes:
                all_fixes.extend(ref_fixes)
        
        # Fix 3: Missing base window measures (LLM repair if needed)
        missing = validate_measure_references(measures)
        if missing and all_sql:
            repair_prompt = build_repair_prompt(missing, all_sql)
            try:
                repair_response = call_foundation_model(repair_prompt, pat_token)
                json_match = re.search(r'\[[\s\S]*\]', repair_response)
                if json_match:
                    repaired = json.loads(json_match.group())
                    # Insert base measures BEFORE existing measures
                    ds['measures'] = repaired + measures
                    all_fixes.append({
                        'type': 'repaired_missing_measures',
                        'dataset': ds.get('dataset_name', ''),
                        'added': [m['name'] for m in repaired]
                    })
            except Exception:
                all_fixes.append({
                    'type': 'repair_failed',
                    'dataset': ds.get('dataset_name', ''),
                    'missing': list(missing.keys())
                })
        
        # Fix 4: Ensure all window measures have required 'semiadditive' property
        # Re-read measures in case Fix 3 modified ds['measures']
        measures = ds.get('measures', [])
        measures_missing_semi = [
            m for m in measures
            if m.get('window') and any('semiadditive' not in w for w in m['window'])
        ]
        if measures_missing_semi:
            if all_sql:
                semi_prompt = build_semiadditive_prompt(measures_missing_semi, all_sql)
                try:
                    semi_response = call_foundation_model(semi_prompt, pat_token)
                    semi_match = re.search(r'\{[\s\S]*\}', semi_response)
                    if semi_match:
                        semi_map = json.loads(semi_match.group())
                        for m in measures_missing_semi:
                            value = semi_map.get(m['name'], 'last')
                            if value not in ('first', 'last'):
                                value = 'last'
                            for w in m.get('window', []):
                                if 'semiadditive' not in w:
                                    w['semiadditive'] = value
                                    all_fixes.append({
                                        'type': 'added_semiadditive',
                                        'measure': m.get('name', ''),
                                        'value': value
                                    })
                except Exception:
                    # Fallback: default to 'last' since semiadditive is required
                    for m in measures_missing_semi:
                        for w in m.get('window', []):
                            if 'semiadditive' not in w:
                                w['semiadditive'] = 'last'
                                all_fixes.append({
                                    'type': 'added_semiadditive_fallback',
                                    'measure': m.get('name', ''),
                                    'value': 'last'
                                })
            else:
                # No SQL context available, default to 'last'
                for m in measures_missing_semi:
                    for w in m.get('window', []):
                        if 'semiadditive' not in w:
                            w['semiadditive'] = 'last'
                            all_fixes.append({
                                'type': 'added_semiadditive_fallback',
                                'measure': m.get('name', ''),
                                'value': 'last'
                            })
    
    return consolidated, all_fixes


# Main execution
try:
    # Initialize WorkspaceClient with PAT token
    w = WorkspaceClient(
        host=DATABRICKS_HOST,
        token=pat_token
    )
    
    # Fetch dashboard definition
    dashboard = w.lakeview.get(dashboard_id=dashboard_id)
    
    if not dashboard.serialized_dashboard:
        return json.dumps({
            "dashboard_id": dashboard_id,
            "error": "Dashboard has no serialized content",
            "status": "error"
        })
    
    dashboard_json = json.loads(dashboard.serialized_dashboard)
    
    # Call LLM to analyze dashboard structure
    analysis_prompt = build_analysis_prompt(dashboard_json, target_catalog_schema)
    llm_response = call_foundation_model(analysis_prompt, pat_token)
    
    # Parse LLM response
    try:
        # Try to extract JSON from response (in case there's extra text)
        json_match = re.search(r'\{[\s\S]*\}', llm_response)
        if json_match:
            analysis_result = json.loads(json_match.group())
        else:
            analysis_result = json.loads(llm_response)
    except json.JSONDecodeError as e:
        return json.dumps({
            "dashboard_id": dashboard_id,
            "error": f"Failed to parse LLM response as JSON: {str(e)}",
            "llm_response": llm_response[:1000],
            "status": "error"
        })
    
    # Consolidate datasets sharing the same primary source table
    datasets_analysis = analysis_result.get('datasets_analysis', [])
    consolidated = consolidate_datasets(datasets_analysis)
    
    # Validation layer: fix nested joins, field references, and missing measures
    consolidated, validation_fixes = validate_and_fix_analysis(consolidated, dashboard_json, pat_token)
    
    # Generate SQL statements with execution order
    execution_steps = []
    step_order = 1
    
    for ds in consolidated:
        dataset_name = ds.get('dataset_name', 'unknown')
        source_type = ds.get('source_type', 'single_table')
        primary_table = ds.get('primary_table', '')
        tables = ds.get('tables', [])
        joins = ds.get('joins')
        source_query = ds.get('source_query')
        dimensions = ds.get('dimensions', [])
        measures = ds.get('measures', [])
        
        safe_name = normalize_name(dataset_name)
        
        if source_type == 'joined' and joins:
            # Use native metric view joins (preferred approach)
            source_table = primary_table or (tables[0] if tables else "unknown_table")
            metrics_view_name = f"{target_catalog_schema}.{safe_name}_metrics_view"
            
            yaml_content = generate_metrics_view_yaml(
                dimensions=dimensions,
                measures=measures,
                source=source_table,
                comment=f"Metrics view for {dataset_name} (with joins)",
                joins=joins
            )
            metrics_sql = generate_create_metrics_view_sql(metrics_view_name, yaml_content)
            
            execution_steps.append({
                "step": step_order,
                "type": "create_metrics_view",
                "description": f"Create metrics view for: {dataset_name} (with native joins)",
                "dataset_name": dataset_name,
                "view_name": metrics_view_name,
                "source": source_table,
                "source_type": "joined_native",
                "tables": tables,
                "dimensions_count": len(dimensions),
                "measures_count": len(measures),
                "sql": metrics_sql
            })
            step_order += 1
            
        elif source_type == 'joined' and source_query:
            # Complex query: create intermediate source view + metrics view
            source_view_name = f"{target_catalog_schema}.{safe_name}_source"
            prerequisite_sql = f"CREATE OR REPLACE VIEW {source_view_name} AS\n{source_query}"
            
            execution_steps.append({
                "step": step_order,
                "type": "create_source_view",
                "description": f"Create source view for joined dataset: {dataset_name}",
                "dataset_name": dataset_name,
                "view_name": source_view_name,
                "sql": prerequisite_sql
            })
            step_order += 1
            
            metrics_view_name = f"{target_catalog_schema}.{safe_name}_metrics_view"
            yaml_content = generate_metrics_view_yaml(
                dimensions=dimensions,
                measures=measures,
                source=source_view_name,
                comment=f"Metrics view for {dataset_name} (joined source)"
            )
            metrics_sql = generate_create_metrics_view_sql(metrics_view_name, yaml_content)
            
            execution_steps.append({
                "step": step_order,
                "type": "create_metrics_view",
                "description": f"Create metrics view for: {dataset_name}",
                "dataset_name": dataset_name,
                "view_name": metrics_view_name,
                "source": source_view_name,
                "source_type": "joined",
                "tables": tables,
                "dimensions_count": len(dimensions),
                "measures_count": len(measures),
                "sql": metrics_sql
            })
            step_order += 1
            
        else:
            # Single table - just create the metrics view
            source_table = primary_table or (tables[0] if tables else "unknown_table")
            metrics_view_name = f"{target_catalog_schema}.{safe_name}_metrics_view"
            
            yaml_content = generate_metrics_view_yaml(
                dimensions=dimensions,
                measures=measures,
                source=source_table,
                comment=f"Metrics view for {dataset_name}"
            )
            metrics_sql = generate_create_metrics_view_sql(metrics_view_name, yaml_content)
            
            execution_steps.append({
                "step": step_order,
                "type": "create_metrics_view",
                "description": f"Create metrics view for: {dataset_name}",
                "dataset_name": dataset_name,
                "view_name": metrics_view_name,
                "source": source_table,
                "source_type": "single_table",
                "tables": tables,
                "dimensions_count": len(dimensions),
                "measures_count": len(measures),
                "sql": metrics_sql
            })
            step_order += 1
    
    # Build summary
    total_steps = len(execution_steps)
    source_view_steps = sum(1 for s in execution_steps if s['type'] == 'create_source_view')
    metrics_view_steps = sum(1 for s in execution_steps if s['type'] == 'create_metrics_view')
    
    return json.dumps({
        "dashboard_id": dashboard_id,
        "target_catalog_schema": target_catalog_schema,
        "total_steps": total_steps,
        "source_views_to_create": source_view_steps,
        "metrics_views_to_create": metrics_view_steps,
        "execution_steps": execution_steps,
        "validation_fixes": validation_fixes,
        "instructions": "Execute each step in order. For joined sources, the source view must be created before the metrics view.",
        "status": "success"
    })

except requests.exceptions.RequestException as e:
    return json.dumps({
        "dashboard_id": dashboard_id,
        "error": f"API request failed: {str(e)}",
        "status": "error"
    })
except Exception as e:
    import traceback
    return json.dumps({
        "dashboard_id": dashboard_id,
        "error": str(e),
        "traceback": traceback.format_exc(),
        "status": "error"
    })
$$;


-- ============================================================================
-- Example Usage
-- ============================================================================

-- Extract metrics from a dashboard using LLM analysis
-- SELECT <catalog>.<schema>.extract_dashboard_metrics(
--     '<DASHBOARD_ID>',
--     'my_catalog.my_schema',
--     secret('<your-scope>', '<your-token-key>')
-- ) as result;

-- Parse the result to see execution steps
-- SELECT 
--     get_json_object(result, '$.total_steps') as total_steps,
--     get_json_object(result, '$.source_views_to_create') as source_views,
--     get_json_object(result, '$.metrics_views_to_create') as metrics_views,
--     get_json_object(result, '$.execution_steps') as steps,
--     get_json_object(result, '$.instructions') as instructions
-- FROM (
--     SELECT <catalog>.<schema>.extract_dashboard_metrics(
--         '<DASHBOARD_ID>',
--         'my_catalog.my_schema',
--         secret('<your-scope>', '<your-token-key>')
--     ) as result
-- );

-- ============================================================================
-- Output Structure
-- ============================================================================
-- {
--   "dashboard_id": "...",
--   "target_catalog_schema": "my_catalog.my_schema",
--   "total_steps": 4,
--   "source_views_to_create": 1,
--   "metrics_views_to_create": 3,
--   "execution_steps": [
--     {
--       "step": 1,
--       "type": "create_source_view",
--       "description": "Create source view for joined dataset: Customer Orders",
--       "dataset_name": "Customer Orders",
--       "view_name": "my_catalog.my_schema.customer_orders_source",
--       "sql": "CREATE OR REPLACE VIEW ... AS SELECT ... JOIN ..."
--     },
--     {
--       "step": 2,
--       "type": "create_metrics_view",
--       "description": "Create metrics view for: Customer Orders",
--       "dataset_name": "Customer Orders",
--       "view_name": "my_catalog.my_schema.customer_orders_metrics_view",
--       "source": "my_catalog.my_schema.customer_orders_source",
--       "source_type": "joined",
--       "sql": "CREATE OR REPLACE VIEW ... WITH METRICS ..."
--     },
--     {
--       "step": 3,
--       "type": "create_metrics_view",
--       "description": "Create metrics view for: Sales Data",
--       "view_name": "my_catalog.my_schema.sales_data_metrics_view",
--       "source": "catalog.schema.sales",
--       "source_type": "single_table",
--       "sql": "CREATE OR REPLACE VIEW ... WITH METRICS ..."
--     }
--   ],
--   "instructions": "Execute each step in order...",
--   "status": "success"
-- }

-- ============================================================================
-- Execution Instructions for Databricks Assistant
-- ============================================================================
-- 1. Call this function to get the execution_steps
-- 2. For each step in order (step 1, 2, 3, ...):
--    a. Execute the SQL in the "sql" field
--    b. Verify success before proceeding to next step
-- 3. For "create_source_view" steps: must complete before dependent "create_metrics_view"
