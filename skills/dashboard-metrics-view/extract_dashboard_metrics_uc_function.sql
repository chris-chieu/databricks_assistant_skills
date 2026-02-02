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

1. **Identify measures from widgets** - Look at widget fields for aggregate functions (SUM, COUNT, AVG, etc.)
2. **Identify measures from custom calculations** - Check datasets[].columns[] for aggregations
3. **Only include datasets that have at least 1 measure** - Skip datasets with only dimensions
4. **Determine source tables** - Single table or joined tables
5. **For joined tables** - Preserve the full SQL query including CTEs and JOIN conditions

## Output Format

Return a JSON object with this exact structure:

```json
{{
  "datasets_analysis": [
    {{
      "dataset_name": "display name",
      "source_type": "single_table" or "joined",
      "tables": ["catalog.schema.table1", "catalog.schema.table2"],
      "source_query": "full SQL query for joined tables (null for single table)",
      "dimensions": [
        {{"name": "field_name", "expr": "expression", "description": "optional"}}
      ],
      "measures": [
        {{"name": "field_name", "expr": "SUM(column)", "description": "optional"}}
      ]
    }}
  ]
}}
```

## Critical Rules

- **ONLY include datasets that have at least 1 measure** - A metrics view without measures is useless
- **Remove SET statements** - Ignore any `SET ansi_mode = true`, `SET timezone = ...`, or similar SET statements from queries. They are session configuration and should NOT be included in source_query
- **USE EXACT COLUMN NAMES ONLY** - For dimensions and measures:
  - For single_table: Use ONLY column names that exist in the source table
  - For joined sources: Use ONLY column names from the SELECT clause of the source_query
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
- For joined: source will be a view created from source_query
- **MEASURE NAMING**: Use the `widget_title` as the measure name when available (normalized to snake_case). If no widget_title exists, fall back to pattern: `aggregation_field` (e.g., `sum_revenue`, `count_orders`, `avg_price`)
- Normalize all names to snake_case (for the `name` field, but keep `expr` as exact column references)

## Target Catalog/Schema

All views will be created in: {target_catalog_schema}

Return ONLY the JSON object, no additional text."""

    return prompt


def generate_metrics_view_yaml(dimensions, measures, source, comment):
    """Generate the YAML content for a Databricks Metrics View."""
    lines = [
        "version: 1.1",
        f"source: {source}",
        f'comment: "{comment}"',
    ]
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
            lines.append(f"  - name: {name}")
            lines.append(f"    expr: {expr}")
            if desc:
                lines.append(f'    comment: "{desc}"')
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
        import re
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
    
    # Generate SQL statements with execution order
    execution_steps = []
    step_order = 1
    
    datasets_analysis = analysis_result.get('datasets_analysis', [])
    
    for ds in datasets_analysis:
        dataset_name = ds.get('dataset_name', 'unknown')
        source_type = ds.get('source_type', 'single_table')
        tables = ds.get('tables', [])
        source_query = ds.get('source_query')
        dimensions = ds.get('dimensions', [])
        measures = ds.get('measures', [])
        
        safe_name = normalize_name(dataset_name)
        
        if source_type == 'joined' and source_query:
            # Step 1: Create the prerequisite source view
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
            
            # Step 2: Create the metrics view
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
            source_table = tables[0] if tables else "unknown_table"
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
