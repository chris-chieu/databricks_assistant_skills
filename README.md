# Databricks Assistant Skills

Agent Skills for Databricks Assistant to query Genie spaces and Multi-Agent Systems.

## Setup

1. **Import into your Databricks workspace**

   Copy the `skills` folder into your Databricks workspace at:
   ```
   /Workspace/Users/<your-email>/.assistant/skills/
   ```
   
   Create the `.assistant` folder if it doesn't exist.

2. **Configure placeholders**

   Replace all `<INSERT_...>` placeholders in the files with your actual values:

   | Placeholder | Description | Example |
   |-------------|-------------|---------|
   | `<INSERT_DATABRICKS_HOST>` | Your Databricks workspace URL | `https://my-workspace.cloud.databricks.com` |
   | `<INSERT_SECRET_SCOPE>` | Databricks secret scope name | `my_scope` |
   | `<INSERT_TOKEN_SECRET_KEY>` | Secret key for personal access token | `my_token` |
   | `<INSERT_CLIENT_ID_SECRET_KEY>` | Secret key for service principal client ID | `sp_client_id` |
   | `<INSERT_CLIENT_SECRET_SECRET_KEY>` | Secret key for service principal client secret | `sp_client_secret` |
   | `<INSERT_CATALOG>` | Unity Catalog name for UC functions | `my_catalog` |
   | `<INSERT_SCHEMA>` | Schema name for UC functions | `my_schema` |
   | `<INSERT_GENIE_SPACE_ID>` | Genie space ID (from URL) | `01f0f9f0b9c41c74a5adfb46bfc836dd` |
   | `<INSERT_MAS_ENDPOINT_NAME>` | Model serving endpoint name for MAS | `my-mas-endpoint` |

3. **Create secrets** (if not already done)

   ```bash
   # Create secret scope
   databricks secrets create-scope <INSERT_SECRET_SCOPE>
   
   # Add personal access token
   databricks secrets put-secret <INSERT_SECRET_SCOPE> <INSERT_TOKEN_SECRET_KEY>
   
   # For Genie UC functions (service principal)
   databricks secrets put-secret <INSERT_SECRET_SCOPE> <INSERT_CLIENT_ID_SECRET_KEY>
   databricks secrets put-secret <INSERT_SECRET_SCOPE> <INSERT_CLIENT_SECRET_SECRET_KEY>
   ```

4. **Register UC functions** (for SQL Editor usage)

   Run the SQL in `genie_query_uc_function.sql` and `mas_query_uc_function.sql` to create the Unity Catalog functions.

## Folder Structure

```
skills/
├── genie-skill/
│   ├── SKILL.md                    # Skill configuration for Genie queries
│   ├── genie_query_uc_function.sql # UC functions for SQL Editor
│   └── scripts/
│       └── genie_query.py          # Python script for notebooks
└── mas-skill/
    ├── SKILL.md                    # Skill configuration for MAS queries
    ├── mas_query_uc_function.sql   # UC function for SQL Editor
    └── scripts/
        └── mas_query.py            # Python script for notebooks
```

## Skills

### genie-skill

Query Databricks Genie spaces using natural language. Supports:
- Starting conversations
- Asking follow-up questions
- Deleting conversations when done

### mas-skill

Query a Multi-Agent System (MAS) endpoint that coordinates multiple specialized agents for cross-domain questions.

## Usage Options

### Option 1: Python Scripts (Notebooks)

Use the Python scripts directly in Databricks notebooks:

```python
from scripts.genie_query import start_conversation, ask_followup, delete_conversation

response = start_conversation("<SPACE_ID>", "What is our churn rate?")
```

### Option 2: UC Functions (SQL Editor)

For SQL Editor, use the registered Unity Catalog functions:

```sql
SELECT my_catalog.my_schema.genie_start_conversation(
    '<SPACE_ID>',
    'What is our churn rate?',
    secret('my_scope', 'sp_client_id'),
    secret('my_scope', 'sp_client_secret')
) as result;
```

## Permissions

Ensure your service principal or user has:
- **CAN USE** permission on Genie spaces
- **CAN USE** permission on SQL warehouses
- **SELECT** permission on Unity Catalog tables used by Genie
- **CAN QUERY** permission on model serving endpoints (for MAS)

## License

MIT
