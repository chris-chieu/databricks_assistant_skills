-- Unity Catalog Python Function for Multi-Agent System (MAS) Query
-- This function allows querying the MAS endpoint from SQL Editor

CREATE OR REPLACE FUNCTION christophe_chieu.certified_tables.query_mas(
    question STRING,
    token STRING
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Query the Multi-Agent System (MAS) endpoint with a natural language question'
AS $$
import json
import requests

DATABRICKS_HOST = "https://fe-vm-vdm-christophe-chieu.cloud.databricks.com"
MAS_ENDPOINT = "mas-beb9b9ee-endpoint"

try:
    url = f"{DATABRICKS_HOST}/serving-endpoints/{MAS_ENDPOINT}/invocations"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "input": [
            {
                "role": "user",
                "content": question
            }
        ]
    }
    
    response = requests.post(url, headers=headers, json=payload, timeout=300)
    response.raise_for_status()
    
    result_json = response.json()
    
    # Extract text from response.output
    texts = []
    if "output" in result_json:
        for output in result_json["output"]:
            if "content" in output:
                for content in output["content"]:
                    if "text" in content:
                        texts.append(content["text"])
    
    response_text = " ".join(texts)
    
    result = json.dumps({
        "question": question,
        "response": response_text,
        "status": "success"
    })
    
except requests.exceptions.Timeout:
    result = json.dumps({
        "question": question,
        "error": "Request timed out after 300 seconds",
        "status": "error"
    })
except requests.exceptions.RequestException as e:
    result = json.dumps({
        "question": question,
        "error": str(e),
        "status": "error"
    })
except Exception as e:
    import traceback
    result = json.dumps({
        "question": question,
        "error": str(e),
        "traceback": traceback.format_exc(),
        "status": "error"
    })

return result
$$;


-- ============================================================================
-- Example Usage
-- ============================================================================

-- Query the MAS with a question
-- SELECT christophe_chieu.certified_tables.query_mas(
--     'Can you cross-reference the performance seen among our customers with our company strategy?',
--     secret('vm_cchieu', 'my_token_secret')
-- ) as result;

-- Query about churn and strategy
-- SELECT christophe_chieu.certified_tables.query_mas(
--     'What factors are driving customer churn and how should we adjust our marketing strategy?',
--     secret('vm_cchieu', 'my_token_secret')
-- ) as result;

-- Query about support and retention
-- SELECT christophe_chieu.certified_tables.query_mas(
--     'How do support ticket trends relate to customer retention?',
--     secret('vm_cchieu', 'my_token_secret')
-- ) as result;
