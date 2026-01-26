"""
Multi-Agent System (MAS) Query Functions

Query Databricks MAS serving endpoints using requests library.
"""

import requests
import json
from typing import Optional


DATABRICKS_HOST = "<INSERT_DATABRICKS_HOST>"
MAS_ENDPOINT = "<INSERT_MAS_ENDPOINT_NAME>"


def query_mas(
    question: str,
    token: str,
    endpoint_name: str = MAS_ENDPOINT,
    host: str = DATABRICKS_HOST
) -> str:
    """
    Query the Multi-Agent System endpoint.
    
    Args:
        question: The user's question
        token: Databricks personal access token
        endpoint_name: The MAS endpoint name (default: mas-beb9b9ee-endpoint)
        host: Databricks workspace host URL
    
    Returns:
        str: The MAS response text
    """
    url = f"{host}/serving-endpoints/{endpoint_name}/invocations"
    
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
    
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    
    result = response.json()
    
    # Extract text from response.output
    texts = []
    if "output" in result:
        for output in result["output"]:
            if "content" in output:
                for content in output["content"]:
                    if "text" in content:
                        texts.append(content["text"])
    
    return " ".join(texts)
