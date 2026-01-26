"""
Genie API Query Functions

Simple interface to interact with Databricks Genie API
for natural language querying of data spaces.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieMessage
from datetime import timedelta
from typing import Optional


def get_workspace_client() -> WorkspaceClient:
    """
    Initialize and return a Databricks WorkspaceClient.
    
    Uses environment variables or .databrickscfg for authentication:
    - DATABRICKS_HOST
    - DATABRICKS_TOKEN (or other auth methods)
    """
    return WorkspaceClient(host="<INSERT_DATABRICKS_HOST>",
                           token=dbutils.secrets.get(scope="<INSERT_SECRET_SCOPE>", key="<INSERT_TOKEN_SECRET_KEY>"))


def start_conversation(
    space_id: str,
    question: str,
    timeout_minutes: int = 20,
    client: Optional[WorkspaceClient] = None
) -> GenieMessage:
    """
    Start a new conversation in a Genie space with an initial question.
    
    Args:
        space_id: The ID of the Genie space to query
        question: The natural language question to ask
        timeout_minutes: Maximum time to wait for response (default: 20)
        client: Optional WorkspaceClient instance
    
    Returns:
        GenieMessage: The Genie response message
    """
    if client is None:
        client = get_workspace_client()
    
    response = client.genie.start_conversation_and_wait(
        space_id=space_id,
        content=question,
        timeout=timedelta(minutes=timeout_minutes)
    )
    
    return response


def ask_followup(
    space_id: str,
    conversation_id: str,
    question: str,
    timeout_minutes: int = 20,
    client: Optional[WorkspaceClient] = None
) -> GenieMessage:
    """
    Ask a follow-up question in an existing conversation.
    
    Args:
        space_id: The ID of the Genie space
        conversation_id: The ID of the existing conversation
        question: The follow-up question
        timeout_minutes: Maximum time to wait for response (default: 20)
        client: Optional WorkspaceClient instance
    
    Returns:
        GenieMessage: The Genie response message
    """
    if client is None:
        client = get_workspace_client()
    
    response = client.genie.create_message_and_wait(
        space_id=space_id,
        conversation_id=conversation_id,
        content=question,
        timeout=timedelta(minutes=timeout_minutes)
    )
    
    return response


def delete_conversation(
    space_id: str,
    conversation_id: str,
    client: Optional[WorkspaceClient] = None
) -> None:
    """
    Delete a conversation when done.
    
    Args:
        space_id: The Genie space ID
        conversation_id: The conversation ID to delete
        client: Optional WorkspaceClient instance
    """
    if client is None:
        client = get_workspace_client()
    
    client.genie.delete_conversation(
        space_id=space_id,
        conversation_id=conversation_id
    )
