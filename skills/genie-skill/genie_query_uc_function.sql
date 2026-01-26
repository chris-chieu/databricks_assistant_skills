-- Unity Catalog Python Functions for Genie Query
-- These functions allow querying Genie spaces from SQL Editor

-- ============================================================================
-- Function 1: Start a new Genie conversation
-- ============================================================================
CREATE OR REPLACE FUNCTION <INSERT_CATALOG>.<INSERT_SCHEMA>.genie_start_conversation(
    space_id STRING,
    question STRING,
    client_id STRING,
    client_secret STRING
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Start a new conversation in a Genie space and return the response with conversation_id'
AS $$
import json
from databricks.sdk import WorkspaceClient
from datetime import timedelta

DATABRICKS_HOST = "<INSERT_DATABRICKS_HOST>"

w = WorkspaceClient(
    host=DATABRICKS_HOST,
    client_id=client_id,
    client_secret=client_secret
)

try:
    # Start conversation (don't wait, so we can handle failures better)
    op = w.genie.start_conversation(
        space_id=space_id,
        content=question
    )
    
    try:
        response = op.result(timeout=timedelta(minutes=20))
    except Exception as wait_error:
        # If waiting failed, try to get the message directly for more info
        if hasattr(op, 'conversation_id') and hasattr(op, 'message_id'):
            try:
                msg = w.genie.get_message(
                    space_id=space_id,
                    conversation_id=op.conversation_id,
                    message_id=op.message_id
                )
                return json.dumps({
                    "conversation_id": op.conversation_id,
                    "message_id": op.message_id,
                    "message_status": msg.status.value if msg.status else None,
                    "error": str(wait_error),
                    "error_details": msg.error.message if hasattr(msg, 'error') and msg.error else None,
                    "status": "failed"
                })
            except:
                pass
        raise wait_error
    
    # Extract response data
    result = {
        "conversation_id": response.conversation_id,
        "message_id": response.id,
        "message_status": response.status.value if response.status else None,
        "content": None,
        "attachments": []
    }
    
    # Extract message content if available
    if response.attachments:
        for attachment in response.attachments:
            att_info = {
                "id": attachment.id if hasattr(attachment, 'id') else None,
                "type": str(type(attachment).__name__)
            }
            # Check for text content
            if hasattr(attachment, 'text') and attachment.text:
                if hasattr(attachment.text, 'content'):
                    result["content"] = attachment.text.content
            # Check for query attachment
            if hasattr(attachment, 'query') and attachment.query:
                att_info["has_query"] = True
                if hasattr(attachment.query, 'description'):
                    att_info["description"] = attachment.query.description
            result["attachments"].append(att_info)
    
    result["status"] = "success"
    return json.dumps(result)
    
except Exception as e:
    import traceback
    return json.dumps({
        "error": str(e),
        "traceback": traceback.format_exc(),
        "status": "error"
    })
$$;

-- ============================================================================
-- Function 2: Ask a follow-up question in an existing conversation
-- ============================================================================
CREATE OR REPLACE FUNCTION <INSERT_CATALOG>.<INSERT_SCHEMA>.genie_ask_followup(
    space_id STRING,
    conversation_id STRING,
    question STRING,
    client_id STRING,
    client_secret STRING
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Ask a follow-up question in an existing Genie conversation'
AS $$
import json
from databricks.sdk import WorkspaceClient
from datetime import timedelta

DATABRICKS_HOST = "<INSERT_DATABRICKS_HOST>"

w = WorkspaceClient(
    host=DATABRICKS_HOST,
    client_id=client_id,
    client_secret=client_secret
)

try:
    # Send follow-up message (don't wait, so we can handle failures better)
    op = w.genie.create_message(
        space_id=space_id,
        conversation_id=conversation_id,
        content=question
    )
    
    try:
        response = op.result(timeout=timedelta(minutes=20))
    except Exception as wait_error:
        # If waiting failed, try to get the message directly for more info
        if hasattr(op, 'message_id'):
            try:
                msg = w.genie.get_message(
                    space_id=space_id,
                    conversation_id=conversation_id,
                    message_id=op.message_id
                )
                return json.dumps({
                    "conversation_id": conversation_id,
                    "message_id": op.message_id,
                    "message_status": msg.status.value if msg.status else None,
                    "error": str(wait_error),
                    "error_details": msg.error.message if hasattr(msg, 'error') and msg.error else None,
                    "status": "failed"
                })
            except:
                pass
        raise wait_error
    
    # Extract response data
    result = {
        "conversation_id": response.conversation_id,
        "message_id": response.id,
        "message_status": response.status.value if response.status else None,
        "content": None,
        "attachments": []
    }
    
    # Extract message content if available
    if response.attachments:
        for attachment in response.attachments:
            att_info = {
                "id": attachment.id if hasattr(attachment, 'id') else None,
                "type": str(type(attachment).__name__)
            }
            # Check for text content
            if hasattr(attachment, 'text') and attachment.text:
                if hasattr(attachment.text, 'content'):
                    result["content"] = attachment.text.content
            # Check for query attachment
            if hasattr(attachment, 'query') and attachment.query:
                att_info["has_query"] = True
                if hasattr(attachment.query, 'description'):
                    att_info["description"] = attachment.query.description
            result["attachments"].append(att_info)
    
    result["status"] = "success"
    return json.dumps(result)
    
except Exception as e:
    import traceback
    return json.dumps({
        "error": str(e),
        "traceback": traceback.format_exc(),
        "status": "error"
    })
$$;

-- ============================================================================
-- Function 3: Delete a conversation
-- ============================================================================
CREATE OR REPLACE FUNCTION <INSERT_CATALOG>.<INSERT_SCHEMA>.genie_delete_conversation(
    space_id STRING,
    conversation_id STRING,
    client_id STRING,
    client_secret STRING
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Delete a Genie conversation when done'
AS $$
import json
from databricks.sdk import WorkspaceClient

DATABRICKS_HOST = "<INSERT_DATABRICKS_HOST>"

w = WorkspaceClient(
    host=DATABRICKS_HOST,
    client_id=client_id,
    client_secret=client_secret
)

try:
    w.genie.delete_conversation(
        space_id=space_id,
        conversation_id=conversation_id
    )
    
    return json.dumps({
        "conversation_id": conversation_id,
        "deleted": True,
        "status": "success"
    })
    
except Exception as e:
    import traceback
    return json.dumps({
        "error": str(e),
        "traceback": traceback.format_exc(),
        "status": "error"
    })
$$;


-- ============================================================================
-- Example Usage
-- ============================================================================

-- Start a conversation with a Genie space
-- SELECT <INSERT_CATALOG>.<INSERT_SCHEMA>.genie_start_conversation(
--     '<INSERT_GENIE_SPACE_ID>',
--     'What is our customer churn rate?',
--     secret('<INSERT_SECRET_SCOPE>', '<INSERT_CLIENT_ID_SECRET_KEY>'),
--     secret('<INSERT_SECRET_SCOPE>', '<INSERT_CLIENT_SECRET_SECRET_KEY>')
-- ) as result;

-- Ask a follow-up question (use conversation_id from previous response)
-- SELECT <INSERT_CATALOG>.<INSERT_SCHEMA>.genie_ask_followup(
--     '<INSERT_GENIE_SPACE_ID>',
--     '<CONVERSATION_ID_FROM_START>',
--     'Break that down by customer tenure',
--     secret('<INSERT_SECRET_SCOPE>', '<INSERT_CLIENT_ID_SECRET_KEY>'),
--     secret('<INSERT_SECRET_SCOPE>', '<INSERT_CLIENT_SECRET_SECRET_KEY>')
-- ) as result;

-- Delete conversation when done
-- SELECT <INSERT_CATALOG>.<INSERT_SCHEMA>.genie_delete_conversation(
--     '<INSERT_GENIE_SPACE_ID>',
--     '<CONVERSATION_ID>',
--     secret('<INSERT_SECRET_SCOPE>', '<INSERT_CLIENT_ID_SECRET_KEY>'),
--     secret('<INSERT_SECRET_SCOPE>', '<INSERT_CLIENT_SECRET_SECRET_KEY>')
-- ) as result;
