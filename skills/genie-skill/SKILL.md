---
name: genie-skill
description: Query Databricks Genie spaces to answer questions about telecommunications customer data. Use this skill when the user asks about customer churn, demographics, retention, service usage, support tickets, resolution times, SLA compliance, agent performance, or customer satisfaction. This skill manages conversations with Genie including starting, following up, and cleaning up.
---

# Genie Skill

Query Databricks Genie spaces using natural language to answer questions about telecommunications customer data.

## Available Genie Spaces

| Space | ID | Domain | Use For |
|-------|-----|--------|---------|
| Customer Churn Analytics | `01f0f9f0b9c41c74a5adfb46bfc836dd` | Customer demographics & behavior | Churn analysis, retention, tenure, service usage, marketing strategies |
| Support Tickets | `01f0f9f6fb1c13f08595fffdd1fc82d3` | Support operations | Resolution times, response times, satisfaction, SLA, agent performance |

## Space Selection

Route questions to the appropriate space:

| Question Topic | Space |
|----------------|-------|
| Customer demographics, attributes | Customer Churn Analytics |
| Churn prediction, retention rates | Customer Churn Analytics |
| Service usage, tenure, subscriptions | Customer Churn Analytics |
| Marketing strategies, customer segments | Customer Churn Analytics |
| Ticket resolution times, response times | Support Tickets |
| Customer satisfaction, survey results | Support Tickets |
| Agent performance, SLA compliance | Support Tickets |
| Issue trends, support volume | Support Tickets |

### Routing Examples

| Question | Route To |
|----------|----------|
| "What's our churn rate this quarter?" | Customer Churn Analytics |
| "Show me customers by tenure" | Customer Churn Analytics |
| "Which services have highest churn?" | Customer Churn Analytics |
| "Average ticket resolution time?" | Support Tickets |
| "Which agents have best satisfaction scores?" | Support Tickets |
| "Are we meeting SLA targets?" | Support Tickets |

## When to Use This Skill

Use this skill when the user:
- Asks about customer churn, retention, or demographics
- Wants data on service usage, tenure, or customer attributes
- Asks about support tickets, resolution times, or response times
- Wants customer satisfaction or survey data
- Asks about agent performance or SLA compliance
- Mentions "Genie", "Genie space", or "Genie room"

## Implementation Options

### Option 1: Python Script

Execute the functions from [scripts/genie_query.py](scripts/genie_query.py).

| Function | Purpose |
|----------|---------|
| `start_conversation(space_id, question)` | Start a new Genie conversation |
| `ask_followup(space_id, conversation_id, question)` | Ask a follow-up question |
| `delete_conversation(space_id, conversation_id)` | Clean up when done |

### Option 2: UC Functions (SQL Editor)

For SQL Editor which can only run SQL, use the Unity Catalog functions defined in [genie_query_uc_function.sql](genie_query_uc_function.sql).

| UC Function | Purpose |
|-------------|---------|
| `christophe_chieu.certified_tables.genie_start_conversation` | Start a new conversation |
| `christophe_chieu.certified_tables.genie_ask_followup` | Ask a follow-up question |
| `christophe_chieu.certified_tables.genie_delete_conversation` | Delete conversation |

#### SQL Usage Examples

**Start a conversation:**
```sql
SELECT christophe_chieu.certified_tables.genie_start_conversation(
    '01f0f9f0b9c41c74a5adfb46bfc836dd',
    'What is our customer churn rate?',
    secret('vm_cchieu', 'sp_client_id'),
    secret('vm_cchieu', 'sp_client_secret')
) as result;
```

**Ask a follow-up (use conversation_id from previous result):**
```sql
SELECT christophe_chieu.certified_tables.genie_ask_followup(
    '01f0f9f0b9c41c74a5adfb46bfc836dd',
    '<CONVERSATION_ID>',
    'Break that down by customer tenure',
    secret('vm_cchieu', 'sp_client_id'),
    secret('vm_cchieu', 'sp_client_secret')
) as result;
```

**Delete conversation when done:**
```sql
SELECT christophe_chieu.certified_tables.genie_delete_conversation(
    '01f0f9f0b9c41c74a5adfb46bfc836dd',
    '<CONVERSATION_ID>',
    secret('vm_cchieu', 'sp_client_id'),
    secret('vm_cchieu', 'sp_client_secret')
) as result;
```

All UC functions require OAuth credentials (service principal) passed as the last two parameters.

## Workflow Instructions

### Step 1: Select the Right Space

Based on the user's question, select the appropriate `space_id` from the table above.

### Step 2: Start a Conversation

```python
from scripts.genie_query import start_conversation

response = start_conversation(
    space_id="<SELECTED_SPACE_ID>",
    question="<USER_QUESTION>"
)

# IMPORTANT: Save these for follow-ups
conversation_id = response.conversation_id
```

Extract the answer from `response` and present it to the user.

### Step 3: Handle Follow-up Questions

Use `ask_followup()` with the saved `conversation_id`:

```python
from scripts.genie_query import ask_followup

response = ask_followup(
    space_id="<SELECTED_SPACE_ID>",
    conversation_id=conversation_id,
    question="<FOLLOWUP_QUESTION>"
)
```

You can call `ask_followup()` multiple times. Each follow-up uses context from all previous messages.

### Step 4: End the Conversation

When done or moving to a different topic:

```python
from scripts.genie_query import delete_conversation

delete_conversation(
    space_id="<SELECTED_SPACE_ID>",
    conversation_id=conversation_id
)
```

### Refreshing / Starting a New Conversation

If the user wants to start fresh or reset:

1. Delete the current conversation:
   ```python
   delete_conversation(space_id, conversation_id)
   ```

2. Ask the user: "What would you like to know?"

3. Start a fresh conversation with `start_conversation()`

**Triggers for refresh:** "start over", "new question", "reset", "fresh start"

### Switching Between Spaces

If a follow-up question belongs to a different space:

1. Delete the current conversation
2. Start a new conversation in the appropriate space
3. Inform the user you're querying a different data source

## Example Session

```python
from scripts.genie_query import start_conversation, ask_followup, delete_conversation

# User: "What's our customer churn rate?"
# → Route to Customer Churn Analytics
churn_space_id = "01f0f9f0b9c41c74a5adfb46bfc836dd"
response = start_conversation(churn_space_id, "What's our customer churn rate?")
conversation_id = response.conversation_id
# → Present response

# User: "Break that down by tenure"
response = ask_followup(churn_space_id, conversation_id, "Break that down by tenure")
# → Present response

# User: "Which services do churned customers use most?"
response = ask_followup(churn_space_id, conversation_id, "Which services do churned customers use most?")
# → Present response

# User done with churn analysis
delete_conversation(churn_space_id, conversation_id)

# User: "Now show me average ticket resolution time"
# → Route to Support Tickets (different space)
support_space_id = "01f0f9f6fb1c13f08595fffdd1fc82d3"
response = start_conversation(support_space_id, "What's the average ticket resolution time?")
conversation_id = response.conversation_id
# → Present response
```

## Conversation State

**Track these values during the conversation:**

- `space_id` - The current Genie space ID
- `conversation_id` - Returned from `start_conversation()`, reuse for follow-ups

## Function Parameters

### start_conversation
- `space_id` (str): The Genie space ID to query
- `question` (str): The natural language question
- `timeout_minutes` (int, optional): Max wait time, default 20

### ask_followup
- `space_id` (str): The Genie space ID
- `conversation_id` (str): From the start_conversation response
- `question` (str): The follow-up question
- `timeout_minutes` (int, optional): Max wait time, default 20

### delete_conversation
- `space_id` (str): The Genie space ID
- `conversation_id` (str): The conversation to delete
