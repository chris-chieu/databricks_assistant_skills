---
name: mas-skill
description: Query the Multi-Agent System (MAS) which coordinates multiple specialized agents. Use when the user asks about SaaS strategy, financial planning, marketing, sales, customer success, business planning, customer churn, demographics, tenure, service usage, retention, support tickets, resolution times, response times, customer satisfaction, SLA compliance, or agent performance. Best for complex questions that may require cross-referencing multiple data sources.
---

# MAS Skill

Query the Multi-Agent System (MAS) endpoint which coordinates multiple specialized agents to answer complex questions.

## Agents in MAS

The MAS contains three specialized agents:

| Agent | Domain | Use For |
|-------|--------|---------|
| Strategy & Planning | SaaS strategy, financial planning, marketing, sales, customer success | Business planning, strategy questions, marketing insights, sales analysis |
| Customer Churn | Telecom customer demographics, behavior, churn | Tenure analysis, service usage, retention, churn factors, customer attributes |
| Support Tickets | Ticket resolution, response times, satisfaction | Agent performance, SLA compliance, issue trends, customer service optimization |

## When to Use This Skill

Use this skill when the user asks about:

**Strategy & Business Planning:**
- SaaS strategy or business planning
- Financial planning or forecasting
- Marketing strategies or campaigns
- Sales performance or analysis
- Customer success metrics

**Customer Churn & Demographics:**
- Customer churn or retention
- Customer demographics or behavior
- Service usage patterns
- Customer tenure analysis
- Factors contributing to churn

**Support & Customer Service:**
- Support ticket metrics
- Resolution times or response times
- Customer satisfaction or surveys
- Agent performance evaluation
- SLA compliance monitoring
- Issue trends or support volume

**Cross-Domain Questions:**
- Questions that span multiple areas above
- Cross-referencing performance with strategy
- Synthesizing insights across domains

## Implementation Options

### Option 1: Python Script

Execute the function from [scripts/mas_query.py](scripts/mas_query.py).

| Function | Purpose |
|----------|---------|
| `query_mas(question, token)` | Query the MAS endpoint with a question |

### Option 2: UC Function (SQL Editor)

For SQL Editor which can only run SQL, use the Unity Catalog function defined in [mas_query_uc_function.sql](mas_query_uc_function.sql).

| UC Function | Purpose |
|-------------|---------|
| `christophe_chieu.certified_tables.query_mas` | Query MAS via SQL |

**SQL Usage:**
```sql
SELECT christophe_chieu.certified_tables.query_mas(
    'Can you cross-reference customer performance with our company strategy?',
    secret('vm_cchieu', 'my_token_secret')
) as result;
```

## Workflow Instructions

### Step 1: Get the Token

```python
token = dbutils.secrets.get(scope="vm_cchieu", key="my_token_secret")
```

### Step 2: Query MAS

```python
from scripts.mas_query import query_mas

result = query_mas(
    question="<USER_QUESTION>",
    token=token
)
```

### Step 3: Present the Response

Present the result to the user.

## Example Questions by Domain

**Strategy & Planning:**
- "What should our SaaS pricing strategy be for next quarter?"
- "How can we improve our customer success metrics?"
- "What marketing strategies would help reduce churn?"

**Customer Churn:**
- "What factors are contributing to customer churn?"
- "Which customer segments have the highest retention?"
- "How does service usage correlate with churn?"

**Support Tickets:**
- "What is our average ticket resolution time?"
- "Are we meeting SLA targets?"
- "Which agents have the best satisfaction scores?"

**Cross-Domain:**
- "Can you cross-reference customer performance with our company strategy?"
- "How do support ticket trends relate to customer churn?"
- "What's the relationship between service usage and customer satisfaction?"

## Example Session

```python
from scripts.mas_query import query_mas

token = dbutils.secrets.get(scope="vm_cchieu", key="my_token_secret")

# User: "Can you cross-reference the performance seen among our customers with our company's strategy?"
result = query_mas(
    question="Can you cross-reference the performance seen among our customers with our company's strategy?",
    token=token
)
# → Present result to user

# User: "What factors are driving churn and how should we adjust our marketing strategy?"
result = query_mas(
    question="What factors are driving churn and how should we adjust our marketing strategy?",
    token=token
)
# → Present result to user
```

## Function Parameters

### query_mas
- `question` (str): The user's natural language question
- `token` (str): Databricks personal access token
- `endpoint_name` (str, optional): The MAS endpoint name (default: mas-beb9b9ee-endpoint)
- `host` (str, optional): Databricks workspace host URL

## Notes

- MAS coordinates multiple agents to synthesize information
- Best for complex questions spanning multiple domains
- Can cross-reference data from different sources
