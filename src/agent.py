"""Deep Agent configuration for NL2SQL."""
from deepagents import create_deep_agent
from langgraph.checkpoint.memory import MemorySaver

from src.config import get_settings
from src.tools import parse_intent, retrieve_schema, generate_sql, execute_sql


def create_nl2sql_agent():
    """
    Create Deep Agent for NL2SQL with:
    - Custom tools for intent parsing, schema retrieval, SQL generation/execution
    - HITL approval for SQL execution (sensitive operation)
    - TodoList middleware for automatic task planning
    """
    settings = get_settings()

    agent = create_deep_agent(
        name="gouchen-nl2sql",
        model=settings.llm_model,
        tools=[parse_intent, retrieve_schema, generate_sql, execute_sql],
        system_prompt="""你是钩沉 (Gouchen) 智能数据查询助手。

你的任务是将用户的自然语言查询转换为 SQL 并执行。

工作流程：
1. 使用 parse_intent 解析用户查询意图
2. 使用 retrieve_schema 检索相关数据表
3. 使用 generate_sql 生成 SQL 查询
4. 使用 execute_sql 执行查询（此操作需要人工审批）

安全规则：
- 只查询用户有权限的数据
- execute_sql 是敏感操作，系统会要求人工确认
- 如果查询涉及敏感字段，提醒用户

返回格式：
- SQL 语句
- 查询结果摘要
- 数据解释""",
        # HITL: Require approval for SQL execution
        interrupt_on={"execute_sql": True},
        # Persistence for conversation state
        checkpointer=MemorySaver(),
    )

    return agent


# Global agent instance
_nl2sql_agent = None

def get_nl2sql_agent():
    """Get or create NL2SQL agent."""
    global _nl2sql_agent
    if _nl2sql_agent is None:
        _nl2sql_agent = create_nl2sql_agent()
    return _nl2sql_agent
