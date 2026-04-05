"""Deep Agent configuration for NL2SQL."""
from deepagents import create_deep_agent
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver

from .config import get_settings
from .tools import parse_intent, retrieve_schema, generate_sql, execute_sql


def create_nl2sql_agent():
    """
    Create Deep Agent for NL2SQL with:
    - Custom tools for intent parsing, schema retrieval, SQL generation/execution
    - HITL approval for SQL execution (sensitive operation)
    - TodoList middleware for automatic task planning
    """
    settings = get_settings()

    # Configure Kimi model using ChatOpenAI
    kimi_model = ChatOpenAI(
        model=settings.llm_model,
        api_key=settings.llm_api_key,
        base_url=settings.llm_base_url,
        temperature=1,  # Kimi model requires temperature=1
    )

    agent = create_deep_agent(
        name="gouchen-nl2sql",
        model=kimi_model,  # Pass configured model instance
        tools=[parse_intent, retrieve_schema, generate_sql, execute_sql],
        system_prompt="""你是钩沉 (Gouchen) 智能数据查询助手。

你的任务是将用户的自然语言查询转换为 SQL 并执行。

**必须遵循的工作流程**：
1. 使用 parse_intent 解析用户查询意图
2. 使用 retrieve_schema 检索相关数据表
3. 使用 generate_sql 生成 SQL 查询
4. **必须调用 execute_sql 执行查询**（此操作需要人工审批，等待用户确认后再继续）

**重要规则**：
- 你必须按顺序调用所有工具
- 生成 SQL 后，必须调用 execute_sql 来执行
- execute_sql 会触发人工审批流程，这是正常的行为
- 不要只是说要执行，必须实际调用 execute_sql 工具

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
