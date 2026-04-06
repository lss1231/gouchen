"""SQL Generator tool for NL2SQL."""
import json
import re
from langchain.tools import tool
from langchain_openai import ChatOpenAI

from ..config import get_settings
from ..models import GeneratedSQL


SQL_GENERATOR_PROMPT = """你是一个专业的SQL生成器。你的任务是根据用户的查询意图和相关表结构生成正确的SQL查询。

重要规则：
1. 只允许生成SELECT语句，禁止生成DROP、DELETE、UPDATE、INSERT、CREATE等修改数据的语句
2. 根据意图中的metrics、dimensions、filters、time_range等信息构建SQL
3. 使用正确的表名和字段名
4. 确保SQL语法正确
5. **关键：datasource字段必须从表结构中获取** - 查看表的datasource属性（mysql或doris），返回正确的值
6. 返回JSON格式，包含sql、datasource、tables、explanation字段

请只返回JSON格式的结果，不要包含任何其他解释文字。

返回格式示例：
{
    "sql": "SELECT ...",
    "datasource": "mysql",  // 必须从表结构中获取，不要猜测
    "tables": ["fact_order"],
    "explanation": "..."
}
"""


@tool
def generate_sql(intent_json: str, schema_json: str) -> str:
    """Generate SQL query based on intent and schema.

    Args:
        intent_json: JSON string containing QueryIntent with fields:
            - metrics: List of metric fields
            - dimensions: List of dimension fields
            - filters: List of filter conditions
            - time_range: Time range specification
            - aggregation: Aggregation method
            - sort_by: Sort field
            - sort_order: Sort direction
            - limit: Result limit
        schema_json: JSON string containing relevant tables with fields:
            - tables: List of table metadata

    Returns:
        JSON string containing GeneratedSQL with fields:
        - sql: The generated SQL query
        - datasource: Data source type (mysql/doris)
        - tables: List of tables used
        - explanation: Explanation of the SQL
    """
    try:
        # Validate that we only generate SELECT statements
        settings = get_settings()
        llm = ChatOpenAI(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            model=settings.llm_model,
            temperature=0.1,
        )

        messages = [
            {"role": "system", "content": SQL_GENERATOR_PROMPT},
            {"role": "user", "content": f"""请根据以下查询意图和表结构生成SQL查询：

查询意图：
{intent_json}

相关表结构：
{schema_json}

请生成SQL查询并返回JSON格式结果。"""}
        ]

        response = llm.invoke(messages)
        content = response.content.strip()

        # Try to parse and validate the response
        try:
            sql_data = json.loads(content)

            # Validate SQL is SELECT only
            sql = sql_data.get("sql", "")
            if not _is_safe_sql(sql):
                raise ValueError("Generated SQL contains unsafe operations. Only SELECT statements are allowed.")

            # Force datasource from schema (don't trust LLM to get it right)
            schema_data = json.loads(schema_json)
            tables = schema_data.get("tables", [])
            if tables:
                # Get datasource from first table (all tables in query should be from same source)
                first_table = tables[0]
                correct_datasource = first_table.get("datasource", "mysql")
                sql_data["datasource"] = correct_datasource

            # Validate by creating GeneratedSQL model
            generated = GeneratedSQL(**sql_data)
            return json.dumps(generated.model_dump(), ensure_ascii=False)

        except json.JSONDecodeError:
            # If not valid JSON, try to extract JSON from the response
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                sql_data = json.loads(json_match.group())
                sql = sql_data.get("sql", "")
                if not _is_safe_sql(sql):
                    raise ValueError("Generated SQL contains unsafe operations. Only SELECT statements are allowed.")

                # Force datasource from schema
                schema_data = json.loads(schema_json)
                tables = schema_data.get("tables", [])
                if tables:
                    first_table = tables[0]
                    correct_datasource = first_table.get("datasource", "mysql")
                    sql_data["datasource"] = correct_datasource

                generated = GeneratedSQL(**sql_data)
                return json.dumps(generated.model_dump(), ensure_ascii=False)
            raise ValueError(f"Invalid JSON response: {content}")

    except Exception as e:
        return json.dumps({
            "error": str(e),
            "sql": "",
            "datasource": "",
            "tables": [],
            "explanation": ""
        }, ensure_ascii=False)


def _is_safe_sql(sql: str) -> bool:
    """Check if SQL is safe (SELECT only, no dangerous operations)."""
    if not sql or not isinstance(sql, str):
        return False

    sql_upper = sql.strip().upper()

    # Must start with SELECT
    if not sql_upper.startswith("SELECT"):
        return False

    # Check for dangerous keywords
    dangerous_keywords = ["DROP", "DELETE", "UPDATE", "INSERT", "CREATE", "ALTER", "TRUNCATE"]
    for keyword in dangerous_keywords:
        if keyword in sql_upper:
            return False

    return True
