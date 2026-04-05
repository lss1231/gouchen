"""Intent Parser tool for NL2SQL."""
import json
from langchain.tools import tool
from langchain_openai import ChatOpenAI

from src.config import get_settings
from src.models import QueryIntent


INTENT_PARSER_PROMPT = """你是一个专业的查询意图解析器。你的任务是将用户的自然语言查询解析为结构化的查询意图。

请分析用户的查询，提取以下信息：
- metrics: 指标字段（如销售额、订单量、用户数等）
- dimensions: 维度字段（如地区、品类、时间等）
- filters: 过滤条件（如状态=已完成、金额>1000等）
- time_range: 时间范围（如最近7天、本月、2024年Q1等）
- aggregation: 聚合方式（sum, count, avg, max, min等，默认为sum）
- sort_by: 排序字段
- sort_order: 排序方向（asc或desc，默认为desc）
- limit: 返回条数限制（默认为1000）

请只返回JSON格式的结果，不要包含任何其他解释文字。
"""


@tool
def parse_intent(query: str) -> str:
    """Parse user query into structured intent.

    Args:
        query: User's natural language query

    Returns:
        JSON string containing QueryIntent with fields:
        - metrics: List of metric fields
        - dimensions: List of dimension fields
        - filters: List of filter conditions
        - time_range: Time range specification
        - aggregation: Aggregation method
        - sort_by: Sort field
        - sort_order: Sort direction
        - limit: Result limit
    """
    try:
        settings = get_settings()
        llm = ChatOpenAI(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            model=settings.llm_model,
            temperature=0.1,
        )

        messages = [
            {"role": "system", "content": INTENT_PARSER_PROMPT},
            {"role": "user", "content": f"请解析以下查询：\n\n{query}"}
        ]

        response = llm.invoke(messages)
        content = response.content.strip()

        # Try to parse and validate the response
        try:
            intent_data = json.loads(content)
            # Validate by creating QueryIntent model
            intent = QueryIntent(**intent_data)
            return json.dumps(intent.model_dump(), ensure_ascii=False)
        except json.JSONDecodeError:
            # If not valid JSON, try to extract JSON from the response
            import re
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                intent_data = json.loads(json_match.group())
                intent = QueryIntent(**intent_data)
                return json.dumps(intent.model_dump(), ensure_ascii=False)
            raise ValueError(f"Invalid JSON response: {content}")

    except Exception as e:
        return json.dumps({
            "error": str(e),
            "query": query
        }, ensure_ascii=False)
