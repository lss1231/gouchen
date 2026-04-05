"""Intent Parser tool for NL2SQL."""
import json
from typing import Dict, Any

from langchain.tools import tool
from langchain_openai import ChatOpenAI

from ..config import get_settings


@tool
def parse_intent(query: str) -> str:
    """Parse natural language query into structured intent.

    This tool analyzes user queries and extracts structured information about
    what metrics, dimensions, filters, and time ranges they want to query.

    Args:
        query: The natural language query from the user (e.g., "上个月销售额")

    Returns:
        A JSON string containing the parsed intent with fields:
        - metrics: List of metrics to calculate (e.g., ["销售额"])
        - dimensions: List of dimensions to group by (e.g., ["地区", "品类"])
        - filters: List of filter conditions
        - time_range: Time range specification
        - aggregation: Aggregation method (sum, count, avg, etc.)
        - limit: Maximum number of results
    """
    try:
        settings = get_settings()
        llm = ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            temperature=0.1,
        )

        prompt = f"""Parse the following query into structured intent.

Query: {query}

Return ONLY a JSON object with this structure:
{{
    "metrics": ["metric1", "metric2"],
    "dimensions": ["dimension1", "dimension2"],
    "filters": [],
    "time_range": {{"type": "last_month"}},
    "aggregation": "sum",
    "limit": 1000
}}

Supported metrics: 销售额, 订单量, 用户数, 客单价
Supported dimensions: 时间, 地区, 品类, 商品, 品牌
Supported time ranges: today, yesterday, last_7_days, last_30_days, this_month, last_month"""

        response = llm.invoke(prompt)
        content = response.content.strip()

        # Extract JSON if wrapped in code blocks
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()

        # Validate JSON
        intent_data = json.loads(content)
        return json.dumps(intent_data, ensure_ascii=False)

    except Exception as e:
        return json.dumps({
            "error": f"Failed to parse intent: {str(e)}",
            "query": query
        }, ensure_ascii=False)
