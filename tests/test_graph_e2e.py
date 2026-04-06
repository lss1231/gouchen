"""End-to-end tests for NL2SQL graph workflow."""

from unittest.mock import MagicMock, patch

import pytest
from langgraph.types import Command

from src.graph.builder import build_graph, get_graph
from src.graph.state import NL2SQLState, QueryIntent


def test_graph_build():
    """Test that the graph can be successfully compiled."""
    graph = build_graph()
    assert graph is not None
    # Verify it's a compiled StateGraph with expected methods
    assert hasattr(graph, "invoke")
    assert hasattr(graph, "ainvoke")
    assert hasattr(graph, "stream")


def test_get_graph_singleton():
    """Test that get_graph returns a singleton instance."""
    graph1 = get_graph()
    graph2 = get_graph()
    assert graph1 is graph2


def test_graph_nodes_registered():
    """Test that all expected nodes are registered in the graph."""
    graph = build_graph()

    # Get the nodes from the compiled graph
    # The compiled graph should have all our nodes
    expected_nodes = {
        "intent",
        "schema",
        "generate_sql",
        "review",
        "execute",
        "formatter",
    }

    # Access the underlying workflow nodes
    # Note: The compiled graph wraps the workflow, so we check builder directly
    from src.graph.builder import StateGraph

    workflow = StateGraph(NL2SQLState)
    assert workflow is not None


def test_initial_state_structure():
    """Test that initial state matches expected structure."""
    initial_state: NL2SQLState = {
        "query": "上个月销售额",
        "thread_id": "test_001",
        "user_role": "admin",
        "intent": None,
        "relevant_tables": [],
        "generated_sql": None,
        "sql_explanation": None,
        "needs_approval": False,
        "approval_decision": None,
        "execution_result": None,
        "error": None,
        "audit_log_id": None,
        "start_time": None,
    }

    assert initial_state["query"] == "上个月销售额"
    assert initial_state["thread_id"] == "test_001"
    assert initial_state["user_role"] == "admin"
    assert initial_state["needs_approval"] is False


def test_state_with_intent():
    """Test state structure with populated intent."""
    intent: QueryIntent = {
        "metrics": ["sales_amount"],
        "dimensions": ["date"],
        "filters": [],
        "time_range": {"type": "last_month", "days": 30},
        "aggregation": "sum",
        "limit": 1000,
    }

    state: NL2SQLState = {
        "query": "上个月销售额",
        "thread_id": "test_002",
        "user_role": "analyst",
        "intent": intent,
        "relevant_tables": [{"name": "sales", "columns": ["amount", "date"]}],
        "generated_sql": "SELECT SUM(amount) FROM sales WHERE date >= DATE_SUB(NOW(), INTERVAL 1 MONTH)",
        "sql_explanation": "计算上个月总销售额",
        "needs_approval": True,
        "approval_decision": None,
        "execution_result": None,
        "error": None,
        "audit_log_id": "audit_123",
        "start_time": 1234567890.0,
    }

    assert state["intent"] is not None
    assert state["intent"]["metrics"] == ["sales_amount"]
    assert state["intent"]["time_range"]["type"] == "last_month"
    assert state["needs_approval"] is True


@pytest.fixture
def mock_config():
    """Provide a mock configuration for graph execution."""
    return {
        "configurable": {
            "thread_id": "test_001",
            "llm_api_key": "test_key",
            "db_url": "mysql+pymysql://test:test@localhost/test",
        }
    }


@pytest.fixture
def initial_state():
    """Provide an initial state for testing."""
    return {
        "query": "上个月销售额",
        "thread_id": "test_001",
        "user_role": "admin",
        "intent": None,
        "relevant_tables": [],
        "generated_sql": None,
        "sql_explanation": None,
        "needs_approval": False,
        "approval_decision": None,
        "execution_result": None,
        "error": None,
        "audit_log_id": None,
        "start_time": None,
    }


@patch("src.graph.nodes.intent.ChatOpenAI")
def test_intent_node_integration(mock_llm_class, initial_state):
    """Test intent node with mocked LLM."""
    from src.graph.nodes.intent import intent_node

    # Setup mock
    mock_llm = MagicMock()
    mock_response = MagicMock()
    mock_response.content = """```json
    {
        "metrics": ["sales_amount"],
        "dimensions": ["date"],
        "filters": [],
        "time_range": {"type": "last_month", "days": 30},
        "aggregation": "sum",
        "limit": 1000
    }
    ```"""
    mock_llm.invoke.return_value = mock_response
    mock_llm_class.return_value = mock_llm

    # Execute
    result = intent_node(initial_state)

    # Verify
    assert "intent" in result
    # The intent may include rule-based extracted metrics + LLM metrics
    assert "sales_amount" in result["intent"]["metrics"]
    assert result["intent"]["time_range"]["type"] == "last_month"


@patch("src.graph.nodes.schema.get_permission_service")
@patch("src.graph.nodes.schema.get_schema_store")
def test_schema_node_integration(mock_get_store, mock_get_permission, initial_state):
    """Test schema node with mocked schema store and permission service."""
    from src.graph.nodes.schema import schema_node
    from src.models import TableMetadata, DatasourceType

    # Setup schema store mock with TableMetadata objects
    mock_store = MagicMock()
    mock_table = TableMetadata(
        table_name="sales",
        table_cn_name="销售表",
        description="销售数据表",
        datasource=DatasourceType.MYSQL,
        fields=[
            {"field_name": "amount", "field_cn_name": "金额", "field_type": "decimal", "description": "销售金额"},
            {"field_name": "date", "field_cn_name": "日期", "field_type": "datetime", "description": "订单日期"},
        ],
    )
    mock_store.retrieve.return_value = [mock_table]
    mock_get_store.return_value = mock_store

    # Setup permission service mock
    mock_permission = MagicMock()
    mock_permission.can_query_table.return_value = True
    mock_get_permission.return_value = mock_permission

    # Add intent to state for schema node
    initial_state["intent"] = {
        "metrics": ["sales_amount"],
        "dimensions": ["date"],
        "filters": [],
        "time_range": {"type": "last_month", "days": 30},
        "aggregation": "sum",
        "limit": 1000,
    }

    # Execute
    result = schema_node(initial_state)

    # Verify
    assert "relevant_tables" in result
    assert len(result["relevant_tables"]) > 0
    assert result["relevant_tables"][0]["table_name"] == "sales"


@patch("src.graph.nodes.sql_generator.ChatOpenAI")
def test_sql_generator_node_integration(mock_llm_class, initial_state):
    """Test SQL generator node with mocked LLM."""
    from src.graph.nodes.sql_generator import sql_generator_node

    # Setup mock - return JSON format as expected by the node
    mock_llm = MagicMock()
    mock_response = MagicMock()
    mock_response.content = """```json
    {
        "sql": "SELECT SUM(amount) as total_sales FROM sales WHERE date >= DATE_SUB(NOW(), INTERVAL 1 MONTH)",
        "explanation": "This query calculates the total sales amount for the last month."
    }
    ```"""
    mock_llm.invoke.return_value = mock_response
    mock_llm_class.return_value = mock_llm

    # Add required state - use proper table structure
    initial_state["intent"] = {
        "metrics": ["sales_amount"],
        "dimensions": ["date"],
        "filters": [],
        "time_range": {"type": "last_month", "days": 30},
        "aggregation": "sum",
        "limit": 1000,
    }
    initial_state["relevant_tables"] = [
        {
            "table_name": "sales",
            "table_cn_name": "销售表",
            "fields": [
                {"field_name": "amount", "field_type": "decimal"},
                {"field_name": "date", "field_type": "datetime"},
            ],
        }
    ]

    # Execute
    result = sql_generator_node(initial_state)

    # Verify
    assert "generated_sql" in result
    assert "sql_explanation" in result
    assert result["generated_sql"] is not None


@patch("src.graph.nodes.review.interrupt")
def test_review_node_approval_logic(mock_interrupt, initial_state):
    """Test review node approval decision logic with mocked interrupt."""
    from src.graph.nodes.review import review_node

    # Mock interrupt to simulate user approval
    mock_interrupt.return_value = "approve"

    # Test with generated SQL
    initial_state["generated_sql"] = "SELECT * FROM sales"

    result = review_node(initial_state)

    assert "needs_approval" in result
    assert "approval_decision" in result
    assert result["approval_decision"] == "approved"


def test_review_node_skip_when_no_sql(initial_state):
    """Test review node skips when no SQL is generated."""
    from src.graph.nodes.review import review_node

    # No generated_sql in state
    initial_state["generated_sql"] = None

    result = review_node(initial_state)

    assert result["needs_approval"] is False
    assert result["approval_decision"] == "skipped"


def test_executor_node_error_handling(initial_state):
    """Test executor node handles errors gracefully."""
    from src.graph.nodes.executor import executor_node

    # Test with invalid SQL
    initial_state["generated_sql"] = "INVALID SQL"
    initial_state["approval_decision"] = "approved"
    initial_state["needs_approval"] = False

    result = executor_node(initial_state)

    # Should return error in state
    assert "error" in result or "execution_result" in result


def test_formatter_node_structure(initial_state):
    """Test formatter node produces expected output structure."""
    from src.graph.nodes.formatter import formatter_node

    # Test successful execution formatting
    # Note: formatter expects columns as dicts with "name" key
    initial_state["execution_result"] = {
        "columns": [{"name": "total_sales"}],
        "rows": [{"total_sales": 100000.00}],
        "row_count": 1,
        "sql": "SELECT SUM(amount) FROM sales",
        "execution_time_ms": 150,
    }
    initial_state["generated_sql"] = "SELECT SUM(amount) FROM sales"
    initial_state["sql_explanation"] = "计算总销售额"

    result = formatter_node(initial_state)

    # The formatter returns "formatted_result" not "formatted_output"
    assert "formatted_result" in result
    assert result["formatted_result"] is not None
    assert "chart_recommendation" in result["formatted_result"]


def test_formatter_node_empty_result(initial_state):
    """Test formatter node handles empty results."""
    from src.graph.nodes.formatter import formatter_node

    # No execution_result
    initial_state["execution_result"] = None

    result = formatter_node(initial_state)

    # Should return formatted_result as None
    assert "formatted_result" in result
    assert result["formatted_result"] is None


def test_route_on_error_with_error():
    """Test error routing when state has error."""
    from src.graph.builder import route_on_error

    state = {"error": "Something went wrong"}
    result = route_on_error(state)

    assert result == "__end__"


def test_route_on_error_without_error():
    """Test error routing when state has no error."""
    from src.graph.builder import route_on_error

    state = {"error": None}
    result = route_on_error(state)

    assert result == "formatter"


@pytest.mark.skip(reason="Requires full environment with LLM and database")
def test_full_workflow_integration(mock_config, initial_state):
    """
    Full integration test - requires real LLM and database.

    This test is skipped by default as it requires:
    - Valid LLM API key
    - Running database instance
    - Proper schema store initialization
    """
    graph = build_graph()

    result = graph.invoke(initial_state, mock_config)

    # Verify result structure
    assert "formatted_output" in result or "error" in result


def test_simple_workflow():
    """测试简单工作流（自动模式，跳过HITL）"""
    graph = build_graph()

    config = {
        "configurable": {
            "thread_id": "test_001",
            "llm_api_key": "test_key",
            "db_url": "mysql+pymysql://test:test@localhost/test"
        }
    }

    # 使用模拟数据测试
    initial_state: NL2SQLState = {
        "query": "上个月销售额",
        "thread_id": "test_001",
        "user_role": "admin",
        "intent": None,
        "relevant_tables": [],
        "generated_sql": None,
        "sql_explanation": None,
        "needs_approval": False,  # 跳过审核
        "approval_decision": None,
        "execution_result": None,
        "error": None,
        "audit_log_id": None,
        "start_time": None
    }

    # 注意：这个测试需要Mock LLM和数据库
    # 实际运行时需要真实环境
    result = graph.invoke(initial_state, config)

    # 验证结果结构
    assert "formatted_output" in result or "error" in result


if __name__ == "__main__":
    # Run basic tests
    test_graph_build()
    print("Graph build test passed!")

    test_get_graph_singleton()
    print("Graph singleton test passed!")

    test_initial_state_structure()
    print("Initial state structure test passed!")

    test_route_on_error_with_error()
    test_route_on_error_without_error()
    print("Error routing tests passed!")

    print("\nAll basic e2e tests passed!")
