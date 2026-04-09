"""Clarification node for handling ambiguous queries."""

from typing import Any, Dict, List

from langgraph.types import interrupt

from ..state import NL2SQLState
from .ambiguity_detector import detect_ambiguities, update_intent_with_clarifications


def clarification_node(state: NL2SQLState) -> Dict[str, Any]:
    """
    Multi-round clarification node using interrupt().

    This node handles ambiguous queries by:
    1. Detecting ambiguities in the parsed intent
    2. If ambiguities exist: interrupt and ask clarifying questions
    3. If no ambiguities or max rounds reached: proceed with current intent

    Args:
        state: Current graph state with intent and clarification_history

    Returns:
        State updates with clarification results and possibly updated intent
    """
    query = state.get("query", "")
    intent = state.get("intent", {})
    history = state.get("clarification_history", [])
    current_round = state.get("current_clarification_round", 0)
    max_rounds = state.get("max_clarification_rounds", 3)

    # Detect ambiguities
    ambiguities = detect_ambiguities(query, intent, history)

    # If no ambiguities or max rounds reached, proceed
    if not ambiguities or current_round >= max_rounds:
        # Update intent with clarifications if history exists
        updated_intent = intent
        if history:
            updated_intent = update_intent_with_clarifications(intent, history)

        return {
            "clarification_needed": False,
            "clarification_questions": [],
            "intent": updated_intent,
        }

    # Need clarification - build questions list
    questions = []
    for amb in ambiguities[:2]:  # Max 2 questions per round
        questions.append({
            "type": amb["type"],
            "field": amb["field"],
            "question": amb["question"],
            "options": amb.get("options", []),
            "context": amb.get("context", ""),
        })

    # Use interrupt to pause execution and wait for user response
    user_response = interrupt({
        "action": "clarification",
        "query": query,
        "current_intent": intent,
        "questions": questions,
        "round": current_round + 1,
        "max_rounds": max_rounds,
        "message": f"需要澄清（第 {current_round + 1}/{max_rounds} 轮）",
    })

    # Parse user response
    # Expected format: {"answers": [{"field": "metric", "answer": "销售额"}, ...]}
    answers = user_response.get("answers", []) if isinstance(user_response, dict) else []

    # Update clarification history
    new_rounds = []
    for ans in answers:
        new_rounds.append({
            "question": next(
                (q["question"] for q in questions if q["field"] == ans.get("field")), ""
            ),
            "answer": ans.get("answer", ""),
            "field": ans.get("field", ""),
        })

    updated_history = history + new_rounds

    # Update intent with clarifications
    updated_intent = update_intent_with_clarifications(intent, updated_history)

    # Check if more ambiguities need clarification
    remaining_ambiguities = detect_ambiguities(query, updated_intent, updated_history)
    still_needs_clarification = bool(remaining_ambiguities) and (current_round + 1) < max_rounds

    return {
        "clarification_needed": still_needs_clarification,
        "clarification_questions": questions,
        "clarification_responses": state.get("clarification_responses", []) + [user_response],
        "clarification_history": updated_history,
        "current_clarification_round": current_round + 1,
        "intent": updated_intent,
    }
