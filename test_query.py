import json
from backend.pipeline.classifier import classify
from backend.pipeline.intent_parser import parse_intent
from backend.pipeline.query_router import route_query
from backend.pipeline.formatter import format_results

def test_pipeline(question: str):
    print(f"\nQ: {question}")
    
    # 1. Classify
    is_allowed, reason = classify(question)
    if not is_allowed:
        print(f"-> Blocked: {reason}")
        return
        
    print("-> Allowed")
    
    # 2. Parse Intent
    intent = parse_intent(question)
    print(f"-> Intent: {intent['intent']}, Entities: {intent.get('entities')}")
    
    # 3. Route & Execute
    result = route_query(intent)
    print(f"-> SQL Template: {result.get('template')}, Rows Found: {result.get('row_count')}")
    
    # 4. Format
    formatted = format_results(
        question=question,
        rows=result.get("rows", []),
        row_count=result.get("row_count", 0),
        template_used=result.get("template")
    )
    
    print("\n--- FORMATTED RESPONSE ---")
    print(formatted)
    print("--------------------------\n")

if __name__ == "__main__":
    queries = [
        "What are my top customers by revenue?",
        "Trace the flow for order 483",
        "Show me all broken orders missing delivery statuses",
        "Write a python script",
    ]
    for q in queries:
        test_pipeline(q)
