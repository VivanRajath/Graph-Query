"""
Pure-Python intent parser for SAP O2C queries.

Extracts entities (sales order IDs, billing doc IDs, customer IDs, product IDs, etc.)
and classifies the user's intent into one of the predefined types
for downstream SQL template selection.
"""

import re
import difflib

# ── Entity extraction patterns (SAP IDs are typically 6-10 digits) ────

_SO_PATTERN = re.compile(r"(?:sales\s*order|order|so)\s*#?\s*(\d{5,10})", re.I)
_DELIVERY_PATTERN = re.compile(r"(?:delivery|deliver[y]?\s*doc(?:ument)?|del)\s*#?\s*(\d{7,10})", re.I)
_BILLING_PATTERN = re.compile(r"(?:billing\s*doc(?:ument)?|invoice|bill)\s*#?\s*(\d{7,10})", re.I)
_CUSTOMER_PATTERN = re.compile(r"(?:customer|sold\s*to\s*party|cust(?:omer)?)\s*#?\s*(\d{6,12})", re.I)
_PRODUCT_PATTERN = re.compile(r"(?:product|material|item)\s*#?\s*([A-Z0-9_-]{3,20})", re.I)
_ACCT_DOC_PATTERN = re.compile(r"(?:accounting\s*doc(?:ument)?|acct\s*doc)\s*#?\s*(\d{7,12})", re.I)
_GENERIC_ID = re.compile(r"\b(\d{6,10})\b")

# Domain vocabulary for fuzzy matching
VOCAB = [
    "trace", "track", "follow", "flow", "lifecycle", "journey", "end-to-end",
    "broken", "missing", "incomplete", "stuck", "blocked", "without",
    "detail", "info", "show", "get", "what",
    "order", "orders", "sales", "delivery", "deliveries", "shipping", "shipment",
    "billing", "invoice", "invoices", "bill", "payment", "paid", "receivable",
    "clearing", "cleared", "customer", "customers", "sold to", "party", "partner",
    "product", "material", "status", "state", "where", "pending", "complete",
    "cancelled", "cancellation", "amount", "outstanding", "top", "highest",
    "largest", "biggest", "most", "revenue", "how many", "count", "sum",
    "total", "average", "avg", "summary", "overview", "breakdown", "report", "all", "list"
]

def _fuzzy_fix_query(query: str) -> str:
    words = query.split()
    fixed_words = []
    for w in words:
        if w.isdigit() or len(w) < 4:
            fixed_words.append(w)
            continue
        clean_w = re.sub(r'[^\w]', '', w.lower())
        if not clean_w:
            fixed_words.append(w)
            continue
        if clean_w in VOCAB:
            fixed_words.append(w)
        else:
            matches = difflib.get_close_matches(clean_w, VOCAB, n=1, cutoff=0.75)
            if matches:
                 fixed_words.append(matches[0]) # Use corrected version
            else:
                 fixed_words.append(w)
    return " ".join(fixed_words)


# ── Granular Intent classification ─────────────────────────────────────
# Order matters: more specific intents must be checked first!

INTENT_PATTERNS = [
    # Top/aggregate analysis (Granular)
    ("COUNT_DELIVERIES", [re.compile(r"(how\s*many|count|total)\b.*\b(delivery|deliveries)", re.I)]),
    ("COUNT_BILLING", [re.compile(r"(how\s*many|count|total)\b.*\b(billing|invoice|bills?)", re.I)]),
    ("COUNT_PAYMENT", [re.compile(r"(how\s*many|count|total)\b.*\b(payment|paid|cleared)", re.I)]),
    ("COUNT_CUSTOMER", [re.compile(r"(how\s*many|count|total)\b.*\b(customers?|partners?)", re.I)]),
    ("COUNT_PRODUCT", [re.compile(r"(how\s*many|count|total)\b.*\b(products?|materials?|items?)", re.I)]),
    ("COUNT_ORDERS", [re.compile(r"(how\s*many|count|total|all)\b.*\b(order|orders?|sales)", re.I)]),
    
    ("TOTAL_AMOUNT", [
        re.compile(r"(total|sum)\b.*\b(amount|revenue|sales|budget|value|worth)", re.I),
        re.compile(r"(amount|revenue|sales|budget|value|worth)\b.*\b(total|sum)", re.I)
    ]),
    ("TOP_CUSTOMERS", [re.compile(r"(top|highest|best|most)\b.*\b(customers?|partners?)", re.I)]),
    ("TOP_ORDERS", [re.compile(r"(top|highest|biggest|largest)\b.*\b(orders?|sales)", re.I)]),
    ("TOP_PRODUCTS_BY_BILLING", [
        re.compile(r"(top|highest|most).*(product|material|item).*(billing|invoice|bill)", re.I),
        re.compile(r"(product|material|item).*(highest|most).*(billing|invoice|bill)", re.I)
    ]),
    
    # Status Overview / Summary (Granular)
    ("DELIVERY_STATUS_SUMMARY", [
        re.compile(r"(summary|overview|breakdown|report)\b.*\b(delivery|shipping)", re.I),
        re.compile(r"(delivery|shipping)\b.*\b(summary|overview|breakdown|report)", re.I),
    ]),
    ("BILLING_SUMMARY", [
        re.compile(r"(summary|overview|breakdown|report)\b.*\b(billing|invoice|bills?)", re.I),
        re.compile(r"(billing|invoice|bills?)\b.*\b(summary|overview|breakdown|report)", re.I),
    ]),
    ("ORDER_STATUS_SUMMARY", [
        re.compile(r"(summary|overview|breakdown|report)\b.*\b(order|status)", re.I),
        re.compile(r"(order|status)\b.*\b(summary|overview|breakdown|report)", re.I),
    ]),

    # Specific operational queries
    ("TRACE_FLOW", [
        re.compile(r"(trace|track|follow|flow|lifecycle|end.?to.?end|journey)\b.*\b(order|sales)", re.I),
        re.compile(r"(order|sales)\b.*\b(trace|track|flow|lifecycle|journey)", re.I),
    ]),
    ("BROKEN_FLOW", [
        re.compile(r"(broken|missing|incomplete|stuck|blocked|no\s+delivery|no\s+billing|no\s+invoice)", re.I),
        re.compile(r"(order|delivery)\b.*\b(without|missing|no)\b.*\b(delivery|billing|invoice|payment|billed)", re.I),
    ]),
    ("DELIVERY_STATUS", [
        re.compile(r"(delivery|shipping|shipment)\b.*\b(status|state|where|track)", re.I),
        re.compile(r"(status|state)\b.*\b(delivery|shipping)", re.I),
        re.compile(r"(goods\s*movement|picking)\b.*\b(status|complete|pending)", re.I),
    ]),
    ("BILLING_INFO", [
        re.compile(r"(billing|invoice|bill)\b.*\b(detail|info|amount|status|show|cancelled)", re.I),
        re.compile(r"(cancelled|cancellation)\b.*\b(billing|invoice|bill)", re.I),
    ]),
    ("PAYMENT_INFO", [
        re.compile(r"(payment|paid|receivable|clearing|cleared)\b.*\b(status|detail|amount|info|customer)", re.I),
        re.compile(r"(customer)\b.*\b(payment|paid|outstanding|receivable)", re.I),
    ]),
    ("ORDER_DETAIL", [
        re.compile(r"(detail|info|show|get|what)\b.*\b(order|sales\s*order)", re.I),
        re.compile(r"order\s*#?\s*\d{5,}", re.I),
    ]),
    ("CUSTOMER_INFO", [
        re.compile(r"(customer|sold\s*to\s*party|business\s*partner)\b.*\b(detail|info|orders|list|show)", re.I),
        re.compile(r"(orders|deliveries|invoices)\b.*\b(customer|sold\s*to\s*party)", re.I),
    ]),
    ("PRODUCT_INFO", [
        re.compile(r"(product|material)\b.*\b(detail|info|description|list|show)", re.I),
    ]),
]


def extract_entities(query: str) -> dict:
    """Extract SAP entity IDs from the query."""
    entities = {}
    so_match = _SO_PATTERN.search(query)
    if so_match: entities["sales_order"] = so_match.group(1)
    del_match = _DELIVERY_PATTERN.search(query)
    if del_match: entities["delivery"] = del_match.group(1)
    bill_match = _BILLING_PATTERN.search(query)
    if bill_match: entities["billing_doc"] = bill_match.group(1)
    cust_match = _CUSTOMER_PATTERN.search(query)
    if cust_match: entities["customer"] = cust_match.group(1)
    prod_match = _PRODUCT_PATTERN.search(query)
    if prod_match: entities["product"] = prod_match.group(1)
    acct_match = _ACCT_DOC_PATTERN.search(query)
    if acct_match: entities["acct_doc"] = acct_match.group(1)
    
    if not entities:
        gen = _GENERIC_ID.search(query)
        if gen: entities["generic_id"] = gen.group(1)
        
    return entities


def parse_intent(query: str) -> dict:
    """Parse intent, returning granular types with confidence score."""
    entities = extract_entities(query)
    
    # Fuzzy fix typos
    fixed_query = _fuzzy_fix_query(query)
    confidence = 1.0 if fixed_query == query else 0.85
    
    print(f"[Intent] Original: '{query}' -> Fixed: '{fixed_query}'")
    
    # Try each intent pattern
    for intent_name, patterns in INTENT_PATTERNS:
        for pat in patterns:
            if pat.search(fixed_query):
                print(f"[Intent] Matched {intent_name} (conf: {confidence})")
                return {
                    "intent": intent_name,
                    "entities": entities,
                    "original_query": query,
                    "fixed_query": fixed_query,
                    "confidence": confidence
                }

    print("[Intent] Fallback to GENERAL_QUERY")
    return {
        "intent": "GENERAL_QUERY",
        "entities": entities,
        "original_query": query,
        "fixed_query": fixed_query,
        "confidence": 0.5
    }
