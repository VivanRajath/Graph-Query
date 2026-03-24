"""
Pure-Python intent parser for SAP O2C queries.

Extracts entities (sales order IDs, billing doc IDs, customer IDs, product IDs, etc.)
and classifies the user's intent into one of the predefined types
for downstream SQL template selection.
"""

import re

# ── Entity extraction patterns (SAP IDs are typically 6-10 digits) ────

# Sales Order IDs: "740506", "order 740506"
_SO_PATTERN = re.compile(
    r"(?:sales\s*order|order|so)\s*#?\s*(\d{5,10})", re.I
)

# Delivery document IDs: "80737721", "delivery 80737721"
_DELIVERY_PATTERN = re.compile(
    r"(?:delivery|deliver[y]?\s*doc(?:ument)?|del)\s*#?\s*(\d{7,10})", re.I
)

# Billing document IDs: "90504248", "invoice 90504248"
_BILLING_PATTERN = re.compile(
    r"(?:billing\s*doc(?:ument)?|invoice|bill)\s*#?\s*(\d{7,10})", re.I
)

# Customer IDs: "320000083", "customer 320000083"
_CUSTOMER_PATTERN = re.compile(
    r"(?:customer|sold\s*to\s*party|cust(?:omer)?)\s*#?\s*(\d{6,12})", re.I
)

# Product IDs
_PRODUCT_PATTERN = re.compile(
    r"(?:product|material|item)\s*#?\s*([A-Z0-9_-]{3,20})", re.I
)

# Accounting document IDs
_ACCT_DOC_PATTERN = re.compile(
    r"(?:accounting\s*doc(?:ument)?|acct\s*doc)\s*#?\s*(\d{7,12})", re.I
)

# Generic large number (fallback for any SAP ID)
_GENERIC_ID = re.compile(r"\b(\d{6,10})\b")

# ── Intent classification ─────────────────────────────────────────────

INTENT_PATTERNS = [
    # Trace full O2C flow for an order
    ("TRACE_FLOW", [
        re.compile(r"(trace|track|follow|flow|lifecycle|end.to.end)\b.*\b(order|sales)", re.I),
        re.compile(r"(order|sales)\b.*\b(trace|track|flow|lifecycle|journey)", re.I),
    ]),
    # Broken/missing flow detection
    ("BROKEN_FLOW", [
        re.compile(r"(broken|missing|incomplete|stuck|blocked|no\s+delivery|no\s+billing|no\s+invoice)", re.I),
        re.compile(r"(order|delivery)\b.*\b(without|missing|no)\b.*\b(delivery|billing|invoice|payment)", re.I),
    ]),
    # Order details
    ("ORDER_DETAIL", [
        re.compile(r"(detail|info|show|get|what)\b.*\b(order|sales\s*order)", re.I),
        re.compile(r"order\s*#?\s*\d{5,}", re.I),
    ]),
    # Delivery status
    ("DELIVERY_STATUS", [
        re.compile(r"(delivery|shipping|shipment)\b.*\b(status|state|where|track)", re.I),
        re.compile(r"(status|state)\b.*\b(delivery|shipping)", re.I),
        re.compile(r"(goods\s*movement|picking)\b.*\b(status|complete|pending)", re.I),
    ]),
    # Billing/invoice queries
    ("BILLING_INFO", [
        re.compile(r"(billing|invoice|bill)\b.*\b(detail|info|amount|status|show|cancelled)", re.I),
        re.compile(r"(cancelled|cancellation)\b.*\b(billing|invoice|bill)", re.I),
    ]),
    # Payment queries
    ("PAYMENT_INFO", [
        re.compile(r"(payment|paid|receivable|clearing|cleared)\b.*\b(status|detail|amount|info|customer)", re.I),
        re.compile(r"(customer)\b.*\b(payment|paid|outstanding|receivable)", re.I),
    ]),
    # Customer queries
    ("CUSTOMER_INFO", [
        re.compile(r"(customer|sold\s*to\s*party|business\s*partner)\b.*\b(detail|info|orders|list|show)", re.I),
        re.compile(r"(orders|deliveries|invoices)\b.*\b(customer|sold\s*to\s*party)", re.I),
    ]),
    # Product queries
    ("PRODUCT_INFO", [
        re.compile(r"(product|material)\b.*\b(detail|info|description|list|show)", re.I),
    ]),
    # Top/aggregate analysis
    ("TOP_ANALYSIS", [
        re.compile(r"(top|highest|largest|biggest|most)\b.*\b(order|customer|product|amount|revenue|invoice)", re.I),
        re.compile(r"(total|how\s*many|count|sum|average|avg)\b.*\b(order|delivery|invoice|billing|payment|customer|product)", re.I),
    ]),
    # Status overview / summary
    ("STATUS_OVERVIEW", [
        re.compile(r"(summary|overview|breakdown|report)\b.*\b(order|delivery|billing|payment|status)", re.I),
        re.compile(r"(order|delivery|billing)\b.*\b(summary|overview|breakdown)", re.I),
        re.compile(r"(all|list)\b.*\b(order|delivery|billing|payment|customer|product)", re.I),
    ]),
]


def extract_entities(query: str) -> dict:
    """
    Extract SAP entity IDs from the query.
    Returns dict with keys: sales_order, delivery, billing_doc, customer, product, acct_doc.
    """
    entities = {}

    so_match = _SO_PATTERN.search(query)
    if so_match:
        entities["sales_order"] = so_match.group(1)

    del_match = _DELIVERY_PATTERN.search(query)
    if del_match:
        entities["delivery"] = del_match.group(1)

    bill_match = _BILLING_PATTERN.search(query)
    if bill_match:
        entities["billing_doc"] = bill_match.group(1)

    cust_match = _CUSTOMER_PATTERN.search(query)
    if cust_match:
        entities["customer"] = cust_match.group(1)

    prod_match = _PRODUCT_PATTERN.search(query)
    if prod_match:
        entities["product"] = prod_match.group(1)

    acct_match = _ACCT_DOC_PATTERN.search(query)
    if acct_match:
        entities["acct_doc"] = acct_match.group(1)

    # Fallback: if no entity found but there's a large number, guess it's a sales order
    if not entities:
        gen = _GENERIC_ID.search(query)
        if gen:
            entities["generic_id"] = gen.group(1)

    return entities


def parse_intent(query: str) -> dict:
    """
    Parse a user query and return:
        {
            "intent": "TRACE_FLOW" | "ORDER_DETAIL" | ... | "GENERAL_QUERY",
            "entities": { ... extracted IDs ... },
            "original_query": "..."
        }
    """
    entities = extract_entities(query)

    # Try each intent pattern
    for intent_name, patterns in INTENT_PATTERNS:
        for pat in patterns:
            if pat.search(query):
                return {
                    "intent": intent_name,
                    "entities": entities,
                    "original_query": query,
                }

    # Default fallback
    return {
        "intent": "GENERAL_QUERY",
        "entities": entities,
        "original_query": query,
    }
