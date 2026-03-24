"""
Regex / keyword-based classifier – blocks off-topic queries.

Returns:
    (True,  None)      → on-topic: proceed to intent parser
    (False, reason)    → off-topic: return ``reason`` to user
"""

import re

# ── SAP O2C domain keywords ───────────────────────────────────────────
_DOMAIN_KEYWORDS = {
    # Core SAP entities
    "order", "orders", "sales", "sales order", "purchase",
    "delivery", "deliveries", "outbound", "inbound", "shipping",
    "billing", "invoice", "invoices", "bill", "bills",
    "payment", "payments", "receivable", "receivables",
    "customer", "customers", "partner", "partners", "business partner",
    "product", "products", "material", "materials",
    "plant", "plants", "storage", "warehouse",
    # SAP specific
    "sap", "o2c", "order to cash", "order-to-cash",
    "sold to party", "soldtoparty", "distribution channel",
    "company code", "fiscal year", "accounting document",
    "goods movement", "picking", "proof of delivery",
    "credit", "debit", "clearing", "journal entry",
    "billing document", "cancellation", "cancelled",
    # Statuses & actions
    "status", "pending", "completed", "blocked", "overdue",
    "delayed", "shipped", "delivered", "cancelled", "open",
    "amount", "total", "net", "gross", "revenue", "quantity",
    # Analytics
    "top", "highest", "lowest", "average", "count", "how many",
    "total amount", "summary", "breakdown", "trend", "volume",
    "trace", "flow", "track", "broken", "missing", "delayed",
}

# ── Off-topic patterns ────────────────────────────────────────────────
_OFFTOPIC_PATTERNS = [
    re.compile(r"\b(who is|what is|tell me about|explain)\b.*\b(president|capital|country|planet|earth|sun|moon)\b", re.I),
    re.compile(r"\b(write|generate|create)\b.*\b(code|program|script|function|class)\b", re.I),
    re.compile(r"\b(recipe|weather|joke|poem|song|story|translate)\b", re.I),
    re.compile(r"\b(python|javascript|java|c\+\+|html|css|sql)\b.*\b(program|code|script)\b", re.I),
]


def classify(query: str) -> tuple[bool, str | None]:
    """
    Return ``(True, None)``  if the query is related to SAP O2C data,
    or ``(False, reason_string)`` if it is clearly off-topic.
    """
    q_lower = query.lower().strip()

    # 1. Empty / too short
    if len(q_lower) < 3:
        return False, "Please ask a question about your SAP Order-to-Cash data (orders, deliveries, billing, payments, products, etc.)."

    # 2. Explicit off-topic patterns
    for pat in _OFFTOPIC_PATTERNS:
        if pat.search(q_lower):
            return False, "I can only answer questions about your SAP O2C data – sales orders, deliveries, invoices, payments, products, and related topics."

    # 3. Domain keyword match (at least one must appear)
    for kw in _DOMAIN_KEYWORDS:
        if kw in q_lower:
            return True, None

    # 4. Check for numbers that look like SAP IDs (6-10 digit numbers)
    if re.search(r"\b\d{6,10}\b", q_lower):
        return True, None

    # 5. Default: allow it through (better to answer than block)
    return True, None
