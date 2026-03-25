"""Format SQL results using a single Gemini API call via REST (no gRPC)."""

import os
import json
import urllib.request
import urllib.error
from dotenv import load_dotenv

load_dotenv(override=True)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()

# ── Vercel diagnostic: confirm env var is present in function logs ─────
print("[Env] GEMINI_API_KEY:", "SET" if GEMINI_API_KEY else "MISSING")
print("[Env] GROQ_API_KEY:", "SET" if GROQ_API_KEY else "MISSING")
print("[Env] OPENAI_API_KEY:", "SET" if OPENAI_API_KEY else "MISSING")
print("[Env] ANTHROPIC_API_KEY:", "SET" if ANTHROPIC_API_KEY else "MISSING")
# ──────────────────────────────────────────────────────────────────────

GEMINI_REST_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
)

SYSTEM_PROMPT = (
    "You are a business data assistant. Your job is to provide a brief, conversational, and human-friendly "
    "summary of the data. Do NOT output large tables or raw lists of rows—the raw data will be shown to the "
    "user in a separate 'Data Tree' tab. Just give a helpful 1-3 sentence summary of what was found. "
    "Do not answer questions outside of orders, deliveries, invoices and payments. "
    "If the result is empty, say that no matching records were found.\n"
    "CRITICAL: Whenever you mention a specific business ID (such as a Sales Order number, "
    "Delivery Document, Billing Document, Customer ID, or Product ID), you MUST format it "
    "as a special markdown link using the scheme `id:`, like this: `[1000214](id:1000214)`. "
    "The frontend uses this to make the ID clickable so the user can zoom to the node in the graph."
)


def _call_gemini_rest(prompt: str, timeout: float = 10.0) -> str | None:
    """
    Call Gemini via REST API with the given prompt.
    Returns the response text, or None on any failure.
    """
    if not GEMINI_API_KEY or GEMINI_API_KEY == "your_key_here":
        return None

    url = f"{GEMINI_REST_URL}?key={GEMINI_API_KEY}"

    payload = {
        "contents": [
            {
                "parts": [{"text": SYSTEM_PROMPT + "\n\n" + prompt}]
            }
        ],
        "generationConfig": {
            "maxOutputTokens": 512,
            "temperature": 0.3,
        },
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            # Extract text from the first candidate
            candidates = body.get("candidates", [])
            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                if parts:
                    return parts[0].get("text", "").strip()
        return None
    except urllib.error.HTTPError as e:
        print(f"[WARN] Gemini REST API HTTP error: {e.code} {e.reason}")
        if e.code == 429:
            return "__RATE_LIMIT__"
        return None
    except Exception as e:
        print(f"[WARN] Gemini REST API error: {type(e).__name__}: {e}")
        return None


def _call_groq_rest(prompt: str, timeout: float = 10.0) -> str | None:
    if not GROQ_API_KEY or "your_" in GROQ_API_KEY:
        return None
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 512
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
        },
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body.get("choices", [])[0].get("message", {}).get("content", "").strip() or None
    except urllib.error.HTTPError as e:
        print(f"[WARN] Groq API HTTP error: {e.code} {e.reason}")
        if e.code == 429: return "__RATE_LIMIT__"
        if e.code == 403: print("[WARN] Groq key may be invalid or key lacks permission. Check console.groq.com")
        return None
    except Exception as e:
        print(f"[WARN] Groq API error: {e}")
        return None


def _call_openai_rest(prompt: str, timeout: float = 10.0) -> str | None:
    if not OPENAI_API_KEY or "your_" in OPENAI_API_KEY:
        return None
    url = "https://api.openai.com/v1/chat/completions"
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 512
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json", "Authorization": f"Bearer {OPENAI_API_KEY}"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body.get("choices", [])[0].get("message", {}).get("content", "").strip() or None
    except urllib.error.HTTPError as e:
        print(f"[WARN] OpenAI API HTTP error: {e.code} {e.reason}")
        if e.code == 429: return "__RATE_LIMIT__"
        return None
    except Exception as e:
        print(f"[WARN] OpenAI API error: {e}")
        return None


def _call_anthropic_rest(prompt: str, timeout: float = 10.0) -> str | None:
    if not ANTHROPIC_API_KEY or "your_" in ANTHROPIC_API_KEY:
        return None
    url = "https://api.anthropic.com/v1/messages"
    payload = {
        "model": "claude-3-haiku-20240307",
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": 512
    }
    headers = {"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            content = body.get("content", [])
            if content and content[0].get("type") == "text":
                return content[0].get("text").strip()
        return None
    except urllib.error.HTTPError as e:
        print(f"[WARN] Anthropic API HTTP error: {e.code} {e.reason}")
        if e.code == 429: return "__RATE_LIMIT__"
        return None
    except Exception as e:
        print(f"[WARN] Anthropic API error: {e}")
        return None

def _run_llm_fallback_chain(prompt: str, timeout: float = 10.0) -> tuple[str | None, str | None]:
    """Tries configured LLMs in sequence. Returns (result_text, used_model_name)."""
    providers = [
        ("Gemini", _call_gemini_rest),
        ("Groq", _call_groq_rest),
        ("OpenAI", _call_openai_rest),
        ("Claude", _call_anthropic_rest),
    ]
    rate_limited = False
    
    for name, func in providers:
        res = func(prompt, timeout)
        if res:
            if res == "__RATE_LIMIT__":
                rate_limited = True
                continue
            return res, name
            
    if rate_limited:
        return "__RATE_LIMIT__", "Fallback"
    return None, None


def format_results(question: str, rows: list[dict], row_count: int, template_used: str = "") -> str:
    """
    Format query results into natural language using Gemini.

    Args:
        question: The original user question
        rows: List of result row dicts
        row_count: Total number of rows
        template_used: Which SQL template was used (for context)

    Returns:
        Formatted natural language string
    """
    # Truncate rows for the prompt to avoid token limits
    display_rows = rows[:20]
    rows_text = json.dumps(display_rows, indent=2, default=str)

    prompt = (
        f"User question: {question}\n\n"
        f"Query returned {row_count} result(s).\n"
        f"Results:\n{rows_text}\n\n"
        f"Please format this data as a helpful response to the user's question."
    )

    result, provider = _run_llm_fallback_chain(prompt, timeout=10.0)
    
    if result:
        if result == "__RATE_LIMIT__":
            base_fallback = _fallback_format(question, rows, row_count, template_used)
            return base_fallback + "\n\n*(Note: AI summarization is currently unavailable across all configured providers due to API rate limits. You are seeing the structured fallback response.)*"
        return result + f"\n\n*(Summarized by {provider})*"

    # Fallback: format without Gemini
    return _fallback_format(question, rows, row_count, template_used)


def _fallback_format(question: str, rows: list[dict], row_count: int, template_used: str = "") -> str:
    """Format results without Gemini (plain text fallback)."""
    if not rows:
        return f"No matching records found for your query: \"{question}\""

    # Helper to linkify IDs
    def linkify(val):
        s = str(val)
        return f"[{s}](id:{s})" if s.isdigit() else s

    t = template_used.upper()

    # Generate a smart summary based on the template
    if t == "CUSTOMER_ORDERS":
        return f"I found **{row_count}** orders for this customer. You can view the details of these orders in the Data Tree tab."
    elif t == "ORDER_DETAIL" and len(rows) > 0:
        so = rows[0].get("salesOrder", rows[0].get("id", ""))
        return f"Here are the details for Sales Order **{linkify(so)}**. It has a total net amount of **{rows[0].get('totalNetAmount', 'unknown')}**."
    elif t == "DELIVERY_STATUS" and len(rows) == 1:
        del_doc = rows[0].get("deliveryDocument", "")
        status = rows[0].get("overallGoodsMovementStatus", "")
        return f"Delivery Document **{linkify(del_doc)}** currently has a goods movement status of '{status}'."
    elif t == "TOP_CUSTOMERS":
        top = rows[0]
        cust = top.get('customer', top.get('soldToParty', 'unknown'))
        return f"Based on the data, I found the top {row_count} customers. Your #1 customer is **{linkify(cust)}** with a total amount of **{top.get('total_amount', top.get('totalNetAmount', ''))}** across {top.get('order_count', 'several')} orders."
    elif t == "TOP_PRODUCTS_BY_BILLING":
        top = rows[0]
        return f"Based on the billing data, product **{linkify(top.get('product', ''))}** has the highest volume with **{top.get('billing_count', '')}** separate billing documents."
    elif t == "TOP_ORDERS":
        top = rows[0]
        return f"Here are the top {row_count} highest-value orders. The biggest order is Sales Order **{linkify(top.get('salesOrder', ''))}** for **{top.get('totalNetAmount', '')}**."
    elif t == "COUNT_ORDERS":
        return f"You have a total of **{rows[0].get('count', row_count)}** orders."
    elif t == "COUNT_DELIVERIES":
        return f"There are **{rows[0].get('count', row_count)}** delivery documents recorded."
    elif t == "COUNT_BILLING":
        return f"There are **{rows[0].get('count', row_count)}** billing records."
    elif t == "COUNT_CUSTOMER":
        return f"You currently have **{rows[0].get('count', row_count)}** unique customers."
    elif "COUNT" in t:
        count_val = rows[0].get('count', row_count)
        return f"There are **{count_val}** total records that match your request."
    elif "TOTAL_AMOUNT" in t:
        total = sum(float(r.get("total_amount", 0)) for r in rows if r.get("total_amount"))
        q_low = question.lower()
        if "budget" in q_low or "value" in q_low:
            return f"The total budget/value is **${total:,.2f}**."
        return f"The total order revenue is **${total:,.2f}**."
    elif t == "BROKEN_FLOW":
        return f"I found **{row_count}** orders that appear to have incomplete flows or missing delivery/billing documents. Check the Data Tree to troubleshoot them."
    elif "SUMMARY" in t:
        return f"Here is the status summary you requested, broken down into {row_count} different categories."
    elif t == "RECENT_ORDERS":
        return f"Here are the **{row_count}** most recent orders in the system. You can view their details in the Data Tree tab."
    elif t.startswith("LOOKUP_"):
        return f"I found the record you were looking for! Check the Data Tree for full details."
    
    # Generic fallback
    return f"I found **{row_count}** items. They are available in the Data Tree tab for you to review."
