"""Format SQL results using a single Gemini API call."""

import os
import json
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

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
    if not GEMINI_API_KEY or GEMINI_API_KEY == "your_key_here":
        # Fallback: format without Gemini
        return _fallback_format(question, rows, row_count, template_used)

    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-2.0-flash")

        # Truncate rows for the prompt to avoid token limits
        display_rows = rows[:50]
        rows_text = json.dumps(display_rows, indent=2, default=str)

        prompt = (
            f"User question: {question}\n\n"
            f"Query returned {row_count} result(s).\n"
            f"Results:\n{rows_text}\n\n"
            f"Please format this data as a helpful response to the user's question."
        )

        response = model.generate_content(
            [
                {"role": "user", "parts": [{"text": SYSTEM_PROMPT + "\n\n" + prompt}]}
            ]
        )

        return response.text.strip()

    except Exception as e:
        print(f"[WARN] Gemini API error: {e}")
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
    elif t == "TOP_ORDERS":
        top = rows[0]
        return f"Here are the top {row_count} highest-value orders. The biggest order is Sales Order **{linkify(top.get('salesOrder', ''))}** for **{top.get('totalNetAmount', '')}**."
    elif "COUNT" in t:
        return f"The total count for your query is **{rows[0].get('count', row_count)}**."
    elif "TOTAL_AMOUNT" in t:
        total = sum(float(r.get("total_amount", 0)) for r in rows if r.get("total_amount"))
        return f"The total aggregated amount is **{total:.2f}** across {row_count} specific groups/records."
    elif t == "BROKEN_FLOW":
        return f"I found **{row_count}** orders that appear to have broken flows or missing delivery/billing documents. Check the Data Tree to troubleshoot them."
    elif "SUMMARY" in t:
        return f"Here is the status summary you requested. There are {row_count} different status groupings found."
    
    # Generic fallback
    return f"I processed your query and found **{row_count}** matching record(s). Please check the **Data Tree** tab for the structured results and the **SQL View** to see how I fetched this data."
