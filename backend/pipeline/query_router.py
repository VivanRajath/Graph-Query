"""
Query router – maps parsed intents to parameterized SQL templates
and executes them against the SQLite database.

Uses the actual SAP O2C table/column names from the JSONL data.
"""

import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT / "backend" / "data.db"


def _get_conn():
    uri = f"file:{DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _execute(sql: str, params: tuple = ()) -> list[dict]:
    """Execute SQL and return list of dicts."""
    conn = _get_conn()
    try:
        cur = conn.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        conn.close()


def _get_tables() -> list[str]:
    conn = _get_conn()
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    conn.close()
    return tables


# ── SQL Templates per Intent ───────────────────────────────────────────

def route_query(parsed: dict) -> dict:
    """
    Given a parsed intent dict, execute the appropriate SQL and return:
        {
            "intent": str,
            "template": str,
            "rows": list[dict],
            "row_count": int,
            "sql": str
        }
    """
    intent = parsed["intent"]
    entities = parsed["entities"]
    tables = _get_tables()

    # ── TRACE_FLOW: Full O2C trace for a sales order ──────────────
    if intent == "TRACE_FLOW":
        so_id = entities.get("sales_order") or entities.get("generic_id")
        if so_id:
            sql = """
                SELECT 'Sales Order' as step, salesOrder as id, totalNetAmount as amount,
                       overallDeliveryStatus as status, creationDate, soldToParty as customer
                FROM sales_order_headers WHERE salesOrder = ?
            """
            rows = _execute(sql, (so_id,))

            # Get delivery links via items
            if "sales_order_items" in tables and "outbound_delivery_items" in tables:
                del_sql = """
                    SELECT DISTINCT odi.deliveryDocument
                    FROM sales_order_items soi
                    JOIN outbound_delivery_items odi ON soi.salesOrder = odi.referenceSDDocument
                    WHERE soi.salesOrder = ?
                """
                del_rows = _execute(del_sql, (so_id,))
                for dr in del_rows:
                    dd = dr["deliveryDocument"]
                    hdr = _execute("SELECT * FROM outbound_delivery_headers WHERE deliveryDocument = ?", (dd,))
                    if hdr:
                        rows.append({"step": "Delivery", "id": dd, "status": hdr[0].get("overallGoodsMovementStatus"), **hdr[0]})

            # Get billing links via items
            if "billing_document_items" in tables:
                bill_sql = """
                    SELECT DISTINCT billingDocument FROM billing_document_items WHERE salesDocument = ?
                """
                bill_rows = _execute(bill_sql, (so_id,))
                for br in bill_rows:
                    bd = br["billingDocument"]
                    hdr = _execute("SELECT * FROM billing_document_headers WHERE billingDocument = ?", (bd,))
                    if hdr:
                        rows.append({"step": "Billing", "id": bd, "amount": hdr[0].get("totalNetAmount"), **hdr[0]})

            return {"intent": intent, "template": "TRACE_FLOW", "rows": rows, "row_count": len(rows), "sql": "trace_flow"}

        # No ID provided - show sample orders
        rows = _execute("SELECT salesOrder, soldToParty, totalNetAmount, overallDeliveryStatus, creationDate FROM sales_order_headers LIMIT 10")
        return {"intent": intent, "template": "TRACE_FLOW_SAMPLE", "rows": rows, "row_count": len(rows), "sql": "sample"}

    # ── BROKEN_FLOW: Orders without deliveries or billing ─────────
    if intent == "BROKEN_FLOW":
        if "outbound_delivery_items" in tables:
            sql = """
                SELECT soh.salesOrder, soh.soldToParty, soh.totalNetAmount, soh.creationDate,
                       soh.overallDeliveryStatus
                FROM sales_order_headers soh
                LEFT JOIN (
                    SELECT DISTINCT referenceSDDocument FROM outbound_delivery_items
                ) odi ON soh.salesOrder = odi.referenceSDDocument
                WHERE odi.referenceSDDocument IS NULL
                LIMIT 20
            """
        else:
            sql = """
                SELECT salesOrder, soldToParty, totalNetAmount, creationDate, overallDeliveryStatus
                FROM sales_order_headers
                WHERE overallDeliveryStatus != 'C'
                LIMIT 20
            """
        rows = _execute(sql)
        return {"intent": intent, "template": "BROKEN_FLOW", "rows": rows, "row_count": len(rows), "sql": sql}

    # ── ORDER_DETAIL ──────────────────────────────────────────────
    if intent == "ORDER_DETAIL":
        so_id = entities.get("sales_order") or entities.get("generic_id")
        if so_id:
            rows = _execute("SELECT * FROM sales_order_headers WHERE salesOrder = ?", (so_id,))
            # Also get items if available
            if "sales_order_items" in tables:
                items = _execute("SELECT * FROM sales_order_items WHERE salesOrder = ?", (so_id,))
                return {"intent": intent, "template": "ORDER_DETAIL", "rows": rows, "items": items, "row_count": len(rows), "sql": "order_detail"}
            return {"intent": intent, "template": "ORDER_DETAIL", "rows": rows, "row_count": len(rows), "sql": "order_detail"}
        rows = _execute("SELECT salesOrder, soldToParty, totalNetAmount, overallDeliveryStatus, creationDate FROM sales_order_headers ORDER BY creationDate DESC LIMIT 10")
        return {"intent": intent, "template": "ORDER_LIST", "rows": rows, "row_count": len(rows), "sql": "order_list"}

    # ── DELIVERY_STATUS ───────────────────────────────────────────
    if intent == "DELIVERY_STATUS":
        dd = entities.get("delivery") or entities.get("generic_id")
        if dd:
            rows = _execute("SELECT * FROM outbound_delivery_headers WHERE deliveryDocument = ?", (dd,))
        else:
            rows = _execute("""
                SELECT deliveryDocument, creationDate, shippingPoint,
                       overallGoodsMovementStatus, overallPickingStatus, actualGoodsMovementDate
                FROM outbound_delivery_headers
                ORDER BY creationDate DESC LIMIT 15
            """)
        return {"intent": intent, "template": "DELIVERY_STATUS", "rows": rows, "row_count": len(rows), "sql": "delivery_status"}

    # ── BILLING_INFO ──────────────────────────────────────────────
    if intent == "BILLING_INFO":
        bd = entities.get("billing_doc") or entities.get("generic_id")
        if bd:
            rows = _execute("SELECT * FROM billing_document_headers WHERE billingDocument = ?", (bd,))
        else:
            # Check if asking about cancellations
            q_lower = parsed.get("original_query", "").lower()
            if "cancel" in q_lower:
                rows = _execute("""
                    SELECT billingDocument, billingDocumentType, totalNetAmount, transactionCurrency,
                           soldToParty, creationDate, billingDocumentIsCancelled
                    FROM billing_document_headers
                    WHERE billingDocumentIsCancelled = '1' OR billingDocumentIsCancelled = 'True'
                    LIMIT 20
                """)
            else:
                rows = _execute("""
                    SELECT billingDocument, billingDocumentType, totalNetAmount, transactionCurrency,
                           soldToParty, creationDate, billingDocumentIsCancelled
                    FROM billing_document_headers
                    ORDER BY creationDate DESC LIMIT 15
                """)
        return {"intent": intent, "template": "BILLING_INFO", "rows": rows, "row_count": len(rows), "sql": "billing_info"}

    # ── PAYMENT_INFO ──────────────────────────────────────────────
    if intent == "PAYMENT_INFO":
        cust = entities.get("customer") or entities.get("generic_id")
        if "payments_accounts_receivable" in tables:
            if cust:
                rows = _execute("""
                    SELECT accountingDocument, amountInTransactionCurrency, transactionCurrency,
                           customer, postingDate, clearingDate, clearingAccountingDocument
                    FROM payments_accounts_receivable
                    WHERE customer = ?
                    ORDER BY postingDate DESC LIMIT 20
                """, (cust,))
            else:
                rows = _execute("""
                    SELECT accountingDocument, amountInTransactionCurrency, transactionCurrency,
                           customer, postingDate, clearingDate
                    FROM payments_accounts_receivable
                    ORDER BY postingDate DESC LIMIT 15
                """)
        else:
            rows = []
        return {"intent": intent, "template": "PAYMENT_INFO", "rows": rows, "row_count": len(rows), "sql": "payment_info"}

    # ── CUSTOMER_INFO ─────────────────────────────────────────────
    if intent == "CUSTOMER_INFO":
        cust = entities.get("customer") or entities.get("generic_id")
        if cust:
            # Get customer's orders
            rows = _execute("""
                SELECT salesOrder, totalNetAmount, overallDeliveryStatus, creationDate, transactionCurrency
                FROM sales_order_headers
                WHERE soldToParty = ?
                ORDER BY creationDate DESC
            """, (cust,))
            return {"intent": intent, "template": "CUSTOMER_ORDERS", "rows": rows, "row_count": len(rows), "sql": "customer_orders"}
        # List distinct customers
        rows = _execute("""
            SELECT soldToParty as customer,
                   COUNT(*) as order_count,
                   SUM(CAST(totalNetAmount AS REAL)) as total_amount
            FROM sales_order_headers
            GROUP BY soldToParty
            ORDER BY total_amount DESC
            LIMIT 15
        """)
        return {"intent": intent, "template": "CUSTOMER_LIST", "rows": rows, "row_count": len(rows), "sql": "customer_list"}

    # ── PRODUCT_INFO ──────────────────────────────────────────────
    if intent == "PRODUCT_INFO":
        if "products" in tables:
            pid = entities.get("product")
            if pid:
                rows = _execute("SELECT * FROM products WHERE product = ?", (pid,))
            else:
                rows = _execute("SELECT * FROM products LIMIT 20")
        else:
            rows = []
        return {"intent": intent, "template": "PRODUCT_INFO", "rows": rows, "row_count": len(rows), "sql": "product_info"}

    # ── TOP_ANALYSIS ──────────────────────────────────────────────
    if intent == "TOP_ANALYSIS":
        q_lower = parsed.get("original_query", "").lower()

        # How many orders/deliveries/etc
        if "how many" in q_lower or "count" in q_lower:
            if "delivery" in q_lower or "deliveries" in q_lower:
                rows = _execute("SELECT COUNT(*) as count FROM outbound_delivery_headers")
            elif "billing" in q_lower or "invoice" in q_lower:
                rows = _execute("SELECT COUNT(*) as count FROM billing_document_headers")
            elif "payment" in q_lower:
                rows = _execute("SELECT COUNT(*) as count FROM payments_accounts_receivable")
            elif "customer" in q_lower:
                rows = _execute("SELECT COUNT(DISTINCT soldToParty) as count FROM sales_order_headers")
            elif "product" in q_lower:
                if "products" in tables:
                    rows = _execute("SELECT COUNT(*) as count FROM products")
                else:
                    rows = [{"count": "N/A"}]
            else:
                rows = _execute("SELECT COUNT(*) as count FROM sales_order_headers")
            return {"intent": intent, "template": "COUNT", "rows": rows, "row_count": len(rows), "sql": "count"}

        # Total amount
        if "total" in q_lower and ("amount" in q_lower or "revenue" in q_lower or "sales" in q_lower):
            rows = _execute("""
                SELECT SUM(CAST(totalNetAmount AS REAL)) as total_amount,
                       transactionCurrency,
                       COUNT(*) as order_count
                FROM sales_order_headers
                GROUP BY transactionCurrency
            """)
            return {"intent": intent, "template": "TOTAL_AMOUNT", "rows": rows, "row_count": len(rows), "sql": "total_amount"}

        # Top customers
        if "customer" in q_lower:
            rows = _execute("""
                SELECT soldToParty as customer,
                       COUNT(*) as order_count,
                       SUM(CAST(totalNetAmount AS REAL)) as total_amount
                FROM sales_order_headers
                GROUP BY soldToParty
                ORDER BY total_amount DESC
                LIMIT 10
            """)
            return {"intent": intent, "template": "TOP_CUSTOMERS", "rows": rows, "row_count": len(rows), "sql": "top_customers"}

        # Top orders by amount
        rows = _execute("""
            SELECT salesOrder, soldToParty, totalNetAmount, transactionCurrency, creationDate
            FROM sales_order_headers
            ORDER BY CAST(totalNetAmount AS REAL) DESC
            LIMIT 10
        """)
        return {"intent": intent, "template": "TOP_ORDERS", "rows": rows, "row_count": len(rows), "sql": "top_orders"}

    # ── STATUS_OVERVIEW ───────────────────────────────────────────
    if intent == "STATUS_OVERVIEW":
        q_lower = parsed.get("original_query", "").lower()

        if "delivery" in q_lower or "deliveries" in q_lower:
            rows = _execute("""
                SELECT overallGoodsMovementStatus as status, COUNT(*) as count
                FROM outbound_delivery_headers
                GROUP BY overallGoodsMovementStatus
            """)
            return {"intent": intent, "template": "DELIVERY_STATUS_SUMMARY", "rows": rows, "row_count": len(rows), "sql": "delivery_summary"}

        if "billing" in q_lower or "invoice" in q_lower:
            rows = _execute("""
                SELECT billingDocumentType as type,
                       billingDocumentIsCancelled as cancelled,
                       COUNT(*) as count,
                       SUM(CAST(totalNetAmount AS REAL)) as total_amount
                FROM billing_document_headers
                GROUP BY billingDocumentType, billingDocumentIsCancelled
            """)
            return {"intent": intent, "template": "BILLING_SUMMARY", "rows": rows, "row_count": len(rows), "sql": "billing_summary"}

        # Default: order status overview
        rows = _execute("""
            SELECT overallDeliveryStatus as status, COUNT(*) as count,
                   SUM(CAST(totalNetAmount AS REAL)) as total_amount
            FROM sales_order_headers
            GROUP BY overallDeliveryStatus
        """)
        return {"intent": intent, "template": "ORDER_STATUS_SUMMARY", "rows": rows, "row_count": len(rows), "sql": "order_summary"}

    # ── GENERAL_QUERY (fallback) ──────────────────────────────────
    # Try a simple keyword-based search across sales orders
    q_lower = parsed.get("original_query", "").lower()

    # If there's a generic ID, try to find it across key tables
    gid = entities.get("generic_id")
    if gid:
        for table, col in [
            ("sales_order_headers", "salesOrder"),
            ("outbound_delivery_headers", "deliveryDocument"),
            ("billing_document_headers", "billingDocument"),
        ]:
            if table in tables:
                rows = _execute(f'SELECT * FROM "{table}" WHERE "{col}" = ?', (gid,))
                if rows:
                    return {"intent": "GENERAL_QUERY", "template": f"LOOKUP_{table}", "rows": rows, "row_count": len(rows), "sql": f"lookup_{table}"}

    # Last resort: show recent orders
    rows = _execute("""
        SELECT salesOrder, soldToParty, totalNetAmount, overallDeliveryStatus, creationDate
        FROM sales_order_headers
        ORDER BY creationDate DESC LIMIT 10
    """)
    return {"intent": "GENERAL_QUERY", "template": "RECENT_ORDERS", "rows": rows, "row_count": len(rows), "sql": "recent_orders"}
