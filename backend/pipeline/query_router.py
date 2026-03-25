"""
Query router – maps parsed intents to parameterized SQL templates
and executes them against the SQLite database.

Uses the actual SAP O2C table/column names from the JSONL data.
"""

from backend.db.sqlite_client import execute_query as _execute, get_tables as _get_tables


# ── SQL Templates per Intent ───────────────────────────────────────────

def route_query(parsed: dict, limit: int = 50, offset: int = 0) -> dict:
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

    # ── Granular Aggregations ────────────────────────────────────────
    if intent == "COUNT_DELIVERIES":
        rows = _execute("SELECT COUNT(*) as count FROM outbound_delivery_headers")
        return {"intent": intent, "template": "COUNT_DELIVERIES", "rows": rows, "row_count": len(rows), "sql": "count_deliveries"}
    if intent == "COUNT_BILLING":
        rows = _execute("SELECT COUNT(*) as count FROM billing_document_headers")
        return {"intent": intent, "template": "COUNT_BILLING", "rows": rows, "row_count": len(rows), "sql": "count_billing"}
    if intent == "COUNT_PAYMENT":
        if "payments_accounts_receivable" in tables:
            rows = _execute("SELECT COUNT(*) as count FROM payments_accounts_receivable")
        else:
            rows = [{"count": 0}]
        return {"intent": intent, "template": "COUNT_PAYMENT", "rows": rows, "row_count": len(rows), "sql": "count_payment"}
    if intent == "COUNT_CUSTOMER":
        rows = _execute("SELECT COUNT(DISTINCT soldToParty) as count FROM sales_order_headers")
        return {"intent": intent, "template": "COUNT_CUSTOMER", "rows": rows, "row_count": len(rows), "sql": "count_customer"}
    if intent == "COUNT_PRODUCT":
        if "products" in tables:
            rows = _execute("SELECT COUNT(*) as count FROM products")
        else:
            rows = [{"count": "N/A"}]
        return {"intent": intent, "template": "COUNT_PRODUCT", "rows": rows, "row_count": len(rows), "sql": "count_product"}
    if intent == "COUNT_ORDERS":
        rows = _execute("SELECT COUNT(*) as count FROM sales_order_headers")
        return {"intent": intent, "template": "COUNT_ORDERS", "rows": rows, "row_count": len(rows), "sql": "count_orders"}

    if intent == "TOTAL_AMOUNT":
        rows = _execute("""
            SELECT SUM(CAST(totalNetAmount AS REAL)) as total_amount,
                   transactionCurrency,
                   COUNT(*) as order_count
            FROM sales_order_headers
            GROUP BY transactionCurrency
        """)
        return {"intent": intent, "template": "TOTAL_AMOUNT", "rows": rows, "row_count": len(rows), "sql": "total_amount"}

    if intent == "TOP_CUSTOMERS":
        rows = _execute(f"""
            SELECT soldToParty as customer,
                   COUNT(*) as order_count,
                   SUM(CAST(totalNetAmount AS REAL)) as total_amount
            FROM sales_order_headers
            GROUP BY soldToParty
            ORDER BY total_amount DESC
            LIMIT {limit} OFFSET {offset}
        """)
        return {"intent": intent, "template": "TOP_CUSTOMERS", "rows": rows, "row_count": len(rows), "sql": "top_customers"}

    if intent == "TOP_ORDERS":
        rows = _execute(f"""
            SELECT salesOrder, soldToParty, totalNetAmount, transactionCurrency, creationDate
            FROM sales_order_headers
            ORDER BY CAST(totalNetAmount AS REAL) DESC
            LIMIT {limit} OFFSET {offset}
        """)
        return {"intent": intent, "template": "TOP_ORDERS", "rows": rows, "row_count": len(rows), "sql": "top_orders"}

    if intent == "TOP_PRODUCTS_BY_BILLING":
        if "billing_document_items" in tables and "products" in tables:
            rows = _execute(f"""
                SELECT p.product, p.productGroup, COUNT(DISTINCT b.billingDocument) as billing_count, SUM(CAST(b.netAmount AS REAL)) as total_billed
                FROM billing_document_items b
                JOIN products p ON b.material = p.product
                GROUP BY p.product
                ORDER BY billing_count DESC
                LIMIT {limit} OFFSET {offset}
            """)
        elif "billing_document_items" in tables:
            rows = _execute(f"""
                SELECT material as product, COUNT(DISTINCT billingDocument) as billing_count, SUM(CAST(netAmount AS REAL)) as total_billed
                FROM billing_document_items
                GROUP BY material
                ORDER BY billing_count DESC
                LIMIT {limit} OFFSET {offset}
            """)
        else:
            rows = []
        return {"intent": intent, "template": "TOP_PRODUCTS_BY_BILLING", "rows": rows, "row_count": len(rows), "sql": "top_products_billing"}

    # ── Granular Status Overview ─────────────────────────────────────
    if intent == "DELIVERY_STATUS_SUMMARY":
        rows = _execute("""
            SELECT overallGoodsMovementStatus as status, COUNT(*) as count
            FROM outbound_delivery_headers
            GROUP BY overallGoodsMovementStatus
        """)
        return {"intent": intent, "template": "DELIVERY_STATUS_SUMMARY", "rows": rows, "row_count": len(rows), "sql": "delivery_summary"}

    if intent == "BILLING_SUMMARY":
        rows = _execute("""
            SELECT billingDocumentType as type,
                   billingDocumentIsCancelled as cancelled,
                   COUNT(*) as count,
                   SUM(CAST(totalNetAmount AS REAL)) as total_amount
            FROM billing_document_headers
            GROUP BY billingDocumentType, billingDocumentIsCancelled
        """)
        return {"intent": intent, "template": "BILLING_SUMMARY", "rows": rows, "row_count": len(rows), "sql": "billing_summary"}

    if intent == "ORDER_STATUS_SUMMARY":
        rows = _execute("""
            SELECT overallDeliveryStatus as status, COUNT(*) as count,
                   SUM(CAST(totalNetAmount AS REAL)) as total_amount
            FROM sales_order_headers
            GROUP BY overallDeliveryStatus
        """)
        return {"intent": intent, "template": "ORDER_STATUS_SUMMARY", "rows": rows, "row_count": len(rows), "sql": "order_summary"}

    # ── TRACE_FLOW: Full O2C trace for a sales order ──────────────
    if intent == "TRACE_FLOW":
        root_id = entities.get("sales_order") or entities.get("delivery") or entities.get("billing_doc") or entities.get("generic_id")
        if root_id:
            rows: list[dict] = []
            so_ids = set()

            if _execute("SELECT 1 FROM sales_order_headers WHERE salesOrder = ?", (root_id,)):
                so_ids.add(root_id)
            if "outbound_delivery_items" in tables:
                del_items = _execute("SELECT referenceSDDocument FROM outbound_delivery_items WHERE deliveryDocument = ?", (root_id,))
                for di in del_items:
                    so_ids.add(di["referenceSDDocument"])
            if "billing_document_items" in tables:
                bill_items = _execute("SELECT salesDocument FROM billing_document_items WHERE billingDocument = ?", (root_id,))
                for bi in bill_items:
                    so_ids.add(bi["salesDocument"])

            for so_id in so_ids:
                sql = """
                    SELECT 'Sales Order' as step, salesOrder as id, totalNetAmount as amount,
                           overallDeliveryStatus as status, creationDate, soldToParty as customer
                    FROM sales_order_headers WHERE salesOrder = ?
                """
                so_hdr = _execute(sql, (so_id,))
                if so_hdr:
                    rows.extend(so_hdr)

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

                if "billing_document_items" in tables:
                    bill_sql = """
                        SELECT DISTINCT billingDocument FROM billing_document_items WHERE salesDocument = ?
                    """
                    bill_rows = _execute(bill_sql, (so_id,))
                    for br in bill_rows:
                        bd = br["billingDocument"]
                        hdr = _execute("SELECT * FROM billing_document_headers WHERE billingDocument = ?", (bd,))
                        if hdr:
                            acc_doc = hdr[0].get("accountingDocument")
                            rows.append({"step": "Billing", "id": bd, "amount": hdr[0].get("totalNetAmount"), "accountingDocument": acc_doc, **hdr[0]})
                            
                            if acc_doc and "payments_accounts_receivable" in tables:
                                pay_hdr = _execute("SELECT * FROM payments_accounts_receivable WHERE accountingDocument = ?", (acc_doc,))
                                for ph in pay_hdr:
                                    rows.append({"step": "Journal Entry", "id": ph["accountingDocument"], "status": "Cleared" if ph.get("clearingDate") else "Open", **ph})

            if rows:
                return {"intent": intent, "template": "TRACE_FLOW", "rows": rows, "row_count": len(rows), "sql": "trace_flow"}

        # No ID provided - show sample orders
        rows = _execute(f"SELECT salesOrder, soldToParty, totalNetAmount, overallDeliveryStatus, creationDate FROM sales_order_headers LIMIT {limit} OFFSET {offset}")
        return {"intent": intent, "template": "TRACE_FLOW_SAMPLE", "rows": rows, "row_count": len(rows), "sql": "sample"}

    # ── BROKEN_FLOW: Orders without deliveries or billing ─────────
    if intent == "BROKEN_FLOW":
        if "outbound_delivery_items" in tables and "billing_document_items" in tables:
            sql = f"""
                SELECT soh.salesOrder, soh.soldToParty, soh.totalNetAmount, soh.creationDate,
                       soh.overallDeliveryStatus,
                       CASE 
                           WHEN odi.deliveryDocument IS NULL THEN 'Missing Delivery'
                           WHEN bdi.billingDocument IS NULL THEN 'Delivered but Not Billed'
                           ELSE 'Unknown Issue'
                       END as broken_reason
                FROM sales_order_headers soh
                LEFT JOIN outbound_delivery_items odi ON soh.salesOrder = odi.referenceSDDocument
                LEFT JOIN billing_document_items bdi ON soh.salesOrder = bdi.salesDocument
                WHERE (odi.deliveryDocument IS NULL OR bdi.billingDocument IS NULL)
                GROUP BY soh.salesOrder
                LIMIT {limit} OFFSET {offset}
            """
        elif "outbound_delivery_items" in tables:
            sql = f"""
                SELECT soh.salesOrder, soh.soldToParty, soh.totalNetAmount, soh.creationDate,
                       soh.overallDeliveryStatus,
                       'Missing Delivery' as broken_reason
                FROM sales_order_headers soh
                LEFT JOIN (
                    SELECT DISTINCT referenceSDDocument FROM outbound_delivery_items
                ) odi ON soh.salesOrder = odi.referenceSDDocument
                WHERE odi.referenceSDDocument IS NULL
                LIMIT {limit} OFFSET {offset}
            """
        else:
            sql = f"""
                SELECT salesOrder, soldToParty, totalNetAmount, creationDate, overallDeliveryStatus, 'Missing Terminal State' as broken_reason
                FROM sales_order_headers
                WHERE overallDeliveryStatus != 'C'
                LIMIT {limit} OFFSET {offset}
            """
        rows = _execute(sql)
        
        # Calculate actual total instead of relying on the limited rows
        total_count = len(rows)
        if len(rows) == limit:
            if "outbound_delivery_items" in tables and "billing_document_items" in tables:
                c_sql = """
                    SELECT COUNT(DISTINCT soh.salesOrder) as c
                    FROM sales_order_headers soh
                    LEFT JOIN outbound_delivery_items odi ON soh.salesOrder = odi.referenceSDDocument
                    LEFT JOIN billing_document_items bdi ON soh.salesOrder = bdi.salesDocument
                    WHERE (odi.deliveryDocument IS NULL OR bdi.billingDocument IS NULL)
                """
            elif "outbound_delivery_items" in tables:
                c_sql = """
                    SELECT COUNT(DISTINCT soh.salesOrder) as c
                    FROM sales_order_headers soh
                    LEFT JOIN (SELECT DISTINCT referenceSDDocument FROM outbound_delivery_items) odi 
                    ON soh.salesOrder = odi.referenceSDDocument
                    WHERE odi.referenceSDDocument IS NULL
                """
            else:
                c_sql = "SELECT COUNT(*) as c FROM sales_order_headers WHERE overallDeliveryStatus != 'C'"
            
            c_res = _execute(c_sql)
            if c_res:
                total_count = c_res[0]["c"]

        return {"intent": intent, "template": "BROKEN_FLOW", "rows": rows, "row_count": total_count, "sql": "broken_flow"}

    # ── ORDER_DETAIL ──────────────────────────────────────────────
    if intent == "ORDER_DETAIL":
        so_id = entities.get("sales_order") or entities.get("generic_id")
        if so_id:
            rows = _execute("SELECT * FROM sales_order_headers WHERE salesOrder = ?", (so_id,))
            # Also get items if available
            if "sales_order_items" in tables:
                items = _execute(f"SELECT * FROM sales_order_items WHERE salesOrder = ? LIMIT {limit} OFFSET {offset}", (so_id,))
                return {"intent": intent, "template": "ORDER_DETAIL", "rows": rows, "items": items, "row_count": len(rows), "sql": "order_detail"}
            return {"intent": intent, "template": "ORDER_DETAIL", "rows": rows, "row_count": len(rows), "sql": "order_detail"}
        rows = _execute(f"SELECT salesOrder, soldToParty, totalNetAmount, overallDeliveryStatus, creationDate FROM sales_order_headers ORDER BY creationDate DESC LIMIT {limit} OFFSET {offset}")
        return {"intent": intent, "template": "ORDER_LIST", "rows": rows, "row_count": len(rows), "sql": "order_list"}

    # ── DELIVERY_STATUS ───────────────────────────────────────────
    if intent == "DELIVERY_STATUS":
        dd = entities.get("delivery") or entities.get("generic_id")
        if dd:
            rows = _execute("SELECT * FROM outbound_delivery_headers WHERE deliveryDocument = ?", (dd,))
        else:
            rows = _execute(f"""
                SELECT deliveryDocument, creationDate, shippingPoint,
                       overallGoodsMovementStatus, overallPickingStatus, actualGoodsMovementDate
                FROM outbound_delivery_headers
                ORDER BY creationDate DESC LIMIT {limit} OFFSET {offset}
            """)
        return {"intent": intent, "template": "DELIVERY_STATUS", "rows": rows, "row_count": len(rows), "sql": "delivery_status"}

    # ── BILLING_INFO ──────────────────────────────────────────────
    if intent == "BILLING_INFO":
        bd = entities.get("billing_doc") or entities.get("generic_id")
        if bd:
            rows = _execute("SELECT * FROM billing_document_headers WHERE billingDocument = ?", (bd,))
        else:
            q_lower = (parsed.get("fixed_query") or parsed.get("original_query") or "").lower()
            if "cancel" in q_lower:
                rows = _execute(f"""
                    SELECT billingDocument, billingDocumentType, totalNetAmount, transactionCurrency,
                           soldToParty, creationDate, billingDocumentIsCancelled
                    FROM billing_document_headers
                    WHERE billingDocumentIsCancelled = '1' OR billingDocumentIsCancelled = 'True'
                    LIMIT {limit} OFFSET {offset}
                """)
            else:
                rows = _execute(f"""
                    SELECT billingDocument, billingDocumentType, totalNetAmount, transactionCurrency,
                           soldToParty, creationDate, billingDocumentIsCancelled
                    FROM billing_document_headers
                    ORDER BY creationDate DESC LIMIT {limit} OFFSET {offset}
                """)
        return {"intent": intent, "template": "BILLING_INFO", "rows": rows, "row_count": len(rows), "sql": "billing_info"}

    # ── PAYMENT_INFO ──────────────────────────────────────────────
    if intent == "PAYMENT_INFO":
        cust = entities.get("customer") or entities.get("generic_id")
        if "payments_accounts_receivable" in tables:
            if cust:
                rows = _execute(f"""
                    SELECT accountingDocument, amountInTransactionCurrency, transactionCurrency,
                           customer, postingDate, clearingDate, clearingAccountingDocument
                    FROM payments_accounts_receivable
                    WHERE customer = ?
                    ORDER BY postingDate DESC LIMIT {limit} OFFSET {offset}
                """, (cust,))
            else:
                rows = _execute(f"""
                    SELECT accountingDocument, amountInTransactionCurrency, transactionCurrency,
                           customer, postingDate, clearingDate
                    FROM payments_accounts_receivable
                    ORDER BY postingDate DESC LIMIT {limit} OFFSET {offset}
                """)
        else:
            rows = []
        return {"intent": intent, "template": "PAYMENT_INFO", "rows": rows, "row_count": len(rows), "sql": "payment_info"}

    # ── CUSTOMER_INFO ─────────────────────────────────────────────
    if intent == "CUSTOMER_INFO":
        cust = entities.get("customer") or entities.get("generic_id")
        if cust:
            # Get customer's orders
            rows = _execute(f"""
                SELECT salesOrder, totalNetAmount, overallDeliveryStatus, creationDate, transactionCurrency
                FROM sales_order_headers
                WHERE soldToParty = ?
                ORDER BY creationDate DESC
                LIMIT {limit} OFFSET {offset}
            """, (cust,))
            return {"intent": intent, "template": "CUSTOMER_ORDERS", "rows": rows, "row_count": len(rows), "sql": "customer_orders"}
        # List distinct customers
        rows = _execute(f"""
            SELECT soldToParty as customer,
                   COUNT(*) as order_count,
                   SUM(CAST(totalNetAmount AS REAL)) as total_amount
            FROM sales_order_headers
            GROUP BY soldToParty
            ORDER BY total_amount DESC
            LIMIT {limit} OFFSET {offset}
        """)
        return {"intent": intent, "template": "CUSTOMER_LIST", "rows": rows, "row_count": len(rows), "sql": "customer_list"}

    # ── PRODUCT_INFO ──────────────────────────────────────────────
    if intent == "PRODUCT_INFO":
        if "products" in tables:
            pid = entities.get("product")
            if pid:
                rows = _execute("SELECT * FROM products WHERE product = ?", (pid,))
            else:
                rows = _execute(f"SELECT * FROM products LIMIT {limit} OFFSET {offset}")
        else:
            rows = []
        return {"intent": intent, "template": "PRODUCT_INFO", "rows": rows, "row_count": len(rows), "sql": "product_info"}

    # ── GENERAL_QUERY (fallback) ──────────────────────────────────
    # Try a simple keyword-based search across key tables
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
    rows = _execute(f"""
        SELECT salesOrder, soldToParty, totalNetAmount, overallDeliveryStatus, creationDate
        FROM sales_order_headers
        ORDER BY creationDate DESC LIMIT {limit} OFFSET {offset}
    """)
    return {"intent": "GENERAL_QUERY", "template": "RECENT_ORDERS", "rows": rows, "row_count": len(rows), "sql": "recent_orders"}
