"""
Optimized SQLite -> Neo4j graph ingestion using batched UNWIND.

Usage:
    python -m backend.ingest.load_neo4j
"""

import os
import sqlite3
import sys
from pathlib import Path

from dotenv import load_dotenv
from neo4j import GraphDatabase

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / ".env")

DB_PATH = ROOT / "backend" / "data.db"
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")


def get_sqlite_tables(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return [r[0] for r in cur.fetchall()]


def fetch_all(conn, table):
    conn.row_factory = sqlite3.Row
    cur = conn.execute(f'SELECT * FROM "{table}"')
    rows = [dict(r) for r in cur.fetchall()]
    conn.row_factory = None
    return rows


def clear_neo4j(driver):
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")
    print("  [DEL]  Cleared existing Neo4j data")


def create_constraints(driver):
    constraints = [
        ("SalesOrder", "salesOrder"),
        ("Customer", "customerId"),
        ("Delivery", "deliveryDocument"),
        ("BillingDocument", "billingDocument"),
        ("Product", "product"),
        ("Plant", "plant"),
        ("BusinessPartner", "businessPartner"),
        ("Payment", "paymentId"),
    ]
    with driver.session() as s:
        for label, prop in constraints:
            try:
                s.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE")
            except Exception:
                pass
    print("  [KEY]  Created constraints")


def batched_unwind(driver, query, rows, batch_size=2000):
    """Execute a Cypher query with UNWIND in batches."""
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        with driver.session() as s:
            s.run(query, batch=batch)


def load_sales_orders(driver, conn):
    rows = fetch_all(conn, "sales_order_headers")
    if not rows: return
    
    # Nodes
    query = """
    UNWIND $batch AS row
    MERGE (so:SalesOrder {salesOrder: row.salesOrder})
    SET so.salesOrderType = row.salesOrderType,
        so.salesOrganization = row.salesOrganization,
        so.distributionChannel = row.distributionChannel,
        so.soldToParty = row.soldToParty,
        so.creationDate = row.creationDate,
        so.totalNetAmount = row.totalNetAmount,
        so.overallDeliveryStatus = row.overallDeliveryStatus,
        so.transactionCurrency = row.transactionCurrency
    """
    batched_unwind(driver, query, rows)
    
    # Customer links
    cust_query = """
    UNWIND $batch AS row
    WITH row WHERE row.soldToParty IS NOT NULL
    MERGE (c:Customer {customerId: row.soldToParty})
    WITH c, row
    MATCH (so:SalesOrder {salesOrder: row.salesOrder})
    MERGE (c)-[:PLACED_ORDER]->(so)
    """
    batched_unwind(driver, cust_query, rows)
    print(f"  [OK]  SalesOrder nodes: {len(rows)}")


def load_deliveries(driver, conn):
    rows = fetch_all(conn, "outbound_delivery_headers")
    if not rows: return
    
    query = """
    UNWIND $batch AS row
    MERGE (d:Delivery {deliveryDocument: row.deliveryDocument})
    SET d.creationDate = row.creationDate,
        d.shippingPoint = row.shippingPoint,
        d.overallGoodsMovementStatus = row.overallGoodsMovementStatus,
        d.overallPickingStatus = row.overallPickingStatus,
        d.actualGoodsMovementDate = row.actualGoodsMovementDate
    """
    batched_unwind(driver, query, rows)
    print(f"  [OK]  Delivery nodes: {len(rows)}")


def load_delivery_items_links(driver, conn):
    tables = get_sqlite_tables(conn)
    if "outbound_delivery_items" not in tables: return
    rows = fetch_all(conn, "outbound_delivery_items")
    if not rows: return
    
    # Ensure referenceSDDocument exists
    valid_rows = [r for r in rows if r.get("deliveryDocument") and (r.get("referenceSDDocument") or r.get("referenceSdDocument"))]
    for r in valid_rows:
        r["ref"] = r.get("referenceSDDocument") or r.get("referenceSdDocument")
        
    query = """
    UNWIND $batch AS row
    MATCH (d:Delivery {deliveryDocument: row.deliveryDocument})
    MATCH (so:SalesOrder {salesOrder: row.ref})
    MERGE (so)-[:HAS_DELIVERY]->(d)
    """
    batched_unwind(driver, query, valid_rows)
    print(f"  [OK]  Delivery links: {len(valid_rows)}")


def load_billing_docs(driver, conn):
    rows = fetch_all(conn, "billing_document_headers")
    if not rows: return
    
    query = """
    UNWIND $batch AS row
    MERGE (b:BillingDocument {billingDocument: row.billingDocument})
    SET b.billingDocumentType = row.billingDocumentType,
        b.creationDate = row.creationDate,
        b.totalNetAmount = row.totalNetAmount,
        b.transactionCurrency = row.transactionCurrency,
        b.billingDocumentIsCancelled = row.billingDocumentIsCancelled,
        b.soldToParty = row.soldToParty,
        b.companyCode = row.companyCode,
        b.accountingDocument = row.accountingDocument
    """
    batched_unwind(driver, query, rows)
    
    cust_query = """
    UNWIND $batch AS row
    WITH row WHERE row.soldToParty IS NOT NULL
    MERGE (c:Customer {customerId: row.soldToParty})
    WITH c, row
    MATCH (b:BillingDocument {billingDocument: row.billingDocument})
    MERGE (c)-[:HAS_INVOICE]->(b)
    """
    batched_unwind(driver, cust_query, rows)
    print(f"  [OK]  BillingDocument nodes: {len(rows)}")


def load_billing_items_links(driver, conn):
    tables = get_sqlite_tables(conn)
    if "billing_document_items" not in tables: return
    rows = fetch_all(conn, "billing_document_items")
    valid_rows = [r for r in rows if r.get("billingDocument") and r.get("salesDocument")]
    
    query = """
    UNWIND $batch AS row
    MATCH (b:BillingDocument {billingDocument: row.billingDocument})
    MATCH (so:SalesOrder {salesOrder: row.salesDocument})
    MERGE (so)-[:HAS_BILLING]->(b)
    """
    batched_unwind(driver, query, valid_rows)
    print(f"  [OK]  Billing links: {len(valid_rows)}")


def load_products(driver, conn):
    tables = get_sqlite_tables(conn)
    if "products" not in tables: return
    rows = fetch_all(conn, "products")
    
    for r in rows:
        r["pid"] = r.get("product") or r.get("Product")
        r["pt"] = r.get("productType") or r.get("ProductType")
        r["bu"] = r.get("baseUnit") or r.get("BaseUnit")
        
    query = """
    UNWIND $batch AS row
    WITH row WHERE row.pid IS NOT NULL
    MERGE (p:Product {product: row.pid})
    SET p.productType = row.pt, p.baseUnit = row.bu
    """
    batched_unwind(driver, query, rows)
    
    if "product_descriptions" in tables:
        descs = fetch_all(conn, "product_descriptions")
        for d in descs:
            d["pid"] = d.get("product") or d.get("Product")
            d["desc"] = d.get("productDescription") or d.get("ProductDescription")
        
        desc_query = """
        UNWIND $batch AS row
        WITH row WHERE row.pid IS NOT NULL AND row.desc IS NOT NULL
        MATCH (p:Product {product: row.pid})
        SET p.description = row.desc
        """
        batched_unwind(driver, desc_query, descs)
    print(f"  [OK]  Product nodes: {len(rows)}")


def load_sales_item_product_links(driver, conn):
    tables = get_sqlite_tables(conn)
    if "sales_order_items" not in tables: return
    rows = fetch_all(conn, "sales_order_items")
    
    valid_rows = []
    for r in rows:
        so = r.get("salesOrder") or r.get("SalesOrder")
        mat = r.get("material") or r.get("Material") or r.get("product") or r.get("Product")
        if so and mat:
            valid_rows.append({"so": so, "mat": mat})
            
    query = """
    UNWIND $batch AS row
    MATCH (so:SalesOrder {salesOrder: row.so})
    MERGE (p:Product {product: row.mat})
    MERGE (so)-[:CONTAINS_PRODUCT]->(p)
    """
    batched_unwind(driver, query, valid_rows)
    print(f"  [OK]  Product links: {len(valid_rows)}")


def load_plants(driver, conn):
    tables = get_sqlite_tables(conn)
    if "plants" not in tables: return
    rows = fetch_all(conn, "plants")
    
    for r in rows:
        r["pid"] = r.get("plant") or r.get("Plant")
        r["pn"] = r.get("plantName") or r.get("PlantName")
        
    query = """
    UNWIND $batch AS row
    WITH row WHERE row.pid IS NOT NULL
    MERGE (pl:Plant {plant: row.pid})
    SET pl.plantName = row.pn
    """
    batched_unwind(driver, query, rows)
    print(f"  [OK]  Plant nodes: {len(rows)}")


def load_payments(driver, conn):
    tables = get_sqlite_tables(conn)
    if "payments_accounts_receivable" not in tables: return
    rows = fetch_all(conn, "payments_accounts_receivable")
    
    for r in rows:
        r["pay_id"] = f"{r.get('accountingDocument','')}-{r.get('accountingDocumentItem','')}"
        
    query = """
    UNWIND $batch AS row
    MERGE (pay:Payment {paymentId: row.pay_id})
    SET pay.accountingDocument = row.accountingDocument,
        pay.amountInTransactionCurrency = row.amountInTransactionCurrency,
        pay.transactionCurrency = row.transactionCurrency,
        pay.customer = row.customer,
        pay.postingDate = row.postingDate,
        pay.clearingDate = row.clearingDate,
        pay.clearingAccountingDocument = row.clearingAccountingDocument
    """
    batched_unwind(driver, query, rows)
    
    cust_query = """
    UNWIND $batch AS row
    WITH row WHERE row.customer IS NOT NULL
    MERGE (c:Customer {customerId: row.customer})
    WITH c, row
    MATCH (pay:Payment {paymentId: row.pay_id})
    MERGE (c)-[:MADE_PAYMENT]->(pay)
    """
    batched_unwind(driver, cust_query, rows)
    print(f"  [OK]  Payment nodes: {len(rows)}")


def main():
    if not DB_PATH.exists():
        print(f"[FAIL] SQLite DB not found: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    tables = get_sqlite_tables(conn)
    print(f"[STATS] Found {len(tables)} tables in SQLite")

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    print("[OK] Neo4j connected")

    clear_neo4j(driver)
    create_constraints(driver)

    if "sales_order_headers" in tables: load_sales_orders(driver, conn)
    if "outbound_delivery_headers" in tables: load_deliveries(driver, conn)
    if "billing_document_headers" in tables: load_billing_docs(driver, conn)

    load_products(driver, conn)
    load_plants(driver, conn)
    load_payments(driver, conn)

    load_delivery_items_links(driver, conn)
    load_billing_items_links(driver, conn)
    load_sales_item_product_links(driver, conn)

    conn.close()
    driver.close()
    print("\n[DONE] Neo4j graph loaded successfully!")


if __name__ == "__main__":
    main()
