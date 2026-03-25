"""
In-memory graph fallback.
Builds a basic topological graph from the SQLite data.db so the frontend can still visualize the O2C flow even if Neo4j is not installed.
"""

import sqlite3
from backend.db.sqlite_client import get_connection

class LocalGraphClient:
    def __init__(self):
        self.nodes = {}
        self.edges = []
        self._adjacency = {}
        self._loaded = False

    def _load_data(self):
        conn = get_connection()
        
        def add_node(nid, label, ntype, props):
            if nid not in self.nodes:
                self.nodes[nid] = {"id": nid, "label": label or nid, "type": ntype, "properties": props}
                self._adjacency[nid] = []

        def add_edge(src, tgt, rel_type):
            if src in self.nodes and tgt in self.nodes:
                edge = {"source": src, "target": tgt, "type": rel_type}
                self.edges.append(edge)
                self._adjacency[src].append(edge)
                self._adjacency[tgt].append(edge)

        # 1. Sales Orders
        for r in conn.execute("SELECT * FROM sales_order_headers"):
            d = dict(r)
            add_node(f"so_{d['salesOrder']}", d['salesOrder'], "SalesOrder", d)
            if d.get("soldToParty"):
                add_node(f"cust_{d['soldToParty']}", d['soldToParty'], "Customer", {"customerId": d['soldToParty']})
                add_edge(f"cust_{d['soldToParty']}", f"so_{d['salesOrder']}", "PLACED_ORDER")

        # 2. Deliveries
        for r in conn.execute("SELECT * FROM outbound_delivery_headers"):
            d = dict(r)
            add_node(f"del_{d['deliveryDocument']}", d['deliveryDocument'], "Delivery", d)

        # Delivery Links
        try:
            for r in conn.execute("SELECT referenceSDDocument, deliveryDocument FROM outbound_delivery_items"):
                if r[0] and r[1]:
                    add_edge(f"so_{r[0]}", f"del_{r[1]}", "HAS_DELIVERY")
        except sqlite3.OperationalError:
            pass

        # 3. Billing
        for r in conn.execute("SELECT * FROM billing_document_headers"):
            d = dict(r)
            b_id = f"bill_{d['billingDocument']}"
            add_node(b_id, d['billingDocument'], "BillingDocument", d)
            if d.get("soldToParty"):
                add_edge(f"cust_{d['soldToParty']}", b_id, "HAS_INVOICE")

        # Billing Links
        try:
            for r in conn.execute("SELECT salesDocument, billingDocument FROM billing_document_items"):
                if r[0] and r[1]:
                    add_edge(f"so_{r[0]}", f"bill_{r[1]}", "HAS_BILLING")
        except sqlite3.OperationalError:
            pass

        # 4. Payments
        try:
            for r in conn.execute("SELECT * FROM payments_accounts_receivable"):
                d = dict(r)
                pay_id = f"pay_{d['accountingDocument']}-{d['accountingDocumentItem']}"
                add_node(pay_id, d['accountingDocument'], "Payment", d)
                if d.get("customer"):
                    add_edge(f"cust_{d['customer']}", pay_id, "MADE_PAYMENT")
        except sqlite3.OperationalError:
            pass

        conn.close()
        self._loaded = True

    def get_nodes(self, limit: int = 150) -> dict:
        self._load_data()
        # Return a sample of nodes and their connected edges
        sample_nodes = list(self.nodes.values())[:limit]
        sample_ids = {n["id"] for n in sample_nodes}
        
        sample_edges = []
        for e in self.edges:
            if e["source"] in sample_ids and e["target"] in sample_ids:
                sample_edges.append(e)
                
        return {"nodes": sample_nodes, "edges": sample_edges}

    def expand_node(self, node_id: str) -> dict:
        self._load_data()
        if node_id not in self.nodes:
            return {"nodes": [], "edges": []}
            
        nodes_out = {self.nodes[node_id]["id"]: self.nodes[node_id]}
        edges_out = []
        
        for e in self._adjacency.get(node_id, []):
            edges_out.append(e)
            src, tgt = e["source"], e["target"]
            if src not in nodes_out: nodes_out[src] = self.nodes[src]
            if tgt not in nodes_out: nodes_out[tgt] = self.nodes[tgt]
            
        return {"nodes": list(nodes_out.values()), "edges": edges_out}

local_graph = LocalGraphClient()
