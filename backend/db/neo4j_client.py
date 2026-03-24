"""Neo4j connection and graph queries for visualization.
Falls back instantly to SQLite-based local graph when Neo4j is unavailable."""

import os
from dotenv import load_dotenv
from backend.db.local_graph import local_graph

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "")
NEO4J_USER = os.getenv("NEO4J_USER", "")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")

# Check if neo4j package is even installed
_HAS_NEO4J_PACKAGE = False
try:
    from neo4j import GraphDatabase
    _HAS_NEO4J_PACKAGE = True
except ImportError:
    print("[Graph] neo4j package not installed. Using local SQLite graph.")


def _check_neo4j_available() -> bool:
    """Try to connect to Neo4j once at startup. Returns True only if reachable."""
    if not _HAS_NEO4J_PACKAGE:
        return False
    if not NEO4J_URI:
        print("[Graph] No NEO4J_URI configured. Using local SQLite graph.")
        return False
    try:
        driver = GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD),
            connection_timeout=5, max_connection_lifetime=60,
        )
        # Use a short timeout so startup isn't blocked
        driver.verify_connectivity()
        driver.close()
        print(f"[Graph] Neo4j connected at {NEO4J_URI}")
        return True
    except Exception as e:
        print(f"[Graph] Neo4j unavailable ({type(e).__name__}). Using local SQLite graph.")
        return False


class Neo4jClient:
    """Wrapper that uses Neo4j when available, otherwise delegates to local SQLite graph."""

    def __init__(self):
        self._driver = None
        self._use_neo4j = _check_neo4j_available()
        if self._use_neo4j:
            print("[Graph] Mode: Neo4j")
        else:
            print("[Graph] Mode: Local SQLite")

    @property
    def mode(self) -> str:
        """Return 'neo4j' or 'sqlite' based on the active backend."""
        return "neo4j" if self._use_neo4j else "sqlite"

    def _get_driver(self):
        if self._driver is None and _HAS_NEO4J_PACKAGE:
            self._driver = GraphDatabase.driver(
                NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD),
                connection_timeout=5, max_connection_lifetime=60,
            )
        return self._driver

    def close(self):
        """Close Neo4j driver. No-op in SQLite mode."""
        if not self._use_neo4j:
            return
        if self._driver:
            self._driver.close()
            self._driver = None

    def get_nodes(self, limit: int = 150) -> dict:
        """Return nodes and edges for graph visualization."""
        if not self._use_neo4j:
            return local_graph.get_nodes(limit=limit)

        try:
            driver = self._get_driver()
            nodes = []
            edges = []
            seen_nodes = set()

            with driver.session() as session:
                result = session.run(
                    """
                    MATCH (a)-[r]->(b)
                    RETURN a, r, b, labels(a) AS a_labels, labels(b) AS b_labels,
                           elementId(a) AS a_id, elementId(b) AS b_id, type(r) AS rel_type
                    LIMIT $limit
                    """,
                    limit=limit,
                )

                for record in result:
                    a_id = record["a_id"]
                    b_id = record["b_id"]
                    a_node = record["a"]
                    b_node = record["b"]
                    a_labels = record["a_labels"]
                    b_labels = record["b_labels"]
                    rel_type = record["rel_type"]

                    if a_id not in seen_nodes:
                        seen_nodes.add(a_id)
                        props = dict(a_node)
                        nodes.append({
                            "id": a_id,
                            "label": props.get("name") or props.get("id") or a_id[:12],
                            "type": a_labels[0] if a_labels else "Unknown",
                            "properties": props,
                        })

                    if b_id not in seen_nodes:
                        seen_nodes.add(b_id)
                        props = dict(b_node)
                        nodes.append({
                            "id": b_id,
                            "label": props.get("name") or props.get("id") or b_id[:12],
                            "type": b_labels[0] if b_labels else "Unknown",
                            "properties": props,
                        })

                    edges.append({
                        "source": a_id,
                        "target": b_id,
                        "type": rel_type,
                    })

            return {"nodes": nodes, "edges": edges}

        except Exception as e:
            print(f"[Graph] Neo4j query failed ({e}). Switching to SQLite.")
            self._use_neo4j = False
            return local_graph.get_nodes(limit=limit)

    def expand_node(self, node_id: str) -> dict:
        """Return a node's immediate neighbors."""
        if not self._use_neo4j:
            return local_graph.expand_node(node_id)

        try:
            driver = self._get_driver()
            nodes = []
            edges = []
            seen_nodes = set()

            with driver.session() as session:
                result = session.run(
                    """
                    MATCH (a)-[r]-(b)
                    WHERE elementId(a) = $node_id
                    RETURN a, r, b, labels(a) AS a_labels, labels(b) AS b_labels,
                           elementId(a) AS a_id, elementId(b) AS b_id, type(r) AS rel_type,
                           startNode(r) = a AS is_outgoing
                    LIMIT 50
                    """,
                    node_id=node_id,
                )

                for record in result:
                    a_id = record["a_id"]
                    b_id = record["b_id"]
                    b_node = record["b"]
                    b_labels = record["b_labels"]
                    is_outgoing = record["is_outgoing"]
                    rel_type = record["rel_type"]

                    if b_id not in seen_nodes:
                        seen_nodes.add(b_id)
                        props = dict(b_node)
                        nodes.append({
                            "id": b_id,
                            "label": props.get("name") or props.get("id") or b_id[:12],
                            "type": b_labels[0] if b_labels else "Unknown",
                            "properties": props,
                        })

                    if is_outgoing:
                        edges.append({"source": a_id, "target": b_id, "type": rel_type})
                    else:
                        edges.append({"source": b_id, "target": a_id, "type": rel_type})

            return {"nodes": nodes, "edges": edges}

        except Exception:
            self._use_neo4j = False
            return local_graph.expand_node(node_id)


# Module-level singleton
neo4j_client = Neo4jClient()
