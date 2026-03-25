# Graph Query System: End-to-End Architecture

This document explains the end-to-end architecture, data flow, and components of the Graph Query System, primarily focusing on how the system parses natural language questions into database queries, formats the results, and visualizes the structured data.

---

## 🏗️ 1. High-Level Architecture

The system is a decoupled **Client-Server Architecture** deployed on a serverless infrastructure:

- **Frontend (Client)**: A Vanilla JavaScript + HTML/CSS web application that provides the user interface for chatting, viewing structured data, and interacting with the network graph.
- **Backend (Server)**: A Python FastAPI application that processes natural language, queries databases, interfaces with LLMs, and serves graph data.
- **Databases**:
  - **SQLite (`data.db`)**: Acts as the primary operational database for chat queries and as a fallback local graph database.
  - **Neo4j (AuraDB)**: An external graph database used primarily for advanced node-and-edge network visualization.
- **External APIs (Gemini)**: Google's Gemini 2.0 Flash model is used strictly for formatting raw database rows into conversational responses.
- **Deployment (Vercel)**: Configured to run the Python backend as stateless serverless functions (`api/index.py`) and serve the frontend as static files.

---

## 🌊 2. The Chat Pipeline (Data Flow)

When a user types a question in the chat (e.g., *"How many orders does customer 310000108 have?"*), the data moves through a strict 4-step backend pipeline:

### Step 1: Classifier (`classifier.py`)
- **Purpose**: Security and Relevance.
- **Action**: Uses regex and keyword matching to ensure the user's question is actually about the SAP Order-to-Cash (O2C) domain (orders, deliveries, invoices).
- **Result**: Stops off-topic questions (e.g., *"Write me a poem"*) immediately, saving processing time and preventing prompt injection.

### Step 2: Intent Parser (`intent_parser.py`)
- **Purpose**: Understanding the user's goal.
- **Action**: Extracts the core intent (e.g., `ORDER_DETAIL`, `TOP_CUSTOMERS`) and specific entities (e.g., `sales_order: 740506`) from the raw text without needing an LLM.
- **Result**: Converts unstructured text into a structured JSON payload: `{"intent": "CUSTOMER_ORDERS", "entities": {"customer": "310000108"}}`.

### Step 3: Query Router (`query_router.py`)
- **Purpose**: Fetching the actual truth from the database.
- **Action**: Maps the validated intent to a safe, parameterized SQL template.
- **Execution**: Runs the SQL query against the read-only **SQLite (`data.db`)** database (which holds the SAP tabular data).
- **Result**: Returns raw matching rows (e.g., `[{salesOrder: "...", totalAmount: "..."}]`).
- Note: *The chat system never queries Neo4j. It relies entirely on structured SQL to guarantee 100% accurate, hallucination-free answers.*

### Step 4: Formatter (`formatter.py`)
- **Purpose**: Making the data human-readable.
- **Action**: Takes the raw JSON rows and the user's original question, and sends them to the **Gemini API**.
- **Prompting Strategy**: Instructs Gemini to summarize the output in 1-2 conversational sentences and generate clickable markdown links (e.g., `[1000214](id:1000214)`) for business IDs.
- **Resilience**: If Gemini times out (takes >10 seconds) or the API key is missing, a built-in strict timeout thread catches the failure and instantly returns a hardcoded Python string fallback (e.g., *"I found 5 orders for this customer"*).

---

## 🕸️ 3. The Graph Visualization Flow

The right pane of the UI displays an interactive node network. This operates independently of the chat pipeline.

### Loading the Graph (`neo4j_client.py` & `local_graph.py`)
1. **Initial Request**: The frontend calls `/api/graph/nodes` on page load.
2. **Neo4j Attempt**: The backend first attempts to connect to the external **Neo4j AuraDB**. It uses a strict 15-second connection timeout to accommodate serverless wake-up times.
3. **Cypher Query**: If Neo4j connects, it runs a Cypher query (`MATCH (a)-[r]->(b) RETURN...`) to fetch nodes and relationships.
4. **Local Fallback**: If Neo4j fails to connect, times out, or no credentials exist, the system gracefully and silently falls back to `local_graph.py`.
5. **Local Generation**: The local graph engine reads the SQLite `data.db` tables and constructs nodes and edges in-memory on the fly, mimicking Neo4j's output format.
6. **Delivery to Frontend**: The frontend receives standardized format `{"nodes": [...], "edges": [...]}` regardless of which database actually generated it.

### Frontend Interactions
- Clicking a specific node (or clicking a blue ID link in the chat) triggers `/api/graph/expand/{node_id}` to fetch only that node's neighbors, keeping the UI fast.

---

## 🌩️ 4. Serverless Deployment Dynamics (Vercel)

Deploying a stateful application to Vercel's stateless functions required specific architectural choices:

1. **Stateless Operations**: Every API request spins up a fresh isolated instance of Python. Standard features like launching a browser (`webbrowser.open()`) or mounting static files inside FastAPI had to be guarded by an `IS_VERCEL` environment check since Vercel handles static routing directly.
2. **Read-Only SQLite**: Vercel's file system is read-only. `data.db` is bundled into the deployment (via `.gitignore` inclusion) and accessed using URI parameters (`file:data.db?mode=ro`) to prevent SQLite from attempting to create lock/journal files and crashing.
3. **Execution Timeouts**: Free-tier Vercel functions terminate instantly at 10 seconds by default. The `vercel.json` config overrides this to `maxDuration: 60`. Furthermore, strict Python threading timeouts were added to Gemini and Neo4j connections to ensure they never breach the 60s hard limit and crash the endpoint.

---

## 🎯 Summary of Key Design Principles

1. **Anti-Hallucination**: LLMs (Gemini) are **only** used for formatting text, never for querying data. The SQLite Query Router enforces deterministic, 100% accurate data retrieval.
2. **Graceful Degradation**: 
   - If Neo4j is down -> Falls back to local SQLite graph.
   - If Gemini is down / times out -> Falls back to plain text formatting.
3. **Serverless-First**: Optimized for cold starts, read-only file systems, and strict timeout boundaries.
