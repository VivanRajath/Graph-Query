# Graph Query System
**Assessment for Forward Deployed Engineer (FDE) Role at Dodge AI**

This repository contains a full-stack Conversational Graph Query System built as an assessment for the FDE role at Dodge AI. The system ingests a sample dataset (covering the Order-to-Cash process), models it into an interconnected graph, and provides both a rich visualization UI and an intelligent natural language chat interface.

---

## 🎯 High-Level Overview

- **Dataset to Graph Integration**: The dataset is fully parsed and ingested into a graph database (Neo4j), with a robust offline fallback to a local SQLite/NetworkX representation.
- **Interactive Visualization**: The interconnected graph is visualized using **Cytoscape.js** offering features such as zooming, expanding nodes, hovering for detailed node metadata, and interactive floating controls.
- **Conversational Interface**: A specialized chat module translates natural language user queries directly into structured data operations (such as SQL or Cypher), executing them dynamically.
- **Data-Backed Answers**: The system responds _only_ with verifiable data from the underlying datastores. Off-topic prompts or general capability requests are firmly blocked.

---

## 🚀 Features Implemented (Based on Requirements)

### 1. Graph Construction
- **Nodes & Relationships**: Models core business entities—including `SalesOrder`, `PurchaseOrder`, `Delivery`, `BillingDocument`, `Payment`, `Plant`, `Customer`, `Address`, and `Product`. 
- **Robust Ingestion Pipeline**: Scripts construct the nodes and map the hierarchical and flow-based relationships (e.g., `SalesOrder` → `Delivery` → `BillingDocument`).

### 2. Graph Visualization (UI)
- **Interactive Cytoscape Interface**: A powerful implementation with pan/zoom mechanics, node layout restructuring, and detail tooltips on hover/click.
- **Aesthetic Dark & Light Themes**: The entire application (both graph and chat interface) fully supports theme switching with automatic dark mode persistence.
- **Floating Tooling**: Included UI floating widgets for "Toggle Node IDs", "Hide Granular Overlays", and Legend maps.

### 3. Conversational Query Interface & Guardrails
- **Natural Language Parsing**: Translates user questions into structured operations dynamically using state-of-the-art LLMs.
- **Guardrails System**: System actively rejects non-domain queries (e.g., "Write me a poem", "What is the weather?"). The conversational loop strictly respects boundaries.
- **Advanced Query Support**: Easily handles inquiries such as tracing flow bottlenecks, identifying top customers by revenue, or finding incomplete document trails.

### 4. Optional Extensions Included (Bonus)
- **Model Agnosticism**: Multi-model compatibility configured to utilize any major provider (Gemini, OpenAI, Claude, Groq).
- **Natural language to SQL/Graph Pipeline**: Full structured translation capabilities for extracting granular answers safely.
- **Batch Embeddings & Caching**: Substantially optimized vector generation logic scaling up execution performance.
- **Local Fallback Methodology**: Zero-configuration DB mode mapping SQLite relations to NetworkX without requiring Neo4j credentials, enabling immediate local execution.

---

## 🛠 Tech Stack

- **Backend**: Python 3.10+, FastAPI, Uvicorn, LangGraph/LangChain, Neo4j, SQLite, NetworkX
- **Frontend**: Vanilla JavaScript (ES6+), CSS3 with CSS Variables, HTML5, Cytoscape.js
- **Deployment**: Configured for Serverless function deployment via Vercel (`vercel.json` included).

---

## 📦 Local Setup & Execution

### 1. Requirements Installation
Using a virtual environment, install the backend dependencies:
```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Unix/MacOS
source venv/bin/activate

pip install -r backend/requirements.txt
```

### 2. Environment Variables
Create a `.env` file in the root based on the project requirements containing necessary API keys (e.g., `GEMINI_API_KEY`, `NEO4J_URI`, etc.).

### 3. Start the Server
Navigate to the root directory and start the FastAPI server via Uvicorn:
```bash
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```
The application will automatically serve the UI at `http://localhost:8000/static/index.html`.

---

