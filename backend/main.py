"""FastAPI application — main entry point."""

import os
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sys
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

# Ensure the parent directory is in the Python path so "backend.X" imports work
# even if uvicorn is started from inside the backend/ folder.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from backend.pipeline.classifier import classify
from backend.pipeline.intent_parser import parse_intent
from backend.pipeline.query_router import route_query
from backend.pipeline.formatter import format_results
from backend.db.neo4j_client import neo4j_client

app = FastAPI(
    title="Graph Query System",
    description="Natural language query system for business data with graph visualization",
    version="1.0.0",
)

# CORS for local frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend static files (local dev only; Vercel serves frontend separately)
IS_VERCEL = os.getenv("VERCEL", "") == "1"
FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")
if not IS_VERCEL and os.path.isdir(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

# Auto-open browser flag (disabled on Vercel)
AUTO_OPEN = (not IS_VERCEL) and os.getenv("AUTO_OPEN_BROWSER", "true").lower() in ("true", "1", "yes")


class QueryRequest(BaseModel):
    question: str


class QueryResponse(BaseModel):
    answer: str
    intent: str = ""
    template_used: str = ""
    row_count: int = 0
    sql: str = ""
    data: list = []


# ─── Startup ─────────────────────────────────────────────────────────────


@app.on_event("startup")
async def startup():
    """Open the frontend in the default browser on startup."""
    if AUTO_OPEN:
        try:
            import webbrowser
            # Short delay to let uvicorn finish binding
            async def _open():
                await asyncio.sleep(1.0)
                url = "http://localhost:8000/static/index.html"
                print(f"[Startup] Opening browser → {url}")
                webbrowser.open(url)

            asyncio.create_task(_open())
        except Exception:
            pass


# ─── Endpoints ───────────────────────────────────────────────────────────────


@app.get("/api/health")
def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/api/graph/status")
def graph_status():
    """Return which graph backend is active and basic stats."""
    mode = neo4j_client.mode
    try:
        data = neo4j_client.get_nodes(limit=1)
        node_count = len(data.get("nodes", []))
    except Exception:
        node_count = 0
    return {"mode": mode, "available": True, "node_count": node_count}


@app.post("/api/query", response_model=QueryResponse)
def query(request: QueryRequest):
    """
    Main chat endpoint.
    Pipeline: classifier → intent_parser → query_router → formatter
    """
    question = request.question.strip()

    # Step 1: Classify (reject off-topic)
    is_allowed, reason = classify(question)
    if not is_allowed:
        raise HTTPException(status_code=400, detail=reason)

    # Step 2: Parse intent and extract entities
    intent = parse_intent(question)

    # Step 3: Route to SQL template and execute
    result = route_query(intent)

    if result.get("error") and not result["rows"]:
        raise HTTPException(status_code=400, detail=result["error"])

    # Step 4: Format results with Gemini
    formatted = format_results(
        question=question,
        rows=result["rows"],
        row_count=result["row_count"],
        template_used=result["template"],
    )

    return QueryResponse(
        answer=formatted,
        intent=intent["intent"],
        template_used=result["template"],
        row_count=result["row_count"],
        sql=result.get("sql", ""),
        data=result.get("rows", []),
    )


@app.get("/api/graph/nodes")
def graph_nodes(limit: int = 150):
    """Return nodes and edges for graph visualization."""
    try:
        data = neo4j_client.get_nodes(limit=limit)
        return data
    except Exception as e:
        return {"nodes": [], "edges": [], "error": str(e)}


@app.get("/api/graph/expand/{node_id:path}")
def graph_expand(node_id: str):
    """Expand a node's neighbors in the graph."""
    try:
        data = neo4j_client.expand_node(node_id)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to expand node: {str(e)}")


@app.on_event("shutdown")
def shutdown():
    """Clean up Neo4j connection on shutdown (no-op for SQLite mode)."""
    neo4j_client.close()
