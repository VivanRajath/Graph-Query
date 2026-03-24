"""Vercel serverless entry point.

Vercel's @vercel/python builder looks for a WSGI/ASGI `app` object
in files under the api/ directory.  We simply re-export the FastAPI
instance from the backend package.
"""

import sys
import os

# Ensure the project root is on sys.path so "backend.*" imports work
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from backend.main import app  # noqa: F401 — Vercel picks this up
