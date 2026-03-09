from __future__ import annotations
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from models import (
    CustomerCreate, CustomerResponse,
    TouchpointCreate, TouchpointResponse,
    PlaybookCreate, PlaybookResponse,
    HealthScoreUpdate, CSMStats,
)
from engine import (
    init_db, create_customer, list_customers, get_customer, update_health,
    add_touchpoint, list_touchpoints,
    create_playbook, list_playbooks, get_csm_stats,
)

DB_PATH = os.getenv("DB_PATH", "csmflow.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(DB_PATH)
    yield
    await app.state.db.close()


app = FastAPI(
    title="CSMFlow",
    description="Customer success management pipeline: health scores, touchpoints, playbooks, QBR tracker.",
    version="0.1.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}


# ── Customers ────────────────────────────────────────────────────────────

@app.post("/customers", response_model=CustomerResponse, status_code=201)
async def add_customer(body: CustomerCreate):
    """Add a new customer to the CS pipeline."""
    return await create_customer(app.state.db, body.model_dump())


@app.get("/customers", response_model=list[CustomerResponse])
async def get_customers(
    health: str | None = Query(None, description="Filter: critical | at_risk | neutral | healthy | champion"),
    plan: str | None = Query(None, description="Filter by plan tier"),
):
    """List customers sorted by MRR. Filter by health label or plan."""
    return await list_customers(app.state.db, health, plan)


@app.get("/customers/{customer_id}", response_model=CustomerResponse)
async def get_customer_detail(customer_id: int):
    """Get customer details with health score and onboarding age."""
    c = await get_customer(app.state.db, customer_id)
    if not c:
        raise HTTPException(404, "Customer not found")
    return c


@app.post("/customers/{customer_id}/health", response_model=CustomerResponse)
async def update_customer_health(customer_id: int, body: HealthScoreUpdate):
    """
    Recalculate health score from usage signals.
    Automatically triggers playbooks for low-health customers.
    """
    c = await get_customer(app.state.db, customer_id)
    if not c:
        raise HTTPException(404, "Customer not found")
    return await update_health(
        app.state.db, customer_id,
        body.login_frequency, body.feature_adoption,
        body.support_tickets, body.nps_score, body.days_to_value,
    )


# ── Touchpoints ──────────────────────────────────────────────────────────

@app.post("/touchpoints", response_model=TouchpointResponse, status_code=201)
async def log_touchpoint(body: TouchpointCreate):
    """Log a customer interaction: call, email, QBR, NPS, etc."""
    c = await get_customer(app.state.db, body.customer_id)
    if not c:
        raise HTTPException(404, "Customer not found")
    return await add_touchpoint(app.state.db, body.model_dump())


@app.get("/touchpoints", response_model=list[TouchpointResponse])
async def get_touchpoints(
    customer_id: int | None = Query(None, description="Filter by customer"),
):
    """List touchpoints. Filter by customer to see full interaction history."""
    return await list_touchpoints(app.state.db, customer_id)


# ── Playbooks ────────────────────────────────────────────────────────────

@app.post("/playbooks", response_model=PlaybookResponse, status_code=201)
async def add_playbook(body: PlaybookCreate):
    """
    Create a CS playbook.
    Trigger options: low_health | onboarding | renewal_90d | nps_detractor | expansion
    """
    return await create_playbook(app.state.db, body.model_dump())


@app.get("/playbooks", response_model=list[PlaybookResponse])
async def get_playbooks():
    """List playbooks sorted by times triggered."""
    return await list_playbooks(app.state.db)


# ── Stats ────────────────────────────────────────────────────────────────

@app.get("/stats", response_model=CSMStats)
async def csm_stats():
    """
    CS dashboard stats: total MRR, avg health score, at-risk count,
    touchpoints this month, upcoming next actions.
    """
    return await get_csm_stats(app.state.db)
