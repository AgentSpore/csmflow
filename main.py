from __future__ import annotations
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from models import (
    CustomerCreate, CustomerUpdate, CustomerResponse,
    TouchpointCreate, TouchpointResponse,
    PlaybookCreate, PlaybookResponse,
    HealthScoreUpdate, CSMStats,
    RenewalUpdate, RenewalPipelineItem,
    QBRCreate, QBRComplete, QBRResponse,
    SegmentUpdate, SegmentStats,
    CustomerTimeline, ExpansionCreate, ExpansionResponse,
    ExpansionStageUpdate, ExpansionPipeline, CSMPerformance,
)
from engine import (
    init_db, create_customer, list_customers, get_customer, update_customer, update_health,
    add_touchpoint, list_touchpoints,
    create_playbook, list_playbooks, get_csm_stats,
    list_upcoming_actions, get_stats_by_owner,
    set_renewal, get_renewal_pipeline, get_at_risk_renewals, delete_customer,
    create_qbr, list_qbrs, get_qbr, complete_qbr, get_upcoming_qbrs,
    set_customer_segment, get_segment_stats,
    get_customer_timeline,
    create_expansion, list_expansions, update_expansion_stage, get_expansion_pipeline,
    get_team_performance,
)

DB_PATH = os.getenv("DB_PATH", "csmflow.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(DB_PATH)
    yield
    await app.state.db.close()


app = FastAPI(
    title="CSMFlow",
    description="Customer success management pipeline: health scores, touchpoints, playbooks, renewal tracking, QBRs, segments.",
    version="0.6.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.6.0"}


# ── Customers ────────────────────────────────────────────────────────────

@app.post("/customers", response_model=CustomerResponse, status_code=201)
async def add_customer(body: CustomerCreate):
    return await create_customer(app.state.db, body.model_dump())


@app.get("/customers", response_model=list[CustomerResponse])
async def get_customers(
    health: str | None = Query(None, description="critical | at_risk | neutral | healthy | champion"),
    plan: str | None = Query(None),
    segment: str | None = Query(None, description="enterprise | mid_market | smb | startup | general"),
):
    return await list_customers(app.state.db, health, plan, segment)


@app.get("/customers/{customer_id}", response_model=CustomerResponse)
async def get_customer_detail(customer_id: int):
    c = await get_customer(app.state.db, customer_id)
    if not c:
        raise HTTPException(404, "Customer not found")
    return c


@app.patch("/customers/{customer_id}", response_model=CustomerResponse)
async def patch_customer(customer_id: int, body: CustomerUpdate):
    c = await update_customer(app.state.db, customer_id, body.model_dump(exclude_unset=True))
    if not c:
        raise HTTPException(404, "Customer not found")
    return c


@app.delete("/customers/{customer_id}", status_code=204)
async def remove_customer(customer_id: int):
    deleted = await delete_customer(app.state.db, customer_id)
    if not deleted:
        raise HTTPException(404, "Customer not found")


@app.post("/customers/{customer_id}/health", response_model=CustomerResponse)
async def update_customer_health(customer_id: int, body: HealthScoreUpdate):
    c = await get_customer(app.state.db, customer_id)
    if not c:
        raise HTTPException(404, "Customer not found")
    return await update_health(
        app.state.db, customer_id,
        body.login_frequency, body.feature_adoption,
        body.support_tickets, body.nps_score, body.days_to_value,
    )


@app.post("/customers/{customer_id}/renewal", response_model=CustomerResponse)
async def update_renewal(customer_id: int, body: RenewalUpdate):
    c = await set_renewal(app.state.db, customer_id, body.renewal_date, body.contract_value)
    if not c:
        raise HTTPException(404, "Customer not found")
    return c


@app.put("/customers/{customer_id}/segment", response_model=CustomerResponse)
async def update_segment(customer_id: int, body: SegmentUpdate):
    c = await set_customer_segment(app.state.db, customer_id, body.segment)
    if not c:
        raise HTTPException(404, "Customer not found")
    return c


# ── Renewals ─────────────────────────────────────────────────────────────

@app.get("/renewals/at-risk", response_model=list[RenewalPipelineItem])
async def at_risk_renewals(
    days: int = Query(90, ge=1, le=365, description="Look-ahead window in days"),
):
    """Renewals where customer health is at_risk or critical, sorted by urgency."""
    return await get_at_risk_renewals(app.state.db, days)


@app.get("/renewals/pipeline", response_model=list[RenewalPipelineItem])
async def renewal_pipeline(
    days: int = Query(90, ge=1, le=365, description="Look-ahead window in days"),
):
    """All upcoming renewals within the next N days, sorted by date."""
    return await get_renewal_pipeline(app.state.db, days)


# ── Touchpoints ──────────────────────────────────────────────────────────

@app.post("/touchpoints", response_model=TouchpointResponse, status_code=201)
async def log_touchpoint(body: TouchpointCreate):
    c = await get_customer(app.state.db, body.customer_id)
    if not c:
        raise HTTPException(404, "Customer not found")
    return await add_touchpoint(app.state.db, body.model_dump())


# /touchpoints/upcoming BEFORE /touchpoints to avoid ambiguity
@app.get("/touchpoints/upcoming")
async def upcoming_actions(
    days: int = Query(7, ge=1, le=90, description="Look-ahead window in days"),
    customer_id: int | None = Query(None),
):
    """List all pending CSM next-actions due within the next N days, sorted by date."""
    return await list_upcoming_actions(app.state.db, days, customer_id)


@app.get("/touchpoints", response_model=list[TouchpointResponse])
async def get_touchpoints(
    customer_id: int | None = Query(None),
):
    return await list_touchpoints(app.state.db, customer_id)


# ── Playbooks ────────────────────────────────────────────────────────────

@app.post("/playbooks", response_model=PlaybookResponse, status_code=201)
async def add_playbook(body: PlaybookCreate):
    return await create_playbook(app.state.db, body.model_dump())

@app.get("/playbooks", response_model=list[PlaybookResponse])
async def get_playbooks():
    return await list_playbooks(app.state.db)


# ── QBRs ─────────────────────────────────────────────────────────────────

@app.post("/qbrs", response_model=QBRResponse, status_code=201)
async def schedule_qbr(body: QBRCreate):
    c = await get_customer(app.state.db, body.customer_id)
    if not c:
        raise HTTPException(404, "Customer not found")
    return await create_qbr(app.state.db, body.model_dump())


@app.get("/qbrs/upcoming", response_model=list[QBRResponse])
async def upcoming_qbrs(
    days: int = Query(30, ge=1, le=365, description="Look-ahead window in days"),
):
    """Scheduled QBRs within the next N days, with customer info."""
    return await get_upcoming_qbrs(app.state.db, days)


@app.get("/qbrs", response_model=list[QBRResponse])
async def get_qbrs(
    customer_id: int | None = Query(None),
    status: str | None = Query(None, description="scheduled | completed"),
):
    return await list_qbrs(app.state.db, customer_id, status)


@app.get("/qbrs/{qbr_id}", response_model=QBRResponse)
async def get_qbr_detail(qbr_id: int):
    q = await get_qbr(app.state.db, qbr_id)
    if not q:
        raise HTTPException(404, "QBR not found")
    return q


@app.post("/qbrs/{qbr_id}/complete", response_model=QBRResponse)
async def complete_qbr_endpoint(qbr_id: int, body: QBRComplete):
    q = await complete_qbr(app.state.db, qbr_id, body.outcome, body.action_items)
    if not q:
        raise HTTPException(404, "QBR not found")
    return q


# ── Stats ────────────────────────────────────────────────────────────────

@app.get("/stats/by-owner")
async def stats_by_owner():
    """Per-CSM breakdown: customers, MRR, avg health, at-risk count, touchpoints last 30d."""
    return await get_stats_by_owner(app.state.db)


@app.get("/stats/segments", response_model=list[SegmentStats])
async def segment_stats():
    """Per-segment breakdown: customers, MRR, avg health, at-risk count."""
    return await get_segment_stats(app.state.db)


@app.get("/stats", response_model=CSMStats)
async def csm_stats():
    return await get_csm_stats(app.state.db)


# ── Customer Timeline ───────────────────────────────────────────────────

@app.get("/customers/{customer_id}/timeline", response_model=CustomerTimeline)
async def customer_timeline(
    customer_id: int,
    limit: int = Query(50, ge=1, le=500),
):
    """Unified activity feed: touchpoints, QBRs, lifecycle events."""
    result = await get_customer_timeline(app.state.db, customer_id, limit)
    if not result:
        raise HTTPException(404, "Customer not found")
    return result


# ── Expansion Tracking ──────────────────────────────────────────────────

@app.post("/expansions", response_model=ExpansionResponse, status_code=201)
async def add_expansion(body: ExpansionCreate):
    c = await get_customer(app.state.db, body.customer_id)
    if not c:
        raise HTTPException(404, "Customer not found")
    return await create_expansion(app.state.db, body.model_dump())


@app.get("/expansions/pipeline", response_model=ExpansionPipeline)
async def expansion_pipeline():
    """Revenue pipeline grouped by opportunity stage."""
    return await get_expansion_pipeline(app.state.db)


@app.get("/expansions", response_model=list[ExpansionResponse])
async def get_expansions(
    customer_id: int | None = Query(None),
    stage: str | None = Query(None, description="identified | qualified | proposal | negotiation | won | lost"),
):
    return await list_expansions(app.state.db, customer_id, stage)


@app.put("/expansions/{expansion_id}/stage", response_model=ExpansionResponse)
async def change_expansion_stage(expansion_id: int, body: ExpansionStageUpdate):
    result = await update_expansion_stage(app.state.db, expansion_id, body.stage)
    if result is None:
        raise HTTPException(404, "Expansion opportunity not found")
    if isinstance(result, str):
        raise HTTPException(422, result)
    return result


# ── Team Performance ────────────────────────────────────────────────────

@app.get("/stats/team", response_model=list[CSMPerformance])
async def team_performance():
    """Per-CSM performance: customers, MRR, health, touchpoint frequency, retention."""
    return await get_team_performance(app.state.db)
