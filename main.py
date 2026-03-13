from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import uvicorn
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse, Response
from typing import Optional

from models import (
    CustomerCreate, CustomerUpdate,
    HealthScoreUpdate,
    TouchpointCreate,
    PlaybookCreate,
    SegmentUpdate,
    RenewalUpdate,
    NPSSurveyCreate,
    TagRequest,
    QBRCreate, QBRComplete,
    ExpansionCreate, ExpansionStageUpdate,
    ContactCreate,
    GoalCreate, GoalUpdate,
    HandoffCreate,
    MilestoneCreate,
    EscalationCreate, EscalationUpdate,
    HealthAlertCreate, HealthAlertUpdate,
    SuccessPlanCreate, SuccessPlanUpdate,
    PlanTaskCreate, PlanTaskUpdate,
    FeedbackCreate, FeedbackUpdate, FeedbackVote,
)
import engine as _engine


# ---------------------------------------------------------------------------
# Thin wrapper so app.state.engine holds one object with a .db attribute
# ---------------------------------------------------------------------------

class CSMFlowEngine:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = None

    async def initialize(self):
        self.db = await _engine.init_db(self.db_path)


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(title="CSMFlow", version="1.0.0")


@app.on_event("startup")
async def startup():
    eng = CSMFlowEngine("csmflow.db")
    await eng.initialize()
    app.state.engine = eng


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _engine_db(request: Request):
    return request.app.state.engine.db


def _404(detail: str = "Not found"):
    raise HTTPException(status_code=404, detail=detail)


def _500(detail: str = "Internal server error"):
    raise HTTPException(status_code=500, detail=detail)


# ===========================================================================
# Customers CRUD
# ===========================================================================

@app.post("/customers", status_code=201)
async def create_customer(body: CustomerCreate, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.create_customer(db, body.model_dump())
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/customers")
async def list_customers(
    request: Request,
    health: Optional[str] = Query(None),
    plan: Optional[str] = Query(None),
    segment: Optional[str] = Query(None),
    tag: Optional[str] = Query(None),
):
    try:
        db = _engine_db(request)
        return await _engine.list_customers(db, health=health, plan=plan, segment=segment, tag=tag)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/customers/{customer_id}")
async def get_customer(customer_id: int, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.get_customer(db, customer_id)
        if result is None:
            _404(f"Customer {customer_id} not found")
        return result
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/customers/{customer_id}")
async def update_customer(customer_id: int, body: CustomerUpdate, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.update_customer(db, customer_id, body.model_dump(exclude_none=True))
        if result is None:
            _404(f"Customer {customer_id} not found")
        return result
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/customers/{customer_id}", status_code=204)
async def delete_customer(customer_id: int, request: Request):
    try:
        db = _engine_db(request)
        deleted = await _engine.delete_customer(db, customer_id)
        if not deleted:
            _404(f"Customer {customer_id} not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# Health scores
# ===========================================================================

@app.post("/customers/{customer_id}/health", status_code=201)
async def update_health(customer_id: int, body: HealthScoreUpdate, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.update_health(
            db, customer_id,
            body.login_frequency, body.feature_adoption,
            body.support_tickets, body.nps_score, body.days_to_value,
        )
        if result is None:
            _404(f"Customer {customer_id} not found")
        return result
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/customers/{customer_id}/health-history")
async def get_health_history(customer_id: int, request: Request, limit: int = Query(20, ge=1, le=200)):
    try:
        db = _engine_db(request)
        customer = await _engine.get_customer(db, customer_id)
        if customer is None:
            _404(f"Customer {customer_id} not found")
        rows = await db.execute_fetchall(
            "SELECT * FROM touchpoints WHERE customer_id = ? ORDER BY created_at DESC LIMIT ?",
            (customer_id, limit),
        )
        return {
            "customer_id": customer_id,
            "current_health_score": customer["health_score"],
            "current_health_label": customer["health_label"],
            "history": [dict(r) for r in rows],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# Touchpoints
# ===========================================================================

@app.post("/customers/{customer_id}/touchpoints", status_code=201)
async def create_touchpoint(customer_id: int, body: TouchpointCreate, request: Request):
    try:
        db = _engine_db(request)
        customer = await _engine.get_customer(db, customer_id)
        if customer is None:
            _404(f"Customer {customer_id} not found")
        data = body.model_dump()
        data["customer_id"] = customer_id
        result = await _engine.add_touchpoint(db, data)
        return result
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/customers/{customer_id}/touchpoints")
async def list_customer_touchpoints(customer_id: int, request: Request):
    try:
        db = _engine_db(request)
        customer = await _engine.get_customer(db, customer_id)
        if customer is None:
            _404(f"Customer {customer_id} not found")
        return await _engine.list_touchpoints(db, customer_id=customer_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/touchpoints/{touchpoint_id}")
async def get_touchpoint(touchpoint_id: int, request: Request):
    try:
        db = _engine_db(request)
        rows = await db.execute_fetchall("SELECT * FROM touchpoints WHERE id = ?", (touchpoint_id,))
        if not rows:
            _404(f"Touchpoint {touchpoint_id} not found")
        return _engine._touchpoint_row(rows[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/touchpoints/{touchpoint_id}")
async def update_touchpoint(touchpoint_id: int, body: dict, request: Request):
    try:
        db = _engine_db(request)
        rows = await db.execute_fetchall("SELECT * FROM touchpoints WHERE id = ?", (touchpoint_id,))
        if not rows:
            _404(f"Touchpoint {touchpoint_id} not found")
        allowed = {"type", "summary", "outcome", "next_action", "next_action_date"}
        fields = {k: v for k, v in body.items() if k in allowed and v is not None}
        if fields:
            set_clause = ", ".join(f"{k} = ?" for k in fields)
            values = list(fields.values()) + [touchpoint_id]
            await db.execute(f"UPDATE touchpoints SET {set_clause} WHERE id = ?", values)
            await db.commit()
        rows = await db.execute_fetchall("SELECT * FROM touchpoints WHERE id = ?", (touchpoint_id,))
        return _engine._touchpoint_row(rows[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/touchpoints/{touchpoint_id}", status_code=204)
async def delete_touchpoint(touchpoint_id: int, request: Request):
    try:
        db = _engine_db(request)
        rows = await db.execute_fetchall("SELECT id FROM touchpoints WHERE id = ?", (touchpoint_id,))
        if not rows:
            _404(f"Touchpoint {touchpoint_id} not found")
        await db.execute("DELETE FROM touchpoints WHERE id = ?", (touchpoint_id,))
        await db.commit()
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# Playbooks
# ===========================================================================

@app.post("/playbooks", status_code=201)
async def create_playbook(body: PlaybookCreate, request: Request):
    try:
        db = _engine_db(request)
        return await _engine.create_playbook(db, body.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/playbooks")
async def list_playbooks(request: Request):
    try:
        db = _engine_db(request)
        return await _engine.list_playbooks(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/playbooks/{playbook_id}")
async def get_playbook(playbook_id: int, request: Request):
    try:
        db = _engine_db(request)
        rows = await db.execute_fetchall("SELECT * FROM playbooks WHERE id = ?", (playbook_id,))
        if not rows:
            _404(f"Playbook {playbook_id} not found")
        return _engine._playbook_row(rows[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/playbooks/{playbook_id}")
async def update_playbook(playbook_id: int, request: Request):
    try:
        import json as _json
        db = _engine_db(request)
        rows = await db.execute_fetchall("SELECT * FROM playbooks WHERE id = ?", (playbook_id,))
        if not rows:
            _404(f"Playbook {playbook_id} not found")
        body = await request.json()
        allowed = {"name", "trigger", "description"}
        fields = {k: v for k, v in body.items() if k in allowed and v is not None}
        if "steps" in body and body["steps"] is not None:
            fields["steps"] = _json.dumps(body["steps"])
        if fields:
            set_clause = ", ".join(f"{k} = ?" for k in fields)
            values = list(fields.values()) + [playbook_id]
            await db.execute(f"UPDATE playbooks SET {set_clause} WHERE id = ?", values)
            await db.commit()
        rows = await db.execute_fetchall("SELECT * FROM playbooks WHERE id = ?", (playbook_id,))
        return _engine._playbook_row(rows[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/playbooks/{playbook_id}", status_code=204)
async def delete_playbook(playbook_id: int, request: Request):
    try:
        db = _engine_db(request)
        rows = await db.execute_fetchall("SELECT id FROM playbooks WHERE id = ?", (playbook_id,))
        if not rows:
            _404(f"Playbook {playbook_id} not found")
        await db.execute("DELETE FROM playbooks WHERE id = ?", (playbook_id,))
        await db.commit()
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/playbooks/{playbook_id}/trigger")
async def trigger_playbook(playbook_id: int, request: Request):
    try:
        db = _engine_db(request)
        rows = await db.execute_fetchall("SELECT * FROM playbooks WHERE id = ?", (playbook_id,))
        if not rows:
            _404(f"Playbook {playbook_id} not found")
        await db.execute(
            "UPDATE playbooks SET times_triggered = times_triggered + 1 WHERE id = ?",
            (playbook_id,),
        )
        await db.commit()
        rows = await db.execute_fetchall("SELECT * FROM playbooks WHERE id = ?", (playbook_id,))
        return {"triggered": True, "playbook": _engine._playbook_row(rows[0])}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# Segments
# ===========================================================================

@app.post("/segments", status_code=201)
async def create_segment(request: Request):
    try:
        db = _engine_db(request)
        body = await request.json()
        customer_id = body.get("customer_id")
        segment = body.get("segment")
        if not customer_id or not segment:
            raise HTTPException(status_code=422, detail="customer_id and segment required")
        result = await _engine.set_customer_segment(db, customer_id, segment)
        if result is None:
            _404(f"Customer {customer_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/segments")
async def list_segments(request: Request):
    try:
        db = _engine_db(request)
        return await _engine.get_segment_stats(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/segments/{segment_name}")
async def get_segment(segment_name: str, request: Request):
    try:
        db = _engine_db(request)
        stats = await _engine.get_segment_stats(db)
        for s in stats:
            if s["segment"] == segment_name:
                return s
        _404(f"Segment '{segment_name}' not found")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/segments/{segment_name}")
async def update_segment(segment_name: str, request: Request):
    try:
        db = _engine_db(request)
        body = await request.json()
        new_segment = body.get("segment", segment_name)
        if new_segment != segment_name:
            await db.execute(
                "UPDATE customers SET segment = ? WHERE segment = ?",
                (new_segment, segment_name),
            )
            await db.commit()
        return {"segment": new_segment, "message": "Segment updated"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/segments/{segment_name}", status_code=204)
async def delete_segment(segment_name: str, request: Request):
    try:
        db = _engine_db(request)
        await db.execute(
            "UPDATE customers SET segment = 'general' WHERE segment = ?", (segment_name,)
        )
        await db.commit()
        return Response(status_code=204)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/segments/{segment_name}/evaluate")
async def evaluate_segment(segment_name: str, request: Request):
    try:
        db = _engine_db(request)
        rows = await db.execute_fetchall(
            "SELECT id FROM customers WHERE segment = ?", (segment_name,)
        )
        return {
            "segment": segment_name,
            "customer_count": len(rows),
            "customer_ids": [r["id"] for r in rows],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# Lifecycle stages
# ===========================================================================

@app.post("/customers/{customer_id}/lifecycle", status_code=201)
async def set_lifecycle(customer_id: int, request: Request):
    try:
        db = _engine_db(request)
        customer = await _engine.get_customer(db, customer_id)
        if customer is None:
            _404(f"Customer {customer_id} not found")
        body = await request.json()
        stage = body.get("stage")
        if not stage:
            raise HTTPException(status_code=422, detail="stage is required")
        notes = body.get("notes")
        from engine import _now
        now = _now()
        await db.execute(
            "INSERT INTO touchpoints (customer_id, type, summary, outcome, created_at) VALUES (?, ?, ?, ?, ?)",
            (customer_id, "lifecycle", f"Lifecycle stage changed to: {stage}" + (f". {notes}" if notes else ""), "neutral", now),
        )
        await db.commit()
        return {"customer_id": customer_id, "stage": stage, "recorded_at": now, "notes": notes}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/customers/{customer_id}/lifecycle-history")
async def get_lifecycle_history(customer_id: int, request: Request):
    try:
        db = _engine_db(request)
        customer = await _engine.get_customer(db, customer_id)
        if customer is None:
            _404(f"Customer {customer_id} not found")
        rows = await db.execute_fetchall(
            "SELECT * FROM touchpoints WHERE customer_id = ? AND type = 'lifecycle' ORDER BY created_at DESC",
            (customer_id,),
        )
        return {
            "customer_id": customer_id,
            "customer_name": customer["name"],
            "lifecycle_events": [_engine._touchpoint_row(r) for r in rows],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# Risk scoring
# ===========================================================================

@app.post("/customers/{customer_id}/risk-score", status_code=201)
async def compute_risk_score(customer_id: int, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.compute_churn_risk(db, customer_id)
        if result is None:
            _404(f"Customer {customer_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/customers/{customer_id}/risk-history")
async def get_risk_history(customer_id: int, request: Request):
    try:
        db = _engine_db(request)
        customer = await _engine.get_customer(db, customer_id)
        if customer is None:
            _404(f"Customer {customer_id} not found")
        current_risk = await _engine.compute_churn_risk(db, customer_id)
        return {
            "customer_id": customer_id,
            "customer_name": customer["name"],
            "current_risk": current_risk,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# Renewals
# ===========================================================================

@app.post("/renewals", status_code=201)
async def create_renewal(request: Request):
    try:
        db = _engine_db(request)
        body = await request.json()
        customer_id = body.get("customer_id")
        renewal_date = body.get("renewal_date")
        contract_value = body.get("contract_value", 0)
        if not customer_id or not renewal_date:
            raise HTTPException(status_code=422, detail="customer_id and renewal_date required")
        result = await _engine.set_renewal(db, customer_id, renewal_date, contract_value)
        if result is None:
            _404(f"Customer {customer_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/renewals")
async def list_renewals(request: Request, days: int = Query(90, ge=1, le=365)):
    try:
        db = _engine_db(request)
        return await _engine.get_renewal_pipeline(db, days=days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/renewals/forecast")
async def get_renewals_forecast(request: Request, days: int = Query(90, ge=1, le=365)):
    try:
        db = _engine_db(request)
        pipeline = await _engine.get_renewal_pipeline(db, days=days)
        at_risk = await _engine.get_at_risk_renewals(db, days=days)
        total_value = sum(r["contract_value"] for r in pipeline)
        at_risk_value = sum(r["contract_value"] for r in at_risk)
        return {
            "period_days": days,
            "total_renewals": len(pipeline),
            "total_contract_value": round(total_value, 2),
            "at_risk_count": len(at_risk),
            "at_risk_value": round(at_risk_value, 2),
            "renewals": pipeline,
            "at_risk_renewals": at_risk,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/renewals/{customer_id}")
async def get_renewal(customer_id: int, request: Request):
    try:
        db = _engine_db(request)
        customer = await _engine.get_customer(db, customer_id)
        if customer is None:
            _404(f"Customer {customer_id} not found")
        if not customer.get("renewal_date"):
            _404(f"No renewal set for customer {customer_id}")
        return {
            "customer_id": customer_id,
            "name": customer["name"],
            "renewal_date": customer["renewal_date"],
            "contract_value": customer["contract_value"],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/renewals/{customer_id}")
async def update_renewal(customer_id: int, body: RenewalUpdate, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.set_renewal(db, customer_id, body.renewal_date, body.contract_value)
        if result is None:
            _404(f"Customer {customer_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/renewals/{customer_id}", status_code=204)
async def delete_renewal(customer_id: int, request: Request):
    try:
        db = _engine_db(request)
        cur = await db.execute(
            "UPDATE customers SET renewal_date = NULL, contract_value = NULL WHERE id = ?",
            (customer_id,),
        )
        await db.commit()
        if cur.rowcount == 0:
            _404(f"Customer {customer_id} not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# NPS Surveys
# ===========================================================================

@app.post("/nps", status_code=201)
async def create_nps(body: NPSSurveyCreate, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.record_nps_survey(db, body.model_dump())
        if result is None:
            _404(f"Customer {body.customer_id} not found")
        return result
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/nps")
async def list_nps(
    request: Request,
    customer_id: Optional[int] = Query(None),
    category: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    try:
        db = _engine_db(request)
        return await _engine.list_nps_surveys(db, customer_id=customer_id, category=category, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/nps/trends")
async def get_nps_trends(request: Request):
    try:
        db = _engine_db(request)
        overview = await _engine.get_nps_overview(db)
        return overview
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/nps/{nps_id}")
async def get_nps(nps_id: int, request: Request):
    try:
        db = _engine_db(request)
        rows = await db.execute_fetchall(
            "SELECT n.*, c.name as customer_name FROM nps_surveys n JOIN customers c ON n.customer_id = c.id WHERE n.id = ?",
            (nps_id,),
        )
        if not rows:
            _404(f"NPS survey {nps_id} not found")
        r = rows[0]
        from engine import _nps_category
        return {
            "id": r["id"],
            "customer_id": r["customer_id"],
            "customer_name": r["customer_name"],
            "score": r["score"],
            "category": _nps_category(r["score"]),
            "feedback": r["feedback"],
            "created_at": r["created_at"],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# Customer timeline
# ===========================================================================

@app.get("/customers/{customer_id}/timeline")
async def get_timeline(customer_id: int, request: Request, limit: int = Query(50, ge=1, le=200)):
    try:
        db = _engine_db(request)
        result = await _engine.get_customer_timeline(db, customer_id, limit=limit)
        if result is None:
            _404(f"Customer {customer_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# Dashboard
# ===========================================================================

@app.get("/dashboard")
async def get_dashboard(request: Request):
    try:
        db = _engine_db(request)
        stats = await _engine.get_csm_stats(db)
        segment_stats = await _engine.get_segment_stats(db)
        renewal_pipeline = await _engine.get_renewal_pipeline(db, days=90)
        at_risk = [r for r in renewal_pipeline if r["health_label"] in ("critical", "at_risk")]
        nps_overview = await _engine.get_nps_overview(db)
        return {
            "stats": stats,
            "segments": segment_stats,
            "renewal_pipeline": renewal_pipeline,
            "at_risk_renewals": at_risk,
            "nps": {
                "total_surveys": nps_overview.get("total_surveys"),
                "avg_score": nps_overview.get("avg_score"),
                "nps_score": nps_overview.get("nps_score"),
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# Health Alerts (v1.0.0)
# ===========================================================================

@app.post("/health-alerts", status_code=201)
async def create_health_alert(body: HealthAlertCreate, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.create_health_alert(db, body.model_dump())
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health-alerts")
async def list_health_alerts(
    request: Request,
    customer_id: Optional[int] = Query(None),
    active: Optional[bool] = Query(None),
):
    try:
        db = _engine_db(request)
        return await _engine.list_health_alerts(db, is_enabled=active)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health-alerts/summary")
async def get_alert_summary(request: Request):
    try:
        db = _engine_db(request)
        return await _engine.get_alert_summary(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/health-alerts/evaluate")
async def evaluate_all_alerts(request: Request):
    try:
        db = _engine_db(request)
        customers = await db.execute_fetchall("SELECT id FROM customers")
        triggered_count = 0
        all_fired = []
        for c in customers:
            fired = await _engine.evaluate_health_alerts(db, c["id"])
            triggered_count += len(fired)
            all_fired.extend(fired)
        return {
            "evaluated_customers": len(customers),
            "triggered_count": triggered_count,
            "triggered_alerts": all_fired,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/health-alerts/log/{log_id}/acknowledge")
async def acknowledge_alert(log_id: int, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.acknowledge_alert(db, log_id)
        if result is None:
            _404(f"Alert log entry {log_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health-alerts/{alert_id}")
async def get_health_alert(alert_id: int, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.get_health_alert(db, alert_id)
        if result is None:
            _404(f"Health alert {alert_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/health-alerts/{alert_id}")
async def update_health_alert(alert_id: int, body: HealthAlertUpdate, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.update_health_alert(db, alert_id, body.model_dump(exclude_none=True))
        if result is None:
            _404(f"Health alert {alert_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/health-alerts/{alert_id}", status_code=204)
async def delete_health_alert(alert_id: int, request: Request):
    try:
        db = _engine_db(request)
        deleted = await _engine.delete_health_alert(db, alert_id)
        if not deleted:
            _404(f"Health alert {alert_id} not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health-alerts/{alert_id}/log")
async def get_alert_log(alert_id: int, request: Request, limit: int = Query(50, ge=1, le=500)):
    try:
        db = _engine_db(request)
        alert = await _engine.get_health_alert(db, alert_id)
        if alert is None:
            _404(f"Health alert {alert_id} not found")
        entries = await _engine.list_alert_log(db, alert_id=alert_id, limit=limit)
        return {"alert_id": alert_id, "entries": entries}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# Success Plans (v1.0.0)
# ===========================================================================

@app.post("/success-plans", status_code=201)
async def create_success_plan(body: SuccessPlanCreate, request: Request):
    try:
        db = _engine_db(request)
        customer = await _engine.get_customer(db, body.customer_id)
        if customer is None:
            _404(f"Customer {body.customer_id} not found")
        result = await _engine.create_success_plan(db, body.model_dump())
        return result
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/success-plans")
async def list_success_plans(
    request: Request,
    customer_id: Optional[int] = Query(None),
    status: Optional[str] = Query(None),
):
    try:
        db = _engine_db(request)
        plans = await _engine.list_success_plans(db, customer_id=customer_id, status=status)
        enriched = []
        for p in plans:
            plan_id = p["id"]
            task_total = await db.execute_fetchall(
                "SELECT COUNT(*) as cnt FROM plan_tasks WHERE plan_id = ?", (plan_id,))
            task_completed = await db.execute_fetchall(
                "SELECT COUNT(*) as cnt FROM plan_tasks WHERE plan_id = ? AND status = 'completed'", (plan_id,))
            tasks_total = task_total[0]["cnt"]
            tasks_completed = task_completed[0]["cnt"]
            is_overdue = False
            if p.get("target_date") and p.get("status") not in ("completed", "cancelled"):
                from engine import _now
                today = _now()[:10]
                is_overdue = p["target_date"] < today
            p["tasks_total"] = tasks_total
            p["tasks_completed"] = tasks_completed
            p["is_overdue"] = is_overdue
            enriched.append(p)
        return enriched
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/success-plans/overview")
async def get_success_plan_overview(request: Request):
    try:
        db = _engine_db(request)
        return await _engine.get_plan_overview(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/success-plans/{plan_id}")
async def get_success_plan(plan_id: int, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.get_success_plan(db, plan_id)
        if result is None:
            _404(f"Success plan {plan_id} not found")
        if result.get("target_date") and result.get("status") not in ("completed", "cancelled"):
            from engine import _now
            today = _now()[:10]
            result["is_overdue"] = result["target_date"] < today
        else:
            result["is_overdue"] = False
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/success-plans/{plan_id}")
async def update_success_plan(plan_id: int, body: SuccessPlanUpdate, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.update_success_plan(db, plan_id, body.model_dump(exclude_none=True))
        if result is None:
            _404(f"Success plan {plan_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/success-plans/{plan_id}", status_code=204)
async def delete_success_plan(plan_id: int, request: Request):
    try:
        db = _engine_db(request)
        deleted = await _engine.delete_success_plan(db, plan_id)
        if not deleted:
            _404(f"Success plan {plan_id} not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/success-plans/{plan_id}/tasks", status_code=201)
async def add_plan_task(plan_id: int, body: PlanTaskCreate, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.add_plan_task(db, plan_id, body.model_dump())
        if result is None:
            _404(f"Success plan {plan_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/success-plans/{plan_id}/tasks")
async def list_plan_tasks(
    plan_id: int,
    request: Request,
    status: Optional[str] = Query(None),
):
    try:
        db = _engine_db(request)
        plan = await _engine.get_success_plan(db, plan_id)
        if plan is None:
            _404(f"Success plan {plan_id} not found")
        return await _engine.list_plan_tasks(db, plan_id, status=status)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Plan tasks standalone endpoints

@app.patch("/plan-tasks/{task_id}")
async def update_plan_task(task_id: int, body: PlanTaskUpdate, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.update_plan_task(db, task_id, body.model_dump(exclude_none=True))
        if result is None:
            _404(f"Plan task {task_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/plan-tasks/{task_id}", status_code=204)
async def delete_plan_task(task_id: int, request: Request):
    try:
        db = _engine_db(request)
        deleted = await _engine.delete_plan_task(db, task_id)
        if not deleted:
            _404(f"Plan task {task_id} not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# Customer Feedback (v1.0.0)
# ===========================================================================

@app.post("/feedback", status_code=201)
async def create_feedback(body: FeedbackCreate, request: Request):
    try:
        db = _engine_db(request)
        data = body.model_dump()
        result = await _engine.create_feedback(db, data)
        if result is None:
            _404(f"Customer {body.customer_id} not found")
        customer = await _engine.get_customer(db, body.customer_id)
        result["customer_name"] = customer["name"] if customer else ""
        return result
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/feedback")
async def list_feedback(
    request: Request,
    customer_id: Optional[int] = Query(None),
    category: Optional[str] = Query(None),
    sentiment: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    priority: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    try:
        db = _engine_db(request)
        fb_type = category  # category maps to type in the model
        items = await _engine.list_feedback(
            db, customer_id=customer_id, type=fb_type,
            status=status, priority=priority, limit=limit,
        )
        # Enrich with customer_name
        enriched = []
        for item in items:
            customer = await _engine.get_customer(db, item["customer_id"])
            item["customer_name"] = customer["name"] if customer else ""
            enriched.append(item)
        return enriched
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/feedback/stats")
async def get_feedback_stats(request: Request):
    try:
        db = _engine_db(request)
        stats = await _engine.get_feedback_stats(db)
        enriched_top = []
        for item in stats.get("top_voted", []):
            customer = await _engine.get_customer(db, item["customer_id"])
            item["customer_name"] = customer["name"] if customer else ""
            enriched_top.append(item)
        stats["top_voted"] = enriched_top
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/feedback/{feedback_id}")
async def get_feedback(feedback_id: int, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.get_feedback(db, feedback_id)
        if result is None:
            _404(f"Feedback {feedback_id} not found")
        customer = await _engine.get_customer(db, result["customer_id"])
        result["customer_name"] = customer["name"] if customer else ""
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/feedback/{feedback_id}")
async def update_feedback(feedback_id: int, body: FeedbackUpdate, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.update_feedback(db, feedback_id, body.model_dump(exclude_none=True))
        if result is None:
            _404(f"Feedback {feedback_id} not found")
        customer = await _engine.get_customer(db, result["customer_id"])
        result["customer_name"] = customer["name"] if customer else ""
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/feedback/{feedback_id}", status_code=204)
async def delete_feedback(feedback_id: int, request: Request):
    try:
        db = _engine_db(request)
        deleted = await _engine.delete_feedback(db, feedback_id)
        if not deleted:
            _404(f"Feedback {feedback_id} not found")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/feedback/{feedback_id}/vote")
async def vote_feedback(feedback_id: int, body: FeedbackVote, request: Request):
    try:
        db = _engine_db(request)
        result = await _engine.vote_feedback(db, feedback_id)
        if result is None:
            _404(f"Feedback {feedback_id} not found")
        customer = await _engine.get_customer(db, result["customer_id"])
        result["customer_name"] = customer["name"] if customer else ""
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
