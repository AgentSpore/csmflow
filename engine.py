from __future__ import annotations
import csv
import io
import json
from datetime import datetime, timezone, timedelta

import aiosqlite

SQL = """
CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    company TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    plan TEXT NOT NULL DEFAULT 'starter',
    mrr REAL NOT NULL DEFAULT 0,
    health_score INTEGER NOT NULL DEFAULT 50,
    owner_email TEXT,
    onboarded_at TEXT,
    renewal_date TEXT,
    contract_value REAL,
    notes TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS touchpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    type TEXT NOT NULL DEFAULT 'call',
    summary TEXT NOT NULL,
    outcome TEXT NOT NULL DEFAULT 'neutral',
    next_action TEXT,
    next_action_date TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE IF NOT EXISTS playbooks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    trigger TEXT NOT NULL,
    steps TEXT NOT NULL,
    description TEXT,
    times_triggered INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS qbrs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    scheduled_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'scheduled',
    attendees TEXT NOT NULL DEFAULT '[]',
    agenda TEXT,
    outcome TEXT,
    action_items TEXT NOT NULL DEFAULT '[]',
    completed_at TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_qbrs_customer ON qbrs(customer_id);
CREATE INDEX IF NOT EXISTS idx_qbrs_date ON qbrs(scheduled_date);
"""

MIGRATION_RENEWAL = """
ALTER TABLE customers ADD COLUMN renewal_date TEXT;
ALTER TABLE customers ADD COLUMN contract_value REAL;
"""

HEALTH_LABELS = {
    range(0, 30):  "critical",
    range(30, 50): "at_risk",
    range(50, 70): "neutral",
    range(70, 85): "healthy",
    range(85, 101): "champion",
}

VALID_MILESTONE_TYPES = {
    "onboarding_complete", "first_value", "adoption_milestone",
    "expansion_qualified", "renewal_signed", "champion_identified",
    "executive_sponsor", "integration_complete",
}

VALID_ESCALATION_SEVERITIES = {"critical", "high", "medium", "low"}
VALID_ESCALATION_CATEGORIES = {"support", "billing", "executive", "technical", "legal"}
VALID_ESCALATION_STATUSES = {"open", "investigating", "pending_resolution", "resolved", "closed"}

DEFAULT_SLA_HOURS = {
    "critical": 4,
    "high": 8,
    "medium": 24,
    "low": 72,
}

VALID_HANDOFF_STATUSES = {"pending", "completed", "cancelled"}


def _health_label(score: int) -> str:
    for r, label in HEALTH_LABELS.items():
        if score in r:
            return label
    return "unknown"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log_activity(action: str, detail: str) -> dict:
    """Return an activity dict for audit trail logging."""
    return {"action": action, "detail": detail, "timestamp": _now()}


async def init_db(path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    await db.executescript(SQL)
    # Migration for existing DBs
    try:
        await db.execute("SELECT renewal_date FROM customers LIMIT 1")
    except Exception:
        for stmt in MIGRATION_RENEWAL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    await db.execute(stmt)
                except Exception:
                    pass
    # Migration: add segment column
    try:
        await db.execute("SELECT segment FROM customers LIMIT 1")
    except Exception:
        await db.execute("ALTER TABLE customers ADD COLUMN segment TEXT NOT NULL DEFAULT 'general'")
    # Migration: customer_tags table
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS customer_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            tag TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(customer_id, tag)
        );
        CREATE INDEX IF NOT EXISTS idx_tags_customer ON customer_tags(customer_id);
        CREATE INDEX IF NOT EXISTS idx_tags_tag ON customer_tags(tag);
    """)
    # Migration: nps_surveys table
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS nps_surveys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            score INTEGER NOT NULL,
            feedback TEXT,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_nps_customer ON nps_surveys(customer_id);
        CREATE INDEX IF NOT EXISTS idx_nps_date ON nps_surveys(created_at);
    """)
    # Migration: stakeholder contacts table
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            influence TEXT NOT NULL DEFAULT 'medium',
            phone TEXT,
            notes TEXT,
            last_contacted_at TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(customer_id, email)
        );
        CREATE INDEX IF NOT EXISTS idx_contacts_customer ON contacts(customer_id);
    """)
    # Migration: customer goals table
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS customer_goals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            title TEXT NOT NULL,
            description TEXT,
            target_date TEXT NOT NULL,
            target_value REAL NOT NULL DEFAULT 100,
            current_value REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'active',
            owner_email TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_goals_customer ON customer_goals(customer_id);
        CREATE INDEX IF NOT EXISTS idx_goals_status ON customer_goals(status);
    """)
    # Migration: handoffs table (v0.9.0)
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS handoffs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            from_owner TEXT,
            to_owner TEXT NOT NULL,
            reason TEXT NOT NULL,
            notes TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            completed_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_handoffs_customer ON handoffs(customer_id);
        CREATE INDEX IF NOT EXISTS idx_handoffs_status ON handoffs(status);
        CREATE INDEX IF NOT EXISTS idx_handoffs_from ON handoffs(from_owner);
        CREATE INDEX IF NOT EXISTS idx_handoffs_to ON handoffs(to_owner);
    """)
    # Migration: milestones table (v0.9.0)
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS milestones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            milestone_type TEXT NOT NULL,
            title TEXT NOT NULL,
            notes TEXT,
            achieved_at TEXT NOT NULL,
            created_by TEXT,
            UNIQUE(customer_id, milestone_type)
        );
        CREATE INDEX IF NOT EXISTS idx_milestones_customer ON milestones(customer_id);
        CREATE INDEX IF NOT EXISTS idx_milestones_type ON milestones(milestone_type);
    """)
    # Migration: escalations table (v0.9.0)
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS escalations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            severity TEXT NOT NULL,
            category TEXT NOT NULL,
            assigned_to TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            resolution TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            resolved_at TEXT,
            sla_hours INTEGER NOT NULL DEFAULT 24
        );
        CREATE INDEX IF NOT EXISTS idx_escalations_customer ON escalations(customer_id);
        CREATE INDEX IF NOT EXISTS idx_escalations_status ON escalations(status);
        CREATE INDEX IF NOT EXISTS idx_escalations_severity ON escalations(severity);
        CREATE INDEX IF NOT EXISTS idx_escalations_category ON escalations(category);
    """)
    # Migration: health_alerts + health_alert_log tables (v1.0.0)
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS health_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            condition_type TEXT NOT NULL,
            threshold REAL NOT NULL,
            notification_email TEXT,
            is_enabled INTEGER DEFAULT 1,
            times_triggered INTEGER DEFAULT 0,
            last_triggered_at TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS health_alert_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id INTEGER REFERENCES health_alerts(id) ON DELETE CASCADE,
            customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE,
            customer_name TEXT,
            alert_name TEXT,
            condition_type TEXT,
            threshold REAL,
            actual_value REAL,
            message TEXT,
            is_acknowledged INTEGER DEFAULT 0,
            triggered_at TEXT NOT NULL,
            acknowledged_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_alert_log_alert ON health_alert_log(alert_id);
        CREATE INDEX IF NOT EXISTS idx_alert_log_customer ON health_alert_log(customer_id);
    """)
    # Migration: success_plans + plan_tasks tables (v1.0.0)
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS success_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            title TEXT NOT NULL,
            description TEXT,
            owner_email TEXT,
            status TEXT DEFAULT 'draft',
            start_date TEXT,
            target_date TEXT,
            progress_pct REAL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_plans_customer ON success_plans(customer_id);
        CREATE INDEX IF NOT EXISTS idx_plans_status ON success_plans(status);
        CREATE TABLE IF NOT EXISTS plan_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plan_id INTEGER NOT NULL REFERENCES success_plans(id) ON DELETE CASCADE,
            title TEXT NOT NULL,
            description TEXT,
            assignee_email TEXT,
            status TEXT DEFAULT 'pending',
            priority TEXT DEFAULT 'medium',
            due_date TEXT,
            completed_at TEXT,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_plan_tasks_plan ON plan_tasks(plan_id);
        CREATE INDEX IF NOT EXISTS idx_plan_tasks_status ON plan_tasks(status);
    """)
    # Migration: customer_feedback table (v1.0.0)
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS customer_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            type TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            priority TEXT DEFAULT 'medium',
            status TEXT DEFAULT 'new',
            submitted_by TEXT,
            votes INTEGER DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_feedback_customer ON customer_feedback(customer_id);
        CREATE INDEX IF NOT EXISTS idx_feedback_type ON customer_feedback(type);
        CREATE INDEX IF NOT EXISTS idx_feedback_status ON customer_feedback(status);
    """)
    # Migration: cohorts + cohort_snapshots tables (v1.1.0)
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS cohorts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            cohort_type TEXT NOT NULL,
            criteria TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS cohort_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cohort_id INTEGER NOT NULL REFERENCES cohorts(id) ON DELETE CASCADE,
            snapshot_date TEXT NOT NULL,
            customer_count INTEGER NOT NULL DEFAULT 0,
            avg_health REAL NOT NULL DEFAULT 0,
            avg_mrr REAL NOT NULL DEFAULT 0,
            churned_count INTEGER NOT NULL DEFAULT 0,
            expanded_count INTEGER NOT NULL DEFAULT 0,
            nps_avg REAL NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_cohort_snapshots_cohort ON cohort_snapshots(cohort_id);
        CREATE INDEX IF NOT EXISTS idx_cohort_snapshots_date ON cohort_snapshots(snapshot_date);
    """)
    # Migration: engagement_scores + engagement_config tables (v1.1.0)
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS engagement_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL UNIQUE REFERENCES customers(id) ON DELETE CASCADE,
            score REAL NOT NULL DEFAULT 0,
            touchpoint_frequency REAL NOT NULL DEFAULT 0,
            response_rate REAL NOT NULL DEFAULT 0,
            feature_adoption REAL NOT NULL DEFAULT 0,
            last_interaction_days INTEGER NOT NULL DEFAULT 0,
            decay_factor REAL NOT NULL DEFAULT 1,
            calculated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_engagement_customer ON engagement_scores(customer_id);
        CREATE TABLE IF NOT EXISTS engagement_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            touchpoint_weight REAL NOT NULL DEFAULT 0.4,
            response_weight REAL NOT NULL DEFAULT 0.3,
            adoption_weight REAL NOT NULL DEFAULT 0.3,
            decay_rate_per_day REAL NOT NULL DEFAULT 0.02,
            score_threshold_high REAL NOT NULL DEFAULT 70,
            score_threshold_low REAL NOT NULL DEFAULT 30,
            updated_at TEXT NOT NULL
        );
    """)
    # Seed default engagement config if empty
    existing_cfg = await db.execute_fetchall("SELECT id FROM engagement_config LIMIT 1")
    if not existing_cfg:
        await db.execute(
            "INSERT INTO engagement_config (touchpoint_weight, response_weight, adoption_weight, decay_rate_per_day, score_threshold_high, score_threshold_low, updated_at) VALUES (0.4, 0.3, 0.3, 0.02, 70, 30, ?)",
            (_now(),),
        )
    # Migration: revenue_events table (v1.1.0)
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS revenue_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            mrr_before REAL NOT NULL DEFAULT 0,
            mrr_after REAL NOT NULL DEFAULT 0,
            mrr_delta REAL NOT NULL DEFAULT 0,
            reason TEXT,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_revenue_events_customer ON revenue_events(customer_id);
        CREATE INDEX IF NOT EXISTS idx_revenue_events_type ON revenue_events(event_type);
        CREATE INDEX IF NOT EXISTS idx_revenue_events_date ON revenue_events(created_at);
    """)
    await db.commit()
    return db


async def _get_customer_tags(db: aiosqlite.Connection, customer_id: int) -> list[str]:
    rows = await db.execute_fetchall(
        "SELECT tag FROM customer_tags WHERE customer_id = ? ORDER BY tag", (customer_id,))
    return [r["tag"] for r in rows]


def _customer_row(r: aiosqlite.Row, tags: list[str] | None = None) -> dict:
    days = None
    if r["onboarded_at"]:
        try:
            onboarded = datetime.fromisoformat(r["onboarded_at"]).date()
            days = (datetime.utcnow().date() - onboarded).days
        except Exception:
            pass
    return {
        "id": r["id"], "name": r["name"], "company": r["company"],
        "email": r["email"], "plan": r["plan"], "mrr": r["mrr"],
        "health_score": r["health_score"], "health_label": _health_label(r["health_score"]),
        "owner_email": r["owner_email"], "onboarded_at": r["onboarded_at"],
        "days_since_onboarding": days,
        "renewal_date": r["renewal_date"], "contract_value": r["contract_value"],
        "segment": r["segment"] if "segment" in r.keys() else "general",
        "tags": tags or [],
        "notes": r["notes"], "created_at": r["created_at"],
    }


def _touchpoint_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"], "customer_id": r["customer_id"], "type": r["type"],
        "summary": r["summary"], "outcome": r["outcome"],
        "next_action": r["next_action"], "next_action_date": r["next_action_date"],
        "created_at": r["created_at"],
    }


def _playbook_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"], "name": r["name"], "trigger": r["trigger"],
        "steps": json.loads(r["steps"]), "description": r["description"],
        "times_triggered": r["times_triggered"], "created_at": r["created_at"],
    }


def compute_health_score(login_freq: int, feature_adoption: int,
                         support_tickets: int, nps: int | None, days_to_value: int | None) -> int:
    score = 50
    score += min(login_freq * 2, 20)
    score += min(feature_adoption * 2, 20)
    score -= min(support_tickets * 5, 20)
    if nps is not None:
        score += (nps - 5) * 3
    if days_to_value is not None:
        if days_to_value <= 7:
            score += 10
        elif days_to_value <= 30:
            score += 5
        elif days_to_value > 90:
            score -= 10
    return max(0, min(100, score))


async def create_customer(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    cur = await db.execute(
        """INSERT INTO customers (name, company, email, plan, mrr, owner_email, onboarded_at, notes, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (data["name"], data["company"], data["email"], data.get("plan", "starter"),
         data.get("mrr", 0), data.get("owner_email"), data.get("onboarded_at"),
         data.get("notes"), now)
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM customers WHERE id = ?", (cur.lastrowid,))
    tags = await _get_customer_tags(db, cur.lastrowid)
    return _customer_row(rows[0], tags)


async def list_customers(db: aiosqlite.Connection, health: str | None = None,
                         plan: str | None = None, segment: str | None = None,
                         tag: str | None = None) -> list[dict]:
    q = "SELECT * FROM customers"
    conds, params = [], []
    if plan:
        conds.append("plan = ?"); params.append(plan)
    if segment:
        conds.append("segment = ?"); params.append(segment)
    if tag:
        conds.append("id IN (SELECT customer_id FROM customer_tags WHERE tag = ?)")
        params.append(tag)
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY mrr DESC"
    rows = await db.execute_fetchall(q, params)
    result = []
    for r in rows:
        tags = await _get_customer_tags(db, r["id"])
        result.append(_customer_row(r, tags))
    if health:
        result = [c for c in result if c["health_label"] == health]
    return result


async def get_customer(db: aiosqlite.Connection, customer_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM customers WHERE id = ?", (customer_id,))
    if not rows:
        return None
    tags = await _get_customer_tags(db, customer_id)
    return _customer_row(rows[0], tags)


async def update_customer(db: aiosqlite.Connection, customer_id: int, updates: dict) -> dict | None:
    fields = {k: v for k, v in updates.items() if v is not None}
    if not fields:
        return await get_customer(db, customer_id)
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [customer_id]
    cur = await db.execute(f"UPDATE customers SET {set_clause} WHERE id = ?", values)
    await db.commit()
    if cur.rowcount == 0:
        return None
    return await get_customer(db, customer_id)


async def update_health(db: aiosqlite.Connection, customer_id: int,
                        login_freq: int, feature_adoption: int,
                        support_tickets: int, nps: int | None, days_to_value: int | None) -> dict | None:
    score = compute_health_score(login_freq, feature_adoption, support_tickets, nps, days_to_value)
    await db.execute("UPDATE customers SET health_score = ? WHERE id = ?", (score, customer_id))
    await db.commit()
    label = _health_label(score)
    if label in ("critical", "at_risk"):
        await _trigger_playbooks(db, customer_id, "low_health")
    await evaluate_health_alerts(db, customer_id)
    return await get_customer(db, customer_id)


async def _trigger_playbooks(db: aiosqlite.Connection, customer_id: int, trigger: str):
    rows = await db.execute_fetchall("SELECT id FROM playbooks WHERE trigger = ?", (trigger,))
    for r in rows:
        await db.execute("UPDATE playbooks SET times_triggered = times_triggered + 1 WHERE id = ?", (r["id"],))
    await db.commit()


async def set_renewal(db: aiosqlite.Connection, customer_id: int, renewal_date: str, contract_value: float) -> dict | None:
    cur = await db.execute(
        "UPDATE customers SET renewal_date = ?, contract_value = ? WHERE id = ?",
        (renewal_date, contract_value, customer_id)
    )
    await db.commit()
    if cur.rowcount == 0:
        return None
    return await get_customer(db, customer_id)


async def get_renewal_pipeline(db: aiosqlite.Connection, days: int = 90) -> list[dict]:
    today = datetime.utcnow().date().isoformat()
    until = (datetime.utcnow().date() + timedelta(days=days)).isoformat()
    rows = await db.execute_fetchall(
        """SELECT c.*, (
               SELECT MAX(t.created_at) FROM touchpoints t WHERE t.customer_id = c.id
           ) as last_tp_date
           FROM customers c
           WHERE c.renewal_date IS NOT NULL
             AND c.renewal_date >= ?
             AND c.renewal_date <= ?
           ORDER BY c.renewal_date ASC""",
        (today, until)
    )
    result = []
    for r in rows:
        renewal_dt = datetime.fromisoformat(r["renewal_date"]).date()
        days_until = (renewal_dt - datetime.utcnow().date()).days
        result.append({
            "customer_id": r["id"],
            "name": r["name"],
            "company": r["company"],
            "plan": r["plan"],
            "mrr": r["mrr"],
            "health_score": r["health_score"],
            "health_label": _health_label(r["health_score"]),
            "renewal_date": r["renewal_date"],
            "contract_value": r["contract_value"] or 0,
            "days_until_renewal": days_until,
            "owner_email": r["owner_email"],
            "last_touchpoint_date": r["last_tp_date"],
        })
    return result


async def get_at_risk_renewals(db: aiosqlite.Connection, days: int = 90) -> list[dict]:
    pipeline = await get_renewal_pipeline(db, days)
    return [r for r in pipeline if r["health_label"] in ("critical", "at_risk")]


async def delete_customer(db: aiosqlite.Connection, customer_id: int) -> bool:
    rows = await db.execute_fetchall("SELECT id FROM customers WHERE id = ?", (customer_id,))
    if not rows:
        return False
    await db.execute("DELETE FROM customer_tags WHERE customer_id = ?", (customer_id,))
    await db.execute("DELETE FROM nps_surveys WHERE customer_id = ?", (customer_id,))
    await db.execute("DELETE FROM touchpoints WHERE customer_id = ?", (customer_id,))
    await db.execute("DELETE FROM contacts WHERE customer_id = ?", (customer_id,))
    await db.execute("DELETE FROM customer_goals WHERE customer_id = ?", (customer_id,))
    await db.execute("DELETE FROM handoffs WHERE customer_id = ?", (customer_id,))
    await db.execute("DELETE FROM milestones WHERE customer_id = ?", (customer_id,))
    await db.execute("DELETE FROM escalations WHERE customer_id = ?", (customer_id,))
    await db.execute("DELETE FROM health_alert_log WHERE customer_id = ?", (customer_id,))
    await db.execute("DELETE FROM customer_feedback WHERE customer_id = ?", (customer_id,))
    await db.execute("DELETE FROM engagement_scores WHERE customer_id = ?", (customer_id,))
    await db.execute("DELETE FROM revenue_events WHERE customer_id = ?", (customer_id,))
    for sp in await db.execute_fetchall("SELECT id FROM success_plans WHERE customer_id = ?", (customer_id,)):
        await db.execute("DELETE FROM plan_tasks WHERE plan_id = ?", (sp["id"],))
    await db.execute("DELETE FROM success_plans WHERE customer_id = ?", (customer_id,))
    await db.execute("DELETE FROM customers WHERE id = ?", (customer_id,))
    await db.commit()
    return True


async def add_touchpoint(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    cur = await db.execute(
        """INSERT INTO touchpoints (customer_id, type, summary, outcome, next_action, next_action_date, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (data["customer_id"], data["type"], data["summary"], data.get("outcome", "neutral"),
         data.get("next_action"), data.get("next_action_date"), now)
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM touchpoints WHERE id = ?", (cur.lastrowid,))
    return _touchpoint_row(rows[0])


async def list_touchpoints(db: aiosqlite.Connection, customer_id: int | None = None) -> list[dict]:
    if customer_id:
        rows = await db.execute_fetchall(
            "SELECT * FROM touchpoints WHERE customer_id = ? ORDER BY created_at DESC", (customer_id,))
    else:
        rows = await db.execute_fetchall("SELECT * FROM touchpoints ORDER BY created_at DESC LIMIT 100")
    return [_touchpoint_row(r) for r in rows]


async def create_playbook(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    cur = await db.execute(
        "INSERT INTO playbooks (name, trigger, steps, description, created_at) VALUES (?, ?, ?, ?, ?)",
        (data["name"], data["trigger"], json.dumps(data["steps"]), data.get("description"), now)
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM playbooks WHERE id = ?", (cur.lastrowid,))
    return _playbook_row(rows[0])


async def list_playbooks(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM playbooks ORDER BY times_triggered DESC")
    return [_playbook_row(r) for r in rows]


async def get_csm_stats(db: aiosqlite.Connection) -> dict:
    customers = await db.execute_fetchall("SELECT * FROM customers")
    if not customers:
        return {"total_customers": 0, "total_mrr": 0.0, "avg_health_score": 0.0,
                "at_risk_count": 0, "healthy_count": 0, "touchpoints_this_month": 0,
                "upcoming_actions": 0, "renewals_next_30d": 0, "at_risk_renewal_value": 0.0,
                "total_nps_surveys": 0, "avg_nps": 0.0,
                "total_contacts": 0, "total_goals": 0}
    total_mrr = sum(r["mrr"] for r in customers)
    avg_health = round(sum(r["health_score"] for r in customers) / len(customers), 1)
    at_risk = sum(1 for r in customers if r["health_score"] < 50)
    healthy = sum(1 for r in customers if r["health_score"] >= 70)
    month_start = (datetime.utcnow().replace(day=1)).isoformat()
    tp_rows = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM touchpoints WHERE created_at >= ?", (month_start,))
    tp_count = tp_rows[0]["cnt"] if tp_rows else 0
    upcoming = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM touchpoints WHERE next_action IS NOT NULL AND next_action_date >= date('now')")
    upcoming_count = upcoming[0]["cnt"] if upcoming else 0
    today = datetime.utcnow().date().isoformat()
    until_30 = (datetime.utcnow().date() + timedelta(days=30)).isoformat()
    renew_rows = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM customers WHERE renewal_date IS NOT NULL AND renewal_date >= ? AND renewal_date <= ?",
        (today, until_30))
    renewals_30d = renew_rows[0]["cnt"] if renew_rows else 0
    risk_rows = await db.execute_fetchall(
        "SELECT COALESCE(SUM(contract_value), 0) as val FROM customers WHERE renewal_date IS NOT NULL AND renewal_date >= ? AND renewal_date <= ? AND health_score < 50",
        (today, until_30))
    at_risk_val = risk_rows[0]["val"] if risk_rows else 0
    # NPS stats
    nps_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt, COALESCE(AVG(score), 0) as avg FROM nps_surveys")
    nps_total = nps_rows[0]["cnt"] if nps_rows else 0
    nps_avg = round(nps_rows[0]["avg"], 1) if nps_rows else 0.0
    # Contacts & goals counts
    contacts_count = (await db.execute_fetchall("SELECT COUNT(*) as cnt FROM contacts"))[0]["cnt"]
    goals_count = (await db.execute_fetchall("SELECT COUNT(*) as cnt FROM customer_goals"))[0]["cnt"]
    return {
        "total_customers": len(customers),
        "total_mrr": round(total_mrr, 2),
        "avg_health_score": avg_health,
        "at_risk_count": at_risk,
        "healthy_count": healthy,
        "touchpoints_this_month": tp_count,
        "upcoming_actions": upcoming_count,
        "renewals_next_30d": renewals_30d,
        "at_risk_renewal_value": round(at_risk_val, 2),
        "total_nps_surveys": nps_total,
        "avg_nps": nps_avg,
        "total_contacts": contacts_count,
        "total_goals": goals_count,
    }


async def list_upcoming_actions(db: aiosqlite.Connection,
                                 days: int = 7,
                                 customer_id: int | None = None) -> list[dict]:
    today = datetime.utcnow().date().isoformat()
    until = (datetime.utcnow().date() + timedelta(days=days)).isoformat()
    q = """SELECT t.*, c.name as customer_name, c.company, c.owner_email
           FROM touchpoints t
           JOIN customers c ON t.customer_id = c.id
           WHERE t.next_action IS NOT NULL
             AND t.next_action_date >= ?
             AND t.next_action_date <= ?"""
    params: list = [today, until]
    if customer_id:
        q += " AND t.customer_id = ?"
        params.append(customer_id)
    q += " ORDER BY t.next_action_date ASC"
    rows = await db.execute_fetchall(q, params)
    result = []
    for r in rows:
        d = _touchpoint_row(r)
        d["customer_name"] = r["customer_name"]
        d["company"] = r["company"]
        d["owner_email"] = r["owner_email"]
        result.append(d)
    return result


async def get_stats_by_owner(db: aiosqlite.Connection) -> list[dict]:
    owners = await db.execute_fetchall(
        "SELECT DISTINCT owner_email FROM customers WHERE owner_email IS NOT NULL ORDER BY owner_email"
    )
    result = []
    since_30d = (datetime.utcnow() - timedelta(days=30)).isoformat()
    for o in owners:
        owner = o["owner_email"]
        custs = await db.execute_fetchall(
            "SELECT * FROM customers WHERE owner_email = ?", (owner,)
        )
        total = len(custs)
        mrr = sum(c["mrr"] for c in custs)
        avg_health = round(sum(c["health_score"] for c in custs) / total, 1) if total else 0
        at_risk = sum(1 for c in custs if c["health_score"] < 50)
        ids = tuple(c["id"] for c in custs)
        placeholders = ",".join("?" * len(ids))
        tp_rows = await db.execute_fetchall(
            f"SELECT COUNT(*) as cnt FROM touchpoints WHERE customer_id IN ({placeholders}) AND created_at >= ?",
            (*ids, since_30d)
        ) if ids else []
        tp_count = tp_rows[0]["cnt"] if tp_rows else 0
        result.append({
            "owner_email": owner,
            "customers": total,
            "total_mrr": round(mrr, 2),
            "avg_health_score": avg_health,
            "at_risk_count": at_risk,
            "touchpoints_last_30d": tp_count,
        })
    return result



# ── QBR Tracking ─────────────────────────────────────────────────────────

def _qbr_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"], "customer_id": r["customer_id"],
        "scheduled_date": r["scheduled_date"],
        "status": r["status"],
        "attendees": json.loads(r["attendees"]) if r["attendees"] else [],
        "agenda": r["agenda"],
        "outcome": r["outcome"],
        "action_items": json.loads(r["action_items"]) if r["action_items"] else [],
        "completed_at": r["completed_at"],
        "created_at": r["created_at"],
    }


async def create_qbr(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    cur = await db.execute(
        """INSERT INTO qbrs (customer_id, scheduled_date, attendees, agenda, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (data["customer_id"], data["scheduled_date"],
         json.dumps(data.get("attendees", [])), data.get("agenda"), now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM qbrs WHERE id = ?", (cur.lastrowid,))
    return _qbr_row(rows[0])


async def list_qbrs(db: aiosqlite.Connection, customer_id: int | None = None,
                     status: str | None = None) -> list[dict]:
    q = "SELECT * FROM qbrs WHERE 1=1"
    params: list = []
    if customer_id:
        q += " AND customer_id = ?"
        params.append(customer_id)
    if status:
        q += " AND status = ?"
        params.append(status)
    q += " ORDER BY scheduled_date DESC"
    rows = await db.execute_fetchall(q, params)
    return [_qbr_row(r) for r in rows]


async def get_qbr(db: aiosqlite.Connection, qbr_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM qbrs WHERE id = ?", (qbr_id,))
    return _qbr_row(rows[0]) if rows else None


async def complete_qbr(db: aiosqlite.Connection, qbr_id: int, outcome: str,
                        action_items: list[str]) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM qbrs WHERE id = ?", (qbr_id,))
    if not rows:
        return None
    now = _now()
    await db.execute(
        "UPDATE qbrs SET status = 'completed', outcome = ?, action_items = ?, completed_at = ? WHERE id = ?",
        (outcome, json.dumps(action_items), now, qbr_id),
    )
    await db.commit()
    return await get_qbr(db, qbr_id)


async def get_upcoming_qbrs(db: aiosqlite.Connection, days: int = 30) -> list[dict]:
    today = datetime.utcnow().date().isoformat()
    until = (datetime.utcnow().date() + timedelta(days=days)).isoformat()
    rows = await db.execute_fetchall(
        """SELECT q.*, c.name as customer_name, c.company, c.health_score
           FROM qbrs q JOIN customers c ON q.customer_id = c.id
           WHERE q.status = 'scheduled' AND q.scheduled_date >= ? AND q.scheduled_date <= ?
           ORDER BY q.scheduled_date ASC""",
        (today, until),
    )
    result = []
    for r in rows:
        d = _qbr_row(r)
        d["customer_name"] = r["customer_name"]
        d["company"] = r["company"]
        d["health_score"] = r["health_score"]
        result.append(d)
    return result


# ── Segments ─────────────────────────────────────────────────────────────

async def set_customer_segment(db: aiosqlite.Connection, customer_id: int, segment: str) -> dict | None:
    cur = await db.execute("UPDATE customers SET segment = ? WHERE id = ?", (segment, customer_id))
    await db.commit()
    if cur.rowcount == 0:
        return None
    return await get_customer(db, customer_id)


async def get_segment_stats(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("""
        SELECT segment, COUNT(*) as count, ROUND(SUM(mrr), 2) as total_mrr,
               ROUND(AVG(health_score), 1) as avg_health,
               SUM(CASE WHEN health_score < 50 THEN 1 ELSE 0 END) as at_risk
        FROM customers GROUP BY segment ORDER BY total_mrr DESC
    """)
    return [{"segment": r["segment"], "customers": r["count"], "total_mrr": r["total_mrr"],
             "avg_health_score": r["avg_health"], "at_risk_count": r["at_risk"]} for r in rows]


# ── Customer Timeline ────────────────────────────────────────────────────

async def get_customer_timeline(db: aiosqlite.Connection, customer_id: int,
                                 limit: int = 50) -> dict | None:
    customer = await get_customer(db, customer_id)
    if not customer:
        return None

    events = []

    # Touchpoints
    tp_rows = await db.execute_fetchall(
        "SELECT * FROM touchpoints WHERE customer_id = ? ORDER BY created_at DESC",
        (customer_id,),
    )
    for tp in tp_rows:
        events.append({
            "type": "touchpoint",
            "subtype": tp["type"],
            "summary": tp["summary"],
            "outcome": tp["outcome"],
            "timestamp": tp["created_at"],
        })

    # QBRs
    qbr_rows = await db.execute_fetchall(
        "SELECT * FROM qbrs WHERE customer_id = ? ORDER BY created_at DESC",
        (customer_id,),
    )
    for q in qbr_rows:
        if q["status"] == "completed":
            events.append({
                "type": "qbr_completed",
                "subtype": "qbr",
                "summary": f"QBR completed: {q['outcome'] or 'no outcome recorded'}",
                "outcome": "positive",
                "timestamp": q["completed_at"] or q["created_at"],
            })
        else:
            events.append({
                "type": "qbr_scheduled",
                "subtype": "qbr",
                "summary": f"QBR scheduled for {q['scheduled_date']}",
                "outcome": "neutral",
                "timestamp": q["created_at"],
            })

    # NPS surveys
    nps_rows = await db.execute_fetchall(
        "SELECT * FROM nps_surveys WHERE customer_id = ? ORDER BY created_at DESC",
        (customer_id,),
    )
    for n in nps_rows:
        cat = _nps_category(n["score"])
        events.append({
            "type": "nps_survey",
            "subtype": cat,
            "summary": f"NPS survey: {n['score']}/10 ({cat}){' -- ' + n['feedback'] if n['feedback'] else ''}",
            "outcome": "positive" if cat == "promoter" else ("negative" if cat == "detractor" else "neutral"),
            "timestamp": n["created_at"],
        })

    # Goal updates
    goal_rows = await db.execute_fetchall(
        "SELECT * FROM customer_goals WHERE customer_id = ? ORDER BY created_at DESC",
        (customer_id,),
    )
    for g in goal_rows:
        progress = round(g["current_value"] / max(g["target_value"], 0.01) * 100, 1)
        events.append({
            "type": "goal",
            "subtype": g["status"],
            "summary": f"Goal \"{g['title']}\": {progress}% complete (target: {g['target_date']})",
            "outcome": "positive" if g["status"] == "completed" else ("negative" if g["status"] == "at_risk" else "neutral"),
            "timestamp": g["updated_at"],
        })

    # Milestones (v0.9.0)
    ms_rows = await db.execute_fetchall(
        "SELECT * FROM milestones WHERE customer_id = ? ORDER BY achieved_at DESC",
        (customer_id,),
    )
    for m in ms_rows:
        events.append({
            "type": "milestone",
            "subtype": m["milestone_type"],
            "summary": f"Milestone achieved: {m['title']}",
            "outcome": "positive",
            "timestamp": m["achieved_at"],
        })

    # Escalations (v0.9.0)
    esc_rows = await db.execute_fetchall(
        "SELECT * FROM escalations WHERE customer_id = ? ORDER BY created_at DESC",
        (customer_id,),
    )
    for e in esc_rows:
        if e["status"] in ("resolved", "closed"):
            events.append({
                "type": "escalation_resolved",
                "subtype": e["severity"],
                "summary": f"Escalation resolved: {e['title']} ({e['severity']}){' -- ' + e['resolution'] if e['resolution'] else ''}",
                "outcome": "positive",
                "timestamp": e["resolved_at"] or e["updated_at"],
            })
        else:
            events.append({
                "type": "escalation",
                "subtype": e["severity"],
                "summary": f"Escalation [{e['severity']}]: {e['title']} (status: {e['status']})",
                "outcome": "negative" if e["severity"] in ("critical", "high") else "neutral",
                "timestamp": e["created_at"],
            })

    # Handoffs (v0.9.0)
    ho_rows = await db.execute_fetchall(
        "SELECT * FROM handoffs WHERE customer_id = ? ORDER BY created_at DESC",
        (customer_id,),
    )
    for h in ho_rows:
        from_label = h["from_owner"] or "unassigned"
        events.append({
            "type": "handoff",
            "subtype": h["status"],
            "summary": f"Handoff {h['status']}: {from_label} -> {h['to_owner']} ({h['reason']})",
            "outcome": "positive" if h["status"] == "completed" else "neutral",
            "timestamp": h["completed_at"] or h["created_at"],
        })

    # Feedback (v1.0.0)
    try:
        fb_rows = await db.execute_fetchall(
            "SELECT * FROM customer_feedback WHERE customer_id = ? ORDER BY created_at DESC",
            (customer_id,),
        )
        for fb in fb_rows:
            events.append({
                "type": "feedback",
                "subtype": fb["type"],
                "summary": f"Feedback [{fb['type']}]: {fb['title']} (priority: {fb['priority']}, status: {fb['status']})",
                "outcome": "negative" if fb["priority"] in ("high", "critical") else "neutral",
                "timestamp": fb["created_at"],
            })
    except Exception:
        pass

    # Customer creation
    events.append({
        "type": "customer_created",
        "subtype": "lifecycle",
        "summary": f"Customer {customer['name']} ({customer['company']}) added",
        "outcome": "positive",
        "timestamp": customer["created_at"],
    })

    events.sort(key=lambda e: e["timestamp"], reverse=True)
    return {
        "customer_id": customer_id,
        "customer_name": customer["name"],
        "company": customer["company"],
        "total_events": len(events),
        "events": events[:limit],
    }


# ── Expansion Tracking ──────────────────────────────────────────────────

VALID_OPP_STAGES = {"identified", "qualified", "proposal", "negotiation", "won", "lost"}


async def _migrate_expansions(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS expansions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            type TEXT NOT NULL DEFAULT 'upsell',
            description TEXT NOT NULL,
            expected_mrr REAL NOT NULL DEFAULT 0,
            stage TEXT NOT NULL DEFAULT 'identified',
            owner_email TEXT,
            notes TEXT,
            closed_at TEXT,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_exp_customer ON expansions(customer_id);
        CREATE INDEX IF NOT EXISTS idx_exp_stage ON expansions(stage);
    """)
    await db.commit()


async def create_expansion(db: aiosqlite.Connection, data: dict) -> dict:
    await _migrate_expansions(db)
    now = _now()
    cur = await db.execute(
        """INSERT INTO expansions (customer_id, type, description, expected_mrr, stage, owner_email, notes, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (data["customer_id"], data.get("type", "upsell"), data["description"],
         data.get("expected_mrr", 0), data.get("stage", "identified"),
         data.get("owner_email"), data.get("notes"), now),
    )
    await db.commit()
    return await _get_expansion(db, cur.lastrowid)


async def _get_expansion(db: aiosqlite.Connection, exp_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM expansions WHERE id = ?", (exp_id,))
    if not rows:
        return None
    r = rows[0]
    return {
        "id": r["id"], "customer_id": r["customer_id"], "type": r["type"],
        "description": r["description"], "expected_mrr": r["expected_mrr"],
        "stage": r["stage"], "owner_email": r["owner_email"],
        "notes": r["notes"], "closed_at": r["closed_at"], "created_at": r["created_at"],
    }


async def list_expansions(db: aiosqlite.Connection, customer_id: int | None = None,
                           stage: str | None = None) -> list[dict]:
    await _migrate_expansions(db)
    q = "SELECT * FROM expansions WHERE 1=1"
    params: list = []
    if customer_id:
        q += " AND customer_id = ?"; params.append(customer_id)
    if stage:
        q += " AND stage = ?"; params.append(stage)
    q += " ORDER BY created_at DESC"
    rows = await db.execute_fetchall(q, params)
    return [{
        "id": r["id"], "customer_id": r["customer_id"], "type": r["type"],
        "description": r["description"], "expected_mrr": r["expected_mrr"],
        "stage": r["stage"], "owner_email": r["owner_email"],
        "notes": r["notes"], "closed_at": r["closed_at"], "created_at": r["created_at"],
    } for r in rows]


async def update_expansion_stage(db: aiosqlite.Connection, exp_id: int,
                                  stage: str) -> dict | str | None:
    await _migrate_expansions(db)
    if stage not in VALID_OPP_STAGES:
        return f"Invalid stage. Must be one of: {', '.join(sorted(VALID_OPP_STAGES))}"
    rows = await db.execute_fetchall("SELECT id FROM expansions WHERE id = ?", (exp_id,))
    if not rows:
        return None
    closed_at = None
    if stage in ("won", "lost"):
        closed_at = _now()
    await db.execute(
        "UPDATE expansions SET stage = ?, closed_at = ? WHERE id = ?",
        (stage, closed_at, exp_id),
    )
    await db.commit()
    return await _get_expansion(db, exp_id)


async def get_expansion_pipeline(db: aiosqlite.Connection) -> dict:
    await _migrate_expansions(db)
    rows = await db.execute_fetchall("""
        SELECT stage, COUNT(*) as count, COALESCE(SUM(expected_mrr), 0) as total_mrr
        FROM expansions GROUP BY stage ORDER BY total_mrr DESC
    """)
    stages = [{"stage": r["stage"], "count": r["count"], "total_mrr": round(r["total_mrr"], 2)} for r in rows]
    total = sum(s["total_mrr"] for s in stages)
    won = next((s["total_mrr"] for s in stages if s["stage"] == "won"), 0)
    return {
        "total_opportunities": sum(s["count"] for s in stages),
        "total_pipeline_mrr": round(total, 2),
        "won_mrr": round(won, 2),
        "stages": stages,
    }


# ── Team Performance ────────────────────────────────────────────────────

async def get_team_performance(db: aiosqlite.Connection) -> list[dict]:
    owners = await db.execute_fetchall(
        "SELECT DISTINCT owner_email FROM customers WHERE owner_email IS NOT NULL"
    )
    result = []
    for o in owners:
        email = o["owner_email"]
        custs = await db.execute_fetchall(
            "SELECT * FROM customers WHERE owner_email = ?", (email,))
        total = len(custs)
        if not total:
            continue
        mrr = sum(c["mrr"] for c in custs)
        avg_health = round(sum(c["health_score"] for c in custs) / total, 1)
        at_risk = sum(1 for c in custs if c["health_score"] < 50)
        champions = sum(1 for c in custs if c["health_score"] >= 85)

        ids = tuple(c["id"] for c in custs)
        placeholders = ",".join("?" * len(ids))

        since_30d = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        tp_rows = await db.execute_fetchall(
            f"SELECT COUNT(*) as cnt FROM touchpoints WHERE customer_id IN ({placeholders}) AND created_at >= ?",
            (*ids, since_30d),
        )
        tp_30d = tp_rows[0]["cnt"] if tp_rows else 0

        today = datetime.utcnow().date().isoformat()
        past_renewals = await db.execute_fetchall(
            f"SELECT COUNT(*) as total, SUM(CASE WHEN health_score >= 50 THEN 1 ELSE 0 END) as healthy FROM customers WHERE owner_email = ? AND renewal_date IS NOT NULL AND renewal_date <= ?",
            (email, today),
        )
        r_total = past_renewals[0]["total"] if past_renewals else 0
        r_healthy = past_renewals[0]["healthy"] if past_renewals else 0
        retention_rate = round(r_healthy / max(r_total, 1) * 100, 1)

        result.append({
            "owner_email": email,
            "customers": total,
            "total_mrr": round(mrr, 2),
            "avg_health_score": avg_health,
            "at_risk_count": at_risk,
            "champions": champions,
            "touchpoints_last_30d": tp_30d,
            "touchpoint_frequency": round(tp_30d / total, 1),
            "retention_rate": retention_rate,
        })
    result.sort(key=lambda x: x["total_mrr"], reverse=True)
    return result


# ── Customer Tags ───────────────────────────────────────────────────────

async def add_customer_tag(db: aiosqlite.Connection, customer_id: int, tag: str) -> dict | None:
    customer = await get_customer(db, customer_id)
    if not customer:
        return None
    now = _now()
    try:
        await db.execute(
            "INSERT INTO customer_tags (customer_id, tag, created_at) VALUES (?, ?, ?)",
            (customer_id, tag.strip().lower(), now),
        )
        await db.commit()
    except Exception:
        pass  # UNIQUE constraint -- tag already exists, that's fine
    return await get_customer(db, customer_id)


async def remove_customer_tag(db: aiosqlite.Connection, customer_id: int, tag: str) -> dict | None:
    customer = await get_customer(db, customer_id)
    if not customer:
        return None
    await db.execute(
        "DELETE FROM customer_tags WHERE customer_id = ? AND tag = ?",
        (customer_id, tag.strip().lower()),
    )
    await db.commit()
    return await get_customer(db, customer_id)


# ── NPS Surveys ─────────────────────────────────────────────────────────

def _nps_category(score: int) -> str:
    if score >= 9:
        return "promoter"
    elif score >= 7:
        return "passive"
    return "detractor"


async def record_nps_survey(db: aiosqlite.Connection, data: dict) -> dict | None:
    """Record an NPS survey response. Triggers nps_detractor playbook if score < 7."""
    customer = await get_customer(db, data["customer_id"])
    if not customer:
        return None
    now = _now()
    cur = await db.execute(
        "INSERT INTO nps_surveys (customer_id, score, feedback, created_at) VALUES (?, ?, ?, ?)",
        (data["customer_id"], data["score"], data.get("feedback"), now),
    )
    await db.commit()
    # Trigger playbook for detractors
    if data["score"] < 7:
        await _trigger_playbooks(db, data["customer_id"], "nps_detractor")
    rows = await db.execute_fetchall("SELECT * FROM nps_surveys WHERE id = ?", (cur.lastrowid,))
    r = rows[0]
    return {
        "id": r["id"],
        "customer_id": r["customer_id"],
        "customer_name": customer["name"],
        "score": r["score"],
        "category": _nps_category(r["score"]),
        "feedback": r["feedback"],
        "created_at": r["created_at"],
    }


async def list_nps_surveys(db: aiosqlite.Connection, customer_id: int | None = None,
                            category: str | None = None,
                            limit: int = 100) -> list[dict]:
    q = """SELECT n.*, c.name as customer_name FROM nps_surveys n
           JOIN customers c ON n.customer_id = c.id WHERE 1=1"""
    params: list = []
    if customer_id:
        q += " AND n.customer_id = ?"
        params.append(customer_id)
    q += " ORDER BY n.created_at DESC LIMIT ?"
    params.append(limit)
    rows = await db.execute_fetchall(q, params)
    result = []
    for r in rows:
        cat = _nps_category(r["score"])
        if category and cat != category:
            continue
        result.append({
            "id": r["id"],
            "customer_id": r["customer_id"],
            "customer_name": r["customer_name"],
            "score": r["score"],
            "category": cat,
            "feedback": r["feedback"],
            "created_at": r["created_at"],
        })
    return result


async def get_nps_overview(db: aiosqlite.Connection) -> dict:
    """NPS overview: total, avg score, NPS score, category breakdown, per-segment, 6-month trend."""
    rows = await db.execute_fetchall("SELECT * FROM nps_surveys ORDER BY created_at DESC")
    total = len(rows)
    if not total:
        return {
            "total_surveys": 0, "avg_score": 0.0, "nps_score": 0.0,
            "promoters_pct": 0.0, "passives_pct": 0.0, "detractors_pct": 0.0,
            "by_segment": [], "trend": [],
        }

    scores = [r["score"] for r in rows]
    avg_score = round(sum(scores) / total, 1)
    promoters = sum(1 for s in scores if s >= 9)
    passives = sum(1 for s in scores if 7 <= s < 9)
    detractors = sum(1 for s in scores if s < 7)
    nps = round((promoters - detractors) / total * 100, 1)

    # Per-segment breakdown
    seg_rows = await db.execute_fetchall("""
        SELECT c.segment, COUNT(*) as cnt, ROUND(AVG(n.score), 1) as avg,
               SUM(CASE WHEN n.score >= 9 THEN 1 ELSE 0 END) as promo,
               SUM(CASE WHEN n.score < 7 THEN 1 ELSE 0 END) as detract
        FROM nps_surveys n JOIN customers c ON n.customer_id = c.id
        GROUP BY c.segment ORDER BY avg DESC
    """)
    by_segment = []
    for s in seg_rows:
        seg_total = s["cnt"]
        by_segment.append({
            "segment": s["segment"],
            "responses": seg_total,
            "avg_score": s["avg"],
            "nps_score": round((s["promo"] - s["detract"]) / seg_total * 100, 1) if seg_total else 0.0,
        })

    # Monthly trend (last 6 months)
    six_months_ago = (datetime.utcnow() - timedelta(days=180)).isoformat()
    trend_rows = await db.execute_fetchall(
        "SELECT * FROM nps_surveys WHERE created_at >= ? ORDER BY created_at ASC",
        (six_months_ago,),
    )
    monthly: dict[str, list[int]] = {}
    for r in trend_rows:
        month = r["created_at"][:7]  # YYYY-MM
        monthly.setdefault(month, []).append(r["score"])
    trend = []
    for period, period_scores in sorted(monthly.items()):
        cnt = len(period_scores)
        p = sum(1 for s in period_scores if s >= 9)
        d = sum(1 for s in period_scores if s < 7)
        pa = cnt - p - d
        trend.append({
            "period": period,
            "avg_score": round(sum(period_scores) / cnt, 1),
            "promoters": p,
            "passives": pa,
            "detractors": d,
            "responses": cnt,
            "nps_score": round((p - d) / cnt * 100, 1),
        })

    return {
        "total_surveys": total,
        "avg_score": avg_score,
        "nps_score": nps,
        "promoters_pct": round(promoters / total * 100, 1),
        "passives_pct": round(passives / total * 100, 1),
        "detractors_pct": round(detractors / total * 100, 1),
        "by_segment": by_segment,
        "trend": trend,
    }


# ── Churn Risk Assessment ──────────────────────────────────────────────

async def compute_churn_risk(db: aiosqlite.Connection, customer_id: int) -> dict | None:
    """Compute churn risk score (0-100) based on multiple signals."""
    customer = await get_customer(db, customer_id)
    if not customer:
        return None

    factors = []
    risk_score = 0

    # Factor 1: Health score (0-30 points)
    hs = customer["health_score"]
    if hs < 30:
        impact = 30
        factors.append({"factor": "health_score", "impact": impact, "detail": f"Critical health score: {hs}"})
    elif hs < 50:
        impact = 20
        factors.append({"factor": "health_score", "impact": impact, "detail": f"At-risk health score: {hs}"})
    elif hs < 70:
        impact = 10
        factors.append({"factor": "health_score", "impact": impact, "detail": f"Neutral health score: {hs}"})
    else:
        impact = 0
    risk_score += impact

    # Factor 2: Renewal proximity without engagement (0-20 points)
    if customer["renewal_date"]:
        try:
            renewal_dt = datetime.fromisoformat(customer["renewal_date"]).date()
            days_until = (renewal_dt - datetime.utcnow().date()).days
            if days_until <= 30:
                impact = 20
                factors.append({"factor": "renewal_imminent", "impact": impact,
                                "detail": f"Renewal in {days_until} days"})
            elif days_until <= 60:
                impact = 10
                factors.append({"factor": "renewal_approaching", "impact": impact,
                                "detail": f"Renewal in {days_until} days"})
            else:
                impact = 0
            risk_score += impact
        except Exception:
            pass

    # Factor 3: Touchpoint frequency (0-20 points)
    since_30d = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    tp_rows = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM touchpoints WHERE customer_id = ? AND created_at >= ?",
        (customer_id, since_30d),
    )
    tp_count = tp_rows[0]["cnt"] if tp_rows else 0
    if tp_count == 0:
        impact = 20
        factors.append({"factor": "no_recent_touchpoints", "impact": impact,
                        "detail": "No touchpoints in last 30 days"})
    elif tp_count <= 1:
        impact = 10
        factors.append({"factor": "low_touchpoint_frequency", "impact": impact,
                        "detail": f"Only {tp_count} touchpoint(s) in last 30 days"})
    else:
        impact = 0
    risk_score += impact

    # Factor 4: NPS trend (0-15 points)
    nps_rows = await db.execute_fetchall(
        "SELECT score FROM nps_surveys WHERE customer_id = ? ORDER BY created_at DESC LIMIT 3",
        (customer_id,),
    )
    if nps_rows:
        latest_nps = nps_rows[0]["score"]
        if latest_nps < 7:
            impact = 15
            factors.append({"factor": "nps_detractor", "impact": impact,
                            "detail": f"Latest NPS: {latest_nps} (detractor)"})
        elif latest_nps < 9 and len(nps_rows) >= 2 and nps_rows[1]["score"] >= 9:
            impact = 10
            factors.append({"factor": "nps_declining", "impact": impact,
                            "detail": f"NPS dropped from {nps_rows[1]['score']} to {latest_nps}"})
        else:
            impact = 0
        risk_score += impact
    else:
        impact = 5
        factors.append({"factor": "no_nps_data", "impact": impact,
                        "detail": "No NPS survey data available"})
        risk_score += impact

    # Factor 5: Expansion activity (0-15 points)
    try:
        exp_rows = await db.execute_fetchall(
            "SELECT stage FROM expansions WHERE customer_id = ? AND stage NOT IN ('won', 'lost')",
            (customer_id,),
        )
        if not exp_rows:
            impact = 5
            factors.append({"factor": "no_expansion_activity", "impact": impact,
                            "detail": "No active expansion opportunities"})
            risk_score += impact
        lost_rows = await db.execute_fetchall(
            "SELECT COUNT(*) as cnt FROM expansions WHERE customer_id = ? AND stage = 'lost'",
            (customer_id,),
        )
        if lost_rows and lost_rows[0]["cnt"] >= 2:
            impact = 10
            factors.append({"factor": "multiple_lost_expansions", "impact": impact,
                            "detail": f"{lost_rows[0]['cnt']} lost expansion opportunities"})
            risk_score += impact
    except Exception:
        pass  # expansions table may not exist yet

    risk_score = min(100, risk_score)

    # Risk level
    if risk_score >= 70:
        level = "critical"
        rec = "Immediate intervention required: schedule executive sponsor call, offer incentives, review account strategy"
    elif risk_score >= 50:
        level = "high"
        rec = "Schedule urgent CSM review, increase touchpoint frequency, address detractor feedback"
    elif risk_score >= 30:
        level = "medium"
        rec = "Monitor closely, schedule proactive check-in, explore expansion opportunities"
    else:
        level = "low"
        rec = "Continue regular cadence, look for expansion and advocacy opportunities"

    return {
        "customer_id": customer_id,
        "customer_name": customer["name"],
        "risk_score": risk_score,
        "risk_level": level,
        "factors": factors,
        "recommendation": rec,
    }


# ── Stakeholder Contacts ────────────────────────────────────────────────

def _contact_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"], "customer_id": r["customer_id"],
        "name": r["name"], "email": r["email"],
        "role": r["role"], "influence": r["influence"],
        "phone": r["phone"], "notes": r["notes"],
        "last_contacted_at": r["last_contacted_at"],
        "created_at": r["created_at"],
    }


async def add_contact(db: aiosqlite.Connection, customer_id: int, data: dict) -> dict | str:
    now = _now()
    try:
        cur = await db.execute(
            """INSERT INTO contacts (customer_id, name, email, role, influence, phone, notes, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (customer_id, data["name"], data["email"],
             data.get("role", "user"), data.get("influence", "medium"),
             data.get("phone"), data.get("notes"), now),
        )
        await db.commit()
    except Exception:
        return "duplicate_email"
    rows = await db.execute_fetchall("SELECT * FROM contacts WHERE id = ?", (cur.lastrowid,))
    return _contact_row(rows[0])


async def list_contacts(db: aiosqlite.Connection, customer_id: int) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM contacts WHERE customer_id = ? ORDER BY influence DESC, name ASC",
        (customer_id,),
    )
    return [_contact_row(r) for r in rows]


async def update_contact_last_contacted(db: aiosqlite.Connection, contact_id: int) -> dict | None:
    now = _now()
    cur = await db.execute(
        "UPDATE contacts SET last_contacted_at = ? WHERE id = ?", (now, contact_id),
    )
    await db.commit()
    if cur.rowcount == 0:
        return None
    rows = await db.execute_fetchall("SELECT * FROM contacts WHERE id = ?", (contact_id,))
    return _contact_row(rows[0])


async def delete_contact(db: aiosqlite.Connection, contact_id: int) -> bool:
    cur = await db.execute("DELETE FROM contacts WHERE id = ?", (contact_id,))
    await db.commit()
    return cur.rowcount > 0


# ── Customer Goals ──────────────────────────────────────────────────────

def _goal_row(r: aiosqlite.Row) -> dict:
    target = r["target_value"]
    current = r["current_value"]
    progress = round(current / max(target, 0.01) * 100, 1)
    days_remaining = None
    try:
        target_dt = datetime.fromisoformat(r["target_date"]).date()
        days_remaining = (target_dt - datetime.utcnow().date()).days
    except Exception:
        pass
    return {
        "id": r["id"], "customer_id": r["customer_id"],
        "title": r["title"], "description": r["description"],
        "target_date": r["target_date"],
        "target_value": target, "current_value": current,
        "progress_pct": min(progress, 100.0),
        "status": r["status"],
        "owner_email": r["owner_email"],
        "days_remaining": days_remaining,
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }


async def create_goal(db: aiosqlite.Connection, customer_id: int, data: dict) -> dict:
    now = _now()
    cur = await db.execute(
        """INSERT INTO customer_goals
           (customer_id, title, description, target_date, target_value, current_value, owner_email, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (customer_id, data["title"], data.get("description"),
         data["target_date"], data.get("target_value", 100),
         data.get("current_value", 0), data.get("owner_email"), now, now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM customer_goals WHERE id = ?", (cur.lastrowid,))
    return _goal_row(rows[0])


async def list_goals(db: aiosqlite.Connection, customer_id: int,
                      status: str | None = None) -> list[dict]:
    q = "SELECT * FROM customer_goals WHERE customer_id = ?"
    params: list = [customer_id]
    if status:
        q += " AND status = ?"
        params.append(status)
    q += " ORDER BY target_date ASC"
    rows = await db.execute_fetchall(q, params)
    return [_goal_row(r) for r in rows]


async def update_goal(db: aiosqlite.Connection, goal_id: int, updates: dict) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM customer_goals WHERE id = ?", (goal_id,))
    if not rows:
        return None
    now = _now()
    sets = ["updated_at = ?"]
    params: list = [now]
    if updates.get("current_value") is not None:
        sets.append("current_value = ?")
        params.append(updates["current_value"])
    if updates.get("status"):
        sets.append("status = ?")
        params.append(updates["status"])
    if updates.get("notes"):
        r = rows[0]
        existing = r["description"] or ""
        desc = f"{existing}\n[{now[:10]}] {updates['notes']}" if existing else f"[{now[:10]}] {updates['notes']}"
        sets.append("description = ?")
        params.append(desc)
    params.append(goal_id)
    await db.execute(f"UPDATE customer_goals SET {', '.join(sets)} WHERE id = ?", params)
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM customer_goals WHERE id = ?", (goal_id,))
    return _goal_row(rows[0])


async def get_at_risk_goals(db: aiosqlite.Connection) -> list[dict]:
    """Goals that are past due or behind pace."""
    today = datetime.utcnow().date().isoformat()
    # Past due and not completed/cancelled
    rows = await db.execute_fetchall("""
        SELECT g.*, c.name as customer_name, c.company
        FROM customer_goals g
        JOIN customers c ON g.customer_id = c.id
        WHERE g.status = 'active'
          AND (g.target_date < ? OR g.current_value / MAX(g.target_value, 0.01) < 0.5)
        ORDER BY g.target_date ASC
    """, (today,))
    result = []
    for r in rows:
        d = _goal_row(r)
        d["customer_name"] = r["customer_name"]
        d["company"] = r["company"]
        # Check if actually at risk: past due OR behind pace
        days_remaining = d.get("days_remaining")
        progress = d["progress_pct"]
        is_past_due = days_remaining is not None and days_remaining < 0
        # Behind pace: less than 50% progress with less than 50% time remaining
        total_days = None
        try:
            created_dt = datetime.fromisoformat(r["created_at"]).date()
            target_dt = datetime.fromisoformat(r["target_date"]).date()
            total_days = (target_dt - created_dt).days
        except Exception:
            pass
        is_behind = False
        if total_days and total_days > 0 and days_remaining is not None:
            time_elapsed_pct = ((total_days - max(days_remaining, 0)) / total_days) * 100
            if time_elapsed_pct > 50 and progress < time_elapsed_pct * 0.5:
                is_behind = True
        if is_past_due or is_behind:
            result.append(d)
    return result


# ── Customer CSV Export ──────────────────────────────────────────────────

async def export_customers_csv(db: aiosqlite.Connection, segment: str | None = None,
                                health: str | None = None,
                                plan: str | None = None) -> str:
    customers = await list_customers(db, health, plan, segment)
    buf = io.StringIO()
    fieldnames = [
        "id", "name", "company", "email", "plan", "mrr",
        "health_score", "health_label", "segment", "owner_email",
        "onboarded_at", "renewal_date", "contract_value",
        "tags", "notes", "created_at",
    ]
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for c in customers:
        row = {k: c.get(k, "") for k in fieldnames}
        if isinstance(row.get("tags"), list):
            row["tags"] = ", ".join(row["tags"])
        writer.writerow(row)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════════
# ── Feature 1: Customer Handoff / Transfer (v0.9.0) ─────────────────────
# ══════════════════════════════════════════════════════════════════════════

def _handoff_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "customer_id": r["customer_id"],
        "from_owner": r["from_owner"],
        "to_owner": r["to_owner"],
        "reason": r["reason"],
        "notes": r["notes"],
        "status": r["status"],
        "created_at": r["created_at"],
        "completed_at": r["completed_at"],
    }


async def create_handoff(db: aiosqlite.Connection, customer_id: int, data: dict) -> dict | str | None:
    """Create a handoff: transfer customer ownership to a new CSM.
    Immediately sets customer.owner_email to to_owner and marks handoff as completed.
    """
    customer = await get_customer(db, customer_id)
    if not customer:
        return None

    to_owner = data["to_owner"].strip()
    from_owner = customer.get("owner_email")

    # Prevent self-transfer
    if from_owner and from_owner == to_owner:
        return "cannot_transfer_to_same_owner"

    now = _now()

    # Create handoff record
    cur = await db.execute(
        """INSERT INTO handoffs (customer_id, from_owner, to_owner, reason, notes, status, created_at, completed_at)
           VALUES (?, ?, ?, ?, ?, 'completed', ?, ?)""",
        (customer_id, from_owner, to_owner, data["reason"], data.get("notes"), now, now),
    )

    # Update customer owner
    await db.execute(
        "UPDATE customers SET owner_email = ? WHERE id = ?",
        (to_owner, customer_id),
    )
    await db.commit()

    _log_activity("handoff", f"Customer {customer_id} transferred from {from_owner} to {to_owner}")

    rows = await db.execute_fetchall("SELECT * FROM handoffs WHERE id = ?", (cur.lastrowid,))
    return _handoff_row(rows[0])


async def list_handoffs(db: aiosqlite.Connection,
                         customer_id: int | None = None,
                         from_owner: str | None = None,
                         to_owner: str | None = None,
                         status: str | None = None) -> list[dict]:
    """List handoffs with optional filters."""
    q = "SELECT * FROM handoffs WHERE 1=1"
    params: list = []
    if customer_id:
        q += " AND customer_id = ?"
        params.append(customer_id)
    if from_owner:
        q += " AND from_owner = ?"
        params.append(from_owner)
    if to_owner:
        q += " AND to_owner = ?"
        params.append(to_owner)
    if status:
        if status not in VALID_HANDOFF_STATUSES:
            return []
        q += " AND status = ?"
        params.append(status)
    q += " ORDER BY created_at DESC"
    rows = await db.execute_fetchall(q, params)
    return [_handoff_row(r) for r in rows]


async def complete_handoff(db: aiosqlite.Connection, handoff_id: int) -> dict | str | None:
    """Mark a pending handoff as completed and update customer owner."""
    rows = await db.execute_fetchall("SELECT * FROM handoffs WHERE id = ?", (handoff_id,))
    if not rows:
        return None
    h = rows[0]
    if h["status"] != "pending":
        return f"Handoff is already {h['status']}, cannot complete"

    now = _now()
    await db.execute(
        "UPDATE handoffs SET status = 'completed', completed_at = ? WHERE id = ?",
        (now, handoff_id),
    )
    # Update customer owner
    await db.execute(
        "UPDATE customers SET owner_email = ? WHERE id = ?",
        (h["to_owner"], h["customer_id"]),
    )
    await db.commit()

    _log_activity("handoff_completed", f"Handoff {handoff_id} completed for customer {h['customer_id']}")

    rows = await db.execute_fetchall("SELECT * FROM handoffs WHERE id = ?", (handoff_id,))
    return _handoff_row(rows[0])


async def cancel_handoff(db: aiosqlite.Connection, handoff_id: int) -> dict | str | None:
    """Cancel a pending handoff. Does not revert customer ownership if already completed."""
    rows = await db.execute_fetchall("SELECT * FROM handoffs WHERE id = ?", (handoff_id,))
    if not rows:
        return None
    h = rows[0]
    if h["status"] == "completed":
        return "Cannot cancel a completed handoff"
    if h["status"] == "cancelled":
        return "Handoff is already cancelled"

    now = _now()
    await db.execute(
        "UPDATE handoffs SET status = 'cancelled', completed_at = ? WHERE id = ?",
        (now, handoff_id),
    )
    await db.commit()

    _log_activity("handoff_cancelled", f"Handoff {handoff_id} cancelled for customer {h['customer_id']}")

    rows = await db.execute_fetchall("SELECT * FROM handoffs WHERE id = ?", (handoff_id,))
    return _handoff_row(rows[0])


# ══════════════════════════════════════════════════════════════════════════
# ── Feature 2: Customer Milestones (v0.9.0) ─────────────────────────────
# ══════════════════════════════════════════════════════════════════════════

def _milestone_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "customer_id": r["customer_id"],
        "milestone_type": r["milestone_type"],
        "title": r["title"],
        "notes": r["notes"],
        "achieved_at": r["achieved_at"],
        "created_by": r["created_by"],
    }


async def add_milestone(db: aiosqlite.Connection, customer_id: int, data: dict) -> dict | str | None:
    """Add a milestone for a customer. Prevents duplicate milestone types per customer."""
    customer = await get_customer(db, customer_id)
    if not customer:
        return None

    milestone_type = data["milestone_type"]
    if milestone_type not in VALID_MILESTONE_TYPES:
        return f"Invalid milestone_type. Must be one of: {', '.join(sorted(VALID_MILESTONE_TYPES))}"

    # Default title based on type if not provided
    title = data.get("title") or milestone_type.replace("_", " ").title()

    now = _now()
    try:
        cur = await db.execute(
            """INSERT INTO milestones (customer_id, milestone_type, title, notes, achieved_at, created_by)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (customer_id, milestone_type, title, data.get("notes"), now, data.get("created_by")),
        )
        await db.commit()
    except Exception:
        return "duplicate_milestone"

    _log_activity("milestone_achieved", f"Customer {customer_id} achieved milestone: {milestone_type}")

    rows = await db.execute_fetchall("SELECT * FROM milestones WHERE id = ?", (cur.lastrowid,))
    return _milestone_row(rows[0])


async def list_milestones(db: aiosqlite.Connection, customer_id: int,
                            milestone_type: str | None = None) -> list[dict]:
    """List milestones for a customer, optionally filtered by type."""
    q = "SELECT * FROM milestones WHERE customer_id = ?"
    params: list = [customer_id]
    if milestone_type:
        q += " AND milestone_type = ?"
        params.append(milestone_type)
    q += " ORDER BY achieved_at DESC"
    rows = await db.execute_fetchall(q, params)
    return [_milestone_row(r) for r in rows]


async def delete_milestone(db: aiosqlite.Connection, milestone_id: int) -> bool:
    """Delete a milestone by ID."""
    cur = await db.execute("DELETE FROM milestones WHERE id = ?", (milestone_id,))
    await db.commit()
    return cur.rowcount > 0


async def get_milestone_coverage(db: aiosqlite.Connection, customer_id: int) -> dict | None:
    """Get milestone coverage for a customer: percentage of possible milestones achieved."""
    customer = await get_customer(db, customer_id)
    if not customer:
        return None

    total_possible = len(VALID_MILESTONE_TYPES)
    rows = await db.execute_fetchall(
        "SELECT milestone_type FROM milestones WHERE customer_id = ?",
        (customer_id,),
    )
    achieved_types = [r["milestone_type"] for r in rows]
    achieved_count = len(achieved_types)
    missing_types = sorted(VALID_MILESTONE_TYPES - set(achieved_types))
    coverage_pct = round(achieved_count / total_possible * 100, 1) if total_possible else 0.0

    return {
        "customer_id": customer_id,
        "customer_name": customer["name"],
        "total_possible": total_possible,
        "achieved": achieved_count,
        "coverage_pct": coverage_pct,
        "achieved_types": sorted(achieved_types),
        "missing_types": missing_types,
    }


async def get_milestone_analytics(db: aiosqlite.Connection) -> dict:
    """Analytics: which milestones are most/least achieved across all customers."""
    total_customers_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM customers")
    total_customers = total_customers_rows[0]["cnt"] if total_customers_rows else 0

    rows = await db.execute_fetchall("""
        SELECT milestone_type, COUNT(*) as cnt
        FROM milestones
        GROUP BY milestone_type
        ORDER BY cnt DESC
    """)

    total_achieved = sum(r["cnt"] for r in rows)
    by_type = []
    for r in rows:
        pct = round(r["cnt"] / max(total_customers, 1) * 100, 1)
        by_type.append({
            "type": r["milestone_type"],
            "count": r["cnt"],
            "pct_of_customers": pct,
        })

    # Add types with zero achievements
    achieved_set = {r["milestone_type"] for r in rows}
    for mt in sorted(VALID_MILESTONE_TYPES - achieved_set):
        by_type.append({"type": mt, "count": 0, "pct_of_customers": 0.0})

    most_achieved = by_type[0]["type"] if by_type and by_type[0]["count"] > 0 else None
    # Find least achieved (non-zero first, then zero)
    least_achieved = None
    if by_type:
        # Sort by count ascending to find least achieved
        sorted_by_count = sorted(by_type, key=lambda x: x["count"])
        least_achieved = sorted_by_count[0]["type"] if sorted_by_count else None

    return {
        "total_customers": total_customers,
        "total_milestones_achieved": total_achieved,
        "by_type": by_type,
        "most_achieved": most_achieved,
        "least_achieved": least_achieved,
    }


# ══════════════════════════════════════════════════════════════════════════
# ── Feature 3: Escalation Management (v0.9.0) ───────────────────────────
# ══════════════════════════════════════════════════════════════════════════

def _escalation_row(r: aiosqlite.Row) -> dict:
    """Build escalation response dict with computed is_sla_breached field."""
    sla_hours = r["sla_hours"]
    created_at = r["created_at"]
    resolved_at = r["resolved_at"]

    # Compute SLA breach
    is_sla_breached = False
    try:
        created_dt = datetime.fromisoformat(created_at)
        sla_deadline = created_dt + timedelta(hours=sla_hours)
        if resolved_at:
            resolved_dt = datetime.fromisoformat(resolved_at)
            is_sla_breached = resolved_dt > sla_deadline
        else:
            # Still open — check if current time exceeds SLA
            is_sla_breached = datetime.now(timezone.utc) > sla_deadline
    except Exception:
        pass

    return {
        "id": r["id"],
        "customer_id": r["customer_id"],
        "title": r["title"],
        "description": r["description"],
        "severity": r["severity"],
        "category": r["category"],
        "assigned_to": r["assigned_to"],
        "status": r["status"],
        "resolution": r["resolution"],
        "created_at": created_at,
        "updated_at": r["updated_at"],
        "resolved_at": resolved_at,
        "sla_hours": sla_hours,
        "is_sla_breached": is_sla_breached,
    }


async def create_escalation(db: aiosqlite.Connection, customer_id: int, data: dict) -> dict | str | None:
    """Create an escalation for a customer."""
    customer = await get_customer(db, customer_id)
    if not customer:
        return None

    severity = data["severity"]
    if severity not in VALID_ESCALATION_SEVERITIES:
        return f"Invalid severity. Must be one of: {', '.join(sorted(VALID_ESCALATION_SEVERITIES))}"

    category = data["category"]
    if category not in VALID_ESCALATION_CATEGORIES:
        return f"Invalid category. Must be one of: {', '.join(sorted(VALID_ESCALATION_CATEGORIES))}"

    # Default SLA hours by severity
    sla_hours = data.get("sla_hours") or DEFAULT_SLA_HOURS.get(severity, 24)

    now = _now()
    cur = await db.execute(
        """INSERT INTO escalations
           (customer_id, title, description, severity, category, assigned_to, status, created_at, updated_at, sla_hours)
           VALUES (?, ?, ?, ?, ?, ?, 'open', ?, ?, ?)""",
        (customer_id, data["title"], data["description"], severity, category,
         data.get("assigned_to"), now, now, sla_hours),
    )
    await db.commit()

    _log_activity("escalation_created", f"Escalation created for customer {customer_id}: {data['title']} ({severity})")

    rows = await db.execute_fetchall("SELECT * FROM escalations WHERE id = ?", (cur.lastrowid,))
    return _escalation_row(rows[0])


async def list_escalations(db: aiosqlite.Connection,
                             customer_id: int | None = None,
                             status: str | None = None,
                             severity: str | None = None,
                             category: str | None = None,
                             assigned_to: str | None = None) -> list[dict]:
    """List escalations with optional filters."""
    q = "SELECT * FROM escalations WHERE 1=1"
    params: list = []
    if customer_id:
        q += " AND customer_id = ?"
        params.append(customer_id)
    if status:
        if status not in VALID_ESCALATION_STATUSES:
            return []
        q += " AND status = ?"
        params.append(status)
    if severity:
        if severity not in VALID_ESCALATION_SEVERITIES:
            return []
        q += " AND severity = ?"
        params.append(severity)
    if category:
        if category not in VALID_ESCALATION_CATEGORIES:
            return []
        q += " AND category = ?"
        params.append(category)
    if assigned_to:
        q += " AND assigned_to = ?"
        params.append(assigned_to)
    q += " ORDER BY created_at DESC"
    rows = await db.execute_fetchall(q, params)
    return [_escalation_row(r) for r in rows]


async def get_escalation(db: aiosqlite.Connection, escalation_id: int) -> dict | None:
    """Get a single escalation by ID."""
    rows = await db.execute_fetchall("SELECT * FROM escalations WHERE id = ?", (escalation_id,))
    if not rows:
        return None
    return _escalation_row(rows[0])


async def update_escalation(db: aiosqlite.Connection, escalation_id: int, updates: dict) -> dict | str | None:
    """Update an escalation: status transitions, assignment, resolution."""
    rows = await db.execute_fetchall("SELECT * FROM escalations WHERE id = ?", (escalation_id,))
    if not rows:
        return None

    current = rows[0]
    now = _now()
    sets = ["updated_at = ?"]
    params: list = [now]

    new_status = updates.get("status")
    if new_status:
        if new_status not in VALID_ESCALATION_STATUSES:
            return f"Invalid status. Must be one of: {', '.join(sorted(VALID_ESCALATION_STATUSES))}"

        # Validate status transitions
        current_status = current["status"]
        # Cannot reopen closed/resolved escalations to open
        if current_status in ("resolved", "closed") and new_status == "open":
            return f"Cannot transition from {current_status} to open"

        sets.append("status = ?")
        params.append(new_status)

        # Set resolved_at when transitioning to resolved or closed
        if new_status in ("resolved", "closed") and not current["resolved_at"]:
            sets.append("resolved_at = ?")
            params.append(now)

    if updates.get("assigned_to") is not None:
        sets.append("assigned_to = ?")
        params.append(updates["assigned_to"])

    if updates.get("resolution") is not None:
        sets.append("resolution = ?")
        params.append(updates["resolution"])

    params.append(escalation_id)
    await db.execute(
        f"UPDATE escalations SET {', '.join(sets)} WHERE id = ?",
        params,
    )
    await db.commit()

    _log_activity("escalation_updated", f"Escalation {escalation_id} updated")

    return await get_escalation(db, escalation_id)


async def get_escalation_stats(db: aiosqlite.Connection) -> dict:
    """Escalation analytics: totals, by status, by severity, avg resolution time, SLA breach rate."""
    all_rows = await db.execute_fetchall("SELECT * FROM escalations")
    total = len(all_rows)

    if not total:
        return {
            "total": 0,
            "by_status": {},
            "by_severity": {},
            "avg_resolution_hours": None,
            "sla_breach_rate": 0.0,
        }

    # By status
    by_status: dict[str, int] = {}
    for r in all_rows:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1

    # By severity
    by_severity: dict[str, int] = {}
    for r in all_rows:
        by_severity[r["severity"]] = by_severity.get(r["severity"], 0) + 1

    # Avg resolution hours (only for resolved/closed with resolved_at)
    resolution_hours = []
    sla_breached = 0
    sla_evaluated = 0
    for r in all_rows:
        sla_hours = r["sla_hours"]
        created_at = r["created_at"]
        resolved_at = r["resolved_at"]

        try:
            created_dt = datetime.fromisoformat(created_at)
            sla_deadline = created_dt + timedelta(hours=sla_hours)

            if resolved_at:
                resolved_dt = datetime.fromisoformat(resolved_at)
                hours = (resolved_dt - created_dt).total_seconds() / 3600
                resolution_hours.append(hours)
                sla_evaluated += 1
                if resolved_dt > sla_deadline:
                    sla_breached += 1
            else:
                # Still open -- evaluate SLA breach for open items too
                sla_evaluated += 1
                if datetime.now(timezone.utc) > sla_deadline:
                    sla_breached += 1
        except Exception:
            pass

    avg_resolution = round(sum(resolution_hours) / len(resolution_hours), 1) if resolution_hours else None
    sla_breach_rate = round(sla_breached / max(sla_evaluated, 1) * 100, 1)

    return {
        "total": total,
        "by_status": by_status,
        "by_severity": by_severity,
        "avg_resolution_hours": avg_resolution,
        "sla_breach_rate": sla_breach_rate,
    }


# ══════════════════════════════════════════════════════════════════════════
# ── Health Alerts (v1.0.0) ───────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════

def _health_alert_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "name": r["name"],
        "condition_type": r["condition_type"],
        "threshold": r["threshold"],
        "notification_email": r["notification_email"],
        "is_enabled": bool(r["is_enabled"]),
        "times_triggered": r["times_triggered"],
        "last_triggered_at": r["last_triggered_at"],
        "created_at": r["created_at"],
    }


def _alert_log_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "alert_id": r["alert_id"],
        "customer_id": r["customer_id"],
        "customer_name": r["customer_name"],
        "alert_name": r["alert_name"],
        "condition_type": r["condition_type"],
        "threshold": r["threshold"],
        "actual_value": r["actual_value"],
        "message": r["message"],
        "is_acknowledged": bool(r["is_acknowledged"]),
        "triggered_at": r["triggered_at"],
        "acknowledged_at": r["acknowledged_at"],
    }


async def create_health_alert(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    cur = await db.execute(
        """INSERT INTO health_alerts (name, condition_type, threshold, notification_email, is_enabled, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (data["name"], data["condition_type"], data["threshold"],
         data.get("notification_email"), 1 if data.get("is_enabled", True) else 0, now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM health_alerts WHERE id = ?", (cur.lastrowid,))
    return _health_alert_row(rows[0])


async def list_health_alerts(db: aiosqlite.Connection, is_enabled: bool | None = None) -> list[dict]:
    q = "SELECT * FROM health_alerts WHERE 1=1"
    params: list = []
    if is_enabled is not None:
        q += " AND is_enabled = ?"
        params.append(1 if is_enabled else 0)
    q += " ORDER BY created_at DESC"
    rows = await db.execute_fetchall(q, params)
    return [_health_alert_row(r) for r in rows]


async def get_health_alert(db: aiosqlite.Connection, alert_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM health_alerts WHERE id = ?", (alert_id,))
    return _health_alert_row(rows[0]) if rows else None


async def update_health_alert(db: aiosqlite.Connection, alert_id: int, updates: dict) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM health_alerts WHERE id = ?", (alert_id,))
    if not rows:
        return None
    fields = {}
    for k in ("name", "condition_type", "threshold", "notification_email"):
        if k in updates and updates[k] is not None:
            fields[k] = updates[k]
    if "is_enabled" in updates:
        fields["is_enabled"] = 1 if updates["is_enabled"] else 0
    if not fields:
        return _health_alert_row(rows[0])
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [alert_id]
    await db.execute(f"UPDATE health_alerts SET {set_clause} WHERE id = ?", values)
    await db.commit()
    return await get_health_alert(db, alert_id)


async def delete_health_alert(db: aiosqlite.Connection, alert_id: int) -> bool:
    cur = await db.execute("DELETE FROM health_alerts WHERE id = ?", (alert_id,))
    await db.commit()
    return cur.rowcount > 0


async def evaluate_health_alerts(db: aiosqlite.Connection, customer_id: int) -> list[dict]:
    customer = await get_customer(db, customer_id)
    if not customer:
        return []
    alerts = await db.execute_fetchall(
        "SELECT * FROM health_alerts WHERE is_enabled = 1")
    fired = []
    now = _now()
    for a in alerts:
        condition = a["condition_type"]
        threshold = a["threshold"]
        actual_value = None
        message = None
        violated = False

        if condition == "health_below":
            actual_value = float(customer["health_score"])
            if actual_value < threshold:
                violated = True
                message = f"Health score {actual_value} is below threshold {threshold}"

        elif condition == "health_drop":
            message = "health_drop check not implemented"

        elif condition == "churn_risk_above":
            risk = await compute_churn_risk(db, customer_id)
            if risk:
                actual_value = float(risk["risk_score"])
                if actual_value > threshold:
                    violated = True
                    message = f"Churn risk {actual_value} exceeds threshold {threshold}"

        elif condition == "no_touchpoint_days":
            tp_rows = await db.execute_fetchall(
                "SELECT MAX(created_at) as last_tp FROM touchpoints WHERE customer_id = ?",
                (customer_id,),
            )
            if tp_rows and tp_rows[0]["last_tp"]:
                try:
                    last_dt = datetime.fromisoformat(tp_rows[0]["last_tp"])
                    days_since = (datetime.now(timezone.utc) - last_dt).days
                    actual_value = float(days_since)
                    if actual_value > threshold:
                        violated = True
                        message = f"No touchpoint for {days_since} days (threshold: {threshold})"
                except Exception:
                    pass
            else:
                actual_value = 9999.0
                violated = True
                message = f"No touchpoints ever recorded (threshold: {threshold} days)"

        if violated and message:
            cur = await db.execute(
                """INSERT INTO health_alert_log
                   (alert_id, customer_id, customer_name, alert_name, condition_type, threshold, actual_value, message, triggered_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (a["id"], customer_id, customer["name"], a["name"],
                 condition, threshold, actual_value, message, now),
            )
            await db.execute(
                "UPDATE health_alerts SET times_triggered = times_triggered + 1, last_triggered_at = ? WHERE id = ?",
                (now, a["id"]),
            )
            await db.commit()
            log_rows = await db.execute_fetchall(
                "SELECT * FROM health_alert_log WHERE id = ?", (cur.lastrowid,))
            fired.append(_alert_log_row(log_rows[0]))

    return fired


async def list_alert_log(db: aiosqlite.Connection,
                          alert_id: int | None = None,
                          customer_id: int | None = None,
                          acknowledged: bool | None = None,
                          limit: int = 50) -> list[dict]:
    q = "SELECT * FROM health_alert_log WHERE 1=1"
    params: list = []
    if alert_id is not None:
        q += " AND alert_id = ?"
        params.append(alert_id)
    if customer_id is not None:
        q += " AND customer_id = ?"
        params.append(customer_id)
    if acknowledged is not None:
        q += " AND is_acknowledged = ?"
        params.append(1 if acknowledged else 0)
    q += " ORDER BY triggered_at DESC LIMIT ?"
    params.append(limit)
    rows = await db.execute_fetchall(q, params)
    return [_alert_log_row(r) for r in rows]


async def acknowledge_alert(db: aiosqlite.Connection, log_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM health_alert_log WHERE id = ?", (log_id,))
    if not rows:
        return None
    now = _now()
    await db.execute(
        "UPDATE health_alert_log SET is_acknowledged = 1, acknowledged_at = ? WHERE id = ?",
        (now, log_id),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM health_alert_log WHERE id = ?", (log_id,))
    return _alert_log_row(rows[0])


async def get_alert_summary(db: aiosqlite.Connection) -> dict:
    total_alerts = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM health_alerts")
    enabled_alerts = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM health_alerts WHERE is_enabled = 1")
    total_log = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM health_alert_log")
    unack = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM health_alert_log WHERE is_acknowledged = 0")
    by_type = await db.execute_fetchall("""
        SELECT condition_type, COUNT(*) as cnt
        FROM health_alert_log GROUP BY condition_type ORDER BY cnt DESC
    """)
    recent = await db.execute_fetchall(
        "SELECT * FROM health_alert_log ORDER BY triggered_at DESC LIMIT 5")
    return {
        "total_alerts": total_alerts[0]["cnt"],
        "enabled_alerts": enabled_alerts[0]["cnt"],
        "total_triggered": total_log[0]["cnt"],
        "unacknowledged": unack[0]["cnt"],
        "by_condition_type": {r["condition_type"]: r["cnt"] for r in by_type},
        "recent_triggers": [_alert_log_row(r) for r in recent],
    }


# ══════════════════════════════════════════════════════════════════════════
# ── Success Plans (v1.0.0) ───────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════

def _success_plan_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "customer_id": r["customer_id"],
        "title": r["title"],
        "description": r["description"],
        "owner_email": r["owner_email"],
        "status": r["status"],
        "start_date": r["start_date"],
        "target_date": r["target_date"],
        "progress_pct": r["progress_pct"],
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }


def _plan_task_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "plan_id": r["plan_id"],
        "title": r["title"],
        "description": r["description"],
        "assignee_email": r["assignee_email"],
        "status": r["status"],
        "priority": r["priority"],
        "due_date": r["due_date"],
        "completed_at": r["completed_at"],
        "created_at": r["created_at"],
    }


async def create_success_plan(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    cur = await db.execute(
        """INSERT INTO success_plans
           (customer_id, title, description, owner_email, status, start_date, target_date, progress_pct, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (data["customer_id"], data["title"], data.get("description"),
         data.get("owner_email"), data.get("status", "draft"),
         data.get("start_date"), data.get("target_date"), 0, now, now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM success_plans WHERE id = ?", (cur.lastrowid,))
    return _success_plan_row(rows[0])


async def list_success_plans(db: aiosqlite.Connection,
                               customer_id: int | None = None,
                               status: str | None = None) -> list[dict]:
    q = "SELECT * FROM success_plans WHERE 1=1"
    params: list = []
    if customer_id is not None:
        q += " AND customer_id = ?"
        params.append(customer_id)
    if status:
        q += " AND status = ?"
        params.append(status)
    q += " ORDER BY created_at DESC"
    rows = await db.execute_fetchall(q, params)
    return [_success_plan_row(r) for r in rows]


async def get_success_plan(db: aiosqlite.Connection, plan_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM success_plans WHERE id = ?", (plan_id,))
    if not rows:
        return None
    d = _success_plan_row(rows[0])
    task_total = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM plan_tasks WHERE plan_id = ?", (plan_id,))
    task_completed = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM plan_tasks WHERE plan_id = ? AND status = 'completed'", (plan_id,))
    d["tasks_total"] = task_total[0]["cnt"]
    d["tasks_completed"] = task_completed[0]["cnt"]
    return d


async def update_success_plan(db: aiosqlite.Connection, plan_id: int, updates: dict) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM success_plans WHERE id = ?", (plan_id,))
    if not rows:
        return None
    now = _now()
    fields = {"updated_at": now}
    for k in ("title", "description", "owner_email", "status", "start_date", "target_date"):
        if k in updates and updates[k] is not None:
            fields[k] = updates[k]
    if "progress_pct" in updates and updates["progress_pct"] is not None:
        fields["progress_pct"] = updates["progress_pct"]
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [plan_id]
    await db.execute(f"UPDATE success_plans SET {set_clause} WHERE id = ?", values)
    await db.commit()
    return await get_success_plan(db, plan_id)


async def delete_success_plan(db: aiosqlite.Connection, plan_id: int) -> bool:
    await db.execute("DELETE FROM plan_tasks WHERE plan_id = ?", (plan_id,))
    cur = await db.execute("DELETE FROM success_plans WHERE id = ?", (plan_id,))
    await db.commit()
    return cur.rowcount > 0


async def add_plan_task(db: aiosqlite.Connection, plan_id: int, data: dict) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM success_plans WHERE id = ?", (plan_id,))
    if not rows:
        return None
    now = _now()
    cur = await db.execute(
        """INSERT INTO plan_tasks (plan_id, title, description, assignee_email, status, priority, due_date, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (plan_id, data["title"], data.get("description"),
         data.get("assignee_email"), data.get("status", "pending"),
         data.get("priority", "medium"), data.get("due_date"), now),
    )
    await db.commit()
    task_rows = await db.execute_fetchall("SELECT * FROM plan_tasks WHERE id = ?", (cur.lastrowid,))
    return _plan_task_row(task_rows[0])


async def list_plan_tasks(db: aiosqlite.Connection, plan_id: int,
                            status: str | None = None) -> list[dict]:
    q = "SELECT * FROM plan_tasks WHERE plan_id = ?"
    params: list = [plan_id]
    if status:
        q += " AND status = ?"
        params.append(status)
    q += " ORDER BY created_at ASC"
    rows = await db.execute_fetchall(q, params)
    return [_plan_task_row(r) for r in rows]


async def _recalc_plan_progress(db: aiosqlite.Connection, plan_id: int):
    total = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM plan_tasks WHERE plan_id = ?", (plan_id,))
    completed = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM plan_tasks WHERE plan_id = ? AND status = 'completed'", (plan_id,))
    t = total[0]["cnt"]
    c = completed[0]["cnt"]
    pct = round(c / max(t, 1) * 100, 1)
    now = _now()
    await db.execute(
        "UPDATE success_plans SET progress_pct = ?, updated_at = ? WHERE id = ?",
        (pct, now, plan_id),
    )
    await db.commit()


async def update_plan_task(db: aiosqlite.Connection, task_id: int, updates: dict) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM plan_tasks WHERE id = ?", (task_id,))
    if not rows:
        return None
    task = rows[0]
    fields: dict = {}
    for k in ("title", "description", "assignee_email", "status", "priority", "due_date"):
        if k in updates and updates[k] is not None:
            fields[k] = updates[k]
    new_status = fields.get("status")
    if new_status == "completed" and task["status"] != "completed":
        fields["completed_at"] = _now()
    elif new_status and new_status != "completed":
        fields["completed_at"] = None
    if not fields:
        return _plan_task_row(task)
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [task_id]
    await db.execute(f"UPDATE plan_tasks SET {set_clause} WHERE id = ?", values)
    await db.commit()
    await _recalc_plan_progress(db, task["plan_id"])
    rows = await db.execute_fetchall("SELECT * FROM plan_tasks WHERE id = ?", (task_id,))
    return _plan_task_row(rows[0])


async def delete_plan_task(db: aiosqlite.Connection, task_id: int) -> bool:
    rows = await db.execute_fetchall("SELECT * FROM plan_tasks WHERE id = ?", (task_id,))
    if not rows:
        return False
    plan_id = rows[0]["plan_id"]
    await db.execute("DELETE FROM plan_tasks WHERE id = ?", (task_id,))
    await db.commit()
    await _recalc_plan_progress(db, plan_id)
    return True


async def get_plan_overview(db: aiosqlite.Connection) -> dict:
    total = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM success_plans")
    by_status = await db.execute_fetchall(
        "SELECT status, COUNT(*) as cnt FROM success_plans GROUP BY status ORDER BY cnt DESC")
    avg_progress = await db.execute_fetchall(
        "SELECT COALESCE(AVG(progress_pct), 0) as avg FROM success_plans")
    total_tasks = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM plan_tasks")
    completed_tasks = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM plan_tasks WHERE status = 'completed'")
    overdue = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM success_plans WHERE target_date < ? AND status NOT IN ('completed', 'cancelled')",
        (datetime.utcnow().date().isoformat(),))
    return {
        "total_plans": total[0]["cnt"],
        "by_status": {r["status"]: r["cnt"] for r in by_status},
        "avg_progress_pct": round(avg_progress[0]["avg"], 1),
        "total_tasks": total_tasks[0]["cnt"],
        "completed_tasks": completed_tasks[0]["cnt"],
        "overdue_plans": overdue[0]["cnt"],
    }


# ══════════════════════════════════════════════════════════════════════════
# ── Customer Feedback (v1.0.0) ───────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════

def _feedback_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "customer_id": r["customer_id"],
        "type": r["type"],
        "title": r["title"],
        "description": r["description"],
        "priority": r["priority"],
        "status": r["status"],
        "submitted_by": r["submitted_by"],
        "votes": r["votes"],
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }


async def create_feedback(db: aiosqlite.Connection, data: dict) -> dict | None:
    customer = await get_customer(db, data["customer_id"])
    if not customer:
        return None
    now = _now()
    cur = await db.execute(
        """INSERT INTO customer_feedback
           (customer_id, type, title, description, priority, status, submitted_by, votes, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (data["customer_id"], data["type"], data["title"], data.get("description"),
         data.get("priority", "medium"), data.get("status", "new"),
         data.get("submitted_by"), data.get("votes", 1), now, now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM customer_feedback WHERE id = ?", (cur.lastrowid,))
    return _feedback_row(rows[0])


async def list_feedback(db: aiosqlite.Connection,
                          customer_id: int | None = None,
                          type: str | None = None,
                          status: str | None = None,
                          priority: str | None = None,
                          limit: int = 50) -> list[dict]:
    q = "SELECT * FROM customer_feedback WHERE 1=1"
    params: list = []
    if customer_id is not None:
        q += " AND customer_id = ?"
        params.append(customer_id)
    if type:
        q += " AND type = ?"
        params.append(type)
    if status:
        q += " AND status = ?"
        params.append(status)
    if priority:
        q += " AND priority = ?"
        params.append(priority)
    q += " ORDER BY votes DESC, created_at DESC LIMIT ?"
    params.append(limit)
    rows = await db.execute_fetchall(q, params)
    return [_feedback_row(r) for r in rows]


async def get_feedback(db: aiosqlite.Connection, feedback_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM customer_feedback WHERE id = ?", (feedback_id,))
    return _feedback_row(rows[0]) if rows else None


async def update_feedback(db: aiosqlite.Connection, feedback_id: int, updates: dict) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM customer_feedback WHERE id = ?", (feedback_id,))
    if not rows:
        return None
    now = _now()
    fields = {"updated_at": now}
    for k in ("type", "title", "description", "priority", "status", "submitted_by"):
        if k in updates and updates[k] is not None:
            fields[k] = updates[k]
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [feedback_id]
    await db.execute(f"UPDATE customer_feedback SET {set_clause} WHERE id = ?", values)
    await db.commit()
    return await get_feedback(db, feedback_id)


async def delete_feedback(db: aiosqlite.Connection, feedback_id: int) -> bool:
    cur = await db.execute("DELETE FROM customer_feedback WHERE id = ?", (feedback_id,))
    await db.commit()
    return cur.rowcount > 0


async def vote_feedback(db: aiosqlite.Connection, feedback_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM customer_feedback WHERE id = ?", (feedback_id,))
    if not rows:
        return None
    now = _now()
    await db.execute(
        "UPDATE customer_feedback SET votes = votes + 1, updated_at = ? WHERE id = ?",
        (now, feedback_id),
    )
    await db.commit()
    return await get_feedback(db, feedback_id)


async def get_feedback_stats(db: aiosqlite.Connection) -> dict:
    total = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM customer_feedback")
    by_type = await db.execute_fetchall(
        "SELECT type, COUNT(*) as cnt FROM customer_feedback GROUP BY type ORDER BY cnt DESC")
    by_status = await db.execute_fetchall(
        "SELECT status, COUNT(*) as cnt FROM customer_feedback GROUP BY status ORDER BY cnt DESC")
    by_priority = await db.execute_fetchall(
        "SELECT priority, COUNT(*) as cnt FROM customer_feedback GROUP BY priority ORDER BY cnt DESC")
    top_voted = await db.execute_fetchall(
        "SELECT * FROM customer_feedback ORDER BY votes DESC LIMIT 5")
    total_votes = await db.execute_fetchall(
        "SELECT COALESCE(SUM(votes), 0) as total FROM customer_feedback")
    return {
        "total_feedback": total[0]["cnt"],
        "by_type": {r["type"]: r["cnt"] for r in by_type},
        "by_status": {r["status"]: r["cnt"] for r in by_status},
        "by_priority": {r["priority"]: r["cnt"] for r in by_priority},
        "total_votes": total_votes[0]["total"],
        "top_voted": [_feedback_row(r) for r in top_voted],
    }


# ══════════════════════════════════════════════════════════════════════════
# ── Cohort Analysis (v1.1.0) ─────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════

def _cohort_row(r) -> dict:
    return {
        "id": r["id"],
        "name": r["name"],
        "cohort_type": r["cohort_type"],
        "criteria": json.loads(r["criteria"]) if r["criteria"] else {},
        "created_at": r["created_at"],
    }


def _cohort_snapshot_row(r) -> dict:
    return {
        "id": r["id"],
        "cohort_id": r["cohort_id"],
        "snapshot_date": r["snapshot_date"],
        "customer_count": r["customer_count"],
        "avg_health": round(r["avg_health"], 1),
        "avg_mrr": round(r["avg_mrr"], 2),
        "churned_count": r["churned_count"],
        "expanded_count": r["expanded_count"],
        "nps_avg": round(r["nps_avg"], 1),
        "created_at": r["created_at"],
    }


async def create_cohort(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    criteria = data.get("criteria") or {}
    cur = await db.execute(
        "INSERT INTO cohorts (name, cohort_type, criteria, created_at) VALUES (?, ?, ?, ?)",
        (data["name"], data["cohort_type"], json.dumps(criteria), now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM cohorts WHERE id = ?", (cur.lastrowid,))
    return _cohort_row(rows[0])


async def list_cohorts(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM cohorts ORDER BY created_at DESC")
    return [_cohort_row(r) for r in rows]


async def get_cohort(db: aiosqlite.Connection, cohort_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM cohorts WHERE id = ?", (cohort_id,))
    if not rows:
        return None
    d = _cohort_row(rows[0])
    # Attach latest snapshot
    snap_rows = await db.execute_fetchall(
        "SELECT * FROM cohort_snapshots WHERE cohort_id = ? ORDER BY snapshot_date DESC LIMIT 1",
        (cohort_id,),
    )
    d["latest_snapshot"] = _cohort_snapshot_row(snap_rows[0]) if snap_rows else None
    return d


async def delete_cohort(db: aiosqlite.Connection, cohort_id: int) -> bool:
    await db.execute("DELETE FROM cohort_snapshots WHERE cohort_id = ?", (cohort_id,))
    cur = await db.execute("DELETE FROM cohorts WHERE id = ?", (cohort_id,))
    await db.commit()
    return cur.rowcount > 0


async def _get_cohort_customers(db: aiosqlite.Connection, cohort: dict) -> list:
    """Return customer rows matching cohort criteria."""
    cohort_type = cohort["cohort_type"]
    criteria = cohort.get("criteria") or {}
    if isinstance(criteria, str):
        criteria = json.loads(criteria)

    q = "SELECT * FROM customers WHERE 1=1"
    params: list = []

    if cohort_type == "signup_month":
        month = criteria.get("month")  # e.g. "2025-01"
        if month:
            q += " AND created_at LIKE ?"
            params.append(f"{month}%")
    elif cohort_type == "plan":
        plan = criteria.get("plan")
        if plan:
            q += " AND plan = ?"
            params.append(plan)
    elif cohort_type == "segment":
        segment = criteria.get("segment")
        if segment:
            q += " AND segment = ?"
            params.append(segment)
    elif cohort_type == "custom":
        if criteria.get("min_mrr") is not None:
            q += " AND mrr >= ?"
            params.append(criteria["min_mrr"])
        if criteria.get("max_mrr") is not None:
            q += " AND mrr <= ?"
            params.append(criteria["max_mrr"])
        if criteria.get("plan"):
            q += " AND plan = ?"
            params.append(criteria["plan"])
        if criteria.get("segment"):
            q += " AND segment = ?"
            params.append(criteria["segment"])
        if criteria.get("min_health") is not None:
            q += " AND health_score >= ?"
            params.append(criteria["min_health"])
        if criteria.get("max_health") is not None:
            q += " AND health_score <= ?"
            params.append(criteria["max_health"])

    q += " ORDER BY mrr DESC"
    return await db.execute_fetchall(q, params)


async def take_cohort_snapshot(db: aiosqlite.Connection, cohort_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM cohorts WHERE id = ?", (cohort_id,))
    if not rows:
        return None
    cohort = _cohort_row(rows[0])
    customers = await _get_cohort_customers(db, cohort)
    now = _now()
    count = len(customers)
    avg_health = round(sum(c["health_score"] for c in customers) / max(count, 1), 1)
    avg_mrr = round(sum(c["mrr"] for c in customers) / max(count, 1), 2)

    # Churned: health < 30
    churned = sum(1 for c in customers if c["health_score"] < 30)

    # Expanded: has expansion with stage = closed_won (or 'won')
    expanded = 0
    try:
        for c in customers:
            exp_rows = await db.execute_fetchall(
                "SELECT id FROM expansions WHERE customer_id = ? AND stage = 'won'",
                (c["id"],),
            )
            if exp_rows:
                expanded += 1
    except Exception:
        pass

    # NPS avg
    nps_avg_val = 0.0
    if count > 0:
        cust_ids = [c["id"] for c in customers]
        if cust_ids:
            placeholders = ",".join("?" * len(cust_ids))
            nps_rows = await db.execute_fetchall(
                f"SELECT COALESCE(AVG(score), 0) as avg FROM nps_surveys WHERE customer_id IN ({placeholders})",
                cust_ids,
            )
            nps_avg_val = round(nps_rows[0]["avg"], 1) if nps_rows else 0.0

    cur = await db.execute(
        """INSERT INTO cohort_snapshots
           (cohort_id, snapshot_date, customer_count, avg_health, avg_mrr, churned_count, expanded_count, nps_avg, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (cohort_id, now[:10], count, avg_health, avg_mrr, churned, expanded, nps_avg_val, now),
    )
    await db.commit()
    snap_rows = await db.execute_fetchall("SELECT * FROM cohort_snapshots WHERE id = ?", (cur.lastrowid,))
    return _cohort_snapshot_row(snap_rows[0])


async def list_cohort_snapshots(db: aiosqlite.Connection, cohort_id: int) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM cohort_snapshots WHERE cohort_id = ? ORDER BY snapshot_date ASC",
        (cohort_id,),
    )
    return [_cohort_snapshot_row(r) for r in rows]


async def list_cohort_customers(db: aiosqlite.Connection, cohort_id: int) -> list[dict] | None:
    rows = await db.execute_fetchall("SELECT * FROM cohorts WHERE id = ?", (cohort_id,))
    if not rows:
        return None
    cohort = _cohort_row(rows[0])
    customers = await _get_cohort_customers(db, cohort)
    result = []
    for c in customers:
        tags = await _get_customer_tags(db, c["id"])
        result.append(_customer_row(c, tags))
    return result


async def compare_cohorts(db: aiosqlite.Connection, cohort_ids: list[int] | None = None) -> list[dict]:
    if cohort_ids:
        placeholders = ",".join("?" * len(cohort_ids))
        cohort_rows = await db.execute_fetchall(
            f"SELECT * FROM cohorts WHERE id IN ({placeholders})", cohort_ids
        )
    else:
        cohort_rows = await db.execute_fetchall("SELECT * FROM cohorts ORDER BY created_at DESC")

    result = []
    for cr in cohort_rows:
        cohort = _cohort_row(cr)
        # Get latest snapshot
        snap = await db.execute_fetchall(
            "SELECT * FROM cohort_snapshots WHERE cohort_id = ? ORDER BY snapshot_date DESC LIMIT 1",
            (cr["id"],),
        )
        snapshot = _cohort_snapshot_row(snap[0]) if snap else None
        result.append({
            "cohort_id": cohort["id"],
            "name": cohort["name"],
            "cohort_type": cohort["cohort_type"],
            "customer_count": snapshot["customer_count"] if snapshot else 0,
            "avg_health": snapshot["avg_health"] if snapshot else 0,
            "avg_mrr": snapshot["avg_mrr"] if snapshot else 0,
            "churned_count": snapshot["churned_count"] if snapshot else 0,
            "expanded_count": snapshot["expanded_count"] if snapshot else 0,
            "nps_avg": snapshot["nps_avg"] if snapshot else 0,
            "snapshot_date": snapshot["snapshot_date"] if snapshot else None,
        })
    return result


# ══════════════════════════════════════════════════════════════════════════
# ── Engagement Scoring (v1.1.0) ──────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════

def _engagement_score_row(r) -> dict:
    return {
        "id": r["id"],
        "customer_id": r["customer_id"],
        "score": round(r["score"], 1),
        "touchpoint_frequency": round(r["touchpoint_frequency"], 1),
        "response_rate": round(r["response_rate"], 1),
        "feature_adoption": round(r["feature_adoption"], 1),
        "last_interaction_days": r["last_interaction_days"],
        "decay_factor": round(r["decay_factor"], 3),
        "calculated_at": r["calculated_at"],
    }


def _engagement_config_row(r) -> dict:
    return {
        "id": r["id"],
        "touchpoint_weight": r["touchpoint_weight"],
        "response_weight": r["response_weight"],
        "adoption_weight": r["adoption_weight"],
        "decay_rate_per_day": r["decay_rate_per_day"],
        "score_threshold_high": r["score_threshold_high"],
        "score_threshold_low": r["score_threshold_low"],
        "updated_at": r["updated_at"],
    }


async def get_engagement_config(db: aiosqlite.Connection) -> dict:
    rows = await db.execute_fetchall("SELECT * FROM engagement_config LIMIT 1")
    if not rows:
        return {
            "id": 0, "touchpoint_weight": 0.4, "response_weight": 0.3,
            "adoption_weight": 0.3, "decay_rate_per_day": 0.02,
            "score_threshold_high": 70, "score_threshold_low": 30,
            "updated_at": _now(),
        }
    return _engagement_config_row(rows[0])


async def update_engagement_config(db: aiosqlite.Connection, updates: dict) -> dict:
    config = await get_engagement_config(db)
    now = _now()
    fields = {"updated_at": now}
    for k in ("touchpoint_weight", "response_weight", "adoption_weight",
              "decay_rate_per_day", "score_threshold_high", "score_threshold_low"):
        if k in updates and updates[k] is not None:
            fields[k] = updates[k]
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [config["id"]]
    await db.execute(f"UPDATE engagement_config SET {set_clause} WHERE id = ?", values)
    await db.commit()
    return await get_engagement_config(db)


async def calculate_engagement_score(db: aiosqlite.Connection, customer_id: int) -> dict | None:
    customer = await get_customer(db, customer_id)
    if not customer:
        return None

    config = await get_engagement_config(db)
    now = _now()
    now_dt = datetime.now(timezone.utc)

    # touchpoint_frequency: count touchpoints in last 90 days / 90 * 100 (capped 100)
    since_90d = (now_dt - timedelta(days=90)).isoformat()
    tp_rows = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM touchpoints WHERE customer_id = ? AND created_at >= ?",
        (customer_id, since_90d),
    )
    tp_count = tp_rows[0]["cnt"] if tp_rows else 0
    touchpoint_frequency = min(tp_count / 90 * 100, 100)

    # response_rate: touchpoints with outcome != null / total touchpoints * 100
    all_tp = await db.execute_fetchall(
        "SELECT COUNT(*) as total, SUM(CASE WHEN outcome IS NOT NULL AND outcome != '' THEN 1 ELSE 0 END) as responded FROM touchpoints WHERE customer_id = ?",
        (customer_id,),
    )
    total_tp = all_tp[0]["total"] if all_tp else 0
    responded_tp = all_tp[0]["responded"] if all_tp else 0
    response_rate = (responded_tp / max(total_tp, 1)) * 100

    # feature_adoption: (goals completed + milestones achieved) / max(1, goals + milestones) * 100
    goals_total = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM customer_goals WHERE customer_id = ?", (customer_id,))
    goals_completed = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM customer_goals WHERE customer_id = ? AND status = 'completed'",
        (customer_id,))
    milestones_achieved = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM milestones WHERE customer_id = ?", (customer_id,))
    g_total = (goals_total[0]["cnt"] if goals_total else 0)
    g_completed = (goals_completed[0]["cnt"] if goals_completed else 0)
    m_achieved = (milestones_achieved[0]["cnt"] if milestones_achieved else 0)
    denominator = max(1, g_total + m_achieved)
    feat_adoption = (g_completed + m_achieved) / denominator * 100

    # last_interaction_days
    last_tp = await db.execute_fetchall(
        "SELECT MAX(created_at) as last_tp FROM touchpoints WHERE customer_id = ?",
        (customer_id,),
    )
    last_interaction_days = 9999
    if last_tp and last_tp[0]["last_tp"]:
        try:
            last_dt = datetime.fromisoformat(last_tp[0]["last_tp"])
            last_interaction_days = max(0, (now_dt - last_dt).days)
        except Exception:
            pass

    # decay_factor
    decay_rate = config["decay_rate_per_day"]
    decay_factor = max(0, 1 - decay_rate * last_interaction_days)

    # score
    w1 = config["touchpoint_weight"]
    w2 = config["response_weight"]
    w3 = config["adoption_weight"]
    score = (touchpoint_frequency * w1 + response_rate * w2 + feat_adoption * w3) * decay_factor

    # INSERT OR REPLACE
    await db.execute(
        """INSERT INTO engagement_scores
           (customer_id, score, touchpoint_frequency, response_rate, feature_adoption, last_interaction_days, decay_factor, calculated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(customer_id) DO UPDATE SET
           score=excluded.score, touchpoint_frequency=excluded.touchpoint_frequency,
           response_rate=excluded.response_rate, feature_adoption=excluded.feature_adoption,
           last_interaction_days=excluded.last_interaction_days, decay_factor=excluded.decay_factor,
           calculated_at=excluded.calculated_at""",
        (customer_id, round(score, 1), round(touchpoint_frequency, 1),
         round(response_rate, 1), round(feat_adoption, 1),
         last_interaction_days, round(decay_factor, 3), now),
    )
    await db.commit()
    rows = await db.execute_fetchall(
        "SELECT * FROM engagement_scores WHERE customer_id = ?", (customer_id,))
    return _engagement_score_row(rows[0])


async def calculate_all_engagement_scores(db: aiosqlite.Connection) -> dict:
    customers = await db.execute_fetchall("SELECT id FROM customers")
    results = []
    for c in customers:
        r = await calculate_engagement_score(db, c["id"])
        if r:
            results.append(r)
    avg_score = round(sum(r["score"] for r in results) / max(len(results), 1), 1) if results else 0
    return {
        "calculated": len(results),
        "avg_score": avg_score,
    }


async def list_engagement_scores(db: aiosqlite.Connection,
                                   min_score: float | None = None,
                                   max_score: float | None = None,
                                   sort_by: str | None = None,
                                   limit: int = 100,
                                   offset: int = 0) -> list[dict]:
    q = "SELECT * FROM engagement_scores WHERE 1=1"
    params: list = []
    if min_score is not None:
        q += " AND score >= ?"
        params.append(min_score)
    if max_score is not None:
        q += " AND score <= ?"
        params.append(max_score)
    if sort_by == "score_asc":
        q += " ORDER BY score ASC"
    elif sort_by == "score_desc":
        q += " ORDER BY score DESC"
    elif sort_by == "recent":
        q += " ORDER BY calculated_at DESC"
    else:
        q += " ORDER BY score DESC"
    q += " LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    rows = await db.execute_fetchall(q, params)
    return [_engagement_score_row(r) for r in rows]


async def get_engagement_score(db: aiosqlite.Connection, customer_id: int) -> dict | None:
    rows = await db.execute_fetchall(
        "SELECT * FROM engagement_scores WHERE customer_id = ?", (customer_id,))
    return _engagement_score_row(rows[0]) if rows else None


async def get_engagement_alerts(db: aiosqlite.Connection) -> list[dict]:
    config = await get_engagement_config(db)
    threshold_low = config["score_threshold_low"]
    # Customers below threshold
    low_rows = await db.execute_fetchall(
        "SELECT e.*, c.name as customer_name FROM engagement_scores e JOIN customers c ON e.customer_id = c.id WHERE e.score < ? ORDER BY e.score ASC",
        (threshold_low,),
    )
    alerts = []
    for r in low_rows:
        alerts.append({
            "customer_id": r["customer_id"],
            "customer_name": r["customer_name"],
            "score": round(r["score"], 1),
            "reason": f"Engagement score {round(r['score'], 1)} below threshold {threshold_low}",
        })
    return alerts


async def get_engagement_trends(db: aiosqlite.Connection) -> list[dict]:
    """Weekly average engagement scores for last 12 weeks."""
    now_dt = datetime.now(timezone.utc)
    weeks = []
    for i in range(11, -1, -1):
        week_start = now_dt - timedelta(weeks=i+1)
        week_end = now_dt - timedelta(weeks=i)
        rows = await db.execute_fetchall(
            "SELECT AVG(score) as avg, COUNT(*) as cnt FROM engagement_scores WHERE calculated_at >= ? AND calculated_at < ?",
            (week_start.isoformat(), week_end.isoformat()),
        )
        avg = round(rows[0]["avg"], 1) if rows and rows[0]["avg"] is not None else 0
        cnt = rows[0]["cnt"] if rows else 0
        weeks.append({
            "week": week_start.strftime("%Y-W%V"),
            "avg_score": avg,
            "customer_count": cnt,
        })
    return weeks


# ══════════════════════════════════════════════════════════════════════════
# ── Revenue Waterfall (v1.1.0) ───────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════

def _revenue_event_row(r) -> dict:
    return {
        "id": r["id"],
        "customer_id": r["customer_id"],
        "event_type": r["event_type"],
        "mrr_before": round(r["mrr_before"], 2),
        "mrr_after": round(r["mrr_after"], 2),
        "mrr_delta": round(r["mrr_delta"], 2),
        "reason": r["reason"],
        "created_at": r["created_at"],
    }


async def record_revenue_event(db: aiosqlite.Connection, data: dict) -> dict | None:
    customer = await get_customer(db, data["customer_id"])
    if not customer:
        return None
    now = _now()
    mrr_before = data["mrr_before"]
    mrr_after = data["mrr_after"]
    mrr_delta = round(mrr_after - mrr_before, 2)
    cur = await db.execute(
        """INSERT INTO revenue_events (customer_id, event_type, mrr_before, mrr_after, mrr_delta, reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (data["customer_id"], data["event_type"], mrr_before, mrr_after, mrr_delta,
         data.get("reason"), now),
    )
    # Update customer MRR if different
    if mrr_after != customer["mrr"]:
        await db.execute("UPDATE customers SET mrr = ? WHERE id = ?", (mrr_after, data["customer_id"]))
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM revenue_events WHERE id = ?", (cur.lastrowid,))
    return _revenue_event_row(rows[0])


async def detect_mrr_changes(db: aiosqlite.Connection) -> list[dict]:
    """Compare each customer's current MRR with last known revenue event."""
    customers = await db.execute_fetchall("SELECT * FROM customers")
    events_created = []
    now = _now()
    for c in customers:
        cid = c["id"]
        current_mrr = c["mrr"]
        # Get last revenue event for this customer
        last_event = await db.execute_fetchall(
            "SELECT * FROM revenue_events WHERE customer_id = ? ORDER BY created_at DESC LIMIT 1",
            (cid,),
        )
        if last_event:
            last_mrr = last_event[0]["mrr_after"]
        else:
            last_mrr = current_mrr  # No events yet, assume no change
            continue  # Skip if no prior events

        delta = round(current_mrr - last_mrr, 2)
        if abs(delta) < 0.01:
            continue  # No meaningful change

        # Determine event type
        if last_mrr == 0 and current_mrr > 0:
            event_type = "reactivation"
        elif current_mrr == 0:
            event_type = "churn"
        elif delta > 0:
            event_type = "expansion"
        else:
            event_type = "contraction"

        cur = await db.execute(
            """INSERT INTO revenue_events (customer_id, event_type, mrr_before, mrr_after, mrr_delta, reason, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (cid, event_type, last_mrr, current_mrr, delta, "auto-detected", now),
        )
        await db.commit()
        rows = await db.execute_fetchall("SELECT * FROM revenue_events WHERE id = ?", (cur.lastrowid,))
        events_created.append(_revenue_event_row(rows[0]))
    return events_created


async def list_revenue_events(db: aiosqlite.Connection,
                                event_type: str | None = None,
                                from_date: str | None = None,
                                to_date: str | None = None,
                                customer_id: int | None = None,
                                limit: int = 100,
                                offset: int = 0) -> list[dict]:
    q = "SELECT * FROM revenue_events WHERE 1=1"
    params: list = []
    if event_type:
        q += " AND event_type = ?"
        params.append(event_type)
    if from_date:
        q += " AND created_at >= ?"
        params.append(from_date)
    if to_date:
        q += " AND created_at <= ?"
        params.append(to_date + "T23:59:59")
    if customer_id:
        q += " AND customer_id = ?"
        params.append(customer_id)
    q += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    rows = await db.execute_fetchall(q, params)
    return [_revenue_event_row(r) for r in rows]


async def get_revenue_waterfall(db: aiosqlite.Connection,
                                  from_date: str | None = None,
                                  to_date: str | None = None) -> dict:
    now_dt = datetime.utcnow()
    if not from_date:
        from_date = (now_dt - timedelta(days=30)).strftime("%Y-%m-%d")
    if not to_date:
        to_date = now_dt.strftime("%Y-%m-%d")

    rows = await db.execute_fetchall(
        "SELECT * FROM revenue_events WHERE created_at >= ? AND created_at <= ?",
        (from_date, to_date + "T23:59:59"),
    )

    new_mrr = sum(r["mrr_delta"] for r in rows if r["event_type"] == "new")
    expansion = sum(r["mrr_delta"] for r in rows if r["event_type"] == "expansion")
    contraction = sum(r["mrr_delta"] for r in rows if r["event_type"] == "contraction")
    churn = sum(r["mrr_delta"] for r in rows if r["event_type"] == "churn")
    reactivation = sum(r["mrr_delta"] for r in rows if r["event_type"] == "reactivation")

    # Starting MRR: sum of all customer MRR minus net changes in period
    total_current_mrr = await db.execute_fetchall("SELECT COALESCE(SUM(mrr), 0) as total FROM customers")
    ending_mrr = round(total_current_mrr[0]["total"], 2)
    net_change = round(new_mrr + expansion + contraction + churn + reactivation, 2)
    starting_mrr = round(ending_mrr - net_change, 2)

    # Net retention rate
    if starting_mrr > 0:
        net_retention = round((starting_mrr + expansion + contraction + churn) / starting_mrr * 100, 1)
    else:
        net_retention = 100.0

    return {
        "from_date": from_date,
        "to_date": to_date,
        "starting_mrr": starting_mrr,
        "new": round(new_mrr, 2),
        "expansion": round(expansion, 2),
        "contraction": round(contraction, 2),
        "churn": round(churn, 2),
        "reactivation": round(reactivation, 2),
        "ending_mrr": ending_mrr,
        "net_change": net_change,
        "net_retention_rate": net_retention,
    }


async def get_monthly_waterfall(db: aiosqlite.Connection) -> list[dict]:
    """Monthly waterfall for last 12 months."""
    now_dt = datetime.utcnow()
    result = []
    for i in range(11, -1, -1):
        # First day of month i months ago
        month_dt = now_dt.replace(day=1) - timedelta(days=i * 30)
        month_start = month_dt.replace(day=1).strftime("%Y-%m-%d")
        # Last day of that month
        if month_dt.month == 12:
            next_month = month_dt.replace(year=month_dt.year + 1, month=1, day=1)
        else:
            next_month = month_dt.replace(month=month_dt.month + 1, day=1)
        month_end = (next_month - timedelta(days=1)).strftime("%Y-%m-%d")
        month_label = month_dt.strftime("%Y-%m")

        rows = await db.execute_fetchall(
            "SELECT * FROM revenue_events WHERE created_at >= ? AND created_at <= ?",
            (month_start, month_end + "T23:59:59"),
        )

        new_mrr = sum(r["mrr_delta"] for r in rows if r["event_type"] == "new")
        expansion = sum(r["mrr_delta"] for r in rows if r["event_type"] == "expansion")
        contraction = sum(r["mrr_delta"] for r in rows if r["event_type"] == "contraction")
        churn = sum(r["mrr_delta"] for r in rows if r["event_type"] == "churn")
        reactivation = sum(r["mrr_delta"] for r in rows if r["event_type"] == "reactivation")
        net = round(new_mrr + expansion + contraction + churn + reactivation, 2)

        result.append({
            "month": month_label,
            "starting_mrr": 0,  # Would need historical tracking for accurate value
            "new": round(new_mrr, 2),
            "expansion": round(expansion, 2),
            "contraction": round(contraction, 2),
            "churn": round(churn, 2),
            "reactivation": round(reactivation, 2),
            "ending_mrr": 0,
            "net_change": net,
        })
    return result


async def get_top_revenue_changes(db: aiosqlite.Connection,
                                    limit: int = 10,
                                    from_date: str | None = None,
                                    to_date: str | None = None) -> list[dict]:
    q = """SELECT re.customer_id, c.name as customer_name,
                  SUM(re.mrr_delta) as total_delta, COUNT(*) as event_count
           FROM revenue_events re
           JOIN customers c ON re.customer_id = c.id
           WHERE 1=1"""
    params: list = []
    if from_date:
        q += " AND re.created_at >= ?"
        params.append(from_date)
    if to_date:
        q += " AND re.created_at <= ?"
        params.append(to_date + "T23:59:59")
    q += " GROUP BY re.customer_id ORDER BY ABS(SUM(re.mrr_delta)) DESC LIMIT ?"
    params.append(limit)
    rows = await db.execute_fetchall(q, params)
    return [{
        "customer_id": r["customer_id"],
        "customer_name": r["customer_name"],
        "total_delta": round(r["total_delta"], 2),
        "event_count": r["event_count"],
    } for r in rows]
