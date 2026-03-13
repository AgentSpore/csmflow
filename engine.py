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
