from __future__ import annotations
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
"""

HEALTH_LABELS = {
    range(0, 30):  "critical",
    range(30, 50): "at_risk",
    range(50, 70): "neutral",
    range(70, 85): "healthy",
    range(85, 101): "champion",
}


def _health_label(score: int) -> str:
    for r, label in HEALTH_LABELS.items():
        if score in r:
            return label
    return "unknown"


async def init_db(path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    await db.executescript(SQL)
    await db.commit()
    return db


def _customer_row(r: aiosqlite.Row) -> dict:
    days = None
    if r["onboarded_at"]:
        try:
            onboarded = datetime.fromisoformat(r["onboarded_at"]).date()
            days = (datetime.utcnow().date() - onboarded).days
        except: pass
    return {
        "id": r["id"], "name": r["name"], "company": r["company"],
        "email": r["email"], "plan": r["plan"], "mrr": r["mrr"],
        "health_score": r["health_score"], "health_label": _health_label(r["health_score"]),
        "owner_email": r["owner_email"], "onboarded_at": r["onboarded_at"],
        "days_since_onboarding": days, "notes": r["notes"], "created_at": r["created_at"],
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
    """Compute 0-100 health score from usage signals."""
    score = 50  # baseline
    # Login frequency: up to +20
    score += min(login_freq * 2, 20)
    # Feature adoption: up to +20
    score += min(feature_adoption * 2, 20)
    # Support tickets: -5 per open ticket, max -20
    score -= min(support_tickets * 5, 20)
    # NPS: map 0-10 to -15 to +15
    if nps is not None:
        score += (nps - 5) * 3
    # Days to value: fast = good
    if days_to_value is not None:
        if days_to_value <= 7:
            score += 10
        elif days_to_value <= 30:
            score += 5
        elif days_to_value > 90:
            score -= 10
    return max(0, min(100, score))


async def create_customer(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        """INSERT INTO customers (name, company, email, plan, mrr, owner_email, onboarded_at, notes, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (data["name"], data["company"], data["email"], data.get("plan", "starter"),
         data.get("mrr", 0), data.get("owner_email"), data.get("onboarded_at"),
         data.get("notes"), now)
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM customers WHERE id = ?", (cur.lastrowid,))
    return _customer_row(rows[0])


async def list_customers(db: aiosqlite.Connection, health: str | None = None, plan: str | None = None) -> list[dict]:
    q = "SELECT * FROM customers"
    conds, params = [], []
    if plan:
        conds.append("plan = ?"); params.append(plan)
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY mrr DESC"
    rows = await db.execute_fetchall(q, params)
    result = [_customer_row(r) for r in rows]
    if health:
        result = [c for c in result if c["health_label"] == health]
    return result


async def get_customer(db: aiosqlite.Connection, customer_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM customers WHERE id = ?", (customer_id,))
    return _customer_row(rows[0]) if rows else None


async def update_health(db: aiosqlite.Connection, customer_id: int,
                        login_freq: int, feature_adoption: int,
                        support_tickets: int, nps: int | None, days_to_value: int | None) -> dict | None:
    score = compute_health_score(login_freq, feature_adoption, support_tickets, nps, days_to_value)
    await db.execute("UPDATE customers SET health_score = ? WHERE id = ?", (score, customer_id))
    await db.commit()
    # Auto-trigger playbooks
    label = _health_label(score)
    if label in ("critical", "at_risk"):
        await _trigger_playbooks(db, customer_id, "low_health")
    return await get_customer(db, customer_id)


async def _trigger_playbooks(db: aiosqlite.Connection, customer_id: int, trigger: str):
    rows = await db.execute_fetchall("SELECT id FROM playbooks WHERE trigger = ?", (trigger,))
    for r in rows:
        await db.execute("UPDATE playbooks SET times_triggered = times_triggered + 1 WHERE id = ?", (r["id"],))
    await db.commit()


async def add_touchpoint(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
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
    now = datetime.now(timezone.utc).isoformat()
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
                "at_risk_count": 0, "healthy_count": 0, "touchpoints_this_month": 0, "upcoming_actions": 0}

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

    return {
        "total_customers": len(customers),
        "total_mrr": round(total_mrr, 2),
        "avg_health_score": avg_health,
        "at_risk_count": at_risk,
        "healthy_count": healthy,
        "touchpoints_this_month": tp_count,
        "upcoming_actions": upcoming_count,
    }
