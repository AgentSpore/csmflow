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


def _health_label(score: int) -> str:
    for r, label in HEALTH_LABELS.items():
        if score in r:
            return label
    return "unknown"


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
    await db.execute("DELETE FROM customers WHERE id = ?", (customer_id,))
    await db.commit()
    return True


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
                "at_risk_count": 0, "healthy_count": 0, "touchpoints_this_month": 0,
                "upcoming_actions": 0, "renewals_next_30d": 0, "at_risk_renewal_value": 0.0,
                "total_nps_surveys": 0, "avg_nps": 0.0}
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
    now = datetime.now(timezone.utc).isoformat()
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
    now = datetime.now(timezone.utc).isoformat()
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
            "summary": f"NPS survey: {n['score']}/10 ({cat}){' — ' + n['feedback'] if n['feedback'] else ''}",
            "outcome": "positive" if cat == "promoter" else ("negative" if cat == "detractor" else "neutral"),
            "timestamp": n["created_at"],
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
    now = datetime.now(timezone.utc).isoformat()
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
        closed_at = datetime.now(timezone.utc).isoformat()
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
    now = datetime.now(timezone.utc).isoformat()
    try:
        await db.execute(
            "INSERT INTO customer_tags (customer_id, tag, created_at) VALUES (?, ?, ?)",
            (customer_id, tag.strip().lower(), now),
        )
        await db.commit()
    except Exception:
        pass  # UNIQUE constraint — tag already exists, that's fine
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
    now = datetime.now(timezone.utc).isoformat()
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
