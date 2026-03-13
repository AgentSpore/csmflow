# CSMFlow — Architecture (DEEP.md)

## Overview
Customer Success Management pipeline for B2B SaaS teams. Tracks customer health, engagement touchpoints, playbook automation, renewal pipeline, QBR scheduling, and customer segmentation.

## Stack
- **Runtime**: Python 3.11+ / FastAPI / uvicorn
- **Database**: SQLite (aiosqlite) with WAL mode
- **Models**: Pydantic v2 for request/response validation

## Data Model
```
customers (id, name, company, email, plan, mrr, health_score, segment,
           owner_email, onboarded_at, renewal_date, contract_value, notes)
touchpoints (id, customer_id FK, type, summary, outcome, next_action, next_action_date)
playbooks (id, name, trigger, steps JSON, description, times_triggered)
qbrs (id, customer_id FK CASCADE, scheduled_date, status, attendees JSON,
      agenda, outcome, action_items JSON, completed_at)
```

## Health Scoring Algorithm
Composite score 0-100 based on 5 signals:
- login_frequency (0-10): +2 per login, max +20
- feature_adoption (0-10): +2 per feature, max +20
- support_tickets: -5 per ticket, max -20
- nps_score (0-10): (nps - 5) * 3
- days_to_value: +10 if ≤7d, +5 if ≤30d, -10 if >90d

Labels: critical (<30), at_risk (30-49), neutral (50-69), healthy (70-84), champion (85-100)

## Segments
Customer segmentation: enterprise, mid_market, smb, startup, general (default).
Stats aggregated per segment: count, MRR, avg health, at-risk count.

## QBR Tracking
Quarterly Business Reviews lifecycle:
- scheduled → completed (with outcome + action items)
- Upcoming QBRs joined with customer data for prep
- Filtered by customer_id, status

## Key Endpoints (22 total)
- CRUD: customers (POST/GET/GET:id/PATCH/DELETE)
- Health: POST /customers/{id}/health
- Renewal: POST /customers/{id}/renewal
- Segment: PUT /customers/{id}/segment
- Renewals: GET /renewals/pipeline, /renewals/at-risk
- Touchpoints: POST, GET, GET /touchpoints/upcoming
- Playbooks: POST, GET
- QBRs: POST, GET, GET:id, POST /qbrs/{id}/complete, GET /qbrs/upcoming
- Stats: GET /stats, /stats/by-owner, /stats/segments

## Migrations
Auto-applied on startup via init_db():
1. renewal_date + contract_value columns (v0.4.0)
2. segment column with DEFAULT 'general' (v0.5.0)
