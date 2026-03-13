# CSMFlow — Architecture (DEEP.md)

## Overview
Customer success management pipeline: health scores, touchpoints, playbooks, renewal tracking, QBRs, segments, expansion pipeline, team performance.

## Stack
- **Runtime**: Python 3.11+ / FastAPI / uvicorn
- **Database**: aiosqlite (SQLite WAL mode, foreign keys ON)
- **Models**: Pydantic v2 with Field validation

## API Endpoints (v0.6.0) — 30+ endpoints

### Customers
- POST/GET/GET/{id}/PATCH/{id}/DELETE/{id} — full CRUD
- POST /{id}/health — update health score
- POST /{id}/renewal — set renewal date + contract value
- PUT /{id}/segment — assign segment
- GET /{id}/timeline — unified activity feed

### Renewals
- GET /renewals/pipeline — upcoming renewals
- GET /renewals/at-risk — at-risk renewals

### Touchpoints
- POST /touchpoints — log interaction
- GET /touchpoints — list (filter by customer)
- GET /touchpoints/upcoming — due next-actions

### Playbooks
- POST/GET /playbooks — automation templates

### QBRs
- POST /qbrs — schedule
- GET /qbrs, GET /qbrs/{id}, GET /qbrs/upcoming
- POST /qbrs/{id}/complete

### Expansions
- POST /expansions — create upsell/cross-sell opportunity
- GET /expansions — list (filter by customer, stage)
- GET /expansions/pipeline — revenue pipeline summary
- PUT /expansions/{id}/stage — advance stage

### Stats
- GET /stats — aggregate metrics
- GET /stats/by-owner — per-CSM breakdown
- GET /stats/segments — per-segment analytics
- GET /stats/team — team performance with retention rate

## Key Features
- **Customer Timeline**: Unified feed of touchpoints, QBRs, lifecycle events
- **Expansion Pipeline**: upsell/cross-sell tracking through 6 stages with revenue
- **Team Performance**: Per-CSM metrics (customers, MRR, health, touchpoint frequency, retention)
- **Health Scoring**: 5-factor algorithm (login, adoption, tickets, NPS, time-to-value)
- **Auto-Playbooks**: Trigger on low health score
- **Segments**: enterprise/mid_market/smb/startup/general

## Version History
- v0.1.0: Customers, touchpoints, playbooks, health scoring
- v0.2.0: Customer update (PATCH)
- v0.3.0: Upcoming touchpoints, stats by owner
- v0.4.0: Renewal pipeline, at-risk, delete cascade
- v0.5.0: QBR lifecycle, customer segments
- v0.6.0: Customer timeline, expansion tracking, team performance
