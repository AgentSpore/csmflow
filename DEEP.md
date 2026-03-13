# CSMFlow — Architecture (DEEP.md)

## Overview
Customer success management pipeline: health scores, touchpoints, playbooks, renewal tracking, QBRs, segments, expansion pipeline, team performance, customer tags, NPS surveys, and churn risk assessment.

## Stack
- **Runtime**: Python 3.11+ / FastAPI / uvicorn
- **Database**: aiosqlite (SQLite WAL mode, foreign keys ON)
- **Models**: Pydantic v2 with Field validation

## API Endpoints (v0.7.0) — 35+ endpoints

### Customers
- POST/GET/GET/{id}/PATCH/{id}/DELETE/{id} — full CRUD
- POST /{id}/health — update health score
- POST /{id}/renewal — set renewal date + contract value
- PUT /{id}/segment — assign segment
- GET /{id}/timeline — unified activity feed
- POST /{id}/tags — add tag
- DELETE /{id}/tags — remove tag
- GET /{id}/risk — churn risk assessment

### Renewals
- GET /renewals/pipeline — upcoming renewals
- GET /renewals/at-risk — at-risk renewals

### Touchpoints
- POST /touchpoints — log interaction
- GET /touchpoints — list (filter by customer)
- GET /touchpoints/upcoming — due next-actions

### Playbooks
- POST/GET /playbooks — automation templates

### NPS Surveys
- POST /nps — record NPS survey (auto-triggers detractor playbook)
- GET /nps — list surveys (filter by customer, category)
- GET /nps/overview — NPS score, per-segment breakdown, 6-month trend

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
- GET /stats — aggregate metrics (incl. NPS)
- GET /stats/by-owner — per-CSM breakdown
- GET /stats/segments — per-segment analytics
- GET /stats/team — team performance with retention rate

## Key Features
- **Customer Tags**: Flexible tagging for segmentation, filter by tag
- **NPS Surveys**: Record scores, auto-categorize (promoter/passive/detractor), trend analysis, per-segment breakdown, detractor playbook auto-trigger
- **Churn Risk**: Automated scoring (0-100) based on health, renewal proximity, touchpoint frequency, NPS trend, expansion activity. Risk levels: critical/high/medium/low
- **Customer Timeline**: Unified feed of touchpoints, QBRs, NPS surveys, lifecycle events
- **Expansion Pipeline**: upsell/cross-sell tracking through 6 stages with revenue
- **Team Performance**: Per-CSM metrics (customers, MRR, health, touchpoint frequency, retention)
- **Health Scoring**: 5-factor algorithm (login, adoption, tickets, NPS, time-to-value)
- **Auto-Playbooks**: Trigger on low health score or NPS detractor
- **Segments**: enterprise/mid_market/smb/startup/general

## Database Tables
- customers, touchpoints, playbooks, qbrs, expansions, customer_tags, nps_surveys

## Version History
- v0.1.0: Customers, touchpoints, playbooks, health scoring
- v0.2.0: Customer update (PATCH)
- v0.3.0: Upcoming touchpoints, stats by owner
- v0.4.0: Renewal pipeline, at-risk, delete cascade
- v0.5.0: QBR lifecycle, customer segments
- v0.6.0: Customer timeline, expansion tracking, team performance
- v0.7.0: Customer tags, NPS survey tracking with trend analysis, churn risk assessment
