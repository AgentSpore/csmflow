# CSMFlow — Development Log (MEMORY.md)

## v0.1.0 — Initial MVP
- Health scoring with 5-signal composite algorithm
- Touchpoint logging (call/email/qbr/onboarding/support/nps)
- Playbook engine with auto-trigger on low_health
- Stats dashboard with MRR, health averages

## v0.2.0 — Customer Management
- PATCH /customers/{id} — partial update for plan, MRR, notes, etc.

## v0.3.0 — Touchpoint Enhancements
- GET /touchpoints/upcoming?days=7 — pending actions sorted by date
- GET /stats/by-owner — per-CSM workload breakdown

## v0.4.0 — Renewal Pipeline
- POST /customers/{id}/renewal — set renewal date + contract value
- GET /renewals/pipeline — upcoming renewals with days_until + last touchpoint
- GET /renewals/at-risk — filtered to critical/at_risk health customers
- DELETE /customers/{id} — cascade delete with touchpoints
- Stats now include renewals_next_30d and at_risk_renewal_value

## v0.5.0 — QBR Tracking & Segments
- **QBR lifecycle**: POST /qbrs (schedule), GET /qbrs (list with filters),
  GET /qbrs/{id} (detail), POST /qbrs/{id}/complete (outcome + action items),
  GET /qbrs/upcoming (next N days with customer info)
- **Customer segments**: PUT /customers/{id}/segment (enterprise/mid_market/smb/startup/general),
  GET /stats/segments (per-segment MRR, health, at-risk), segment filter on GET /customers
- New `qbrs` table with indexes on customer_id and scheduled_date
- Auto-migration adds `segment` column with 'general' default
- 22 endpoints total, version 0.5.0
