from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional


class CustomerCreate(BaseModel):
    name: str
    company: str
    email: str
    plan: str = Field("starter", description="Subscription plan tier")
    mrr: float = Field(0.0, ge=0, description="Monthly recurring revenue from this customer")
    owner_email: Optional[str] = None
    onboarded_at: Optional[str] = None
    notes: Optional[str] = None


class CustomerUpdate(BaseModel):
    name: Optional[str] = None
    company: Optional[str] = None
    email: Optional[str] = None
    plan: Optional[str] = None
    mrr: Optional[float] = Field(default=None, ge=0)
    owner_email: Optional[str] = None
    onboarded_at: Optional[str] = None
    notes: Optional[str] = None


class CustomerResponse(BaseModel):
    id: int
    name: str
    company: str
    email: str
    plan: str
    mrr: float
    health_score: int
    health_label: str
    owner_email: Optional[str]
    onboarded_at: Optional[str]
    days_since_onboarding: Optional[int]
    renewal_date: Optional[str]
    contract_value: Optional[float]
    segment: str = "general"
    tags: list[str] = Field(default_factory=list)
    notes: Optional[str]
    created_at: str


class RenewalUpdate(BaseModel):
    renewal_date: str = Field(..., description="ISO date (YYYY-MM-DD) of next renewal")
    contract_value: float = Field(..., ge=0, description="Annual contract value")


class RenewalPipelineItem(BaseModel):
    customer_id: int
    name: str
    company: str
    plan: str
    mrr: float
    health_score: int
    health_label: str
    renewal_date: str
    contract_value: float
    days_until_renewal: int
    owner_email: Optional[str]
    last_touchpoint_date: Optional[str]


class TouchpointCreate(BaseModel):
    customer_id: int
    type: str = Field(..., description="Type: call | email | qbr | onboarding | support | nps")
    summary: str
    outcome: str = Field("neutral", description="Outcome: positive | neutral | negative")
    next_action: Optional[str] = None
    next_action_date: Optional[str] = None


class TouchpointResponse(BaseModel):
    id: int
    customer_id: int
    type: str
    summary: str
    outcome: str
    next_action: Optional[str]
    next_action_date: Optional[str]
    created_at: str


class PlaybookCreate(BaseModel):
    name: str
    trigger: str = Field(..., description="Trigger: low_health | onboarding | renewal_90d | nps_detractor | expansion")
    steps: list[str] = Field(..., description="Ordered list of action steps")
    description: Optional[str] = None


class PlaybookResponse(BaseModel):
    id: int
    name: str
    trigger: str
    steps: list[str]
    description: Optional[str]
    times_triggered: int
    created_at: str


class HealthScoreUpdate(BaseModel):
    login_frequency: int = Field(0, ge=0, le=10, description="Logins per week (0-10)")
    feature_adoption: int = Field(0, ge=0, le=10, description="Features used out of available")
    support_tickets: int = Field(0, ge=0, description="Open support tickets")
    nps_score: Optional[int] = Field(None, ge=0, le=10, description="Latest NPS response")
    days_to_value: Optional[int] = Field(None, description="Days to first key milestone")


# ── QBR Models ───────────────────────────────────────────────────────────

class QBRCreate(BaseModel):
    customer_id: int
    scheduled_date: str = Field(..., description="ISO date (YYYY-MM-DD) of the QBR meeting")
    attendees: list[str] = Field(default_factory=list, description="List of attendee emails")
    agenda: Optional[str] = None


class QBRComplete(BaseModel):
    outcome: str = Field(..., description="Summary of QBR outcome")
    action_items: list[str] = Field(default_factory=list, description="Follow-up action items")


class QBRResponse(BaseModel):
    id: int
    customer_id: int
    scheduled_date: str
    status: str
    attendees: list[str]
    agenda: Optional[str]
    outcome: Optional[str]
    action_items: list[str]
    completed_at: Optional[str]
    created_at: str


# ── Segment Models ───────────────────────────────────────────────────────

class SegmentUpdate(BaseModel):
    segment: str = Field(..., description="Segment: enterprise | mid_market | smb | startup | general")


class SegmentStats(BaseModel):
    segment: str
    customers: int
    total_mrr: float
    avg_health_score: float
    at_risk_count: int


class CSMStats(BaseModel):
    total_customers: int
    total_mrr: float
    avg_health_score: float
    at_risk_count: int
    healthy_count: int
    touchpoints_this_month: int
    upcoming_actions: int
    renewals_next_30d: int
    at_risk_renewal_value: float
    total_nps_surveys: int
    avg_nps: float
    total_contacts: int
    total_goals: int


# ── Timeline ────────────────────────────────────────────────────────────

class TimelineEvent(BaseModel):
    type: str
    subtype: str
    summary: str
    outcome: str
    timestamp: str


class CustomerTimeline(BaseModel):
    customer_id: int
    customer_name: str
    company: str
    total_events: int
    events: list[TimelineEvent]


# ── Expansion ───────────────────────────────────────────────────────────

class ExpansionCreate(BaseModel):
    customer_id: int
    type: str = Field("upsell", description="upsell | cross_sell | add_on")
    description: str = Field(..., min_length=1)
    expected_mrr: float = Field(0, ge=0, description="Expected monthly revenue increase")
    stage: str = Field("identified", description="identified | qualified | proposal | negotiation | won | lost")
    owner_email: Optional[str] = None
    notes: Optional[str] = None


class ExpansionResponse(BaseModel):
    id: int
    customer_id: int
    type: str
    description: str
    expected_mrr: float
    stage: str
    owner_email: Optional[str]
    notes: Optional[str]
    closed_at: Optional[str]
    created_at: str


class ExpansionStageUpdate(BaseModel):
    stage: str = Field(..., description="identified | qualified | proposal | negotiation | won | lost")


class ExpansionPipelineStage(BaseModel):
    stage: str
    count: int
    total_mrr: float


class ExpansionPipeline(BaseModel):
    total_opportunities: int
    total_pipeline_mrr: float
    won_mrr: float
    stages: list[ExpansionPipelineStage]


# ── Team Performance ────────────────────────────────────────────────────

class CSMPerformance(BaseModel):
    owner_email: str
    customers: int
    total_mrr: float
    avg_health_score: float
    at_risk_count: int
    champions: int
    touchpoints_last_30d: int
    touchpoint_frequency: float
    retention_rate: float


# ── Tags ────────────────────────────────────────────────────────────────

class TagRequest(BaseModel):
    tag: str = Field(..., min_length=1, max_length=50)


# ── NPS Surveys ─────────────────────────────────────────────────────────

class NPSSurveyCreate(BaseModel):
    customer_id: int
    score: int = Field(..., ge=0, le=10, description="NPS score 0-10")
    feedback: Optional[str] = Field(None, max_length=2000)


class NPSSurveyResponse(BaseModel):
    id: int
    customer_id: int
    customer_name: str
    score: int
    category: str  # promoter | passive | detractor
    feedback: Optional[str]
    created_at: str


class NPSTrendPoint(BaseModel):
    period: str
    avg_score: float
    promoters: int
    passives: int
    detractors: int
    responses: int
    nps_score: float  # (promoters - detractors) / total * 100


class NPSOverview(BaseModel):
    total_surveys: int
    avg_score: float
    nps_score: float
    promoters_pct: float
    passives_pct: float
    detractors_pct: float
    by_segment: list[dict]
    trend: list[NPSTrendPoint]


# ── Churn Risk ──────────────────────────────────────────────────────────

class ChurnRiskFactor(BaseModel):
    factor: str
    impact: int  # negative impact points
    detail: str


class ChurnRisk(BaseModel):
    customer_id: int
    customer_name: str
    risk_score: int  # 0-100, higher = more risk
    risk_level: str  # critical | high | medium | low
    factors: list[ChurnRiskFactor]
    recommendation: str


# ── Stakeholder Contacts ────────────────────────────────────────────────

class ContactCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    email: str = Field(min_length=3, max_length=200)
    role: str = Field("user", description="champion | decision_maker | influencer | user | technical")
    influence: str = Field("medium", description="high | medium | low")
    phone: Optional[str] = Field(None, max_length=30)
    notes: Optional[str] = None


class ContactResponse(BaseModel):
    id: int
    customer_id: int
    name: str
    email: str
    role: str
    influence: str
    phone: Optional[str]
    notes: Optional[str]
    last_contacted_at: Optional[str]
    created_at: str


# ── Customer Goals ──────────────────────────────────────────────────────

class GoalCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    description: Optional[str] = None
    target_date: str = Field(..., description="ISO date YYYY-MM-DD")
    target_value: float = Field(100, ge=0, description="Target metric value")
    current_value: float = Field(0, ge=0, description="Current metric value")
    owner_email: Optional[str] = None


class GoalUpdate(BaseModel):
    current_value: Optional[float] = Field(None, ge=0)
    status: Optional[str] = Field(None, description="active | completed | at_risk | cancelled")
    notes: Optional[str] = None


class GoalResponse(BaseModel):
    id: int
    customer_id: int
    title: str
    description: Optional[str]
    target_date: str
    target_value: float
    current_value: float
    progress_pct: float
    status: str
    owner_email: Optional[str]
    days_remaining: Optional[int]
    created_at: str
    updated_at: str
