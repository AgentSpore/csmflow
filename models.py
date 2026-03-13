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


# ── QBR Models ──────────────────────────────────────────────────────────

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


# ── Segment Models ──────────────────────────────────────────────────────

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
