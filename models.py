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
    notes: Optional[str]
    created_at: str


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


class CSMStats(BaseModel):
    total_customers: int
    total_mrr: float
    avg_health_score: float
    at_risk_count: int
    healthy_count: int
    touchpoints_this_month: int
    upcoming_actions: int
