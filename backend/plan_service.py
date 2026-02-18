"""
plan_service.py — Single source of truth for plan configuration and enforcement.

All plan checks are centralised here. Route handlers should call these
functions instead of implementing business logic inline.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

from models import (
    Building, BuildingPlan, OrgMembership, OrgPlan, Organization,
    FaultEvent, Analysis,
)

# ── Plan configuration (frozen, no DB access) ───────────────────

@dataclass(frozen=True)
class PlanConfig:
    sensor_pairs_max: Optional[int]  # None = unlimited
    api_access: bool
    webhooks: bool
    custom_thresholds: bool
    retention_days: int
    # Portfolio (org-level) features
    org_features: bool = False
    team_roles: bool = False
    central_rules: bool = False


PLAN_CONFIGS: dict[str, PlanConfig] = {
    "starter": PlanConfig(
        sensor_pairs_max=20,
        api_access=False,
        webhooks=False,
        custom_thresholds=False,
        retention_days=90,
    ),
    "professional": PlanConfig(
        sensor_pairs_max=None,
        api_access=True,
        webhooks=True,
        custom_thresholds=True,
        retention_days=365,
    ),
    "portfolio": PlanConfig(
        sensor_pairs_max=None,
        api_access=True,
        webhooks=True,
        custom_thresholds=True,
        retention_days=365,
        org_features=True,
        team_roles=True,
        central_rules=True,
    ),
}


def get_plan_config(plan_name: str) -> PlanConfig:
    """Return PlanConfig for a plan tier, defaulting to starter."""
    return PLAN_CONFIGS.get(plan_name, PLAN_CONFIGS["starter"])


# ── Plan resolution ──────────────────────────────────────────────

def get_effective_building_plan(db: Session, building_id: int) -> str:
    """Return the effective plan tier for a building.

    If the building's org has an active/trialing Portfolio contract,
    the building is treated as Professional.  Otherwise falls back to
    the building's own BuildingPlan record, defaulting to 'starter'.
    """
    b = db.query(Building).filter_by(id=building_id).first()
    if not b:
        return "starter"

    # Check org-level portfolio plan
    if b.org_id:
        org_plan = (
            db.query(OrgPlan)
            .filter(
                OrgPlan.org_id == b.org_id,
                OrgPlan.status.in_(["active", "trialing"]),
            )
            .first()
        )
        if org_plan:
            return "professional"  # portfolio → buildings get professional

    # Fall back to per-building plan
    if b.plan_record and b.plan_record.status in ("active", "trialing"):
        return b.plan_record.plan
    return "starter"


# ── Access control ───────────────────────────────────────────────

def require_org_member(db: Session, uid: int, org_id: int) -> OrgMembership:
    """Return OrgMembership or raise 403."""
    mem = (
        db.query(OrgMembership)
        .filter_by(user_id=uid, org_id=org_id)
        .first()
    )
    if not mem:
        raise HTTPException(403, "Not a member of this organization")
    return mem


def require_building_access(db: Session, bid: int, uid: int) -> Building:
    """Return Building after verifying org membership, else raise 404.

    Delegates to the models-level require_building for the actual query.
    """
    from models import require_building
    return require_building(db, bid, uid)


# ── Feature gating ──────────────────────────────────────────────

def require_feature(db: Session, building_id: int, feature_name: str) -> PlanConfig:
    """Raise 403 if the building's effective plan lacks *feature_name*.

    Returns the PlanConfig on success so callers can inspect limits.
    """
    plan_name = get_effective_building_plan(db, building_id)
    cfg = get_plan_config(plan_name)
    if not getattr(cfg, feature_name, False):
        raise HTTPException(
            403,
            f"Feature '{feature_name}' requires a plan upgrade "
            f"(current: {plan_name})",
        )
    return cfg


def enforce_sensor_pair_limit(db: Session, building_id: int, new_count: int) -> PlanConfig:
    """Raise 403 if *new_count* exceeds the plan's sensor_pairs_max.

    Returns the PlanConfig on success.
    """
    plan_name = get_effective_building_plan(db, building_id)
    cfg = get_plan_config(plan_name)
    if cfg.sensor_pairs_max is not None and new_count > cfg.sensor_pairs_max:
        raise HTTPException(
            403,
            f"Sensor pair limit is {cfg.sensor_pairs_max} on the "
            f"{plan_name} plan (requested {new_count})",
        )
    return cfg


# ── Retention ────────────────────────────────────────────────────

def get_retention_cutoff(db: Session, building_id: int) -> datetime:
    """Return the earliest UTC datetime visible under the building's plan.

    Starter  → 90 days
    Professional → 365 days
    Portfolio org → 365 days (effective plan = professional)
    """
    plan_name = get_effective_building_plan(db, building_id)
    cfg = get_plan_config(plan_name)
    return datetime.now(timezone.utc) - timedelta(days=cfg.retention_days)


# keep old name as alias so existing imports don't break
retention_cutoff = get_retention_cutoff


# ── Retention-filtered query helpers ─────────────────────────────
# These ensure every historical data query is automatically filtered
# by the building's plan retention window.  Routes must use these
# instead of querying FaultEvent / Analysis / AlertEvent directly.

def query_faults(db: Session, building_id: int, limit: int = 50) -> list[FaultEvent]:
    """Return fault history within the plan's retention window."""
    cutoff = get_retention_cutoff(db, building_id)
    return (
        db.query(FaultEvent)
        .filter_by(building_id=building_id)
        .filter(FaultEvent.detected_at >= cutoff)
        .order_by(FaultEvent.detected_at.desc())
        .limit(limit)
        .all()
    )


def query_active_faults(db: Session, building_id: int) -> list[FaultEvent]:
    """Return unresolved faults within retention window."""
    cutoff = get_retention_cutoff(db, building_id)
    return (
        db.query(FaultEvent)
        .filter_by(building_id=building_id, resolved=False)
        .filter(FaultEvent.detected_at >= cutoff)
        .order_by(FaultEvent.detected_at.desc())
        .all()
    )


def count_active_faults(db: Session, building_id: int) -> int:
    """Count unresolved faults within retention window."""
    cutoff = get_retention_cutoff(db, building_id)
    return (
        db.query(FaultEvent)
        .filter_by(building_id=building_id, resolved=False)
        .filter(FaultEvent.detected_at >= cutoff)
        .count()
    )


def query_analyses(db: Session, building_id: int, limit: int = 20) -> list[Analysis]:
    """Return analysis history within the plan's retention window."""
    cutoff = get_retention_cutoff(db, building_id)
    return (
        db.query(Analysis)
        .filter_by(building_id=building_id)
        .filter(Analysis.created_at >= cutoff)
        .order_by(Analysis.created_at.desc())
        .limit(limit)
        .all()
    )


def query_alerts(db, building_id: int, limit: int = 50):
    """Return alert events within the plan's retention window.

    AlertEvent.created_at is stored as ISO-8601 String(64), so we
    compare against the cutoff formatted as an ISO string (lexicographic
    comparison works correctly for ISO timestamps).
    """
    from alert_engine import AlertEvent
    cutoff = get_retention_cutoff(db, building_id)
    cutoff_str = cutoff.isoformat()
    return (
        db.query(AlertEvent)
        .filter_by(building_id=building_id)
        .filter(AlertEvent.created_at >= cutoff_str)
        .order_by(AlertEvent.created_at.desc())
        .limit(limit)
        .all()
    )
