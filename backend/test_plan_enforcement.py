"""Unit tests for plan enforcement logic in plan_service.py.

Tests run against an in-memory SQLite database — no server required.
"""
import pytest
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from models import (
    Base, User, Organization, OrgMembership, Building, BuildingPlan, OrgPlan,
    FaultEvent, Analysis,
)
from plan_service import (
    get_effective_building_plan,
    get_retention_cutoff,
    require_feature,
    enforce_sensor_pair_limit,
    query_faults,
    query_analyses,
    PLAN_CONFIGS,
)


def _make_db() -> Session:
    """In-memory SQLite session with all tables created."""
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return sessionmaker(bind=eng)()


def _seed_user_org_building(db: Session, plan: str | None = None):
    """Create user + org + membership + building, optionally with a BuildingPlan."""
    user = User(email="test@x.com", password_hash="x", name="Test")
    db.add(user)
    db.flush()

    org = Organization(name="Test Org")
    db.add(org)
    db.flush()

    db.add(OrgMembership(org_id=org.id, user_id=user.id, role="owner"))
    db.flush()

    building = Building(owner_id=user.id, org_id=org.id, name="HQ")
    db.add(building)
    db.flush()

    if plan:
        db.add(BuildingPlan(
            building_id=building.id, plan=plan, status="active",
        ))
        db.flush()

    db.commit()
    return user, org, building


# ═══════════════════════════════════════════════════════════════════
# Feature & limit tests
# ═══════════════════════════════════════════════════════════════════

def test_starter_blocks_api_access():
    db = _make_db()
    _user, _org, building = _seed_user_org_building(db)

    with pytest.raises(HTTPException) as exc:
        require_feature(db, building.id, "api_access")
    assert exc.value.status_code == 403
    assert "api_access" in exc.value.detail
    assert "starter" in exc.value.detail


def test_starter_sensor_pair_limit_enforced():
    db = _make_db()
    _user, _org, building = _seed_user_org_building(db)

    cfg = enforce_sensor_pair_limit(db, building.id, 20)
    assert cfg.sensor_pairs_max == 20

    with pytest.raises(HTTPException) as exc:
        enforce_sensor_pair_limit(db, building.id, 21)
    assert exc.value.status_code == 403
    assert "20" in exc.value.detail


def test_portfolio_org_makes_building_professional():
    db = _make_db()
    _user, org, building = _seed_user_org_building(db)
    assert get_effective_building_plan(db, building.id) == "starter"

    db.add(OrgPlan(org_id=org.id, plan="portfolio", status="active"))
    db.commit()

    assert get_effective_building_plan(db, building.id) == "professional"

    cfg = require_feature(db, building.id, "api_access")
    assert cfg.api_access is True

    cfg = enforce_sensor_pair_limit(db, building.id, 1000)
    assert cfg.sensor_pairs_max is None


# ═══════════════════════════════════════════════════════════════════
# Retention tests
# ═══════════════════════════════════════════════════════════════════

def _insert_fault(db: Session, building_id: int, days_ago: int) -> FaultEvent:
    """Insert a FaultEvent detected *days_ago* days in the past."""
    ts = datetime.now(timezone.utc) - timedelta(days=days_ago)
    fe = FaultEvent(
        building_id=building_id,
        pair_name=f"pair-{days_ago}d",
        group="test",
        severity="fault",
        detected_at=ts,
    )
    db.add(fe)
    db.commit()
    return fe


def _insert_analysis(db: Session, building_id: int, days_ago: int) -> Analysis:
    """Insert an Analysis created *days_ago* days in the past."""
    ts = datetime.now(timezone.utc) - timedelta(days=days_ago)
    a = Analysis(
        building_id=building_id,
        filename=f"test-{days_ago}d.csv",
        created_at=ts,
    )
    db.add(a)
    db.commit()
    return a


def test_starter_cannot_access_data_older_than_90_days():
    """Starter plan (90-day retention) hides faults and analyses older than 90 days."""
    db = _make_db()
    _user, _org, building = _seed_user_org_building(db)

    # Verify we're on starter with 90-day retention
    assert get_effective_building_plan(db, building.id) == "starter"
    assert PLAN_CONFIGS["starter"].retention_days == 90

    # Insert faults at various ages
    _insert_fault(db, building.id, days_ago=30)   # within window
    _insert_fault(db, building.id, days_ago=89)   # within window (edge)
    _insert_fault(db, building.id, days_ago=91)   # outside window
    _insert_fault(db, building.id, days_ago=200)  # well outside

    # Insert analyses at various ages
    _insert_analysis(db, building.id, days_ago=30)
    _insert_analysis(db, building.id, days_ago=91)

    faults = query_faults(db, building.id, limit=100)
    assert len(faults) == 2  # only 30d and 89d

    analyses = query_analyses(db, building.id, limit=100)
    assert len(analyses) == 1  # only 30d

    # Verify cutoff is ~90 days ago
    cutoff = get_retention_cutoff(db, building.id)
    expected = datetime.now(timezone.utc) - timedelta(days=90)
    assert abs((cutoff - expected).total_seconds()) < 5


def test_professional_can_access_data_up_to_365_days():
    """Professional plan (365-day retention) sees data within a full year."""
    db = _make_db()
    _user, _org, building = _seed_user_org_building(db, plan="professional")

    assert get_effective_building_plan(db, building.id) == "professional"
    assert PLAN_CONFIGS["professional"].retention_days == 365

    _insert_fault(db, building.id, days_ago=30)
    _insert_fault(db, building.id, days_ago=200)  # within 365
    _insert_fault(db, building.id, days_ago=364)  # edge — within
    _insert_fault(db, building.id, days_ago=400)  # outside

    _insert_analysis(db, building.id, days_ago=200)
    _insert_analysis(db, building.id, days_ago=400)

    faults = query_faults(db, building.id, limit=100)
    assert len(faults) == 3  # 30d, 200d, 364d

    analyses = query_analyses(db, building.id, limit=100)
    assert len(analyses) == 1  # 200d only

    cutoff = get_retention_cutoff(db, building.id)
    expected = datetime.now(timezone.utc) - timedelta(days=365)
    assert abs((cutoff - expected).total_seconds()) < 5


def test_portfolio_org_enforces_365_for_all_buildings():
    """Portfolio org promotes buildings to professional → 365-day retention."""
    db = _make_db()
    _user, org, building = _seed_user_org_building(db)

    # Without portfolio: starter 90-day retention
    _insert_fault(db, building.id, days_ago=30)
    _insert_fault(db, building.id, days_ago=200)

    assert len(query_faults(db, building.id, limit=100)) == 1  # only 30d

    # Attach portfolio to org
    db.add(OrgPlan(org_id=org.id, plan="portfolio", status="active"))
    db.commit()

    # Now effective plan = professional → 365-day retention
    assert get_effective_building_plan(db, building.id) == "professional"

    faults = query_faults(db, building.id, limit=100)
    assert len(faults) == 2  # both 30d and 200d visible now

    cutoff = get_retention_cutoff(db, building.id)
    expected = datetime.now(timezone.utc) - timedelta(days=365)
    assert abs((cutoff - expected).total_seconds()) < 5
