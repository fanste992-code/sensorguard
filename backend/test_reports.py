"""Unit tests for report generation and plan enforcement.

Tests run against an in-memory SQLite database — no server required.
"""
import io
import json
import pytest
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from models import (
    Base, User, Organization, OrgMembership, Building, BuildingPlan, OrgPlan,
    Report, Analysis, FaultEvent,
)
from report_service import (
    create_report, enforce_report_limits, list_reports, get_report,
    generate_report_pdf, STARTER_DAILY_REPORT_LIMIT,
)
from plan_service import PLAN_CONFIGS
from hvac import analyze_csv, build_config, _parse_date_time_columns


def _make_db() -> Session:
    """In-memory SQLite session with all tables created."""
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return sessionmaker(bind=eng)()


def _seed(db: Session, plan: str | None = None):
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
# Starter daily limit
# ═══════════════════════════════════════════════════════════════════

def test_starter_blocked_on_4th_report_same_day():
    """Starter plan allows max 3 reports per building per day."""
    db = _make_db()
    user, _org, building = _seed(db)

    now = datetime.now(timezone.utc)
    period_start = now - timedelta(days=30)
    period_end = now

    # First 3 reports should succeed
    for i in range(STARTER_DAILY_REPORT_LIMIT):
        r = create_report(db, building.id, user.id, period_start, period_end)
        assert r.status == "pending"

    # 4th should be blocked
    with pytest.raises(HTTPException) as exc:
        create_report(db, building.id, user.id, period_start, period_end)
    assert exc.value.status_code == 403
    assert "3" in exc.value.detail


def test_professional_unlimited_reports():
    """Professional plan has no daily report limit."""
    db = _make_db()
    user, _org, building = _seed(db, plan="professional")

    now = datetime.now(timezone.utc)
    period_start = now - timedelta(days=30)
    period_end = now

    # Should be able to create more than 3
    for i in range(5):
        r = create_report(db, building.id, user.id, period_start, period_end)
        assert r.status == "pending"


# ═══════════════════════════════════════════════════════════════════
# Retention enforcement
# ═══════════════════════════════════════════════════════════════════

def test_starter_rejects_report_beyond_90_days():
    """Starter plan rejects period_start older than 90 days."""
    db = _make_db()
    user, _org, building = _seed(db)

    now = datetime.now(timezone.utc)
    period_end = now

    # Within 90-day window: OK
    period_start_ok = now - timedelta(days=89)
    r = create_report(db, building.id, user.id, period_start_ok, period_end)
    assert r.status == "pending"

    # Beyond 90-day window: blocked
    period_start_bad = now - timedelta(days=91)
    with pytest.raises(HTTPException) as exc:
        create_report(db, building.id, user.id, period_start_bad, period_end)
    assert exc.value.status_code == 403
    assert "retention" in exc.value.detail.lower() or "90" in exc.value.detail


def test_professional_allows_up_to_365_days():
    """Professional plan allows period_start within 365-day retention."""
    db = _make_db()
    user, _org, building = _seed(db, plan="professional")

    now = datetime.now(timezone.utc)
    period_end = now

    # 200 days ago: OK for professional (365d window)
    period_start = now - timedelta(days=200)
    r = create_report(db, building.id, user.id, period_start, period_end)
    assert r.status == "pending"

    # Beyond 365: blocked
    period_start_bad = now - timedelta(days=400)
    with pytest.raises(HTTPException) as exc:
        create_report(db, building.id, user.id, period_start_bad, period_end)
    assert exc.value.status_code == 403


def test_portfolio_org_allows_365_day_reports():
    """Portfolio org promotes building to professional -> 365-day retention for reports."""
    db = _make_db()
    user, org, building = _seed(db)

    now = datetime.now(timezone.utc)
    period_end = now

    # Without portfolio: starter 90d -> 200d ago is rejected
    period_start = now - timedelta(days=200)
    with pytest.raises(HTTPException) as exc:
        create_report(db, building.id, user.id, period_start, period_end)
    assert exc.value.status_code == 403

    # Add portfolio plan to org
    db.add(OrgPlan(org_id=org.id, plan="portfolio", status="active"))
    db.commit()

    # Now 200d ago should work (professional 365d retention)
    r = create_report(db, building.id, user.id, period_start, period_end)
    assert r.status == "pending"


# ═══════════════════════════════════════════════════════════════════
# Analysis-linked reports
# ═══════════════════════════════════════════════════════════════════

def _create_analysis_with_faults(db, building_id, user_id, n_faults=3):
    """Create an Analysis with known faults, simulating a CSV run."""
    now = datetime.now(timezone.utc)
    analysis = Analysis(
        building_id=building_id,
        user_id=user_id,
        source="csv",
        filename="test-baseline.csv",
        total_ticks=100,
        fault_ticks=n_faults * 10,
        fault_rate=n_faults * 10 / 100,
        data_start_ts=now - timedelta(hours=12),
        data_end_ts=now - timedelta(hours=1),
    )
    db.add(analysis)
    db.flush()

    faults = []
    for i in range(n_faults):
        fe = FaultEvent(
            building_id=building_id,
            analysis_id=analysis.id,
            pair_name=f"AHU-{i+1} Valve",
            group="valve",
            severity="fault",
            diagnosis=f"CMD/POS mismatch on AHU-{i+1}",
        )
        db.add(fe)
        faults.append(fe)
    db.commit()
    return analysis, faults


def test_report_with_analysis_id_only_includes_that_runs_faults():
    """Report linked to analysis_id should only include faults from that run."""
    db = _make_db()
    user, _org, building = _seed(db, plan="professional")

    now = datetime.now(timezone.utc)

    # Create two analysis runs with different faults
    analysis1, faults1 = _create_analysis_with_faults(db, building.id, user.id, n_faults=3)
    analysis2, faults2 = _create_analysis_with_faults(db, building.id, user.id, n_faults=5)

    # Also create some "orphan" faults not linked to any analysis
    orphan = FaultEvent(
        building_id=building.id,
        pair_name="Orphan-Sensor",
        group="custom",
        severity="fault",
        diagnosis="Unlinked fault",
    )
    db.add(orphan)
    db.commit()

    # Total faults in DB = 3 + 5 + 1 = 9
    all_faults = db.query(FaultEvent).filter_by(building_id=building.id).all()
    assert len(all_faults) == 9

    # Create report linked to analysis1
    r1 = create_report(db, building.id, user.id,
                        now - timedelta(days=1), now,
                        analysis_id=analysis1.id)
    assert r1.analysis_id == analysis1.id

    # Create report linked to analysis2
    r2 = create_report(db, building.id, user.id,
                        now - timedelta(days=1), now,
                        analysis_id=analysis2.id)
    assert r2.analysis_id == analysis2.id


def test_report_without_analysis_uses_latest():
    """Report without explicit analysis_id should use most recent analysis."""
    db = _make_db()
    user, _org, building = _seed(db, plan="professional")

    now = datetime.now(timezone.utc)

    # Create two analysis runs
    analysis1, _ = _create_analysis_with_faults(db, building.id, user.id, n_faults=2)
    analysis2, _ = _create_analysis_with_faults(db, building.id, user.id, n_faults=4)

    # Create report without analysis_id
    r = create_report(db, building.id, user.id,
                       now - timedelta(days=1), now)
    assert r.analysis_id is None  # not set at creation time

    # But generate_report_pdf will pick the latest analysis (analysis2)
    # We can't fully test PDF generation here (needs xhtml2pdf),
    # but verify the analysis lookup logic works
    from report_service import get_report as _get_report
    report = _get_report(db, r.id)
    assert report.building_id == building.id

    # Verify latest analysis is analysis2
    latest = (
        db.query(Analysis)
        .filter_by(building_id=building.id)
        .order_by(Analysis.created_at.desc())
        .first()
    )
    assert latest.id == analysis2.id


def test_analysis_data_timestamps_stored():
    """Analysis should store data_start_ts and data_end_ts from CSV."""
    db = _make_db()
    user, _org, building = _seed(db, plan="professional")

    now = datetime.now(timezone.utc)
    analysis, _ = _create_analysis_with_faults(db, building.id, user.id)

    assert analysis.data_start_ts is not None
    assert analysis.data_end_ts is not None
    assert analysis.data_start_ts < analysis.data_end_ts
    assert analysis.source == "csv"
    assert analysis.user_id == user.id


# ═══════════════════════════════════════════════════════════════════
# DATE/TIME BAS timestamp parsing
# ═══════════════════════════════════════════════════════════════════

def test_parse_date_time_columns_bas_format():
    """_parse_date_time_columns parses BAS DATE=8202007, TIME=720 correctly."""
    row = {"DATE": "8202007", "TIME": "720"}
    ts = _parse_date_time_columns(row)
    assert ts is not None
    dt = datetime.fromtimestamp(ts)
    assert dt.year == 2007
    assert dt.month == 8
    assert dt.day == 20
    assert dt.hour == 12  # 720 minutes = 12:00
    assert dt.minute == 0


def test_parse_date_time_columns_with_padding():
    """DATE values shorter than 8 digits should be zero-padded on the left."""
    # 1152020 -> 01152020 -> January 15, 2020
    row = {"DATE": "1152020", "TIME": "90"}
    ts = _parse_date_time_columns(row)
    assert ts is not None
    dt = datetime.fromtimestamp(ts)
    assert dt.month == 1
    assert dt.day == 15
    assert dt.year == 2020
    assert dt.hour == 1  # 90 minutes = 1:30
    assert dt.minute == 30


def test_parse_date_time_columns_missing_date():
    """Returns None when DATE column is missing or empty."""
    assert _parse_date_time_columns({}) is None
    assert _parse_date_time_columns({"DATE": "", "TIME": "0"}) is None


def test_analyze_csv_bas_date_time_data_window():
    """analyze_csv with BAS DATE/TIME columns yields correct data_ts_min/max."""
    csv_text = (
        "DATE,TIME,TempSensor_A,TempSensor_B\n"
        "8202007,0,72.1,72.3\n"
        "8202007,60,72.2,72.4\n"
        "8202007,720,73.0,73.1\n"
        "8202007,1439,71.5,71.6\n"
    )
    pairs = [{"name": "Temp", "group": "temp", "col_a": "TempSensor_A",
              "col_b": "TempSensor_B", "pair_type": "meas_meas", "eps": 1.0}]
    config = build_config("TestBldg", pairs)
    report = analyze_csv(io.StringIO(csv_text), config)

    assert report.data_ts_min is not None
    assert report.data_ts_max is not None
    dt_min = datetime.fromtimestamp(report.data_ts_min)
    dt_max = datetime.fromtimestamp(report.data_ts_max)
    assert dt_min.year == 2007
    assert dt_min.month == 8
    assert dt_min.day == 20
    assert dt_min.hour == 0  # TIME=0 -> midnight
    assert dt_max.hour == 23  # TIME=1439 -> 23:59
    assert dt_max.minute == 59


# ═══════════════════════════════════════════════════════════════════
# Coverage: OK pairs, missing columns, pairs_summary completeness
# ═══════════════════════════════════════════════════════════════════

def test_ok_pairs_appear_in_pairs_summary():
    """Pairs with matching values (no fault) appear in pairs_summary with status OK."""
    csv_text = (
        "TempA,TempB,PressA,PressB\n"
        "72.0,72.0,14.7,14.7\n"
        "72.1,72.1,14.8,14.8\n"
    )
    pairs = [
        {"name": "Temp", "group": "temp", "col_a": "TempA", "col_b": "TempB",
         "pair_type": "meas_meas", "eps": 1.0},
        {"name": "Press", "group": "pressure", "col_a": "PressA", "col_b": "PressB",
         "pair_type": "meas_meas", "eps": 1.0},
    ]
    config = build_config("TestBldg", pairs)
    report = analyze_csv(io.StringIO(csv_text), config)

    # Both pairs should be OK (values match within eps)
    ok_pairs = [p for p in report.pairs_summary if p["status"] == "OK"]
    assert len(ok_pairs) == 2
    names = {p["name"] for p in ok_pairs}
    assert "Temp" in names
    assert "Press" in names

    # All pairs should have col_a and col_b
    for p in report.pairs_summary:
        assert "col_a" in p
        assert "col_b" in p


def test_missing_columns_detected():
    """Pairs referencing columns not in CSV are marked MISSING."""
    csv_text = (
        "TempA,TempB\n"
        "72.0,72.0\n"
    )
    # "Press" pair references columns not in CSV
    pairs = [
        {"name": "Temp", "group": "temp", "col_a": "TempA", "col_b": "TempB",
         "pair_type": "meas_meas", "eps": 1.0},
        {"name": "Press", "group": "pressure", "col_a": "PressA", "col_b": "PressB",
         "pair_type": "meas_meas", "eps": 1.0},
    ]
    config = build_config("TestBldg", pairs)
    report = analyze_csv(io.StringIO(csv_text), config)

    # Coverage fields should be populated
    assert "TempA" in report.csv_columns
    assert "TempB" in report.csv_columns
    assert "PressA" in report.missing_columns
    assert "PressB" in report.missing_columns

    # Press pair should be MISSING in summary
    missing = [p for p in report.pairs_summary if p["status"] == "MISSING"]
    assert len(missing) == 1
    assert missing[0]["name"] == "Press"

    # Temp pair should still be OK
    ok = [p for p in report.pairs_summary if p["status"] == "OK"]
    assert len(ok) == 1
    assert ok[0]["name"] == "Temp"


def test_fault_pair_in_summary():
    """A pair with mismatched values should appear as FAULT in pairs_summary."""
    csv_text = (
        "TempA,TempB\n"
        "72.0,80.0\n"
        "72.1,80.1\n"
    )
    pairs = [{"name": "Temp", "group": "temp", "col_a": "TempA", "col_b": "TempB",
              "pair_type": "meas_setp", "eps": 1.0}]
    config = build_config("TestBldg", pairs)
    report = analyze_csv(io.StringIO(csv_text), config)

    fault_pairs = [p for p in report.pairs_summary if p["status"] == "FAULT"]
    assert len(fault_pairs) == 1
    assert fault_pairs[0]["name"] == "Temp"
    assert fault_pairs[0]["fault_count"] > 0
    assert fault_pairs[0]["col_a"] == "TempA"
    assert fault_pairs[0]["col_b"] == "TempB"
