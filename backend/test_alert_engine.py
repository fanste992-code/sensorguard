"""Unit tests for alert_engine state machine."""
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from models import Base
import alert_engine
from alert_engine import (
    AlertState, AlertEvent, update,
    CONFIRM_AFTER, CLEAR_AFTER, COOLDOWN_SECONDS,
)
from fault_aggregator import FaultType


def _make_db() -> Session:
    """In-memory SQLite session with alert tables created."""
    eng = create_engine("sqlite:///:memory:")
    # Temporarily swap engine so init_alert_tables uses our in-memory DB
    Base.metadata.create_all(eng)
    _Session = sessionmaker(bind=eng)
    return _Session()


def _root_fault(subsystem="valve", name="VLV-01", pair_type="cmd_pos",
                message="Valve stuck", subsystem_name="Valve/Actuator"):
    """Build a minimal ROOT_CAUSE subsystem fault dict."""
    return {
        "subsystem": subsystem,
        "subsystem_name": subsystem_name,
        "fault_type": FaultType.ROOT_CAUSE.value,
        "severity": "fault",
        "primary_fault": {"name": name, "pair_type": pair_type},
        "cascades": [],
        "message": message,
        "details_message": "",
    }


def _cascade_fault(subsystem="sat", name="SA-TEMP"):
    """Build a CASCADE fault (should be ignored by alert engine)."""
    return {
        "subsystem": subsystem,
        "subsystem_name": "Supply Air Temperature",
        "fault_type": FaultType.CASCADE.value,
        "severity": "cascade",
        "primary_fault": {"name": name, "pair_type": ""},
        "cascades": [],
        "message": "Downstream effect",
        "details_message": "",
    }


def test_confirm_after_threshold():
    """Alert fires only after CONFIRM_AFTER consecutive present cycles."""
    db = _make_db()
    faults = [_root_fault()]

    for i in range(1, CONFIRM_AFTER):
        fired = update(db, 1, faults, now=f"2025-01-01T00:0{i}:00Z")
        db.commit()
        assert fired == [], f"Should not fire on cycle {i}"

    # CONFIRM_AFTER-th cycle should fire
    fired = update(db, 1, faults, now=f"2025-01-01T00:0{CONFIRM_AFTER}:00Z")
    db.commit()
    assert len(fired) == 1
    assert "Valve" in fired[0]["title"]

    # Next cycle: already confirmed, should not fire again
    fired = update(db, 1, faults, now="2025-01-01T00:09:00Z")
    db.commit()
    assert fired == []


def test_clear_after_threshold():
    """Fault clears after CLEAR_AFTER consecutive absent cycles,
    and re-fires if it reappears after clearing."""
    db = _make_db()
    faults = [_root_fault()]

    # Confirm the fault
    for i in range(CONFIRM_AFTER):
        update(db, 1, faults, now=f"2025-01-01T00:{i:02d}:00Z")
        db.commit()

    # Absent for CONFIRM_AFTER cycles (3): should NOT clear yet (need 6)
    for i in range(CONFIRM_AFTER):
        update(db, 1, [], now=f"2025-01-01T01:{i:02d}:00Z")
        db.commit()

    state = db.query(AlertState).first()
    assert state.confirmed is True, "Should still be confirmed after only 3 absent cycles"
    assert state.active is True

    # Absent for remaining cycles up to CLEAR_AFTER - 1: still confirmed
    for i in range(CONFIRM_AFTER, CLEAR_AFTER - 1):
        update(db, 1, [], now=f"2025-01-01T01:{i:02d}:00Z")
        db.commit()

    state = db.query(AlertState).first()
    assert state.confirmed is True, "Should still be confirmed before 6th absent cycle"

    # 6th absent cycle clears it
    update(db, 1, [], now="2025-01-01T02:00:00Z")
    db.commit()

    state = db.query(AlertState).first()
    assert state.confirmed is False
    assert state.active is False

    # Re-appear and re-confirm: should fire a new alert
    for i in range(CONFIRM_AFTER):
        fired = update(db, 1, faults, now=f"2025-01-01T03:{i:02d}:00Z")
        db.commit()

    assert len(fired) == 1
    # Should have 2 total events in history
    events = db.query(AlertEvent).all()
    assert len(events) == 2


def test_cascade_faults_ignored():
    """Only ROOT_CAUSE faults should be tracked; CASCADE faults are skipped."""
    db = _make_db()
    faults = [_cascade_fault()]

    for i in range(CONFIRM_AFTER + 1):
        fired = update(db, 1, faults, now=f"2025-01-01T00:{i:02d}:00Z")
        db.commit()
        assert fired == []

    states = db.query(AlertState).all()
    assert len(states) == 0


def test_multiple_buildings_isolated():
    """Faults in different buildings should not interfere."""
    db = _make_db()
    faults = [_root_fault()]

    # Building A confirms
    for i in range(CONFIRM_AFTER):
        update(db, 10, faults, now=f"2025-01-01T00:{i:02d}:00Z")
        db.commit()

    # Building B only 1 cycle
    update(db, 20, faults, now="2025-01-01T00:00:00Z")
    db.commit()

    state_a = db.query(AlertState).filter_by(building_id=10).first()
    state_b = db.query(AlertState).filter_by(building_id=20).first()
    assert state_a.confirmed is True
    assert state_b.confirmed is False


def test_cooldown_suppresses_refire():
    """Alert should NOT re-emit if fault clears and reappears within cooldown."""
    db = _make_db()
    faults = [_root_fault()]

    # Confirm fault — first alert fires at T=00:02
    for i in range(CONFIRM_AFTER):
        fired = update(db, 1, faults, now=f"2025-01-01T00:{i:02d}:00Z")
        db.commit()
    assert len(fired) == 1  # alert emitted

    # Clear the fault (6 absent cycles)
    for i in range(CLEAR_AFTER):
        update(db, 1, [], now=f"2025-01-01T00:{CONFIRM_AFTER + i:02d}:00Z")
        db.commit()

    state = db.query(AlertState).first()
    assert state.confirmed is False

    # Reappear within cooldown (last alert was at 00:02, cooldown=1800s=30min)
    # Reappear at 00:15 — only 13 minutes later
    for i in range(CONFIRM_AFTER):
        fired = update(db, 1, faults, now=f"2025-01-01T00:{15 + i:02d}:00Z")
        db.commit()
    assert fired == [], "Should NOT emit within cooldown window"

    # State should still be confirmed and active (just suppressed the emit)
    state = db.query(AlertState).first()
    assert state.confirmed is True
    assert state.active is True, "Fault should be active even when alert is suppressed by cooldown"

    # Only 1 event in history (the original)
    events = db.query(AlertEvent).all()
    assert len(events) == 1


def test_cooldown_allows_refire_after_expiry():
    """Alert should re-emit if fault reappears after cooldown expires."""
    db = _make_db()
    faults = [_root_fault()]

    # Confirm fault — first alert fires at T=00:02
    for i in range(CONFIRM_AFTER):
        fired = update(db, 1, faults, now=f"2025-01-01T00:{i:02d}:00Z")
        db.commit()
    assert len(fired) == 1

    # Clear the fault
    for i in range(CLEAR_AFTER):
        update(db, 1, [], now=f"2025-01-01T00:{CONFIRM_AFTER + i:02d}:00Z")
        db.commit()

    # Reappear well after cooldown (last alert at 00:02, reappear at 01:00 = 58min)
    for i in range(CONFIRM_AFTER):
        fired = update(db, 1, faults, now=f"2025-01-01T01:{i:02d}:00Z")
        db.commit()
    assert len(fired) == 1, "Should emit after cooldown expires"

    # 2 events in history
    events = db.query(AlertEvent).all()
    assert len(events) == 2


def test_alert_event_payload_fields():
    """Stored AlertEvent row should have all top-level columns populated
    and details JSON should contain the remaining fault data."""
    db = _make_db()
    faults = [_root_fault()]

    for i in range(CONFIRM_AFTER):
        update(db, 1, faults, now=f"2025-01-01T00:{i:02d}:00Z")
        db.commit()

    event = db.query(AlertEvent).first()
    assert event is not None

    # Top-level columns are non-empty and stable
    assert event.building_id == 1
    assert event.fault_key.startswith("1:valve:")
    assert event.subsystem == "valve"
    assert event.subsystem_name == "Valve/Actuator"
    assert event.severity == "fault"
    assert "Valve/Actuator" in event.title
    assert event.message == "Valve stuck"
    assert event.created_at == "2025-01-01T00:02:00Z"

    # Details JSON contains supplementary data, not duplicating top-level
    details = json.loads(event.details)
    assert "primary_fault" in details
    assert "cascades" in details
    assert "details_message" in details
    assert "fault_type" in details
    # Top-level fields should NOT be in details
    assert "building_id" not in details
    assert "title" not in details
    assert "message" not in details
    assert "subsystem" not in details
    assert "subsystem_name" not in details
    assert "severity" not in details


def test_fault_key_prefix_and_no_collision():
    """fault_key must start with '{building_id}:' and identical faults
    in different buildings must produce distinct keys."""
    db = _make_db()
    fault = _root_fault()

    # Confirm in two buildings
    for i in range(CONFIRM_AFTER):
        update(db, 30, [fault], now=f"2025-01-01T00:{i:02d}:00Z")
        update(db, 40, [fault], now=f"2025-01-01T00:{i:02d}:00Z")
        db.commit()

    events = db.query(AlertEvent).all()
    assert len(events) == 2

    keys = {e.fault_key for e in events}
    # Keys must be distinct
    assert len(keys) == 2, f"Expected 2 distinct keys, got {keys}"

    # Each key starts with its building_id
    for e in events:
        assert e.fault_key.startswith(f"{e.building_id}:"), (
            f"Key '{e.fault_key}' does not start with '{e.building_id}:'"
        )

    # Verify states also have correct prefixes
    states = db.query(AlertState).all()
    for s in states:
        assert s.fault_key.startswith(f"{s.building_id}:"), (
            f"State key '{s.fault_key}' does not start with '{s.building_id}:'"
        )


if __name__ == "__main__":
    test_confirm_after_threshold()
    test_clear_after_threshold()
    test_cascade_faults_ignored()
    test_multiple_buildings_isolated()
    test_cooldown_suppresses_refire()
    test_cooldown_allows_refire_after_expiry()
    test_alert_event_payload_fields()
    test_fault_key_prefix_and_no_collision()
    print("All 8 alert_engine tests passed.")
