"""
alert_engine.py — Minimal alerting state machine.

Tracks fault persistence across analysis cycles and fires alerts
only when faults are confirmed (present for N consecutive cycles).

Tables:
  alert_state  — per-fault streak counters and active/confirmed flags
  alert_events — immutable log of fired alerts
"""
from __future__ import annotations
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Text, Index
from sqlalchemy.orm import Session

from models import Base, engine
from fault_aggregator import FaultType

# ── Thresholds ────────────────────────────────────────────────
CONFIRM_AFTER = 3                    # consecutive present cycles to confirm
CLEAR_AFTER = CONFIRM_AFTER * 2     # consecutive absent cycles to deactivate
COOLDOWN_SECONDS = 1800              # min seconds between alerts for same fault


# ── ORM models ────────────────────────────────────────────────

class AlertState(Base):
    __tablename__ = "alert_state"
    fault_key = Column(String(512), primary_key=True)
    building_id = Column(Integer, ForeignKey("buildings.id"), nullable=False, index=True)
    present_streak = Column(Integer, default=0)
    absent_streak = Column(Integer, default=0)
    active = Column(Boolean, default=False, nullable=False)
    confirmed = Column(Boolean, default=False, nullable=False)
    last_alerted_at = Column(String(64), default="")
    last_seen_at = Column(String(64), default="")


class AlertEvent(Base):
    __tablename__ = "alert_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    building_id = Column(Integer, ForeignKey("buildings.id"), nullable=False)
    fault_key = Column(String(512), nullable=False)
    subsystem = Column(String(64), nullable=False, default="")
    subsystem_name = Column(String(128), nullable=False, default="")
    severity = Column(String(32), nullable=False)
    title = Column(String(512), nullable=False)
    message = Column(Text, default="")
    details = Column(Text, default="{}")
    created_at = Column(String(64), nullable=False)
    __table_args__ = (Index("ix_alert_evt_bldg", "building_id", "created_at"),)


def init_alert_tables():
    """Create alert_state and alert_events tables if they don't exist."""
    Base.metadata.create_all(engine, tables=[
        AlertState.__table__,
        AlertEvent.__table__,
    ])



# ── Helpers ────────────────────────────────────────────────────

def parse_iso(s: str) -> datetime:
    """Parse ISO 8601 timestamp, handling 'Z' suffix for UTC."""
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# ── Core engine ───────────────────────────────────────────────

def _build_fault_key(building_id: int, fault: Dict[str, Any]) -> str:
    pf = fault.get("primary_fault", {})
    return (
        f"{building_id}:{fault.get('subsystem', '')}:"
        f"{pf.get('pair_type', '')}:{pf.get('name', '')}"
    )


def _build_title(fault: Dict[str, Any]) -> str:
    pf = fault.get("primary_fault", {})
    return f"{fault.get('subsystem_name', 'Unknown')}: {pf.get('name', 'Fault')}"


def _build_details(fault: Dict[str, Any]) -> str:
    # Top-level columns: subsystem, subsystem_name, severity, title, message
    # Everything else goes into details JSON
    return json.dumps({
        "details_message": fault.get("details_message", ""),
        "primary_fault": fault.get("primary_fault", {}),
        "cascades": fault.get("cascades", []),
        "fault_type": fault.get("fault_type", ""),
        "rule_applied": fault.get("rule_applied"),
        "cascade_count": fault.get("cascade_count", 0),
    }, default=str)


def update(
    db: Session,
    building_id: int,
    subsystem_faults: List[Dict[str, Any]],
    now: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Run one alerting cycle.

    Args:
        db: SQLAlchemy session (caller manages commit/rollback).
        building_id: Building ID (integer, matches buildings.id).
        subsystem_faults: Output of aggregate_faults()["subsystem_faults"].
        now: ISO timestamp for this cycle (default: utcnow).

    Returns:
        List of newly fired alert dicts (only freshly confirmed this cycle).
    """
    if now is None:
        now = datetime.now(timezone.utc).isoformat()

    # Filter to ROOT_CAUSE faults only
    root_faults = [
        f for f in subsystem_faults
        if f.get("fault_type") == FaultType.ROOT_CAUSE.value
    ]

    # Build lookup of currently present fault keys
    prefix = f"{building_id}:"
    present: Dict[str, Dict[str, Any]] = {}
    for fault in root_faults:
        key = _build_fault_key(building_id, fault)
        if not key.startswith(prefix):
            raise ValueError(
                f"fault_key must start with '{prefix}' but got '{key}' "
                f"for building '{building_id}'"
            )
        present[key] = fault

    # Load all active/tracked states for this building
    states = (
        db.query(AlertState)
        .filter(AlertState.building_id == building_id)
        .all()
    )
    state_by_key: Dict[str, AlertState] = {s.fault_key: s for s in states}

    fired: List[Dict[str, Any]] = []

    # Update present faults
    for key, fault in present.items():
        state = state_by_key.get(key)
        if state is None:
            state = AlertState(
                fault_key=key,
                building_id=building_id,
                present_streak=0,
                absent_streak=0,
                active=False,
                confirmed=False,
                last_alerted_at="",
                last_seen_at="",
            )
            db.add(state)

        state.present_streak += 1
        state.absent_streak = 0
        state.active = True
        state.last_seen_at = now

        # Confirm after threshold consecutive appearances
        if state.present_streak >= CONFIRM_AFTER and not state.confirmed:
            # Check cooldown: skip emit if last alert was too recent
            if state.last_alerted_at:
                try:
                    last = parse_iso(state.last_alerted_at)
                    current = parse_iso(now)
                    if (current - last).total_seconds() < COOLDOWN_SECONDS:
                        state.confirmed = True  # mark confirmed but don't emit
                        state.active = True
                        continue
                except (ValueError, TypeError):
                    pass

            state.confirmed = True
            state.last_alerted_at = now

            subsystem = fault.get("subsystem", "")
            subsystem_name = fault.get("subsystem_name", "")
            title = _build_title(fault)
            message = fault.get("message", "")
            severity = fault.get("severity", "fault")
            details = _build_details(fault)

            event = AlertEvent(
                building_id=building_id,
                fault_key=key,
                subsystem=subsystem,
                subsystem_name=subsystem_name,
                severity=severity,
                title=title,
                message=message,
                details=details,
                created_at=now,
            )
            db.add(event)
            fired.append({
                "fault_key": key,
                "subsystem": subsystem,
                "subsystem_name": subsystem_name,
                "title": title,
                "message": message,
                "severity": severity,
            })

    # Update absent faults (tracked but not present this cycle)
    for key, state in state_by_key.items():
        if key in present:
            continue
        state.absent_streak += 1
        state.present_streak = 0

        if state.absent_streak >= CLEAR_AFTER:
            state.active = False
            state.confirmed = False

    return fired
