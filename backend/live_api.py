"""
live_api.py — Real-time data ingestion API for SensorGuard

Handles:
  - Live data push from edge collectors
  - Real-time TNA analysis
  - Collector heartbeats
  - BACnet point configuration

Add to main.py:
  from live_api import live_router
  app.include_router(live_router, prefix="/api")
"""
from __future__ import annotations

import json
import time
import os

# If a point has not been updated for this many seconds, treat it as OFFLINE in analysis
POINT_STALE_SECONDS = int(os.getenv("POINT_STALE_SECONDS", "120"))

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from collections import deque

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy import func

from models import get_db, Building, FaultEvent, Analysis, require_building
from plan_service import require_feature
from auth import get_user_id
from hvac import analyze_tick, BuildingConfig, SensorPair, TickResult, PairResult

live_router = APIRouter(tags=["live"])


# ══════════════════════════════════════════════════════════════════════════════
# Request/Response Models
# ══════════════════════════════════════════════════════════════════════════════

class PointReading(BaseModel):
    point_name: str
    value: float
    timestamp: float
    quality: str = "good"
    device_id: int = 0


class LiveDataPush(BaseModel):
    building_id: int
    timestamp: float
    readings: List[PointReading]


class LiveDataResponse(BaseModel):
    received: int
    processed: int
    faults_detected: int
    faults: List[Dict[str, Any]]
    system_status: str


class BACnetPointConfig(BaseModel):
    name: str
    device_id: int
    object_type: str
    object_instance: int
    pair_role: str = "a"       # "a" or "b"
    pair_name: str = ""        # Which sensor pair this belongs to
    cov_increment: Optional[float] = None


class BACnetConfig(BaseModel):
    points: List[BACnetPointConfig]
    poll_interval: float = 30.0
    use_cov: bool = True


class CollectorHeartbeat(BaseModel):
    building_id: int
    timestamp: float
    devices_connected: int
    points_monitored: int
    buffer_size: int
    last_values: Dict[str, float] = {}


class CollectorStatus(BaseModel):
    building_id: int
    last_heartbeat: Optional[float]
    devices_connected: int
    points_monitored: int
    status: str  # "online", "offline", "degraded"


# ══════════════════════════════════════════════════════════════════════════════
# In-Memory State (for real-time processing)
# ══════════════════════════════════════════════════════════════════════════════

class LiveState:
    """
    Per-building real-time state.
    
    In production, this would be backed by Redis for horizontal scaling.
    """
    
    def __init__(self, building_id: int):
        self.building_id = building_id
        self.last_values: Dict[str, float] = {}
        self.last_seen: Dict[str, float] = {}
        self.last_update: float = 0
        self.recent_ticks: deque[TickResult] = deque(maxlen=100)
        self.active_faults: Dict[str, Dict] = {}  # pair_name -> fault info
        self.collector_heartbeat: Optional[CollectorHeartbeat] = None


# Global state store (in production: Redis)
_live_states: Dict[int, LiveState] = {}


def get_live_state(building_id: int) -> LiveState:
    if building_id not in _live_states:
        _live_states[building_id] = LiveState(building_id)
    return _live_states[building_id]


# ══════════════════════════════════════════════════════════════════════════════
# Live Data Endpoint
# ══════════════════════════════════════════════════════════════════════════════

@live_router.post("/buildings/{bid}/live-data", response_model=LiveDataResponse)
async def push_live_data(
    bid: int,
    data: LiveDataPush,
    background_tasks: BackgroundTasks,
    uid: int = Depends(get_user_id),
    db=Depends(get_db),
):
    """
    Receive live data from edge collector and run TNA analysis.
    
    This is the core real-time endpoint. Each push:
    1. Updates point values in memory
    2. Runs TNA analysis on sensor pairs
    3. Detects new faults and clears resolved ones
    4. Returns immediate feedback to collector
    
    Faults are persisted asynchronously to not block the collector.
    """
    building = require_building(db, bid, uid)
    require_feature(db, bid, "api_access")

    # Get live state and sensor config
    state = get_live_state(bid)
    pair_mappings = building.get_config()
    
    if not pair_mappings:
        return LiveDataResponse(
            received=len(data.readings),
            processed=0,
            faults_detected=0,
            faults=[],
            system_status="unconfigured",
        )

    # Update point values
    for reading in data.readings:
        state.last_values[reading.point_name] = reading.value
        # Track freshness per point (do NOT infer freshness from last_values alone)
        seen_ts = float(reading.timestamp or data.timestamp)
        prev = state.last_seen.get(reading.point_name, 0.0)
        state.last_seen[reading.point_name] = seen_ts if seen_ts > prev else prev
    state.last_update = data.timestamp

    # Build row dict for TNA analysis (maps pair columns to values)
    # IMPORTANT: each point can go stale independently. If it hasn't been updated
    # recently, treat it as missing so the algebra can surface OFFLINE/REDUCED.
    now_ts = float(data.timestamp)
    row: Dict[str, str] = {}
    for m in pair_mappings:
        for col in (m.get("col_a"), m.get("col_b")):
            if not col:
                continue
            last_seen = state.last_seen.get(col)
            if (last_seen is None) or (now_ts - float(last_seen) > POINT_STALE_SECONDS):
                row[col] = ""  # missing => O_BM => OFFLINE semantics
            else:
                v = state.last_values.get(col)
                row[col] = "" if v is None else str(v)

    # Run TNA analysis
    config = BuildingConfig(
        name=building.name,
        pairs=[
            SensorPair(
                name=m["name"],
                group=m.get("group", "custom"),
                col_a=m["col_a"],
                col_b=m["col_b"],
                pair_type=m.get("pair_type", "meas_setp"),
                eps=m.get("eps", 0.15),
                unit=m.get("unit", ""),
            )
            for m in pair_mappings
        ],
    )
    
    tick = analyze_tick(row, config, ts=data.timestamp)
    state.recent_ticks.append(tick)

    # Detect faults
    new_faults = []
    for pr in tick.pairs:
        if pr.status == "FAULT":
            if pr.name not in state.active_faults:
                fault_info = {
                    "pair": pr.name,
                    "group": pr.group,
                    "severity": pr.severity,
                    "diagnosis": pr.diagnosis,
                    "val_a": pr.val_a,
                    "val_b": pr.val_b,
                    "first_detected": data.timestamp,
                    "last_seen": data.timestamp,
                }
                state.active_faults[pr.name] = fault_info
                new_faults.append(fault_info)
            else:
                state.active_faults[pr.name]["last_seen"] = data.timestamp
        elif pr.name in state.active_faults:
            # Fault cleared
            del state.active_faults[pr.name]

    # Persist faults asynchronously
    if new_faults:
        background_tasks.add_task(_persist_faults, bid, new_faults)

    return LiveDataResponse(
        received=len(data.readings),
        processed=len(pair_mappings),
        faults_detected=len(new_faults),
        faults=new_faults,
        system_status=tick.system_status,
    )


def _persist_faults(building_id: int, faults: List[Dict]):
    """Persist new faults to database (background task).

    NOTE: BackgroundTasks runs after the response, so the request-scoped DB session
    is no longer valid. Create and dispose our own session here.
    """
    from models import SessionLocal
    db = SessionLocal()
    try:
        for fault in faults:
            fe = FaultEvent(
                building_id=building_id,
                pair_name=fault["pair"],
                group=fault["group"],
                severity=fault["severity"],
                diagnosis=fault["diagnosis"],
                val_a=fault.get("val_a"),
                val_b=fault.get("val_b"),
                tick_timestamp=str(fault.get("first_detected", "")),
            )
            db.add(fe)
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# Live State Endpoint
# ══════════════════════════════════════════════════════════════════════════════

@live_router.get("/buildings/{bid}/live-state")
async def get_live_building_state(
    bid: int,
    uid: int = Depends(get_user_id),
    db=Depends(get_db),
):
    """
    Get current live state of a building.
    
    Returns:
    - Current point values
    - Active faults
    - Recent tick history
    - Collector status
    """
    building = require_building(db, bid, uid)

    state = get_live_state(bid)

    # Determine collector status
    now = time.time()
    if state.collector_heartbeat:
        age = now - state.collector_heartbeat.timestamp
        if age < 120:
            collector_status = "online"
        elif age < 600:
            collector_status = "degraded"
        else:
            collector_status = "offline"
    else:
        collector_status = "unknown"

    return {
        "building_id": bid,
        "building_name": building.name,
        "last_update": state.last_update,
        "collector_status": collector_status,
        "current_values": state.last_values,
        "point_last_seen": state.last_seen,
        "stale_points": [k for k,v in state.last_seen.items() if (now - float(v)) > POINT_STALE_SECONDS],
        "active_faults": list(state.active_faults.values()),
        "recent_ticks": [
            {
                "timestamp": t.timestamp,
                "system_status": t.system_status,
                "fault_count": t.fault_count,
                "pairs": [p.to_dict() for p in t.pairs],
            }
            for t in list(state.recent_ticks)[-20:]  # Last 20 ticks
        ],
    }


# ══════════════════════════════════════════════════════════════════════════════
# BACnet Configuration
# ══════════════════════════════════════════════════════════════════════════════

@live_router.get("/buildings/{bid}/bacnet-config")
async def get_bacnet_config(
    bid: int,
    uid: int = Depends(get_user_id),
    db=Depends(get_db),
):
    """
    Get BACnet point configuration for edge collector.
    
    The collector calls this on startup to get its point mappings.
    """
    building = require_building(db, bid, uid)

    # Get BACnet config from building (stored alongside sensor pairs)
    bacnet_config = building.get_bacnet_config()
    
    if not bacnet_config:
        raise HTTPException(404, "No BACnet configuration found")

    return bacnet_config


@live_router.put("/buildings/{bid}/bacnet-config")
async def update_bacnet_config(
    bid: int,
    config: BACnetConfig,
    uid: int = Depends(get_user_id),
    db=Depends(get_db),
):
    """
    Update BACnet point configuration.
    
    This maps BACnet objects to sensor pair columns.
    """
    building = require_building(db, bid, uid)

    building.set_bacnet_config({
        "points": [p.dict() for p in config.points],
        "poll_interval": config.poll_interval,
        "use_cov": config.use_cov,
    })
    db.commit()

    return {"status": "ok", "points_configured": len(config.points)}


# ══════════════════════════════════════════════════════════════════════════════
# Collector Management
# ══════════════════════════════════════════════════════════════════════════════

@live_router.post("/collectors/heartbeat")
async def collector_heartbeat(
    heartbeat: CollectorHeartbeat,
    uid: int = Depends(get_user_id),
    db=Depends(get_db),
):
    """
    Receive heartbeat from edge collector.
    
    Used for monitoring collector health and displaying status in dashboard.
    """
    building = require_building(db, heartbeat.building_id, uid)

    state = get_live_state(heartbeat.building_id)
    state.collector_heartbeat = heartbeat
    
    # Update last values from heartbeat
    state.last_values.update(heartbeat.last_values)

    return {"status": "ok", "timestamp": time.time()}


@live_router.get("/buildings/{bid}/collector-status", response_model=CollectorStatus)
async def get_collector_status(
    bid: int,
    uid: int = Depends(get_user_id),
    db=Depends(get_db),
):
    """Get status of the edge collector for a building."""
    building = require_building(db, bid, uid)

    state = get_live_state(bid)
    hb = state.collector_heartbeat

    now = time.time()
    if hb:
        age = now - hb.timestamp
        if age < 120:
            status = "online"
        elif age < 600:
            status = "degraded"
        else:
            status = "offline"
    else:
        status = "unknown"

    return CollectorStatus(
        building_id=bid,
        last_heartbeat=hb.timestamp if hb else None,
        devices_connected=hb.devices_connected if hb else 0,
        points_monitored=hb.points_monitored if hb else 0,
        status=status,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Device Discovery Proxy
# ══════════════════════════════════════════════════════════════════════════════

@live_router.post("/buildings/{bid}/discover-devices")
async def trigger_device_discovery(
    bid: int,
    uid: int = Depends(get_user_id),
    db=Depends(get_db),
):
    """
    Trigger BACnet device discovery on the edge collector.
    
    This sends a command to the collector to run Who-Is and report back.
    In a full implementation, this would use WebSocket or a message queue.
    """
    building = require_building(db, bid, uid)

    # For now, return cached discovered devices
    state = get_live_state(bid)
    
    return {
        "status": "discovery_requested",
        "message": "Edge collector will discover devices on next heartbeat",
        "cached_devices": [],  # Would contain previously discovered devices
    }
