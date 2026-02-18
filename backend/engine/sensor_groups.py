"""
sensor_groups.py — Group-level fusion with optional sensor support.

Consumes typed sensor readings and produces group/system modes by
delegating to the TNA-powered fusion engine (fusion.py).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set

from tna import S
from sensor_tna import TypedReading
import fusion


@dataclass(frozen=True)
class GroupSpec:
    name: str
    sensors: List[str]
    required_eligible: int = 2
    agree_eps: float = 1.0
    max_outliers: int = 0
    optional_sensors: Set[str] = frozenset()  # NEW: sensors that don't trigger degradation


def decide_group(
    group: GroupSpec, typed_by_sensor: Dict[str, TypedReading],
) -> Dict[str, object]:
    readings: List[TypedReading] = []
    for sid in group.sensors:
        tr = typed_by_sensor.get(sid)
        if tr is not None:
            readings.append(tr)

    fr = fusion.fuse_group(
        readings,
        required_eligible=group.required_eligible,
        agree_eps=group.agree_eps,
        max_disagree=group.max_outliers,
        optional_sensors=set(group.optional_sensors),
    )

    return {
        "group": group.name,
        "group_mode": fr.group_mode,
        "eligible_count": fr.eligible_count,
        "total_count": fr.total_count,
        "real_count": fr.definite_count,
        "confidence": fr.confidence,
        "redundancy": fr.redundancy,
        "fused_value": fr.fused_value,
        "fused_sum": fr.fused_sum,
        "pairwise": fr.pairwise,
        "sensor_alerts": fr.sensor_alerts,
        "reasons": fr.reasons,
    }


def decide_system(
    groups: List[GroupSpec],
    typed_by_sensor: Dict[str, TypedReading],
) -> Dict[str, object]:
    order = {"OK": 0, "REDUCED": 1, "DEGRADED": 2, "INCONSISTENT": 3, "FAILOVER": 4}
    outs = [decide_group(g, typed_by_sensor) for g in groups]
    worst = max(outs, key=lambda o: order.get(o["group_mode"], 0))
    return {"system_mode": worst["group_mode"], "groups": outs}
