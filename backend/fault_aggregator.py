"""
fault_aggregator.py — Hierarchical fault grouping and root-cause analysis.

Reduces alert noise by:
1. Grouping faults by logical subsystem (SAT, VAV, Valve, CHW, Zone)
2. Identifying root causes vs cascade effects
3. Rolling up related faults into single actionable alerts

Causal chain example:
  Valve stuck → SAT deviation → Zone overheating
  ROOT_CAUSE: Valve stuck
  CASCADE: SAT deviation, Zone overheating
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum


class Severity(Enum):
    OK = "ok"
    INFO = "info"           # Single minor deviation
    REDUCED = "reduced"     # Sensor loss / degraded
    WARNING = "warning"     # Deviation approaching limits
    FAULT = "fault"         # Clear contradiction
    CASCADE = "cascade"     # Downstream effect of root cause


class FaultType(Enum):
    ROOT_CAUSE = "root_cause"
    CASCADE = "cascade"
    INDEPENDENT = "independent"


# Subsystem definitions with upstream/downstream relationships
SUBSYSTEMS = {
    "valve": {
        "name": "Valve/Actuator",
        "priority": 1,  # Lower = more upstream (checked first)
        "groups": ["valve", "chw", "damper"],
        "indicators": ["VLV", "VALVE", "DMPR", "DAMPER", "CMD", "POS"],
    },
    "sat": {
        "name": "Supply Air Temperature",
        "priority": 2,
        "groups": ["sat"],
        "indicators": ["SAT", "SA-TEMP", "SA_TEMP", "SUPPLY"],
        "upstream": ["valve"],  # Valve problems cause SAT problems
    },
    "vav": {
        "name": "VAV Box",
        "priority": 3,
        "groups": ["vav"],
        "indicators": ["VAV", "BOX", "TERMINAL"],
        "upstream": ["sat"],
    },
    "chw": {
        "name": "Chilled Water",
        "priority": 2,
        "groups": ["chw"],
        "indicators": ["CHW", "CHWC", "CHILLER"],
        "upstream": ["valve"],
    },
    "zone": {
        "name": "Zone Comfort",
        "priority": 4,  # Most downstream
        "groups": ["zone"],
        "indicators": ["RM-TEMP", "RM_TEMP", "ROOM", "ZONE", "RMCLG", "RMHTG"],
        "upstream": ["sat", "vav", "chw"],
    },
    "imu": {
        "name": "IMU Sensors",
        "priority": 1,
        "groups": ["imu", "custom"],
        "indicators": ["IMU", "ACC", "GYR"],
    },
    "baro": {
        "name": "Barometer",
        "priority": 1,
        "groups": ["baro"],
        "indicators": ["BARO", "ALT", "PRESS"],
    },
    "gps": {
        "name": "GPS",
        "priority": 1,
        "groups": ["gps"],
        "indicators": ["GPS", "LAT", "LNG", "SPD"],
    },
}

# Causal rules: if condition → then root cause
CAUSAL_RULES = [
    {
        "name": "valve_causes_sat",
        "condition": lambda faults: (
            any(f["subsystem"] == "valve" and f["status"] == "FAULT" for f in faults) and
            any(f["subsystem"] == "sat" and f["status"] == "FAULT" for f in faults)
        ),
        "root": "valve",
        "cascades": ["sat"],
        "message": "Supply Air Temperature deviation due to valve mismatch",
    },
    {
        "name": "sat_causes_zone",
        "condition": lambda faults: (
            any(f["subsystem"] == "sat" and f["status"] == "FAULT" for f in faults) and
            any(f["subsystem"] == "zone" and f["status"] == "FAULT" for f in faults)
        ),
        "root": "sat",
        "cascades": ["zone"],
        "message": "Zone comfort deviation due to Supply Air Temperature fault",
    },
    {
        "name": "valve_causes_zone",
        "condition": lambda faults: (
            any(f["subsystem"] == "valve" and f["status"] == "FAULT" for f in faults) and
            any(f["subsystem"] == "zone" and f["status"] == "FAULT" for f in faults) and
            not any(f["subsystem"] == "sat" and f["status"] == "FAULT" for f in faults)
        ),
        "root": "valve",
        "cascades": ["zone"],
        "message": "Zone comfort deviation due to valve actuator fault",
    },
    {
        "name": "chw_causes_sat",
        "condition": lambda faults: (
            any(f["subsystem"] == "chw" and f["status"] == "FAULT" for f in faults) and
            any(f["subsystem"] == "sat" and f["status"] == "FAULT" for f in faults)
        ),
        "root": "chw",
        "cascades": ["sat"],
        "message": "Supply Air Temperature deviation due to chilled water fault",
    },
]


@dataclass
class AggregatedFault:
    """A grouped fault with potential cascades."""
    subsystem: str
    subsystem_name: str
    fault_type: FaultType
    severity: Severity
    primary_fault: Dict[str, Any]
    cascades: List[Dict[str, Any]] = field(default_factory=list)
    message: str = ""
    details_message: str = ""
    rule_applied: Optional[str] = None

    def to_dict(self):
        return {
            "subsystem": self.subsystem,
            "subsystem_name": self.subsystem_name,
            "fault_type": self.fault_type.value,
            "severity": self.severity.value,
            "primary_fault": self.primary_fault,
            "cascades": self.cascades,
            "message": self.message,
            "details_message": self.details_message,
            "rule_applied": self.rule_applied,
            "cascade_count": len(self.cascades),
        }


def identify_subsystem(fault: Dict[str, Any]) -> str:
    """Determine which subsystem a fault belongs to."""
    group = fault.get("group", "").lower()
    name = fault.get("name", "").upper()
    col_a = fault.get("col_a", "").upper()
    col_b = fault.get("col_b", "").upper()

    # Check each subsystem's indicators
    for subsys_id, subsys in SUBSYSTEMS.items():
        # Check group match
        if group in subsys["groups"]:
            return subsys_id

        # Check name/column indicators
        for indicator in subsys["indicators"]:
            if indicator in name or indicator in col_a or indicator in col_b:
                return subsys_id

    return "other"


def generate_human_message(fault: Dict[str, Any], subsystem: str) -> str:
    """Generate a human-readable fault message."""
    name = fault.get("name", "Unknown")
    status = fault.get("status", "")
    val_a = fault.get("val_a")
    val_b = fault.get("val_b")
    unit = fault.get("unit", "")
    pair_type = fault.get("pair_type", "")

    if status != "FAULT":
        return f"{name}: {status}"

    if val_a is None or val_b is None:
        return f"{name}: Sensor data missing"

    delta = abs(val_a - val_b)

    # Valve/damper command vs position
    if pair_type == "cmd_pos":
        if val_a > 90 and val_b < 10:
            return f"{name} stuck closed (commanding {val_a:.0f}% but at {val_b:.0f}%)"
        elif val_a < 10 and val_b > 90:
            return f"{name} stuck open (commanding {val_a:.0f}% but at {val_b:.0f}%)"
        else:
            return f"{name} not following command ({val_a:.0f}% cmd vs {val_b:.0f}% actual)"

    # Temperature setpoint vs measured
    if subsystem == "sat":
        if val_b > val_a:
            return f"Supply air too warm ({val_b:.1f}{unit} vs setpoint {val_a:.1f}{unit})"
        else:
            return f"Supply air too cold ({val_b:.1f}{unit} vs setpoint {val_a:.1f}{unit})"

    if subsystem == "zone":
        if val_b > val_a:
            return f"Zone overheating ({val_b:.1f}{unit} vs setpoint {val_a:.1f}{unit})"
        else:
            return f"Zone too cold ({val_b:.1f}{unit} vs setpoint {val_a:.1f}{unit})"

    # Generic
    return f"{name}: deviation of {delta:.1f}{unit}"


def aggregate_faults(pair_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregate raw pair results into hierarchical fault groups.

    Returns:
        {
            "subsystem_faults": [AggregatedFault, ...],
            "total_faults": int,
            "root_causes": int,
            "cascades": int,
            "by_severity": {"fault": n, "warning": n, ...}
        }
    """
    # Filter to only faults
    faults = [p for p in pair_results if p.get("status") == "FAULT"]

    if not faults:
        return {
            "subsystem_faults": [],
            "total_faults": 0,
            "root_causes": 0,
            "cascades": 0,
            "by_severity": {},
        }

    # Identify subsystem for each fault
    for fault in faults:
        fault["subsystem"] = identify_subsystem(fault)
        fault["human_message"] = generate_human_message(fault, fault["subsystem"])

    # Group faults by subsystem
    by_subsystem: Dict[str, List[Dict]] = {}
    for fault in faults:
        subsys = fault["subsystem"]
        if subsys not in by_subsystem:
            by_subsystem[subsys] = []
        by_subsystem[subsys].append(fault)

    # Apply causal rules
    cascade_subsystems = set()
    root_subsystems = set()
    applied_rules = {}

    for rule in CAUSAL_RULES:
        if rule["condition"](faults):
            root_subsystems.add(rule["root"])
            for cascade in rule["cascades"]:
                cascade_subsystems.add(cascade)
                applied_rules[cascade] = rule

    # Topology-based fallback using SUBSYSTEMS upstream relationships
    faulty_subsystems = set(by_subsystem.keys())

    # Pre-build reverse map: parent → list of children (downstream)
    reverse_upstream_map: Dict[str, List[str]] = {sid: [] for sid in SUBSYSTEMS}
    for child_id, child_def in SUBSYSTEMS.items():
        for up in child_def.get("upstream", []):
            reverse_upstream_map.setdefault(up, []).append(child_id)

    def _collect_faulty_upstreams(subsys_id: str, _visited: set, found: set) -> None:
        """Recursively collect faulty upstream subsystem IDs."""
        if subsys_id in _visited:
            return
        _visited.add(subsys_id)
        for upstream in SUBSYSTEMS.get(subsys_id, {}).get("upstream", []):
            if upstream in faulty_subsystems:
                found.add(upstream)
            _collect_faulty_upstreams(upstream, _visited, found)

    def get_faulty_upstreams(subsys_id: str) -> List[str]:
        """Return deduplicated faulty upstream IDs sorted by priority."""
        found: set[str] = set()
        _collect_faulty_upstreams(subsys_id, set(), found)
        return sorted(found, key=lambda u: SUBSYSTEMS.get(u, {}).get("priority", 99))

    def has_faulty_downstream(subsys_id: str, _visited: set | None = None) -> bool:
        """Check if any downstream subsystem (direct or transitive) is faulty.
        Uses pre-built reverse_upstream_map for O(children) lookup per node."""
        if _visited is None:
            _visited = set()
        if subsys_id in _visited:
            return False
        _visited.add(subsys_id)
        for child_id in reverse_upstream_map.get(subsys_id, []):
            if child_id in faulty_subsystems:
                return True
            if has_faulty_downstream(child_id, _visited):
                return True
        return False

    # Build aggregated faults
    aggregated = []
    for subsys_id, subsys_faults in by_subsystem.items():
        subsys_info = SUBSYSTEMS.get(subsys_id, {"name": subsys_id.title(), "priority": 99})

        # Determine fault type: rules take priority, topology is fallback
        details_message = ""
        if subsys_id in root_subsystems:
            fault_type = FaultType.ROOT_CAUSE
            severity = Severity.FAULT
            message = subsys_faults[0]["human_message"] if subsys_faults else ""
        elif subsys_id in cascade_subsystems:
            fault_type = FaultType.CASCADE
            severity = Severity.CASCADE
            rule = applied_rules.get(subsys_id, {})
            message = rule.get("message", "Downstream effect of upstream fault")
            details_message = subsys_faults[0]["human_message"] if subsys_faults else ""
        else:
            upstream_faults = get_faulty_upstreams(subsys_id)
            if upstream_faults:
                fault_type = FaultType.CASCADE
                severity = Severity.CASCADE
                upstream_names = ", ".join(
                    SUBSYSTEMS.get(u, {}).get("name", u) for u in upstream_faults
                )
                message = f"Likely downstream effect. Investigate upstream first: {upstream_names}"
                details_message = subsys_faults[0]["human_message"] if subsys_faults else ""
            elif has_faulty_downstream(subsys_id):
                fault_type = FaultType.ROOT_CAUSE
                severity = Severity.FAULT
                message = subsys_faults[0]["human_message"] if subsys_faults else ""
            else:
                fault_type = FaultType.INDEPENDENT
                severity = Severity.FAULT
                message = subsys_faults[0]["human_message"] if subsys_faults else ""

        # Pick primary fault (first one)
        primary = subsys_faults[0] if subsys_faults else {}

        agg = AggregatedFault(
            subsystem=subsys_id,
            subsystem_name=subsys_info["name"],
            fault_type=fault_type,
            severity=severity,
            primary_fault=primary,
            cascades=subsys_faults[1:] if len(subsys_faults) > 1 else [],
            message=message,
            details_message=details_message,
            rule_applied=applied_rules.get(subsys_id, {}).get("name"),
        )
        aggregated.append(agg)

    # Sort by priority (root causes first)
    aggregated.sort(key=lambda a: (
        0 if a.fault_type == FaultType.ROOT_CAUSE else 1 if a.fault_type == FaultType.INDEPENDENT else 2,
        SUBSYSTEMS.get(a.subsystem, {}).get("priority", 99)
    ))

    # Count by severity
    by_severity = {}
    for agg in aggregated:
        sev = agg.severity.value
        by_severity[sev] = by_severity.get(sev, 0) + 1

    serialized = [a.to_dict() for a in aggregated]

    return {
        "subsystem_faults": serialized,
        "total_faults": len(faults),
        "root_causes": sum(1 for f in serialized if f["fault_type"] == FaultType.ROOT_CAUSE.value),
        "cascades": sum(1 for f in serialized if f["fault_type"] == FaultType.CASCADE.value),
        "independent": sum(1 for f in serialized if f["fault_type"] == FaultType.INDEPENDENT.value),
        "by_severity": by_severity,
    }
