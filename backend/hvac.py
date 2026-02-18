"""
hvac.py — HVAC bridge between BAS CSV data and the TNA engine.

The ONLY new file that touches TNA. Everything in engine/ is UNCHANGED.

Fixes applied (per review):
  1. STREAMING: analyze_csv streams rows via deque, O(1) memory per row.
  2. REAL TIMESTAMPS: user picks timestamp_col at config time, parsed properly.
  3. FAULT DEDUP: first-occurrence per pair, capped storage.
"""
from __future__ import annotations

import csv
import io
import sys
import os
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any

_engine_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "engine")
if _engine_dir not in sys.path:
    sys.path.insert(0, _engine_dir)

from tna import Real, AbsZero, MeasZero, O_BM, O_M, S, div
from sensor_tna import TypedReading
from fusion import pairwise_consistency, PairwiseResult


# ── Pair Config ───────────────────────────────────────────────────

@dataclass
class SensorPair:
    name: str; group: str; col_a: str; col_b: str
    pair_type: str; eps: float = 0.15; unit: str = ""
    # Physical range bounds for indeterminate detection
    range_min: Optional[float] = None
    range_max: Optional[float] = None

    def to_dict(self):
        return {"name": self.name, "group": self.group, "col_a": self.col_a,
                "col_b": self.col_b, "pair_type": self.pair_type,
                "eps": self.eps, "unit": self.unit,
                "range_min": self.range_min, "range_max": self.range_max}


@dataclass
class BuildingConfig:
    name: str
    pairs: List[SensorPair]
    timestamp_col: Optional[str] = None   # FIX #2
    instance_col: Optional[str] = None    # For multi-instance sensors (IMU_I, BARO_I, GPS_I)


# ── Classification ────────────────────────────────────────────────

def classify_bas_value(sensor_id: str, raw_str: str, ts: float,
                       range_min: Optional[float] = None,
                       range_max: Optional[float] = None) -> TypedReading:
    """
    Classify a sensor value into TNA type.
    If range_min/range_max provided, values outside bounds return O_M (indeterminate).
    """
    if raw_str is None or str(raw_str).strip() == "":
        return TypedReading(sensor_id, O_BM, ts, "BAS", None)
    try:
        val = float(raw_str)
    except (ValueError, TypeError):
        return TypedReading(sensor_id, O_BM, ts, "BAS", None)

    # Physical range check - values outside bounds are indeterminate (sensor failure)
    if range_min is not None and val < range_min:
        return TypedReading(sensor_id, O_M, ts, "BAS", val)  # Out of range low
    if range_max is not None and val > range_max:
        return TypedReading(sensor_id, O_M, ts, "BAS", val)  # Out of range high

    return TypedReading(sensor_id, Real(val), ts, "BAS", val)


# ── Pair result ───────────────────────────────────────────────────

@dataclass
class PairResult:
    name: str; group: str; status: str
    val_a: Optional[float]; val_b: Optional[float]; ratio: Optional[float]
    diagnosis: Optional[str]; severity: str; pair_type: str; unit: str
    # Additional evidence fields
    tna_tag: str = "OK"  # AGREE/DISAGREE/OFFLINE/INAPPLICABLE/UNCERTAIN
    eps: float = 0.0     # Threshold used
    delta: Optional[float] = None  # Computed difference
    col_a: str = ""      # Column name A
    col_b: str = ""      # Column name B

    def to_dict(self):
        return {"name": self.name, "group": self.group, "status": self.status,
                "val_a": self.val_a, "val_b": self.val_b, "ratio": self.ratio,
                "diagnosis": self.diagnosis, "severity": self.severity,
                "pair_type": self.pair_type, "unit": self.unit,
                "tna_tag": self.tna_tag, "eps": self.eps, "delta": self.delta,
                "col_a": self.col_a, "col_b": self.col_b}


@dataclass
class TickResult:
    timestamp: float; pairs: List[PairResult]
    system_status: str; fault_count: int; ok_count: int


def analyze_tick(row: Dict[str, str], config: BuildingConfig, ts: float = 0.0) -> TickResult:
    pair_results = []
    for pair in config.pairs:
        ra = classify_bas_value(f"{pair.name}_A", row.get(pair.col_a, ""), ts,
                                pair.range_min, pair.range_max)
        rb = classify_bas_value(f"{pair.name}_B", row.get(pair.col_b, ""), ts,
                                pair.range_min, pair.range_max)
        val_a, val_b = ra.raw_value, rb.raw_value

        # Compute delta for evidence
        delta = None
        if val_a is not None and val_b is not None:
            delta = abs(val_a - val_b)

        # Detect multi-instance sensors (redundant sensor comparison)
        multi_instance_groups = ("imu", "baro", "gps", "mag", "airspeed")
        is_multi_instance = (pair.pair_type == "custom" and
                             ("_I0" in pair.col_a or "_I1" in pair.col_b))

        # Detect HVAC direct column comparison (setpoint vs measured, command vs feedback)
        is_hvac_direct = pair.pair_type in ("meas_setp", "cmd_pos")

        # Default TNA tag
        tna_tag = "OK"

        if is_multi_instance:
            # Multi-instance: compare same measurement across different sensor instances
            status, diagnosis, severity, ratio_val = _analyze_custom_pair(pair, val_a, val_b)
            # Set TNA tag based on result
            if status == "FAULT":
                tna_tag = "DISAGREE"
            elif status == "OFFLINE":
                tna_tag = "INAPPLICABLE"
            else:
                tna_tag = "AGREE"
        elif is_hvac_direct:
            # HVAC direct: compare two different columns in the same row
            status, diagnosis, severity, ratio_val = _analyze_hvac_pair(pair, val_a, val_b)
            # Set TNA tag based on result
            if status == "FAULT":
                tna_tag = "DISAGREE"
            elif status == "OFFLINE":
                tna_tag = "INAPPLICABLE"
            else:
                tna_tag = "AGREE"
        else:
            # Fallback to TNA engine for other comparison types
            pw = pairwise_consistency(ra, rb, eps=pair.eps)
            tna_tag = pw.agreement  # Use actual TNA result

            # Check for out-of-range values (both agree but physically impossible)
            out_of_range_a = (pair.range_min is not None and val_a is not None and val_a < pair.range_min) or \
                             (pair.range_max is not None and val_a is not None and val_a > pair.range_max)
            out_of_range_b = (pair.range_min is not None and val_b is not None and val_b < pair.range_min) or \
                             (pair.range_max is not None and val_b is not None and val_b > pair.range_max)

            if pw.agreement == "INAPPLICABLE":
                status, diagnosis, severity = "OFFLINE", None, "warning"
            elif pw.agreement in ("INDETERMINATE", "RESOLVED"):
                # Check if indeterminate due to range violation
                if out_of_range_a or out_of_range_b:
                    status = "FAULT"
                    severity = "critical"
                    diagnosis = _build_range_diagnosis(pair, val_a, val_b, out_of_range_a, out_of_range_b)
                else:
                    status, diagnosis, severity = "INACTIVE", None, "ok"
            elif pw.agreement == "AGREE":
                # Even if they agree, check physical bounds
                if out_of_range_a and out_of_range_b:
                    status = "FAULT"
                    severity = "critical"
                    diagnosis = f"Both sensors out of range [{pair.range_min}, {pair.range_max}]: A={val_a:.2f}, B={val_b:.2f}"
                else:
                    status, diagnosis, severity = "OK", None, "ok"
            elif pw.agreement == "DISAGREE":
                status = "FAULT"
                severity = "critical" if pair.group in ("valve", "sat", "chw", "imu", "baro", "gps") else "warning"
                diagnosis = _build_diagnosis(pair, val_a, val_b)
            else:
                status, diagnosis, severity = "OK", None, "ok"

            ratio_val = pw.ratio.v if isinstance(pw.ratio, Real) else None

        pair_results.append(PairResult(
            name=pair.name, group=pair.group, status=status,
            val_a=val_a, val_b=val_b, ratio=ratio_val,
            diagnosis=diagnosis, severity=severity,
            pair_type=pair.pair_type, unit=pair.unit,
            tna_tag=tna_tag, eps=pair.eps, delta=delta,
            col_a=pair.col_a, col_b=pair.col_b))

    fc = sum(1 for p in pair_results if p.status == "FAULT")
    oc = sum(1 for p in pair_results if p.status == "OK")
    return TickResult(ts, pair_results, "FAULT" if fc > 0 else "OK", fc, oc)


def _analyze_hvac_pair(pair: SensorPair, val_a: Optional[float], val_b: Optional[float]):
    """
    Direct HVAC comparison: setpoint vs measured, or command vs feedback.
    Returns (status, diagnosis, severity, ratio).
    """
    # Both missing -> OFFLINE
    if val_a is None and val_b is None:
        return "OFFLINE", "Both values missing", "warning", None

    # One missing -> FAULT
    if val_a is None:
        return "FAULT", f"Setpoint/Command missing, measured={val_b:.1f}{pair.unit}", "critical", None
    if val_b is None:
        return "FAULT", f"Measured/Feedback missing, setpoint={val_a:.1f}{pair.unit}", "critical", None

    # Both present -> compare absolute difference against epsilon
    delta = abs(val_a - val_b)

    if delta > pair.eps:
        # Generate appropriate diagnosis based on pair type
        if pair.pair_type == "cmd_pos":
            diagnosis = f"CMD={val_a:.0f}% but POS={val_b:.0f}% — actuator not following command (Δ={delta:.1f}%)"
            severity = "critical"
        else:  # meas_setp
            diagnosis = f"Setpoint={val_a:.1f}{pair.unit} but Measured={val_b:.1f}{pair.unit} (Δ={delta:.1f}{pair.unit} > eps={pair.eps})"
            severity = "critical" if pair.group in ("sat", "chw", "valve") else "warning"
        return "FAULT", diagnosis, severity, delta

    return "OK", None, "ok", delta


def _analyze_custom_pair(pair: SensorPair, val_a: Optional[float], val_b: Optional[float]):
    """
    Simple symmetric comparison for redundant sensors.
    Returns (status, diagnosis, severity, ratio).
    """
    # Both missing -> OFFLINE
    if val_a is None and val_b is None:
        return "OFFLINE", "Both instances missing data", "warning", None

    # One missing -> FAULT (one sensor failed)
    if val_a is None or val_b is None:
        missing = "Instance 0" if val_a is None else "Instance 1"
        present_val = val_b if val_a is None else val_a
        return "FAULT", f"{missing} missing, other={present_val:.3f}{pair.unit}", "critical", None

    # Both present -> compare absolute difference against epsilon
    delta = abs(val_a - val_b)
    if delta > pair.eps:
        diagnosis = f"I0={val_a:.4f} vs I1={val_b:.4f} (Δ={delta:.4f} > eps={pair.eps})"
        return "FAULT", diagnosis, "critical", delta

    return "OK", None, "ok", delta


def _build_range_diagnosis(pair: SensorPair, val_a, val_b, oor_a: bool, oor_b: bool) -> str:
    parts = []
    if oor_a and val_a is not None:
        parts.append(f"A={val_a:.2f}")
    if oor_b and val_b is not None:
        parts.append(f"B={val_b:.2f}")
    return f"Out of range [{pair.range_min}, {pair.range_max}]: {', '.join(parts)}"


def _build_diagnosis(pair: SensorPair, val_a, val_b) -> str:
    if val_a is None or val_b is None:
        return "Sensor data missing"
    if pair.pair_type == "cmd_pos":
        return f"CMD={val_a:.0f}% but POS={val_b:.0f}% — actuator not following command"
    diff = abs(val_a - val_b)
    # For multi-instance sensors, use Instance 0/1 format
    # Detect by: custom pair_type, or sensor groups (imu, baro, gps, custom), or _I in column names
    multi_instance_groups = ("custom", "imu", "baro", "gps", "mag", "airspeed")
    is_multi_instance = (pair.pair_type == "custom" or
                         pair.group in multi_instance_groups or
                         "_I0" in pair.col_a or "_I1" in pair.col_b)
    if is_multi_instance:
        return f"Instance 0={val_a:.4f}{pair.unit} vs Instance 1={val_b:.4f}{pair.unit} (Δ={diff:.4f}, eps={pair.eps})"
    return f"Measured={val_a:.1f}{pair.unit} vs Setpoint={val_b:.1f}{pair.unit} (Δ={diff:+.1f}{pair.unit})"


# ── FIX #2: Real timestamp parsing ───────────────────────────────

_TS_FORMATS = (
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M",
    "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%d/%m/%Y %H:%M:%S",
)


def _parse_date_time_columns(row: Dict[str, str]) -> Optional[float]:
    """Parse BAS-style DATE + TIME columns.

    DATE: compact int like 8202007 → pad to 8 digits → parse as %m%d%Y (08/20/2007)
    TIME: minutes of day 0..1439
    """
    date_raw = row.get("DATE", "").strip()
    time_raw = row.get("TIME", "").strip()
    if not date_raw:
        return None
    try:
        date_str = str(int(float(date_raw))).zfill(8)
        base_date = datetime.strptime(date_str, "%m%d%Y").date()
        minutes = int(float(time_raw)) if time_raw else 0
        from datetime import time as _time, timedelta as _td
        ts = datetime.combine(base_date, _time(0, 0)) + _td(minutes=minutes)
        return ts.timestamp()
    except (ValueError, OverflowError):
        return None


def _parse_timestamp(row: Dict[str, str], ts_col: Optional[str], idx: int) -> float:
    # Try DATE+TIME columns first (BAS export format)
    dt_result = _parse_date_time_columns(row)
    if dt_result is not None:
        return dt_result

    if not ts_col or ts_col not in row:
        return float(idx)
    raw = row[ts_col].strip()
    if not raw:
        return float(idx)
    try:
        return float(raw)
    except ValueError:
        pass
    for fmt in _TS_FORMATS:
        try:
            return datetime.strptime(raw, fmt).timestamp()
        except ValueError:
            continue
    return float(idx)


# ── FIX #1: Streaming CSV analysis ───────────────────────────────
# O(tail_size + num_pairs) memory, NOT O(num_rows).

TAIL_SIZE = 30
MAX_FAULTS = 500

@dataclass
class AnalysisReport:
    building_name: str; total_ticks: int; fault_ticks: int; ok_ticks: int
    fault_rate: float; pairs_summary: List[Dict[str, Any]]
    faults: List[Dict[str, Any]]; timeline: List[TickResult]
    # New fault window metrics
    fault_presence: bool = False
    active_fault_pct: float = 0.0
    first_fault_tick: Optional[int] = None
    last_fault_tick: Optional[int] = None
    first_fault_time: Optional[float] = None
    last_fault_time: Optional[float] = None
    # Fault evidence with duration info
    fault_evidence: List[Dict[str, Any]] = None  # type: ignore
    # Column coverage
    csv_columns: List[str] = None  # type: ignore
    expected_columns: List[str] = None  # type: ignore
    missing_columns: List[str] = None  # type: ignore
    # Full data time window (all rows, not just tail)
    data_ts_min: Optional[float] = None
    data_ts_max: Optional[float] = None


def _pivot_instance_rows(reader, instance_col: str, timestamp_col: Optional[str]):
    """
    Pivot rows by instance ID so that data from different instances at the same
    timestamp becomes columns in a single row.

    E.g., rows with IMU_I=0 and IMU_I=1 at same timestamp become:
    {IMU_AccX_I0: val, IMU_AccX_I1: val, ...}

    Returns: (pivoted_rows, unique_instances_set)
    """
    from collections import defaultdict, OrderedDict

    # Read all rows first to detect columns
    all_rows = list(reader)
    if not all_rows:
        return [], set()

    # Auto-detect timestamp column if not specified
    if not timestamp_col:
        # Look for common timestamp column names
        first_row = all_rows[0]
        for candidate in ['Time', 'Timestamp', 'time', 'timestamp', 'TimeUS', 'time_boot_ms']:
            if candidate in first_row:
                timestamp_col = candidate
                break

    # Track all unique instance IDs seen
    unique_instances = set()

    # Group rows by timestamp
    # Use OrderedDict to preserve row order for same timestamps
    ts_groups: Dict[str, Dict[str, Dict[str, str]]] = OrderedDict()

    for row_idx, row in enumerate(all_rows):
        instance = row.get(instance_col, "0").strip()
        unique_instances.add(instance)

        # Get timestamp - use row index as fallback
        if timestamp_col and timestamp_col in row:
            ts_key = row[timestamp_col].strip()
        else:
            # Group consecutive rows with same row_idx // 2 (assumes alternating instances)
            ts_key = str(row_idx // 2)

        if ts_key not in ts_groups:
            ts_groups[ts_key] = {}
        ts_groups[ts_key][instance] = row

    # Build pivoted rows
    pivoted = []
    skip_cols = {instance_col}  # Don't duplicate instance column

    for ts_key, instances in ts_groups.items():
        merged = {}

        # Keep timestamp column
        if timestamp_col:
            for inst_data in instances.values():
                if timestamp_col in inst_data:
                    merged[timestamp_col] = inst_data[timestamp_col]
                    break

        # Merge data from each instance with suffixed column names
        for inst_id, inst_data in instances.items():
            for col, val in inst_data.items():
                if col in skip_cols or col == timestamp_col:
                    continue
                # Create instance-suffixed column name: IMU_AccX -> IMU_AccX_I0
                merged[f"{col}_I{inst_id}"] = val

        pivoted.append(merged)

    return pivoted, unique_instances


def analyze_csv(csv_source, config: BuildingConfig,
                tail_size: int = TAIL_SIZE) -> AnalysisReport:
    """
    csv_source: str, TextIO, or any iterable of text lines.
    Accepts both a string (backwards compatible) and a file stream (true streaming).

    If config.instance_col is set, rows are pivoted by instance ID first.
    """
    if isinstance(csv_source, str):
        csv_source = io.StringIO(csv_source)
    reader = csv.DictReader(csv_source)

    # Handle instance-based data (multi-sensor redundancy)
    unique_instances = set()
    if config.instance_col:
        rows, unique_instances = _pivot_instance_rows(reader, config.instance_col, config.timestamp_col)
        row_iter = iter(rows)
    else:
        row_iter = reader

    # Peek at first row to get CSV column names for coverage validation
    csv_columns: List[str] = []
    first_row = None
    if hasattr(reader, 'fieldnames') and reader.fieldnames:
        csv_columns = list(reader.fieldnames)
    # For pivoted rows, get columns from first row
    if config.instance_col and isinstance(row_iter, type(iter([]))):
        rows_list = list(row_iter)
        if rows_list:
            csv_columns = list(rows_list[0].keys())
        row_iter = iter(rows_list)
    elif not csv_columns:
        # Consume first row to get fieldnames, then chain it back
        try:
            first_row = next(row_iter)
            csv_columns = list(first_row.keys())
        except StopIteration:
            pass

    # Build expected columns and find missing ones
    expected_columns = set()
    for pair in config.pairs:
        expected_columns.add(pair.col_a)
        expected_columns.add(pair.col_b)
    expected_columns = sorted(expected_columns)
    csv_col_set = set(csv_columns)
    missing_columns = [c for c in expected_columns if c not in csv_col_set]

    # Check if we have multiple instances - if only one instance, skip pair analysis
    is_single_instance = len(unique_instances) == 1

    # Filter out pairs that reference non-existent instances or missing columns
    active_pairs = []
    skipped_pairs = []
    missing_pairs = []  # pairs with columns not in CSV
    for pair in config.pairs:
        # Check if this pair references instance columns (e.g., _I0, _I1)
        if "_I0" in pair.col_a or "_I1" in pair.col_b:
            if is_single_instance:
                skipped_pairs.append(pair.name)
                continue
            # Check if both referenced instances exist
            col_a_inst = pair.col_a.split("_I")[-1] if "_I" in pair.col_a else None
            col_b_inst = pair.col_b.split("_I")[-1] if "_I" in pair.col_b else None
            if col_a_inst and col_a_inst not in unique_instances:
                skipped_pairs.append(pair.name)
                continue
            if col_b_inst and col_b_inst not in unique_instances:
                skipped_pairs.append(pair.name)
                continue
        # Check if columns exist in CSV (non-instance pairs)
        if pair.col_a not in csv_col_set or pair.col_b not in csv_col_set:
            missing_pairs.append({"name": pair.name, "col_a": pair.col_a, "col_b": pair.col_b,
                                   "col_a_missing": pair.col_a not in csv_col_set,
                                   "col_b_missing": pair.col_b not in csv_col_set})
            continue
        active_pairs.append(pair)

    # Use filtered pairs for analysis
    effective_config = BuildingConfig(
        name=config.name,
        pairs=active_pairs,
        timestamp_col=config.timestamp_col,
        instance_col=config.instance_col
    )

    # If we consumed first_row for column detection, chain it back
    if first_row is not None:
        import itertools
        row_iter = itertools.chain([first_row], row_iter)

    timeline: deque[TickResult] = deque(maxlen=tail_size)
    pair_fc: Dict[str, int] = {p.name: 0 for p in active_pairs}
    pair_oc: Dict[str, int] = {p.name: 0 for p in active_pairs}
    faults: List[Dict[str, Any]] = []
    seen_pairs: set = set()
    total = fault_total = 0

    # Fault window tracking
    first_fault_tick: Optional[int] = None
    last_fault_tick: Optional[int] = None
    first_fault_time: Optional[float] = None
    last_fault_time: Optional[float] = None

    # Per-pair fault duration tracking
    # Track: current run start, longest run info
    @dataclass
    class FaultRun:
        start_tick: int = -1
        start_time: float = 0.0
        current_length: int = 0
        # Longest run
        longest_start_tick: int = -1
        longest_end_tick: int = -1
        longest_start_time: float = 0.0
        longest_end_time: float = 0.0
        longest_duration: int = 0
        # Latest values for evidence
        last_val_a: Optional[float] = None
        last_val_b: Optional[float] = None
        last_delta: Optional[float] = None
        last_tna_tag: str = ""
        last_eps: float = 0.0
        col_a: str = ""
        col_b: str = ""

    fault_runs: Dict[str, FaultRun] = {p.name: FaultRun(col_a=p.col_a, col_b=p.col_b) for p in active_pairs}

    # Track data time window (min/max of ALL rows, not just tail)
    data_ts_min: Optional[float] = None
    data_ts_max: Optional[float] = None

    for i, row in enumerate(row_iter):
        ts = _parse_timestamp(row, effective_config.timestamp_col, i)
        # Track data time bounds
        if ts > 100:  # Skip index-based fallback timestamps
            if data_ts_min is None or ts < data_ts_min:
                data_ts_min = ts
            if data_ts_max is None or ts > data_ts_max:
                data_ts_max = ts
        tick = analyze_tick(row, effective_config, ts)
        timeline.append(tick)
        total += 1
        if tick.system_status == "FAULT":
            fault_total += 1
            # Track fault window
            if first_fault_tick is None:
                first_fault_tick = i
                first_fault_time = ts
            last_fault_tick = i
            last_fault_time = ts

        for pr in tick.pairs:
            run = fault_runs.get(pr.name)
            if run is None:
                continue

            if pr.status == "FAULT":
                pair_fc[pr.name] = pair_fc.get(pr.name, 0) + 1
                # Update fault run tracking
                if run.start_tick == -1:
                    # Start new run
                    run.start_tick = i
                    run.start_time = ts
                    run.current_length = 1
                else:
                    run.current_length += 1

                # Update latest evidence values
                run.last_val_a = pr.val_a
                run.last_val_b = pr.val_b
                run.last_delta = pr.delta
                run.last_tna_tag = pr.tna_tag
                run.last_eps = pr.eps

                if pr.name not in seen_pairs and len(faults) < MAX_FAULTS:
                    seen_pairs.add(pr.name)
                    faults.append({"tick": i, "timestamp": ts, "pair": pr.name,
                                   "group": pr.group, "diagnosis": pr.diagnosis,
                                   "severity": pr.severity, "val_a": pr.val_a,
                                   "val_b": pr.val_b, "tna_tag": pr.tna_tag,
                                   "eps": pr.eps, "delta": pr.delta,
                                   "col_a": pr.col_a, "col_b": pr.col_b,
                                   "unit": pr.unit, "pair_type": pr.pair_type})
            else:
                if pr.status == "OK":
                    pair_oc[pr.name] = pair_oc.get(pr.name, 0) + 1
                # End current fault run if active
                if run.start_tick != -1:
                    # Check if this was the longest run
                    if run.current_length > run.longest_duration:
                        run.longest_duration = run.current_length
                        run.longest_start_tick = run.start_tick
                        run.longest_end_tick = i - 1
                        run.longest_start_time = run.start_time
                        run.longest_end_time = ts  # Approximate
                    # Reset current run
                    run.start_tick = -1
                    run.current_length = 0

    # Finalize any still-active fault runs
    for name, run in fault_runs.items():
        if run.start_tick != -1 and run.current_length > run.longest_duration:
            run.longest_duration = run.current_length
            run.longest_start_tick = run.start_tick
            run.longest_end_tick = total - 1
            run.longest_start_time = run.start_time
            run.longest_end_time = last_fault_time or run.start_time

    pairs_summary = []
    # Include active pairs with their stats
    for pair in active_pairs:
        fc, oc = pair_fc.get(pair.name, 0), pair_oc.get(pair.name, 0)
        active = fc + oc
        pairs_summary.append({"name": pair.name, "group": pair.group,
                              "col_a": pair.col_a, "col_b": pair.col_b,
                              "fault_count": fc, "ok_count": oc,
                              "fault_rate": round(fc / active * 100, 2) if active else 0.0,
                              "status": "FAULT" if fc > 0 else "OK"})

    # Add missing-column pairs
    for mp in missing_pairs:
        pairs_summary.append({"name": mp["name"], "group": "missing",
                              "col_a": mp["col_a"], "col_b": mp["col_b"],
                              "fault_count": 0, "ok_count": 0,
                              "fault_rate": 0.0,
                              "status": "MISSING",
                              "note": f"Column(s) not found in CSV"})

    # Add skipped single-instance sensors with special status
    for pair_name in skipped_pairs:
        pairs_summary.append({"name": pair_name, "group": "skipped",
                              "fault_count": 0, "ok_count": 0,
                              "fault_rate": 0.0,
                              "status": "SINGLE_INSTANCE",
                              "note": "Single instance detected, no redundant sensor to compare"})

    # Calculate fault metrics
    fault_presence = fault_total > 0
    active_fault_pct = (fault_total / total * 100) if total > 0 else 0.0

    # Build fault evidence list with duration info
    fault_evidence = []
    for fault in faults:
        pair_name = fault["pair"]
        run = fault_runs.get(pair_name)
        evidence = {
            "id": pair_name,
            "label": pair_name,
            "pair": pair_name,
            "group": fault["group"],
            "pair_type": fault.get("pair_type", ""),
            "col_a": fault.get("col_a", run.col_a if run else ""),
            "col_b": fault.get("col_b", run.col_b if run else ""),
            "val_a": run.last_val_a if run else fault.get("val_a"),
            "val_b": run.last_val_b if run else fault.get("val_b"),
            "eps": run.last_eps if run else fault.get("eps", 0),
            "delta": run.last_delta if run else fault.get("delta"),
            "tna_tag": run.last_tna_tag if run else fault.get("tna_tag", ""),
            "unit": fault.get("unit", ""),
            "diagnosis": fault.get("diagnosis"),
            "severity": fault.get("severity"),
            # Duration info
            "start_tick": run.longest_start_tick if run and run.longest_duration > 0 else fault["tick"],
            "end_tick": run.longest_end_tick if run and run.longest_duration > 0 else fault["tick"],
            "start_time": run.longest_start_time if run and run.longest_duration > 0 else fault["timestamp"],
            "end_time": run.longest_end_time if run and run.longest_duration > 0 else fault["timestamp"],
            "duration_ticks": run.longest_duration if run else 1,
        }
        fault_evidence.append(evidence)

    return AnalysisReport(
        building_name=config.name, total_ticks=total,
        fault_ticks=fault_total, ok_ticks=total - fault_total,
        fault_rate=fault_total / total if total else 0.0,
        pairs_summary=pairs_summary, faults=faults, timeline=list(timeline),
        fault_presence=fault_presence,
        active_fault_pct=round(active_fault_pct, 2),
        first_fault_tick=first_fault_tick,
        last_fault_tick=last_fault_tick,
        first_fault_time=first_fault_time,
        last_fault_time=last_fault_time,
        fault_evidence=fault_evidence,
        csv_columns=csv_columns,
        expected_columns=expected_columns,
        missing_columns=missing_columns,
        data_ts_min=data_ts_min,
        data_ts_max=data_ts_max)


def build_config(name: str, pair_dicts: List[Dict],
                 timestamp_col: str = None,
                 instance_col: str = None) -> BuildingConfig:
    pairs = [SensorPair(
        name=m["name"], group=m.get("group", "custom"),
        col_a=m["col_a"], col_b=m["col_b"],
        pair_type=m.get("pair_type", "meas_setp"),
        eps=m.get("eps", 0.15), unit=m.get("unit", ""),
        range_min=m.get("range_min"), range_max=m.get("range_max"))
        for m in pair_dicts]
    return BuildingConfig(name=name, pairs=pairs,
                          timestamp_col=timestamp_col, instance_col=instance_col)
