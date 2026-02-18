"""
fusion.py — TNA-powered sensor fusion engine.

Features:
  1. REDUCED mode: surfaces redundancy loss (eligible < total but ≥ required)
  2. Optional sensors: don't trigger degradation when intermittent
  3. Confidence = eligible/total (captures redundancy loss)
  4. Per-sensor alerts derived from pairwise types

All TNA operations (add, mul, div, avg_s, count_s, sum_s) are load-bearing.
The algebra computes the fused estimate, consistency checks, and confidence.
The mode logic reads the algebraic types of the results.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Set

from tna import (
    S, Real, AbsZero, MeasZero, ResToken,
    O_BM, O_M, ONE_T,
    add, mul, div, sum_s, avg_s, count_s,
)
from sensor_tna import TypedReading


# ── Algebraic level function (unchanged) ──────────────────────────

def info_level(x: S) -> int:
    if isinstance(x, AbsZero):  return 0
    if isinstance(x, MeasZero): return 1
    if isinstance(x, Real):     return 2
    if isinstance(x, ResToken): return 3
    raise TypeError(f"Unknown TNA element: {x}")


# ── Pairwise consistency via TNA division (unchanged) ─────────────

@dataclass(frozen=True)
class PairwiseResult:
    sensor_a: str
    sensor_b: str
    ratio: S
    level: int
    agreement: str  # AGREE / DISAGREE / INDETERMINATE / INAPPLICABLE / RESOLVED


def pairwise_consistency(a: TypedReading, b: TypedReading, eps: float = 0.1) -> PairwiseResult:
    ratio = div(a.s, b.s)
    level = info_level(ratio)
    if isinstance(ratio, Real):
        agreement = "AGREE" if abs(ratio.v - 1.0) < eps else "DISAGREE"
    elif isinstance(ratio, MeasZero):
        agreement = "INDETERMINATE"
    elif isinstance(ratio, AbsZero):
        agreement = "INAPPLICABLE"
    elif isinstance(ratio, ResToken):
        agreement = "RESOLVED"
    else:
        agreement = "UNKNOWN"
    return PairwiseResult(a.sensor_id, b.sensor_id, ratio, level, agreement)


def all_pairwise(readings: List[TypedReading], eps: float = 0.1) -> List[PairwiseResult]:
    results = []
    for i in range(len(readings)):
        for j in range(i + 1, len(readings)):
            results.append(pairwise_consistency(readings[i], readings[j], eps))
    return results


# ── Per-sensor health alerts (NEW) ────────────────────────────────

@dataclass(frozen=True)
class SensorAlert:
    """Derived from pairwise types — identifies WHICH sensor and HOW it failed."""
    sensor_id: str
    tna_type: str      # "Real", "0_m", "0_bm"
    alert: str         # "OK", "OFFLINE", "DEGRADED", "UNEXPECTED_VALUE"
    is_optional: bool  # whether this sensor is optional in the group


def derive_sensor_alerts(
    readings: List[TypedReading],
    optional_ids: Set[str],
) -> List[SensorAlert]:
    """Derive per-sensor health from TNA classification."""
    alerts = []
    for r in readings:
        is_opt = r.sensor_id in optional_ids
        if isinstance(r.s, AbsZero):
            alerts.append(SensorAlert(r.sensor_id, "0_bm", "OFFLINE", is_opt))
        elif isinstance(r.s, MeasZero):
            alerts.append(SensorAlert(r.sensor_id, "0_m", "DEGRADED", is_opt))
        elif isinstance(r.s, Real):
            alerts.append(SensorAlert(r.sensor_id, "Real", "OK", is_opt))
        else:
            alerts.append(SensorAlert(r.sensor_id, str(type(r.s)), "UNKNOWN", is_opt))
    return alerts


# ── Fused estimate (TNA operations unchanged, mode logic improved) ─

@dataclass
class FusionResult:
    fused_value: S
    eligible_count: int
    total_count: int
    definite_count: int
    fused_sum: S
    pairwise: List[PairwiseResult]
    sensor_alerts: List[SensorAlert]
    confidence: float        # eligible / total (CHANGED: was definite/eligible)
    redundancy: float        # eligible / total (NEW: explicit redundancy metric)
    group_mode: str          # OK / REDUCED / DEGRADED / INCONSISTENT / FAILOVER
    reasons: List[str]


def fuse_group(
    readings: List[TypedReading],
    *,
    required_eligible: int = 2,
    agree_eps: float = 1.0,
    max_disagree: int = 0,
    optional_sensors: Set[str] | None = None,
) -> FusionResult:
    """
    Fuse a group of sensor readings using the TNA algebra.

    Mode logic (v2):
      1. eligible < required → FAILOVER
      2. pairwise DISAGREE among required sensors → INCONSISTENT
      3. any required sensor 0_bm → REDUCED (lost redundancy)
      4. any required sensor 0_m, or pairwise INDETERMINATE among required → DEGRADED
      5. all good → OK

    Key change: optional sensors going 0_m/0_bm does NOT degrade/reduce the group.
    This fixes the heading group noise where GPS_Crs is inherently intermittent.
    """
    optional = optional_sensors or set()
    reasons: List[str] = []
    typed_values: List[S] = [r.s for r in readings]

    # ── TNA operations (ALL UNCHANGED) ──
    eligible_count = count_s(typed_values)
    total_count = len(typed_values)
    fused_sum = sum_s(typed_values)
    fused_value = avg_s(typed_values)
    definite_count = sum(1 for x in typed_values if isinstance(x, Real))

    # ── Derived metrics (NEW) ──
    redundancy = eligible_count / total_count if total_count > 0 else 0.0
    confidence = eligible_count / total_count if total_count > 0 else 0.0

    # ── Per-sensor alerts (NEW) ──
    sensor_alerts = derive_sensor_alerts(readings, optional)

    # ── Eligibility check (UNCHANGED logic) ──
    if eligible_count < required_eligible:
        reasons.append(f"INSUFFICIENT_ELIGIBLE({eligible_count}/{required_eligible})")
        return FusionResult(
            fused_value=fused_value, eligible_count=eligible_count,
            total_count=total_count, definite_count=definite_count,
            fused_sum=fused_sum, pairwise=[], sensor_alerts=sensor_alerts,
            confidence=confidence, redundancy=redundancy,
            group_mode="FAILOVER", reasons=reasons,
        )

    # ── Pairwise (TNA division UNCHANGED) ──
    pairwise = all_pairwise(readings, eps=agree_eps)

    # ── Mode logic (IMPROVED — filters by optional) ──

    # Only count pairwise among REQUIRED sensors for mode decisions
    required_pairs = [
        p for p in pairwise
        if p.sensor_a not in optional and p.sensor_b not in optional
    ]
    # Pairs involving at least one required sensor
    semi_required_pairs = [
        p for p in pairwise
        if p.sensor_a not in optional or p.sensor_b not in optional
    ]

    disagree_count = sum(1 for p in required_pairs if p.agreement == "DISAGREE")
    # Also count semi-required disagrees (required vs optional) — important but secondary
    semi_disagree = sum(1 for p in semi_required_pairs if p.agreement == "DISAGREE")

    # Required sensors that are offline
    required_offline = [
        a for a in sensor_alerts
        if not a.is_optional and a.alert == "OFFLINE"
    ]
    # Required sensors that are degraded
    required_degraded = [
        a for a in sensor_alerts
        if not a.is_optional and a.alert == "DEGRADED"
    ]
    # Required pairwise that are indeterminate
    required_indeterminate = sum(
        1 for p in required_pairs
        if p.agreement in ("INDETERMINATE", "RESOLVED")
    )

    # ── Mode determination ──
    if isinstance(fused_value, AbsZero):
        group_mode = "FAILOVER"
        reasons.append("FUSED_VALUE_INAPPLICABLE")

    elif disagree_count > max_disagree:
        group_mode = "INCONSISTENT"
        reasons.append(f"REQUIRED_DISAGREE({disagree_count})")

    elif isinstance(fused_value, MeasZero):
        group_mode = "DEGRADED"
        reasons.append("FUSED_VALUE_INDETERMINATE")

    elif required_degraded:
        group_mode = "DEGRADED"
        names = [a.sensor_id for a in required_degraded]
        reasons.append(f"REQUIRED_SENSOR_DEGRADED({','.join(names)})")

    elif required_indeterminate > 0:
        group_mode = "DEGRADED"
        reasons.append(f"REQUIRED_INDETERMINATE_PAIRS({required_indeterminate})")

    elif required_offline:
        group_mode = "REDUCED"
        names = [a.sensor_id for a in required_offline]
        reasons.append(f"REQUIRED_SENSOR_OFFLINE({','.join(names)})")

    elif semi_disagree > max_disagree:
        group_mode = "INCONSISTENT"
        reasons.append(f"SEMI_REQUIRED_DISAGREE({semi_disagree})")

    else:
        group_mode = "OK"

    return FusionResult(
        fused_value=fused_value, eligible_count=eligible_count,
        total_count=total_count, definite_count=definite_count,
        fused_sum=fused_sum, pairwise=pairwise, sensor_alerts=sensor_alerts,
        confidence=confidence, redundancy=redundancy,
        group_mode=group_mode, reasons=reasons,
    )


# ── Weighted fusion (UNCHANGED) ──────────────────────────────────

def weighted_fuse(readings: List[TypedReading], weights: List[float]) -> S:
    assert len(readings) == len(weights)
    weighted: List[S] = []
    total_weight = 0.0
    for r, w in zip(readings, weights):
        weighted_val = mul(Real(w), r.s)
        weighted.append(weighted_val)
        if not isinstance(r.s, AbsZero):
            total_weight += w
    if total_weight == 0.0:
        return O_BM
    result: S = O_BM
    for wv in weighted:
        result = add(result, wv)
    return div(result, Real(total_weight))


# ── Temporal fusion (UNCHANGED) ──────────────────────────────────

def temporal_fuse(current: S, previous: S, alpha: float = 0.7) -> S:
    weighted_current = mul(Real(alpha), current)
    weighted_previous = mul(Real(1.0 - alpha), previous)
    return add(weighted_current, weighted_previous)
