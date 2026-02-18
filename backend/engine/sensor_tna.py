"""
sensor_tna.py — Typed sensor readings and raw-to-TNA conversion.

Converts raw sensor data into typed elements of S, applying threshold
and status logic at the boundary so downstream code operates purely
on TNA elements.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from tna import S, Real, AbsZero, MeasZero, ResToken, O_BM, O_M, ONE_T


@dataclass(frozen=True)
class SensorConfig:
    """Per-sensor configuration for raw-to-typed conversion."""
    sensor_id: str
    noise_floor: float = 0.0         # |readings| below this → 0_m
    max_valid: float = float("inf")   # readings above this → 0_m (out of range)
    weight: float = 1.0               # fusion weight (higher = more trusted)


@dataclass(frozen=True)
class TypedReading:
    """A sensor reading mapped into S with provenance metadata."""
    sensor_id: str
    s: S                        # the TNA element
    timestamp: float            # epoch seconds
    source: str                 # e.g. "LIDAR_FRONT", "SIM"
    raw_value: Optional[float]  # original numeric (None if sensor offline)

    @property
    def is_eligible(self) -> bool:
        return not isinstance(self.s, AbsZero)

    @property
    def is_definite(self) -> bool:
        return isinstance(self.s, Real)

    @property
    def level(self) -> int:
        """Information level under ⊑: 0=0_bm, 1=0_m, 2=Real, 3=1_t."""
        if isinstance(self.s, AbsZero):  return 0
        if isinstance(self.s, MeasZero): return 1
        if isinstance(self.s, Real):     return 2
        if isinstance(self.s, ResToken): return 3
        return -1


def classify_raw(
    sensor_id: str,
    raw: Optional[float],
    timestamp: float,
    source: str,
    config: Optional[SensorConfig] = None,
) -> TypedReading:
    """
    Convert a raw sensor reading into a TypedReading.

    Mapping:
      raw is None          → 0_bm  (sensor offline / inapplicable)
      raw is NaN           → 0_bm  (hardware fault)
      |raw| < noise_floor  → 0_m   (below detection threshold)
      raw > max_valid      → 0_m   (out of range but exists)
      otherwise            → Real(raw)
    """
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return TypedReading(sensor_id, O_BM, timestamp, source, raw)

    nf = config.noise_floor if config else 0.0
    mv = config.max_valid if config else float("inf")

    if abs(raw) < nf or abs(raw) > mv:
        return TypedReading(sensor_id, O_M, timestamp, source, raw)

    return TypedReading(sensor_id, Real(raw), timestamp, source, raw)
