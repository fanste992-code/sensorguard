"""
window_policy.py — Sliding-window debounce + hysteresis for group modes.

Supports 5 modes: OK → REDUCED → DEGRADED → INCONSISTENT → FAILOVER
"""
from __future__ import annotations
from dataclasses import dataclass, field
from collections import deque
from typing import Dict, Deque, List
import sensor_groups as sg
from sensor_tna import TypedReading

@dataclass
class WindowParams:
    degraded_k: int = 3
    reduced_k: int = 3
    inconsistent_k: int = 2
    failover_k: int = 5
    ok_recover_k: int = 3
    max_history: int = 10

@dataclass
class GroupWindowState:
    history: Deque[str] = field(default_factory=lambda: deque(maxlen=10))
    stable_mode: str = "OK"

class WindowedDecider:
    def __init__(self, params: WindowParams | None = None):
        self.params = params or WindowParams()
        self._state: Dict[str, GroupWindowState] = {}

    def reset(self):
        self._state.clear()

    def _get_state(self, name: str) -> GroupWindowState:
        if name not in self._state:
            self._state[name] = GroupWindowState(
                history=deque(maxlen=self.params.max_history))
        return self._state[name]

    @staticmethod
    def _consecutive_suffix(hist: List[str], label: str) -> int:
        c = 0
        for x in reversed(hist):
            if x == label: c += 1
            else: break
        return c

    def update(self, group: sg.GroupSpec, typed_by_sensor: Dict[str, TypedReading]) -> Dict:
        out = sg.decide_group(group, typed_by_sensor)
        base_mode = out["group_mode"]
        st = self._get_state(group.name)
        st.history.append(base_mode)
        hist = list(st.history)

        if self._consecutive_suffix(hist, "FAILOVER") >= self.params.failover_k:
            st.stable_mode = "FAILOVER"
        elif self._consecutive_suffix(hist, "INCONSISTENT") >= self.params.inconsistent_k:
            st.stable_mode = "INCONSISTENT"
        elif self._consecutive_suffix(hist, "DEGRADED") >= self.params.degraded_k:
            st.stable_mode = "DEGRADED"
        elif self._consecutive_suffix(hist, "REDUCED") >= self.params.reduced_k:
            st.stable_mode = "REDUCED"
        elif st.stable_mode in {"DEGRADED", "INCONSISTENT", "FAILOVER", "REDUCED"}:
            if self._consecutive_suffix(hist, "OK") >= self.params.ok_recover_k:
                st.stable_mode = "OK"
        else:
            st.stable_mode = "OK"

        out["stable_group_mode"] = st.stable_mode
        out["base_group_mode"] = base_mode
        return out
