"""Unit tests for aggregate_faults() topology-based root/cascade classification."""
from fault_aggregator import aggregate_faults, FaultType


def _fault(subsystem_group, name="TEST", val_a=72.0, val_b=80.0, unit="°F"):
    """Helper to build a minimal fault dict that will be classified by group."""
    return {
        "group": subsystem_group,
        "name": name,
        "status": "FAULT",
        "col_a": "",
        "col_b": "",
        "val_a": val_a,
        "val_b": val_b,
        "unit": unit,
        "pair_type": "",
    }


def test_topology_cascade_without_causal_rule():
    """VAV has upstream=[sat] in SUBSYSTEMS. If both vav and sat fault,
    topology should mark sat as ROOT and vav as CASCADE even though
    no explicit CAUSAL_RULE covers sat→vav.
    Cascade message should reference upstream, details_message has original."""
    results = aggregate_faults([
        _fault("sat", name="SA-TEMP"),
        _fault("vav", name="VAV-01"),
    ])
    by_sub = {f["subsystem"]: f for f in results["subsystem_faults"]}
    assert by_sub["sat"]["fault_type"] == FaultType.ROOT_CAUSE.value
    assert by_sub["vav"]["fault_type"] == FaultType.CASCADE.value
    # Topology cascade message references upstream subsystem name
    assert "Investigate upstream first" in by_sub["vav"]["message"]
    assert "Supply Air Temperature" in by_sub["vav"]["message"]
    # details_message preserves original human_message for cascades
    assert by_sub["vav"]["details_message"] != ""
    # details_message always present, even for non-cascades
    assert by_sub["sat"]["details_message"] == ""


def test_causal_rule_overrides_topology():
    """When valve + sat fault together, the causal rule 'valve_causes_sat'
    should mark valve as ROOT and sat as CASCADE (rule takes priority)."""
    results = aggregate_faults([
        _fault("valve", name="VLV-01", val_a=95.0, val_b=5.0, unit="%"),
        _fault("sat", name="SA-TEMP"),
    ])
    by_sub = {f["subsystem"]: f for f in results["subsystem_faults"]}
    assert by_sub["valve"]["fault_type"] == FaultType.ROOT_CAUSE.value
    assert by_sub["sat"]["fault_type"] == FaultType.CASCADE.value
    # Rule message should be used for sat cascade
    assert "valve" in by_sub["sat"]["message"].lower()


def test_independent_subsystems_are_independent():
    """Two subsystems with no upstream relationship (imu + gps) should
    both be classified as INDEPENDENT — neither explains the other."""
    results = aggregate_faults([
        _fault("imu", name="IMU-ACC"),
        _fault("gps", name="GPS-01"),
    ])
    by_sub = {f["subsystem"]: f for f in results["subsystem_faults"]}
    assert by_sub["imu"]["fault_type"] == FaultType.INDEPENDENT.value
    assert by_sub["gps"]["fault_type"] == FaultType.INDEPENDENT.value
    # details_message key always present
    assert by_sub["imu"]["details_message"] == ""
    assert by_sub["gps"]["details_message"] == ""
    assert results["root_causes"] == 0
    assert results["cascades"] == 0
    assert results["independent"] == 2


def test_cycle_safety():
    """Graph cycle must not cause infinite recursion.
    Temporarily inject a cycle: sat→valve→sat, with both faulty."""
    import fault_aggregator as fa
    original_valve = fa.SUBSYSTEMS["valve"]
    original_sat = fa.SUBSYSTEMS["sat"]
    try:
        # Create cycle: valve upstream=[sat], sat upstream=[valve]
        fa.SUBSYSTEMS["valve"] = {**original_valve, "upstream": ["sat"]}
        fa.SUBSYSTEMS["sat"] = {**original_sat, "upstream": ["valve"]}

        results = aggregate_faults([
            _fault("valve", name="VLV-01", val_a=95.0, val_b=5.0, unit="%"),
            _fault("sat", name="SA-TEMP"),
        ])
        # Should complete without RecursionError
        by_sub = {f["subsystem"]: f for f in results["subsystem_faults"]}
        # A pure cycle should not produce two roots — at least one must be CASCADE
        assert not (
            by_sub["valve"]["fault_type"] == FaultType.ROOT_CAUSE.value and
            by_sub["sat"]["fault_type"] == FaultType.ROOT_CAUSE.value
        ), "Pure cycle must not classify both subsystems as ROOT_CAUSE"
    finally:
        fa.SUBSYSTEMS["valve"] = original_valve
        fa.SUBSYSTEMS["sat"] = original_sat


def test_dedup_upstream_two_paths():
    """Create a diamond topology where 'leaf' reaches 'top' via two paths:
    leaf→mid_a→top and leaf→mid_b→top. Both mid_a, mid_b, and top are faulty.
    The message for leaf should list each upstream exactly once."""
    import fault_aggregator as fa
    saved = {}
    try:
        # Inject temporary subsystems: top, mid_a, mid_b, leaf
        for sid in ("top", "mid_a", "mid_b", "leaf"):
            saved[sid] = fa.SUBSYSTEMS.get(sid)
        fa.SUBSYSTEMS["top"] = {
            "name": "Top", "priority": 1, "groups": ["top"], "indicators": [],
        }
        fa.SUBSYSTEMS["mid_a"] = {
            "name": "MidA", "priority": 2, "groups": ["mid_a"], "indicators": [],
            "upstream": ["top"],
        }
        fa.SUBSYSTEMS["mid_b"] = {
            "name": "MidB", "priority": 2, "groups": ["mid_b"], "indicators": [],
            "upstream": ["top"],
        }
        fa.SUBSYSTEMS["leaf"] = {
            "name": "Leaf", "priority": 3, "groups": ["leaf"], "indicators": [],
            "upstream": ["mid_a", "mid_b"],
        }

        results = aggregate_faults([
            _fault("top", name="TOP-01"),
            _fault("mid_a", name="MIDA-01"),
            _fault("mid_b", name="MIDB-01"),
            _fault("leaf", name="LEAF-01"),
        ])
        by_sub = {f["subsystem"]: f for f in results["subsystem_faults"]}
        assert by_sub["leaf"]["fault_type"] == FaultType.CASCADE.value
        msg = by_sub["leaf"]["message"]
        # top is reachable via leaf→mid_a→top AND leaf→mid_b→top
        # but should appear exactly once
        assert msg.count("Top") == 1, f"'Top' should appear once in: {msg}"
        assert msg.count("MidA") == 1, f"'MidA' should appear once in: {msg}"
        assert msg.count("MidB") == 1, f"'MidB' should appear once in: {msg}"
    finally:
        for sid, orig in saved.items():
            if orig is None:
                fa.SUBSYSTEMS.pop(sid, None)
            else:
                fa.SUBSYSTEMS[sid] = orig


if __name__ == "__main__":
    test_topology_cascade_without_causal_rule()
    test_causal_rule_overrides_topology()
    test_independent_subsystems_are_independent()
    test_cycle_safety()
    test_dedup_upstream_two_paths()
    print("All 5 tests passed.")
