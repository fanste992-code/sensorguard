"""
full_pipeline.py — Multi-group TNA fusion on real ArduPilot failure data.

Groups:
  1. altitude:  GPS_alt + BARO_alt (2-sensor, reference-frame aligned)
  2. imu_vert:  IMU0_AccZ + IMU1_AccZ (cross-check vertical acceleration)
  3. heading:   MAG_heading + GPS_course (GPS_Crs optional — intermittent when hovering)

Features:
  - REDUCED mode surfaces redundancy loss (sensor offline but system operational)
  - Optional sensors don't trigger false DEGRADED (fixes heading group noise)
  - Per-sensor alerts identify WHICH sensor failed and HOW
  - Confidence = eligible/total tracks redundancy
"""
import copy, math, os, statistics, sys
from collections import Counter
from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from pymavlink import DFReader

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tna import Real, AbsZero, MeasZero, ResToken, O_BM, O_M, S
from sensor_tna import TypedReading
import sensor_groups as sg
import window_policy as wp

# ── Snapshot + classification (UNCHANGED from v1) ────────────────

@dataclass
class Snap:
    ts: int = 0
    gps_status: int = 0; gps_nsats: int = 0; gps_hdop: float = 99
    gps_hacc: float = 99; gps_alt: float = 0; gps_spd: float = 0
    gps_crs: float = 0; gps_valid: bool = False
    baro_alt: float = 0; baro_health: int = 0; baro_crt: float = 0
    baro_valid: bool = False
    imu0_az: float = 0; imu0_gh: int = 1; imu0_ah: int = 1; imu0_valid: bool = False
    imu1_az: float = 0; imu1_gh: int = 1; imu1_ah: int = 1; imu1_valid: bool = False
    vibe0_max: float = 0; vibe0_clip: int = 0; vibe0_valid: bool = False
    vibe1_max: float = 0; vibe1_clip: int = 0; vibe1_valid: bool = False
    mag_x: float = 0; mag_y: float = 0; mag_z: float = 0
    mag_health: int = 1; mag_valid: bool = False
    sim_alt: float = 0; sim_valid: bool = False

def parse_log(filepath, interval_us=200_000):
    log = DFReader.DFReader_binary(filepath)
    snaps, cur, last_ts = [], Snap(), 0
    while True:
        m = log.recv_msg()
        if m is None: break
        t, ts = m.get_type(), getattr(m, 'TimeUS', 0)
        if t == 'GPS' and getattr(m,'I',0)==0:
            cur.gps_status=m.Status; cur.gps_nsats=m.NSats; cur.gps_hdop=m.HDop
            cur.gps_alt=m.Alt; cur.gps_spd=m.Spd; cur.gps_crs=m.GCrs; cur.gps_valid=True; cur.ts=ts
        elif t == 'GPA' and getattr(m,'I',0)==0:
            cur.gps_hacc = m.HAcc
        elif t == 'BARO' and getattr(m,'I',0)==0:
            cur.baro_alt=m.Alt; cur.baro_health=m.Health; cur.baro_crt=m.CRt
            cur.baro_valid=True; cur.ts=ts
        elif t == 'IMU':
            inst = getattr(m,'I',0)
            if inst == 0: cur.imu0_az=m.AccZ; cur.imu0_gh=m.GH; cur.imu0_ah=m.AH; cur.imu0_valid=True; cur.ts=ts
            elif inst == 1: cur.imu1_az=m.AccZ; cur.imu1_gh=m.GH; cur.imu1_ah=m.AH; cur.imu1_valid=True
        elif t == 'VIBE':
            imu = getattr(m,'IMU',0)
            vmax = max(m.VibeX, m.VibeY, m.VibeZ)
            if imu == 0: cur.vibe0_max=vmax; cur.vibe0_clip=m.Clip; cur.vibe0_valid=True
            elif imu == 1: cur.vibe1_max=vmax; cur.vibe1_clip=m.Clip; cur.vibe1_valid=True
        elif t == 'MAG' and getattr(m,'I',0)==0:
            cur.mag_x=m.MagX; cur.mag_y=m.MagY; cur.mag_z=m.MagZ
            cur.mag_health=m.Health; cur.mag_valid=True
        elif t == 'SIM':
            cur.sim_alt=m.Alt; cur.sim_valid=True
        if ts > 0 and ts - last_ts >= interval_us:
            snaps.append(copy.deepcopy(cur)); last_ts = ts
    return snaps

ACCEL_HEALTHY = (7.0, 12.0)
ACCEL_WARN = (5.0, 14.0)

def classify_gps_alt(snap, home):
    if not snap.gps_valid: return TypedReading("GPS_Alt", O_BM, snap.ts, "GPS", None)
    if snap.gps_status < 3 or snap.gps_nsats < 6:
        return TypedReading("GPS_Alt", O_BM, snap.ts, "GPS", snap.gps_alt - home)
    rel = snap.gps_alt - home
    if snap.gps_hdop > 2.5 or snap.gps_hacc > 3.0:
        return TypedReading("GPS_Alt", O_M, snap.ts, "GPS", rel)
    return TypedReading("GPS_Alt", Real(rel), snap.ts, "GPS", rel)

def classify_baro_alt(snap):
    if not snap.baro_valid: return TypedReading("BARO_Alt", O_BM, snap.ts, "BARO", None)
    if snap.baro_health == 0:
        return TypedReading("BARO_Alt", O_BM, snap.ts, "BARO", snap.baro_alt)
    if abs(snap.baro_crt) > 10.0:
        return TypedReading("BARO_Alt", O_M, snap.ts, "BARO", snap.baro_alt)
    return TypedReading("BARO_Alt", Real(snap.baro_alt), snap.ts, "BARO", snap.baro_alt)

def classify_imu(snap, inst):
    if inst == 0: valid, az, gh, ah, vv, vc = snap.imu0_valid, snap.imu0_az, snap.imu0_gh, snap.imu0_ah, snap.vibe0_valid, snap.vibe0_clip
    else: valid, az, gh, ah, vv, vc = snap.imu1_valid, snap.imu1_az, snap.imu1_gh, snap.imu1_ah, snap.vibe1_valid, snap.vibe1_clip
    sid = f"IMU{inst}_Az"
    if not valid: return TypedReading(sid, O_BM, snap.ts, f"IMU{inst}", None)
    if gh == 0 or ah == 0: return TypedReading(sid, O_BM, snap.ts, f"IMU{inst}", az)
    if vv and vc > 0: return TypedReading(sid, O_BM, snap.ts, f"IMU{inst}", az)
    abs_az = abs(az)
    if not (ACCEL_WARN[0] <= abs_az <= ACCEL_WARN[1]):
        return TypedReading(sid, O_BM, snap.ts, f"IMU{inst}", az)
    if not (ACCEL_HEALTHY[0] <= abs_az <= ACCEL_HEALTHY[1]):
        return TypedReading(sid, O_M, snap.ts, f"IMU{inst}", az)
    vibe_max = snap.vibe0_max if inst == 0 else snap.vibe1_max
    vibe_v = snap.vibe0_valid if inst == 0 else snap.vibe1_valid
    if vibe_v and vibe_max > 30:
        return TypedReading(sid, O_M, snap.ts, f"IMU{inst}", az)
    return TypedReading(sid, Real(az), snap.ts, f"IMU{inst}", az)

def classify_mag(snap):
    if not snap.mag_valid: return TypedReading("MAG_Hdg", O_BM, snap.ts, "MAG", None)
    if snap.mag_health == 0: return TypedReading("MAG_Hdg", O_BM, snap.ts, "MAG", None)
    heading = math.atan2(snap.mag_y, snap.mag_x) * 180 / math.pi
    if heading < 0: heading += 360
    return TypedReading("MAG_Hdg", Real(heading), snap.ts, "MAG", heading)

def classify_gps_course(snap):
    if not snap.gps_valid or snap.gps_status < 3:
        return TypedReading("GPS_Crs", O_BM, snap.ts, "GPS", None)
    if snap.gps_spd < 1.0:
        return TypedReading("GPS_Crs", O_M, snap.ts, "GPS", snap.gps_crs)
    return TypedReading("GPS_Crs", Real(snap.gps_crs), snap.ts, "GPS", snap.gps_crs)

# ── Tick result ───────────────────────────────────────────────────

def _lbl(s):
    if isinstance(s, AbsZero): return "0_bm"
    if isinstance(s, MeasZero): return "0_m"
    if isinstance(s, Real): return "Real"
    if isinstance(s, ResToken): return "1_t"
    return "?"

@dataclass
class Tick:
    t: float
    gps_alt_type: str; baro_alt_type: str
    alt_mode: str; alt_stable: str; alt_redundancy: float
    fused_alt: Optional[float]; alt_pw: str; alt_err: Optional[float]
    alt_alerts: str
    imu0_type: str; imu1_type: str
    imu_mode: str; imu_stable: str; imu_redundancy: float
    imu_pw: str; imu_alerts: str
    mag_type: str; gps_crs_type: str
    hdg_mode: str; hdg_stable: str; hdg_redundancy: float
    hdg_pw: str; hdg_alerts: str
    system_mode: str
    sim_alt_rel: Optional[float]

def run_pipeline(snaps, label=""):
    home = next((s.gps_alt for s in snaps if s.gps_valid and s.gps_status>=3), 0.0)
    t0 = snaps[0].ts

    # GROUP DEFINITIONS — KEY CHANGE: optional_sensors for heading
    alt_grp = sg.GroupSpec("altitude", ["GPS_Alt","BARO_Alt"],
                           required_eligible=1, agree_eps=5.0, max_outliers=0)
    imu_grp = sg.GroupSpec("imu_vert", ["IMU0_Az","IMU1_Az"],
                           required_eligible=1, agree_eps=1.0, max_outliers=0)
    hdg_grp = sg.GroupSpec("heading", ["MAG_Hdg","GPS_Crs"],
                           required_eligible=1, agree_eps=30.0, max_outliers=0,
                           optional_sensors=frozenset({"GPS_Crs"}))  # ← THE FIX

    wp_params = wp.WindowParams(degraded_k=3, reduced_k=3, inconsistent_k=2,
                                ok_recover_k=3, failover_k=5)
    alt_dec = wp.WindowedDecider(wp_params)
    imu_dec = wp.WindowedDecider(wp_params)
    hdg_dec = wp.WindowedDecider(wp_params)

    mode_order = {"OK": 0, "REDUCED": 1, "DEGRADED": 2, "INCONSISTENT": 3, "FAILOVER": 4}
    ticks = []

    for snap in snaps:
        time_s = (snap.ts - t0) / 1e6
        ga = classify_gps_alt(snap, home)
        ba = classify_baro_alt(snap)
        i0 = classify_imu(snap, 0)
        i1 = classify_imu(snap, 1)
        mg = classify_mag(snap)
        gc = classify_gps_course(snap)

        alt_out = alt_dec.update(alt_grp, {"GPS_Alt": ga, "BARO_Alt": ba})
        imu_out = imu_dec.update(imu_grp, {"IMU0_Az": i0, "IMU1_Az": i1})
        hdg_out = hdg_dec.update(hdg_grp, {"MAG_Hdg": mg, "GPS_Crs": gc})

        fv = alt_out.get("fused_value")
        fa = fv.v if isinstance(fv, Real) else None
        sr = (snap.sim_alt - home) if snap.sim_valid else None
        ae = abs(fa - sr) if (fa is not None and sr is not None) else None

        all_modes = [alt_out["stable_group_mode"], imu_out["stable_group_mode"],
                     hdg_out["stable_group_mode"]]
        sys_mode = max(all_modes, key=lambda m: mode_order.get(m, 0))

        def fmt_pw(out): return ",".join(p.agreement for p in out.get("pairwise",[]))
        def fmt_alerts(out):
            alerts = out.get("sensor_alerts", [])
            return ",".join(f"{a.sensor_id}={a.alert}" for a in alerts if a.alert != "OK")

        ticks.append(Tick(
            t=time_s,
            gps_alt_type=_lbl(ga.s), baro_alt_type=_lbl(ba.s),
            alt_mode=alt_out["base_group_mode"], alt_stable=alt_out["stable_group_mode"],
            alt_redundancy=alt_out.get("redundancy", 0),
            fused_alt=fa, alt_pw=fmt_pw(alt_out), alt_err=ae,
            alt_alerts=fmt_alerts(alt_out),
            imu0_type=_lbl(i0.s), imu1_type=_lbl(i1.s),
            imu_mode=imu_out["base_group_mode"], imu_stable=imu_out["stable_group_mode"],
            imu_redundancy=imu_out.get("redundancy", 0),
            imu_pw=fmt_pw(imu_out), imu_alerts=fmt_alerts(imu_out),
            mag_type=_lbl(mg.s), gps_crs_type=_lbl(gc.s),
            hdg_mode=hdg_out["base_group_mode"], hdg_stable=hdg_out["stable_group_mode"],
            hdg_redundancy=hdg_out.get("redundancy", 0),
            hdg_pw=fmt_pw(hdg_out), hdg_alerts=fmt_alerts(hdg_out),
            system_mode=sys_mode,
            sim_alt_rel=sr,
        ))

    return ticks

# ── Report ────────────────────────────────────────────────────────

def report(filepath, label):
    print(f"\n{'='*85}")
    print(f"  {label}")
    print(f"{'='*85}")
    snaps = parse_log(filepath)
    dur = (snaps[-1].ts - snaps[0].ts) / 1e6
    print(f"  {len(snaps)} ticks, {dur:.0f}s\n")
    ticks = run_pipeline(snaps, label)
    n = len(ticks)

    for grp, mode_key, stable_key, red_key in [
        ("ALTITUDE", "alt_mode", "alt_stable", "alt_redundancy"),
        ("IMU_VERT", "imu_mode", "imu_stable", "imu_redundancy"),
        ("HEADING",  "hdg_mode", "hdg_stable", "hdg_redundancy"),
    ]:
        bm = Counter(getattr(t, mode_key) for t in ticks)
        sm = Counter(getattr(t, stable_key) for t in ticks)
        reds = [getattr(t, red_key) for t in ticks]
        mean_red = statistics.mean(reds)
        print(f"  {grp:10s} stable: OK={sm.get('OK',0):4d} RED={sm.get('REDUCED',0):4d} "
              f"DEG={sm.get('DEGRADED',0):4d} INC={sm.get('INCONSISTENT',0):4d} "
              f"FAIL={sm.get('FAILOVER',0):4d}  redundancy={mean_red:.2f}")

    sys_modes = Counter(t.system_mode for t in ticks)
    print(f"\n  SYSTEM     stable: OK={sys_modes.get('OK',0):4d} RED={sys_modes.get('REDUCED',0):4d} "
          f"DEG={sys_modes.get('DEGRADED',0):4d} INC={sys_modes.get('INCONSISTENT',0):4d} "
          f"FAIL={sys_modes.get('FAILOVER',0):4d}")

    # Transitions
    trans = []; prev = "OK"
    for t in ticks:
        if t.system_mode != prev:
            trans.append(t); prev = t.system_mode
    print(f"\n  System transitions ({len(trans)}):")
    for t in trans[:15]:
        fa = f"{t.fused_alt:.1f}m" if t.fused_alt is not None else "n/a"
        # Show which alerts are active
        alerts = []
        if t.alt_alerts: alerts.append(f"ALT[{t.alt_alerts}]")
        if t.imu_alerts: alerts.append(f"IMU[{t.imu_alerts}]")
        if t.hdg_alerts: alerts.append(f"HDG[{t.hdg_alerts}]")
        alert_str = " ".join(alerts) if alerts else "(all OK)"
        print(f"    t={t.t:7.1f}s  → {t.system_mode:<14s} {alert_str}")

    return {"label": label, "n": n, "sys_modes": dict(sys_modes), "ticks": ticks}


if __name__ == "__main__":
    log_dir = "/mnt/user-data/uploads"
    logs = [
        ("2022-07-27_07-43-42__No_Failure_.bin",                "No Failure (baseline)"),
        ("2022-07-26_06-46-30__GPS_Failure_.bin",               "GPS Failure"),
        ("2022-07-26_18-37-31__Barometer_Failure_.bin",         "Barometer Failure"),
        ("2022-07-26_17-02-53__Accelerometer_Failure_.bin",     "Accelerometer Failure"),
        ("2022-07-26_18-11-42__Gyro_Failure_.bin",              "Gyro Failure"),
        ("2022-07-27_17-05-17__Compass_Failure_.bin",           "Compass Failure"),
    ]

    all_r = []
    for fname, label in logs:
        path = os.path.join(log_dir, fname)
        if os.path.exists(path):
            all_r.append(report(path, label))

    # Scorecard
    print(f"\n\n{'='*85}")
    print(f"  SCORECARD v2 — with REDUCED mode, optional sensors, fixed confidence")
    print(f"{'='*85}")
    print(f"\n  {'Scenario':<28s} {'OK':>6s} {'RED':>6s} {'DEG':>6s} {'INC':>6s} {'FAIL':>6s}  Verdict")
    print(f"  {'-'*28} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*6}  {'-'*30}")

    baseline = all_r[0]["sys_modes"] if all_r else {}
    for r in all_r:
        n = r["n"]; sm = r["sys_modes"]
        ok_pct = sm.get('OK',0)/n*100
        red_pct = sm.get('REDUCED',0)/n*100
        deg_pct = sm.get('DEGRADED',0)/n*100
        inc_pct = sm.get('INCONSISTENT',0)/n*100
        fail_pct = sm.get('FAILOVER',0)/n*100
        non_ok = 100 - ok_pct

        if "No Fail" in r["label"]:
            bl_non_ok = non_ok
            verdict = f"← BASELINE (non-OK={non_ok:.1f}%)"
        elif fail_pct > 5:
            verdict = f"✓ CLEAR FAILOVER ({fail_pct:.0f}%)"
        elif red_pct > 5 and red_pct > baseline.get('REDUCED',0)/n*100 + 5:
            verdict = f"✓ CLEAR REDUCED ({red_pct:.0f}%)"
        elif non_ok - bl_non_ok > 10:
            verdict = f"✓ DETECTED (Δnon-OK={non_ok-bl_non_ok:+.1f}%)"
        else:
            verdict = f"✗ INDISTINGUISHABLE"

        print(f"  {r['label']:<28s} {ok_pct:5.1f}% {red_pct:5.1f}% {deg_pct:5.1f}% "
              f"{inc_pct:5.1f}% {fail_pct:5.1f}%  {verdict}")
