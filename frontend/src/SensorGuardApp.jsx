import { useState, useEffect, useCallback } from "react";

const API = "http://localhost:8001";
const C = {
  bg: "#0a0a0c", card: "#111114", border: "rgba(255,255,255,0.08)",
  text: "#f5f5f7", dim: "#8e8e93", muted: "#636366",
  green: "#30d158", red: "#ff3b30", orange: "#ff9f0a", blue: "#0a84ff", purple: "#bf5af2",
  amber: "#f5a623", // Amber/yellow for independent faults
};

/* ── Fault Type Severity Styling ── */
// Visual hierarchy: ROOT_CAUSE (strongest) > CASCADE (softer) > INDEPENDENT (amber)
const FAULT_TYPE_STYLES = {
  root_cause: {
    bgColor: "rgba(255,59,48,0.12)",       // Strong red background
    borderColor: "rgba(255,59,48,0.35)",   // Solid red border
    tagBg: "#ff3b30",                       // Full red tag
    tagText: "#fff",
    dotColor: "#ff3b30",
    opacity: 1,
    label: "ROOT CAUSE",
  },
  cascade: {
    bgColor: "rgba(255,59,48,0.05)",       // Lighter red background
    borderColor: "rgba(255,59,48,0.15)",   // Softer red border
    tagBg: "rgba(255,59,48,0.55)",         // Reduced opacity red tag
    tagText: "#fff",
    dotColor: "#ff6b5e",                    // Lighter red dot
    opacity: 0.85,
    label: "CASCADE",
  },
  independent: {
    bgColor: "rgba(245,166,35,0.08)",      // Amber background
    borderColor: "rgba(245,166,35,0.25)",  // Amber border
    tagBg: "#f5a623",                       // Amber tag
    tagText: "#000",
    dotColor: "#f5a623",
    opacity: 0.9,
    label: "INDEPENDENT",
  },
};

// Get fault type style by fault_type string
function getFaultTypeStyle(faultType) {
  return FAULT_TYPE_STYLES[faultType] || FAULT_TYPE_STYLES.independent;
}

/* ── Unit Detection & Formatting ── */
function detectUnit(pointName) {
  const name = (pointName || "").toUpperCase();

  // Damper/Valve position patterns → %
  if (/DMPR|DMP|VLV|VALVE|CMD|POS|OPEN|CLOSE|SPEED|SPD/.test(name)) {
    return "%";
  }

  // Temperature patterns → °F
  if (/TEMP|SAT|DAT|RAT|OAT|MAT|RM[-_]?TEMP|ZONE[-_]?TEMP|CLG|HTG|SUPPLY|RETURN|DISCHARGE|MIXED|OUTSIDE/.test(name)) {
    return "°F";
  }

  // Pressure patterns → in.w.c.
  if (/PRES|PRESS|DP|SP[-_]|STATIC|IN\.?W\.?C|PA$|HPA/.test(name)) {
    return "in.w.c.";
  }

  // Flow patterns → CFM
  if (/CFM|FLOW|AIR[-_]?FLOW/.test(name)) {
    return "CFM";
  }

  // Humidity → %RH
  if (/RH|HUMID/.test(name)) {
    return "%RH";
  }

  // CO2 → ppm
  if (/CO2|PPM/.test(name)) {
    return "ppm";
  }

  // Default: no unit
  return "";
}

function formatDeviation(pointName, value, unitOverride) {
  const unit = unitOverride || detectUnit(pointName);
  const numVal = typeof value === "number" ? value : parseFloat(value);

  if (isNaN(numVal)) {
    return { text: "—", unit, formatted: "—" };
  }

  // Format based on magnitude
  let text;
  if (Math.abs(numVal) < 0.01) {
    text = numVal.toFixed(4);
  } else if (Math.abs(numVal) < 1) {
    text = numVal.toFixed(2);
  } else if (Math.abs(numVal) < 100) {
    text = numVal.toFixed(1);
  } else {
    text = numVal.toFixed(0);
  }

  return {
    text,
    unit,
    formatted: unit ? `${text}${unit}` : text,
    delta: unit ? `Δ${text}${unit}` : `Δ${text}`,
  };
}

function formatValue(pointName, value, unitOverride) {
  const result = formatDeviation(pointName, value, unitOverride);
  return result.formatted;
}

// Format a diagnosis message with proper units based on point name
function formatDiagnosis(diagnosis, pointName, unitOverride) {
  if (!diagnosis) return "";

  const unit = unitOverride || detectUnit(pointName);
  if (!unit) return diagnosis;

  // Replace patterns like "Δ=31.5" or "deviation of 31.5" with unit-aware versions
  let result = diagnosis;

  // Add units to delta patterns: Δ=15.98 → Δ=15.98°F
  result = result.replace(/Δ=(\d+\.?\d*)/g, (match, num) => `Δ=${num}${unit}`);

  // Add units to "deviation of X" patterns
  result = result.replace(/deviation of (\d+\.?\d*)/gi, (match, num) => `deviation of ${num}${unit}`);

  // Add units to bare numbers in common contexts
  // "Setpoint=55 but Measured=71" → "Setpoint=55°F but Measured=71°F"
  if (unit === "°F" || unit === "%") {
    result = result.replace(/Setpoint=(\d+\.?\d*)/gi, (match, num) => `Setpoint=${num}${unit}`);
    result = result.replace(/Measured=(\d+\.?\d*)/gi, (match, num) => `Measured=${num}${unit}`);
    result = result.replace(/CMD=(\d+\.?\d*)/gi, (match, num) => `CMD=${num}${unit}`);
    result = result.replace(/POS=(\d+\.?\d*)/gi, (match, num) => `POS=${num}${unit}`);
  }

  return result;
}

// Extract deviation value from diagnosis string
function extractDeviation(diagnosis, pointName, unitOverride) {
  if (!diagnosis) return null;

  // Try to extract Δ value
  const deltaMatch = diagnosis.match(/Δ=(\d+\.?\d*)/);
  if (deltaMatch) {
    return formatDeviation(pointName, parseFloat(deltaMatch[1]), unitOverride);
  }

  // Try to extract "deviation of X"
  const devMatch = diagnosis.match(/deviation of (\d+\.?\d*)/i);
  if (devMatch) {
    return formatDeviation(pointName, parseFloat(devMatch[1]), unitOverride);
  }

  return null;
}

/* ── API helpers ── */
const store = { token: null };
async function api(path, opts = {}) {
  const h = { ...(opts.headers || {}) };
  if (store.token) h["Authorization"] = `Bearer ${store.token}`;
  if (opts.body && !(opts.body instanceof FormData))
    h["Content-Type"] = "application/json";
  const res = await fetch(API + path, { ...opts, headers: h });
  if (res.status === 401) { store.token = null; throw new Error("Unauthorized"); }
  const data = await res.json();
  if (!res.ok) throw new Error(data.detail || data.error || `HTTP ${res.status}`);
  return data;
}

/* ── Reusable components ── */
function Dot({ status, size = 8 }) {
  const [on, setOn] = useState(true);
  useEffect(() => {
    if (status !== "FAULT" && status !== "fault" && status !== "critical") return;
    const t = setInterval(() => setOn(v => !v), 800);
    return () => clearInterval(t);
  }, [status]);
  const color = (status === "FAULT" || status === "fault" || status === "critical") ? C.red
    : (status === "WATCH" || status === "warning") ? C.orange : C.green;
  return <div style={{
    width: size, height: size, borderRadius: "50%", backgroundColor: color,
    boxShadow: `0 0 ${size}px ${color}60`, opacity: on ? 1 : 0.3,
    transition: "opacity 0.4s", flexShrink: 0,
  }} />;
}

function Btn({ children, onClick, primary, small, disabled, style }) {
  return <button onClick={onClick} disabled={disabled} style={{
    padding: small ? "8px 16px" : "12px 24px", borderRadius: 10, border: "none",
    cursor: disabled ? "default" : "pointer", fontSize: small ? 12 : 14, fontWeight: 700,
    background: primary ? `linear-gradient(135deg, ${C.green}, ${C.blue})` : "rgba(255,255,255,0.06)",
    color: primary ? "#fff" : C.text, opacity: disabled ? 0.4 : 1,
    transition: "all 0.2s", ...style,
  }}>{children}</button>;
}

function Card({ children, style }) {
  return <div style={{
    padding: 20, borderRadius: 14, background: C.card, border: `1px solid ${C.border}`, ...style,
  }}>{children}</div>;
}

function Input({ label, ...props }) {
  return <div style={{ marginBottom: 14 }}>
    {label && <label style={{ fontSize: 11, color: C.dim, display: "block", marginBottom: 4, letterSpacing: 1, textTransform: "uppercase" }}>{label}</label>}
    <input {...props} style={{
      width: "100%", padding: "10px 14px", borderRadius: 8, border: `1px solid ${C.border}`,
      background: "rgba(255,255,255,0.04)", color: C.text, fontSize: 14,
      outline: "none", boxSizing: "border-box", ...props.style,
    }} />
  </div>;
}

/* ── Auth Screen ── */
function AuthScreen({ onAuth }) {
  const [isLogin, setIsLogin] = useState(true);
  const [email, setEmail] = useState(""); const [pw, setPw] = useState(""); const [name, setName] = useState("");
  const [err, setErr] = useState("");
  const submit = async () => {
    try {
      const endpoint = isLogin ? "/api/auth/login" : "/api/auth/signup";
      const body = isLogin ? { email, password: pw } : { email, password: pw, name };
      const r = await api(endpoint, { method: "POST", body: JSON.stringify(body) });
      store.token = r.token;
      onAuth(r.user);
    } catch (e) { setErr(e.message); }
  };
  return <div style={{ display: "flex", justifyContent: "center", alignItems: "center", minHeight: "100vh", padding: 20 }}>
    <Card style={{ width: 380, maxWidth: "100%" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 24 }}>
        <div style={{ width: 32, height: 32, borderRadius: 8, background: `linear-gradient(135deg, ${C.green}, ${C.blue})`,
          display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14, fontWeight: 800, color: "#fff" }}>S</div>
        <span style={{ fontSize: 18, fontWeight: 700 }}>SensorGuard</span>
      </div>
      {!isLogin && <Input label="Name" value={name} onChange={e => setName(e.target.value)} placeholder="Your name" />}
      <Input label="Email" value={email} onChange={e => setEmail(e.target.value)} placeholder="you@company.com" type="email" />
      <Input label="Password" value={pw} onChange={e => setPw(e.target.value)} placeholder="••••••••" type="password"
        onKeyDown={e => e.key === "Enter" && submit()} />
      {err && <div style={{ fontSize: 12, color: C.red, marginBottom: 10 }}>{err}</div>}
      <Btn primary onClick={submit} style={{ width: "100%", marginTop: 6 }}>{isLogin ? "Log In" : "Sign Up"}</Btn>
      <div style={{ textAlign: "center", marginTop: 14, fontSize: 13, color: C.dim, cursor: "pointer" }}
        onClick={() => { setIsLogin(!isLogin); setErr(""); }}>
        {isLogin ? "Need an account? Sign up" : "Already have an account? Log in"}
      </div>
    </Card>
  </div>;
}

/* ── Confirm Modal ── */
function ConfirmModal({ title, message, onConfirm, onCancel, confirming }) {
  useEffect(() => {
    const handleKey = (e) => {
      if (e.key === "Escape" && !confirming) onCancel();
    };
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [onCancel, confirming]);

  return <div style={{
    position: "fixed", inset: 0, zIndex: 1000,
    display: "flex", justifyContent: "center", alignItems: "center",
    background: "rgba(0,0,0,0.6)", backdropFilter: "blur(4px)",
  }} onClick={() => { if (!confirming) onCancel(); }}>
    <Card style={{ width: 380, maxWidth: "90%" }} onClick={e => e.stopPropagation()}>
      <div style={{ fontSize: 16, fontWeight: 700, marginBottom: 8 }}>{title}</div>
      <div style={{ fontSize: 13, color: C.dim, marginBottom: 20, lineHeight: 1.5 }}>{message}</div>
      <div style={{ display: "flex", justifyContent: "flex-end", gap: 10 }}>
        <Btn small onClick={onCancel} disabled={confirming}>Cancel</Btn>
        <Btn small onClick={onConfirm} disabled={confirming}
          style={{ background: C.red, color: "#fff" }}>
          {confirming ? "Deleting..." : "Yes, delete"}
        </Btn>
      </div>
    </Card>
  </div>;
}

/* ── Building List ── */
function BuildingList({ buildings, selected, onSelect, onCreate, onDelete }) {
  const [showNew, setShowNew] = useState(false);
  const [newName, setNewName] = useState("");
  const doCreate = async () => {
    if (!newName.trim()) return;
    await onCreate(newName.trim());
    setNewName(""); setShowNew(false);
  };
  return <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
      <span style={{ fontSize: 10, color: C.muted, letterSpacing: 2, textTransform: "uppercase" }}>Buildings</span>
      <span style={{ fontSize: 18, color: C.blue, cursor: "pointer", lineHeight: 1 }} onClick={() => setShowNew(!showNew)}>+</span>
    </div>
    {showNew && <Card style={{ padding: 12 }}>
      <Input placeholder="Building name" value={newName} onChange={e => setNewName(e.target.value)}
        onKeyDown={e => e.key === "Enter" && doCreate()} />
      <Btn small primary onClick={doCreate}>Add Building</Btn>
    </Card>}
    {buildings.map(b => (
      <div key={b.id} onClick={() => onSelect(b.id)} style={{
        padding: "14px 16px", borderRadius: 12, cursor: "pointer",
        background: selected === b.id ? "rgba(255,255,255,0.06)" : b.status === "fault" ? "rgba(255,59,48,0.04)" : "rgba(48,209,88,0.03)",
        border: `1px solid ${selected === b.id ? "rgba(255,255,255,0.15)" : C.border}`,
        transition: "all 0.2s",
      }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span style={{ fontSize: 14, fontWeight: 600 }}>{b.name}</span>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <Dot status={b.status} size={10} />
            <button onClick={(e) => { e.stopPropagation(); onDelete(b); }} style={{
              background: "none", border: "none", cursor: "pointer", padding: 2,
              fontSize: 13, lineHeight: 1, opacity: 0.4, transition: "opacity 0.2s",
            }} onMouseEnter={e => e.currentTarget.style.opacity = 1}
               onMouseLeave={e => e.currentTarget.style.opacity = 0.4}
            >🗑</button>
          </div>
        </div>
        <div style={{ fontSize: 11, color: C.dim, marginTop: 4 }}>
          {b.pair_count} pairs · {b.active_faults > 0 ? <span style={{ color: C.red }}>{b.active_faults} faults</span> : "healthy"}
        </div>
      </div>
    ))}
    {buildings.length === 0 && <div style={{ fontSize: 13, color: C.muted, textAlign: "center", padding: 20 }}>
      No buildings yet. Click + to add one.
    </div>}
  </div>;
}

/* ── Config Editor ── */
function ConfigEditor({ building, onSave }) {
  const storageKey = `sg_config_collapsed_${building.id}`;
  const [pairs, setPairs] = useState(building.sensor_config || []);
  const [csvCols, setCsvCols] = useState([]);
  const [instanceCol, setInstanceCol] = useState(building.instance_col || "");
  const isConfigIncomplete = (cfg) => {
    if (!cfg || cfg.length === 0) return true;
    return cfg.some(p => !p.name || !p.col_a || !p.col_b);
  };

  const [collapsed, setCollapsed] = useState(() => {
    // Config incomplete → always open regardless of stored preference
    if (isConfigIncomplete(building.sensor_config)) return false;
    const stored = localStorage.getItem(storageKey);
    if (stored !== null) return stored === "true";
    // Has pairs configured → default collapsed
    return true;
  });

  const toggleCollapsed = () => {
    const next = !collapsed;
    setCollapsed(next);
    localStorage.setItem(storageKey, String(next));
  };

  // Reset state only when switching to a different building
  useEffect(() => {
    const cfg = building.sensor_config || [];
    setPairs(cfg);
    setInstanceCol(building.instance_col || "");
    setCsvCols([]);
    // Config incomplete → force open
    if (isConfigIncomplete(cfg)) { setCollapsed(false); return; }
    const stored = localStorage.getItem(`sg_config_collapsed_${building.id}`);
    if (stored !== null) setCollapsed(stored === "true");
    else setCollapsed(cfg.length > 0);
  }, [building.id]); // Only depend on building.id, not the config data

  const addPair = () => setPairs([...pairs, { name: "", group: "valve", col_a: "", col_b: "", pair_type: "cmd_pos", eps: 0.15, unit: "%", range_min: null, range_max: null }]);
  const update = (i, field, val) => { const p = [...pairs]; p[i] = { ...p[i], [field]: val }; setPairs(p); };
  const remove = (i) => setPairs(pairs.filter((_, j) => j !== i));
  const save = async () => { await onSave(pairs, instanceCol); };

  const discoverCols = async (file) => {
    const form = new FormData();
    form.append("file", file);
    try {
      const r = await api(`/api/buildings/${building.id}/discover-columns`, { method: "POST", body: form });
      setCsvCols(r.columns || []);
    } catch (e) { console.error(e); }
  };

  // Auto-detect pairs from CSV columns based on common naming patterns
  const autoDetectPairs = (columns) => {
    const colNames = columns.map(c => c.name);
    const colSamples = {};
    columns.forEach(c => { colSamples[c.name] = c.sample; });

    // Check for instance-based sensors (ArduPilot: IMU_I, BARO_I, GPS_I, etc.)
    // STRICT rules to avoid false positives like "E_ZONE_I" (Energy Zone Interior)
    const instanceCol = colNames.find(c => {
      // Must match EXACTLY: {SHORT_PREFIX}_I or {SHORT_PREFIX}_Instance
      // Short prefix = 2-5 uppercase letters (IMU, BARO, GPS, MAG, etc.)
      const instancePattern = /^[A-Z]{2,5}_I$/;
      const instancePattern2 = /^[A-Z]{2,5}_Instance$/i;

      if (!instancePattern.test(c) && !instancePattern2.test(c)) {
        return false;
      }

      // Sample value must be a small integer (0, 1, 2, 3)
      const sample = colSamples[c];
      if (sample === undefined || sample === null || sample === '') {
        return false;
      }
      const sampleNum = parseInt(sample, 10);
      if (isNaN(sampleNum) || sampleNum < 0 || sampleNum > 9) {
        return false;
      }

      // Also verify there are matching measurement columns for this sensor type
      // e.g., for IMU_I, there should be IMU_AccX, IMU_GyrX, etc.
      const prefix = c.replace(/_I$/i, '').replace(/_Instance$/i, '');
      const hasMeasurements = colNames.some(col =>
        col.startsWith(prefix + '_') && col !== c &&
        !col.endsWith('_I') && !col.endsWith('_Instance')
      );

      return hasMeasurements;
    });

    if (instanceCol) {
      return autoDetectInstancePairs(columns, instanceCol, colSamples);
    }

    // HVAC mode - no valid instance column found
    return autoDetectHvacPairs(colNames);
  };

  // Auto-detect HVAC pairs (setpoint vs measured, command vs feedback)
  const autoDetectHvacPairs = (colNames) => {
    const detected = [];
    const used = new Set();

    // Helper to find column by pattern (case-insensitive, with common separators)
    const findCol = (patterns) => {
      for (const pat of patterns) {
        const found = colNames.find(c => {
          const normalized = c.toUpperCase().replace(/[-_\s]/g, '');
          const patNorm = pat.toUpperCase().replace(/[-_\s]/g, '');
          return normalized === patNorm || normalized.includes(patNorm);
        });
        if (found && !used.has(found)) return found;
      }
      return null;
    };

    // Exact column name matches for known HVAC pairs
    // Format: [name, colA_patterns, colB_patterns, eps, unit, group, pair_type]
    const hvacPairs = [
      // Supply Air Temperature: setpoint vs measured
      ["SAT", ["SAT_SPT", "SAT-SPT", "SATSPT", "SA_TEMP_SP", "SA-TEMP-SP"],
              ["SA-TEMP", "SA_TEMP", "SATEMP", "SAT"], 2, "°F", "sat", "meas_setp"],

      // Room Temperature: cooling setpoint vs measured
      ["Room Cooling", ["RMCLGSPT", "RM_CLG_SPT", "RM-CLG-SPT", "ROOM_CLG_SP"],
                       ["RM-TEMP", "RM_TEMP", "RMTEMP", "ROOM_TEMP"], 2, "°F", "zone", "meas_setp"],

      // Room Temperature: heating setpoint vs measured
      ["Room Heating", ["RMHTGSPT", "RM_HTG_SPT", "RM-HTG-SPT", "ROOM_HTG_SP"],
                       ["RM-TEMP", "RM_TEMP", "RMTEMP", "ROOM_TEMP"], 2, "°F", "zone", "meas_setp"],

      // Supply Static Pressure: setpoint vs measured
      ["SA Static Pressure", ["SA_SPSPT", "SA-SP-SPT", "SASPSPT", "SA_SP_SP"],
                             ["SA-SP", "SA_SP", "SASP"], 0.2, "inWG", "sat", "meas_setp"],

      // Outdoor Air Temperature cross-check
      ["OA Temp", ["OA-TEMP", "OA_TEMP", "OATEMP"],
                  ["OAD-TEMP", "OAD_TEMP", "OADTEMP"], 5, "°F", "zone", "meas_setp"],

      // Discharge Air Temperature
      ["DAT", ["DAT_SPT", "DAT-SPT", "DATSPT", "DA_TEMP_SP"],
              ["DA-TEMP", "DA_TEMP", "DATEMP", "DAT"], 2, "°F", "sat", "meas_setp"],

      // Mixed Air Temperature
      ["MAT", ["MAT_SPT", "MAT-SPT", "MATSPT", "MA_TEMP_SP"],
              ["MA-TEMP", "MA_TEMP", "MATEMP", "MAT"], 2, "°F", "sat", "meas_setp"],

      // Return Air Temperature
      ["RAT", ["RAT_SPT", "RAT-SPT", "RATSPT", "RA_TEMP_SP"],
              ["RA-TEMP", "RA_TEMP", "RATEMP", "RAT"], 2, "°F", "zone", "meas_setp"],

      // Chilled Water Valve
      ["CHW Valve", ["CHWC-VLV", "CHWC_VLV", "CHW_VLV_CMD", "CHWVLV"],
                    ["CHWC-VLV-FB", "CHWC_VLV_FB", "CHW_VLV_POS", "CHWVLVFB"], 5, "%", "chw", "cmd_pos"],

      // Hot Water Valve
      ["HW Valve", ["HWC-VLV", "HWC_VLV", "HW_VLV_CMD", "HWVLV"],
                   ["HWC-VLV-FB", "HWC_VLV_FB", "HW_VLV_POS", "HWVLVFB"], 5, "%", "valve", "cmd_pos"],

      // Outside Air Damper
      ["OA Damper", ["OAD-CMD", "OAD_CMD", "OA_DMPR_CMD", "OADCMD"],
                    ["OAD-POS", "OAD_POS", "OA_DMPR_POS", "OADPOS"], 5, "%", "damper", "cmd_pos"],

      // Return Air Damper
      ["RA Damper", ["RAD-CMD", "RAD_CMD", "RA_DMPR_CMD", "RADCMD"],
                    ["RAD-POS", "RAD_POS", "RA_DMPR_POS", "RADPOS"], 5, "%", "damper", "cmd_pos"],

      // Exhaust Air Damper
      ["EA Damper", ["EAD-CMD", "EAD_CMD", "EA_DMPR_CMD", "EADCMD"],
                    ["EAD-POS", "EAD_POS", "EA_DMPR_POS", "EADPOS"], 5, "%", "damper", "cmd_pos"],

      // Supply Fan Speed
      ["SF Speed", ["SF-SPD-CMD", "SF_SPD_CMD", "SFCMD", "SF_CMD"],
                   ["SF-SPD-FB", "SF_SPD_FB", "SFFB", "SF_FB"], 5, "%", "sat", "cmd_pos"],

      // Return Fan Speed
      ["RF Speed", ["RF-SPD-CMD", "RF_SPD_CMD", "RFCMD", "RF_CMD"],
                   ["RF-SPD-FB", "RF_SPD_FB", "RFFB", "RF_FB"], 5, "%", "sat", "cmd_pos"],
    ];

    // Try to find each HVAC pair
    for (const [name, colAPatterns, colBPatterns, eps, unit, group, pairType] of hvacPairs) {
      const colA = findCol(colAPatterns);
      const colB = findCol(colBPatterns);

      if (colA && colB) {
        used.add(colA);
        used.add(colB);
        detected.push({
          name,
          group,
          col_a: colA,
          col_b: colB,
          pair_type: pairType,
          eps,
          unit
        });
      }
    }

    // Columns to exclude from auto-pairing (non-HVAC measurements)
    const excludePatterns = [
      /LITES/i, /BBHT/i, /^ZONE/i, /E_ZONE/i, /ENERGY/i,
      /SCHED/i, /MODE/i, /STATUS/i, /ALARM/i, /^TIME$/i
    ];
    const shouldExclude = (col) => excludePatterns.some(p => p.test(col));

    // Generic pattern matching disabled - too many false positives
    // Only use the explicit HVAC pairs defined above

    // Fallback: pair columns with similar names (A/B, 1/2 suffixes)
    // But exclude non-HVAC columns
    const abPatterns = [
      [/_a$/i, /_b$/i], [/A$/i, /B$/i], [/_1$/i, /_2$/i], [/1$/i, /2$/i],
      [/_primary$/i, /_secondary$/i], [/_in$/i, /_out$/i]
    ];
    for (const [patA, patB] of abPatterns) {
      for (const colA of colNames) {
        if (used.has(colA) || !patA.test(colA) || shouldExclude(colA)) continue;
        const baseA = colA.replace(patA, "");
        for (const colB of colNames) {
          if (used.has(colB) || !patB.test(colB) || shouldExclude(colB)) continue;
          const baseB = colB.replace(patB, "");
          if (baseA.toLowerCase() === baseB.toLowerCase()) {
            used.add(colA);
            used.add(colB);
            detected.push({
              name: baseA || `Pair ${detected.length + 1}`,
              group: "custom", col_a: colA, col_b: colB, pair_type: "meas_setp",
              eps: 2, unit: ""
            });
            break;
          }
        }
      }
    }

    return { pairs: detected, instanceCol: null };
  };

  // Auto-detect pairs for multi-instance sensors (IMU, BARO, GPS, etc.)
  // Returns { pairs, instanceCol, singleInstanceSensors }
  const autoDetectInstancePairs = (columns, instanceCol, colSamples) => {
    const colNames = columns.map(c => c.name);
    const detected = [];
    const singleInstanceSensors = [];

    // Extract sensor prefix from instance column (IMU_I -> IMU, BARO_I -> BARO)
    const prefix = instanceCol.replace(/_I$/i, "").replace(/_Instance$/i, "");

    // Check instance column sample value - if only "0" exists in sample,
    // we need to warn that this might be single-instance
    const instanceSample = colSamples[instanceCol];
    const sampleInstanceId = instanceSample ? instanceSample.toString().trim() : "0";

    // Columns to SKIP - metadata, health flags, sample rates (NOT actual measurements)
    const skipSuffixes = [
      'EG', 'EA',           // IMU error flags
      'GH', 'AH',           // IMU health flags
      'GHz', 'AHz',         // IMU sample rates
      'CRt', 'SMS', 'Offset', // BARO metadata
      'Status', 'Flag', 'Health', 'Rate', 'Hz'  // Generic metadata
    ];

    // Only include actual sensor measurements
    const validMeasurements = {
      // IMU measurements with eps values
      AccX: { eps: 0.5, unit: "m/s²" },
      AccY: { eps: 0.5, unit: "m/s²" },
      AccZ: { eps: 0.5, unit: "m/s²" },
      GyrX: { eps: 0.1, unit: "rad/s" },
      GyrY: { eps: 0.1, unit: "rad/s" },
      GyrZ: { eps: 0.1, unit: "rad/s" },
      T: { eps: 5, unit: "°C" },
      // BARO measurements
      Alt: { eps: 1.0, unit: "m" },
      Press: { eps: 50, unit: "Pa" },
      Temp: { eps: 5, unit: "°C" },
      // GPS measurements
      Lat: { eps: 0.0001, unit: "°" },
      Lng: { eps: 0.0001, unit: "°" },
      Spd: { eps: 2, unit: "m/s" },
    };

    // Find all measurement columns for this sensor type
    const measureCols = colNames.filter(c => {
      if (!c.startsWith(prefix + "_")) return false;
      if (c === instanceCol) return false;
      if (/_I$/i.test(c) || /_Instance$/i.test(c)) return false;

      const measureName = c.replace(prefix + "_", "");
      // Skip metadata columns
      if (skipSuffixes.some(s => measureName === s || measureName.endsWith(s))) return false;

      return true;
    });

    // Check if this sensor type has multiple instances by looking for other instance columns
    // Look for other _I columns to detect different sensor types
    const allInstanceCols = colNames.filter(c => /_I$/i.test(c) || /_Instance$/i.test(c));

    // For each sensor type with instance column, check if sample shows instance > 0
    // This is a heuristic - the full check happens during analysis when we see all rows
    const hasMultipleInstances = sampleInstanceId !== "0" ||
      // If sample is 0, we assume there might be instance 1 in other rows
      // The backend pivot will handle missing instances gracefully
      true; // For now, assume multi-instance if _I column exists

    // Create pairs only for valid measurement columns
    for (const col of measureCols) {
      const measureName = col.replace(prefix + "_", "");

      // Only create pair if it's a known measurement type
      const defaults = validMeasurements[measureName];
      if (!defaults) continue;  // Skip unknown columns

      detected.push({
        name: `${prefix}_${measureName}`,
        group: "custom",  // Use "custom" for symmetric comparison
        col_a: `${col}_I0`,  // Instance 0
        col_b: `${col}_I1`,  // Instance 1
        pair_type: "custom",  // Symmetric comparison, not Meas/SP
        eps: defaults.eps,
        unit: defaults.unit,
      });
    }

    return { pairs: detected, instanceCol, singleInstanceSensors };
  };

  const handleAutoDetect = async (file) => {
    const form = new FormData();
    form.append("file", file);
    try {
      const r = await api(`/api/buildings/${building.id}/discover-columns`, { method: "POST", body: form });
      const cols = r.columns || [];
      setCsvCols(cols);
      const result = autoDetectPairs(cols);
      const detected = result.pairs || result;  // Handle both old and new return format
      const detectedInstanceCol = result.instanceCol;

      // Always replace pairs with newly detected ones (even if empty)
      setPairs(detected);
      setInstanceCol(detectedInstanceCol || "");

      if (detected.length === 0) {
        alert("No matching pairs found. Please add pairs manually using the column names shown.");
      }
    } catch (e) { console.error(e); }
  };

  return <Card>
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: collapsed ? 0 : 16 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <span onClick={toggleCollapsed} style={{
          cursor: "pointer", fontSize: 12, color: C.dim, width: 20, height: 20,
          display: "inline-flex", alignItems: "center", justifyContent: "center",
          borderRadius: 4, background: "rgba(255,255,255,0.06)", transition: "transform 0.2s",
          transform: collapsed ? "rotate(-90deg)" : "rotate(0deg)",
        }}>&#9660;</span>
        <span style={{ fontSize: 14, fontWeight: 700 }}>Sensor Pair Configuration</span>
        {collapsed && <span style={{ fontSize: 11, color: C.dim }}>({pairs.length} pairs)</span>}
      </div>
      {!collapsed && <div style={{ display: "flex", gap: 8 }}>
        <label style={{
          padding: "6px 14px", borderRadius: 8, cursor: "pointer", fontSize: 12, fontWeight: 600,
          background: "rgba(48,209,88,0.1)", color: C.green, border: `1px solid rgba(48,209,88,0.2)`,
        }}>
          Auto-Detect Pairs
          <input type="file" accept=".csv" hidden onChange={e => e.target.files[0] && handleAutoDetect(e.target.files[0])} />
        </label>
        <label style={{
          padding: "6px 14px", borderRadius: 8, cursor: "pointer", fontSize: 12, fontWeight: 600,
          background: "rgba(191,90,242,0.1)", color: C.purple, border: `1px solid rgba(191,90,242,0.2)`,
        }}>
          Discover Columns
          <input type="file" accept=".csv" hidden onChange={e => e.target.files[0] && discoverCols(e.target.files[0])} />
        </label>
        <Btn small onClick={addPair}>+ Add Pair</Btn>
      </div>}
    </div>

    {!collapsed && <>{csvCols.length > 0 && <div style={{
      marginBottom: 14, padding: 12, borderRadius: 8, background: "rgba(191,90,242,0.04)", border: `1px solid rgba(191,90,242,0.1)`,
    }}>
      <div style={{ fontSize: 10, color: C.purple, letterSpacing: 1, marginBottom: 6, textTransform: "uppercase" }}>CSV Columns Found</div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
        {csvCols.map(c => (
          <span key={c.index} style={{ fontSize: 11, padding: "3px 8px", borderRadius: 4, background: "rgba(255,255,255,0.04)", color: C.dim }}>
            {c.name} <span style={{ color: C.muted }}>({c.sample})</span>
          </span>
        ))}
      </div>
    </div>}

    {/* Instance column setting for multi-instance sensors */}
    {instanceCol && <div style={{
      marginBottom: 14, padding: 12, borderRadius: 8, background: "rgba(10,132,255,0.04)", border: `1px solid rgba(10,132,255,0.1)`,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <div style={{ fontSize: 10, color: C.blue, letterSpacing: 1, textTransform: "uppercase" }}>Instance Column</div>
        <span style={{ fontSize: 12, fontWeight: 600, color: C.text }}>{instanceCol}</span>
        <span style={{ fontSize: 11, color: C.dim }}>(multi-instance sensor mode enabled)</span>
        <span onClick={() => setInstanceCol("")} style={{ cursor: "pointer", color: C.muted, fontSize: 12, marginLeft: "auto" }}>Clear</span>
      </div>
    </div>}

    {pairs.map((p, i) => (
      <div key={i} style={{
        display: "grid", gridTemplateColumns: "1fr 80px 1fr 1fr 90px 60px 50px 30px",
        gap: 6, alignItems: "end", marginBottom: 8, padding: 10, borderRadius: 8,
        background: "rgba(255,255,255,0.02)", border: `1px solid ${C.border}`,
      }}>
        <Input label={i === 0 ? "Name" : ""} placeholder="AHU-1 Valve" value={p.name} onChange={e => update(i, "name", e.target.value)} style={{ marginBottom: 0 }} />
        <div style={{ marginBottom: 0 }}>
          {i === 0 && <label style={{ fontSize: 10, color: C.dim, display: "block", marginBottom: 4, letterSpacing: 1, textTransform: "uppercase" }}>Type</label>}
          <select value={p.pair_type} onChange={e => update(i, "pair_type", e.target.value)} style={{
            width: "100%", padding: "10px 6px", borderRadius: 8, border: `1px solid ${C.border}`,
            background: "rgba(255,255,255,0.04)", color: C.text, fontSize: 12,
          }}>
            <option value="cmd_pos">CMD/POS</option>
            <option value="meas_setp">Meas/SP</option>
          </select>
        </div>
        <Input label={i === 0 ? "Column A" : ""} placeholder="col_cmd" value={p.col_a} onChange={e => update(i, "col_a", e.target.value)} style={{ marginBottom: 0 }} />
        <Input label={i === 0 ? "Column B" : ""} placeholder="col_pos" value={p.col_b} onChange={e => update(i, "col_b", e.target.value)} style={{ marginBottom: 0 }} />
        <div style={{ marginBottom: 0 }}>
          {i === 0 && <label style={{ fontSize: 10, color: C.dim, display: "block", marginBottom: 4, letterSpacing: 1, textTransform: "uppercase" }}>Group</label>}
          <select value={p.group} onChange={e => update(i, "group", e.target.value)} style={{
            width: "100%", padding: "10px 6px", borderRadius: 8, border: `1px solid ${C.border}`,
            background: "rgba(255,255,255,0.04)", color: C.text, fontSize: 12,
          }}>
            {["valve", "sat", "zone", "damper", "chw", "custom"].map(g => <option key={g} value={g}>{g}</option>)}
          </select>
        </div>
        <Input label={i === 0 ? "Eps" : ""} type="number" step="0.01" value={p.eps} onChange={e => update(i, "eps", parseFloat(e.target.value) || 0.15)} style={{ marginBottom: 0 }} />
        <Input label={i === 0 ? "Unit" : ""} placeholder="%" value={p.unit} onChange={e => update(i, "unit", e.target.value)} style={{ marginBottom: 0 }} />
        <span onClick={() => remove(i)} style={{ cursor: "pointer", color: C.red, fontSize: 16, textAlign: "center", paddingBottom: 8 }}>×</span>
      </div>
    ))}
    <div style={{ display: "flex", justifyContent: "flex-end", marginTop: 12 }}>
      <Btn primary small onClick={save}>Save Configuration</Btn>
    </div>
    </>}
  </Card>;
}

/* ── TNA Tag Badge ── */
function TnaTagBadge({ tag }) {
  const tagColors = {
    AGREE: { bg: "rgba(48,209,88,0.15)", color: C.green },
    DISAGREE: { bg: "rgba(255,59,48,0.15)", color: C.red },
    INAPPLICABLE: { bg: "rgba(142,142,147,0.15)", color: C.dim },
    OFFLINE: { bg: "rgba(255,159,10,0.15)", color: C.orange },
    INDETERMINATE: { bg: "rgba(191,90,242,0.15)", color: C.purple },
    UNCERTAIN: { bg: "rgba(191,90,242,0.15)", color: C.purple },
  };
  const style = tagColors[tag] || tagColors.INDETERMINATE;
  return <span style={{
    fontSize: 9, padding: "2px 6px", borderRadius: 4,
    background: style.bg, color: style.color,
    fontWeight: 700, letterSpacing: 0.5, fontFamily: "monospace",
  }}>{tag}</span>;
}

/* ── Fault Evidence Panel ── */
function FaultEvidencePanel({ evidence, fault }) {
  if (!evidence) return null;

  const unit = evidence.unit || detectUnit(evidence.pair || evidence.label);

  // Format time display
  const formatTimeRange = () => {
    if (!evidence.start_time && !evidence.start_tick) return null;

    const hasRealTime = evidence.start_time !== evidence.start_tick;

    if (hasRealTime && evidence.start_time && evidence.end_time) {
      const formatTime = (ts) => {
        const d = new Date(ts * 1000);
        return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
      };
      return `${formatTime(evidence.start_time)} → ${formatTime(evidence.end_time)}`;
    }

    return `tick ${evidence.start_tick} → tick ${evidence.end_tick}`;
  };

  return <div style={{
    padding: "12px 16px", background: "rgba(0,0,0,0.2)",
    borderTop: `1px solid ${C.border}`,
  }}>
    <div style={{
      display: "flex", alignItems: "center", justifyContent: "space-between",
      marginBottom: 10,
    }}>
      <span style={{ fontSize: 10, color: C.blue, letterSpacing: 1, textTransform: "uppercase", fontWeight: 600 }}>
        Why? — Evidence Details
      </span>
      <TnaTagBadge tag={evidence.tna_tag || "DISAGREE"} />
    </div>

    {/* A vs B Comparison Table */}
    <div style={{
      display: "grid", gridTemplateColumns: "1fr 1fr",
      gap: 8, marginBottom: 12,
      padding: 10, borderRadius: 8, background: "rgba(255,255,255,0.02)",
      border: `1px solid ${C.border}`,
    }}>
      <div>
        <div style={{ fontSize: 9, color: C.muted, letterSpacing: 0.5, marginBottom: 4 }}>
          {evidence.pair_type === "cmd_pos" ? "COMMAND" : evidence.pair_type === "meas_setp" ? "SETPOINT" : "SENSOR A"}
        </div>
        <div style={{ fontSize: 11, color: C.dim, fontFamily: "monospace", marginBottom: 2 }}>
          {evidence.col_a}
        </div>
        <div style={{ fontSize: 18, fontWeight: 700, color: C.text }}>
          {evidence.val_a !== null && evidence.val_a !== undefined
            ? `${typeof evidence.val_a === 'number' ? evidence.val_a.toFixed(evidence.val_a < 10 ? 2 : 1) : evidence.val_a}${unit}`
            : "—"}
        </div>
      </div>
      <div>
        <div style={{ fontSize: 9, color: C.muted, letterSpacing: 0.5, marginBottom: 4 }}>
          {evidence.pair_type === "cmd_pos" ? "POSITION" : evidence.pair_type === "meas_setp" ? "MEASURED" : "SENSOR B"}
        </div>
        <div style={{ fontSize: 11, color: C.dim, fontFamily: "monospace", marginBottom: 2 }}>
          {evidence.col_b}
        </div>
        <div style={{ fontSize: 18, fontWeight: 700, color: C.text }}>
          {evidence.val_b !== null && evidence.val_b !== undefined
            ? `${typeof evidence.val_b === 'number' ? evidence.val_b.toFixed(evidence.val_b < 10 ? 2 : 1) : evidence.val_b}${unit}`
            : "—"}
        </div>
      </div>
    </div>

    {/* Metrics Row */}
    <div style={{
      display: "flex", gap: 16, flexWrap: "wrap",
      fontSize: 11, color: C.dim,
    }}>
      <div>
        <span style={{ color: C.muted }}>Δ Delta: </span>
        <span style={{ color: C.red, fontWeight: 600 }}>
          {evidence.delta !== null && evidence.delta !== undefined
            ? `${evidence.delta.toFixed(evidence.delta < 1 ? 3 : 1)}${unit}`
            : "—"}
        </span>
      </div>
      <div>
        <span style={{ color: C.muted }}>ε Threshold: </span>
        <span style={{ fontWeight: 600 }}>
          {evidence.eps}{unit}
        </span>
      </div>
      <div>
        <span style={{ color: C.muted }}>Duration: </span>
        <span style={{ color: C.orange, fontWeight: 600 }}>
          {evidence.duration_ticks} tick{evidence.duration_ticks !== 1 ? "s" : ""}
        </span>
      </div>
      {formatTimeRange() && <div>
        <span style={{ color: C.muted }}>Window: </span>
        <span style={{ fontFamily: "monospace", fontSize: 10 }}>
          {formatTimeRange()}
        </span>
      </div>}
    </div>
  </div>;
}

/* ── Aggregated Fault Card ── */
function AggregatedFaultCard({ fault, expanded, onToggle, faultEvidence }) {
  const [showEvidence, setShowEvidence] = useState(false);

  // Use the centralized fault type styling
  const style = getFaultTypeStyle(fault.fault_type);

  // Get evidence for this fault's primary fault
  const primaryEvidence = fault.primary_fault?.evidence || faultEvidence?.[fault.primary_fault?.name];

  return <div style={{
    marginBottom: 10, borderRadius: 12, overflow: "hidden",
    background: style.bgColor, border: `1px solid ${style.borderColor}`,
    opacity: style.opacity,
  }}>
    <div style={{
      display: "flex", alignItems: "center", gap: 12, padding: "14px 16px",
      cursor: fault.cascade_count > 0 ? "pointer" : "default",
    }} onClick={() => fault.cascade_count > 0 && onToggle()}>
      <div style={{
        width: 10, height: 10, borderRadius: "50%",
        backgroundColor: style.dotColor,
        boxShadow: `0 0 10px ${style.dotColor}60`,
      }} />
      <div style={{ flex: 1 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontSize: 14, fontWeight: 700 }}>{fault.subsystem_name}</span>
          <span style={{
            fontSize: 9, padding: "2px 6px", borderRadius: 4,
            background: style.tagBg, color: style.tagText, fontWeight: 700, letterSpacing: 0.5, textTransform: "uppercase",
          }}>
            {style.label}
          </span>
          {fault.cascade_count > 0 && <span style={{
            fontSize: 10, color: C.dim, marginLeft: 4,
          }}>
            +{fault.cascade_count} related {fault.cascade_count === 1 ? "fault" : "faults"}
          </span>}
        </div>
        <div style={{ fontSize: 13, color: C.text, marginTop: 6 }}>
          {formatDiagnosis(fault.message, fault.primary_fault?.name || fault.subsystem, fault.primary_fault?.unit)}
        </div>
        {/* Why? link */}
        {primaryEvidence && <div
          onClick={(e) => { e.stopPropagation(); setShowEvidence(!showEvidence); }}
          style={{
            fontSize: 11, color: C.blue, marginTop: 6, cursor: "pointer",
            display: "inline-flex", alignItems: "center", gap: 4,
          }}
        >
          <span style={{ textDecoration: "underline" }}>Why?</span>
          <span style={{ fontSize: 10, transform: showEvidence ? "rotate(180deg)" : "rotate(0deg)", transition: "transform 0.2s" }}>▼</span>
        </div>}
      </div>
      {fault.cascade_count > 0 && <span style={{ fontSize: 14, color: C.muted, transform: expanded ? "rotate(180deg)" : "rotate(0deg)", transition: "transform 0.2s" }}>
        v
      </span>}
    </div>

    {/* Evidence panel */}
    {showEvidence && primaryEvidence && <FaultEvidencePanel evidence={primaryEvidence} fault={fault} />}

    {/* Expanded cascade details */}
    {expanded && fault.cascades && fault.cascades.length > 0 && <div style={{
      padding: "0 16px 14px 42px", background: "rgba(0,0,0,0.15)",
    }}>
      <div style={{ fontSize: 9, color: C.muted, letterSpacing: 1, marginBottom: 6, textTransform: "uppercase" }}>
        Related Faults
      </div>
      {fault.cascades.map((c, i) => {
        // Cascades use softer cascade styling
        const cascadeStyle = FAULT_TYPE_STYLES.cascade;
        return (
          <div key={i} style={{
            display: "flex", alignItems: "center", gap: 8, padding: "6px 0",
            borderBottom: i < fault.cascades.length - 1 ? `1px solid ${C.border}` : "none",
            opacity: cascadeStyle.opacity,
          }}>
            <div style={{
              width: 6, height: 6, borderRadius: "50%",
              backgroundColor: cascadeStyle.dotColor,
              boxShadow: `0 0 6px ${cascadeStyle.dotColor}40`,
            }} />
            <span style={{ fontSize: 12, color: C.text }}>{c.name || c.pair}</span>
            <span style={{ fontSize: 11, color: C.dim, marginLeft: "auto" }}>{formatDiagnosis(c.human_message || c.diagnosis, c.name || c.pair, c.unit)}</span>
          </div>
        );
      })}
    </div>}
  </div>;
}

/* ── Analysis Results ── */
function AnalysisResults({ result }) {
  const [expandedFaults, setExpandedFaults] = useState({});

  const toggleExpand = (index) => {
    setExpandedFaults(prev => ({ ...prev, [index]: !prev[index] }));
  };

  if (!result) return null;

  const aggregated = result.aggregated_faults || {};
  const hasAggregated = aggregated.subsystem_faults && aggregated.subsystem_faults.length > 0;

  // Format fault window display
  const formatFaultWindow = () => {
    if (!result.fault_presence) return null;

    // Check if we have real timestamps (not just tick indices)
    const hasRealTime = result.first_fault_time !== result.first_fault_tick;

    if (hasRealTime && result.first_fault_time && result.last_fault_time) {
      // Format as readable timestamps
      const formatTime = (ts) => {
        const d = new Date(ts * 1000);
        return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
      };
      return `${formatTime(result.first_fault_time)} → ${formatTime(result.last_fault_time)}`;
    }

    // Fallback to tick indices
    return `tick ${result.first_fault_tick} → tick ${result.last_fault_tick}`;
  };

  return <Card style={{ marginTop: 16 }}>
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 16 }}>
      <span style={{ fontSize: 14, fontWeight: 700 }}>Analysis: {result.filename}</span>
      <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 3 }}>
        <div style={{ fontSize: 12, color: C.dim }}>
          <span style={{ color: C.muted }}>Ticks analyzed:</span> {result.total_ticks}
        </div>
        <div style={{ fontSize: 12 }}>
          <span style={{ color: C.muted }}>Fault presence:</span>{" "}
          <span style={{ color: result.fault_presence ? C.red : C.green, fontWeight: 600 }}>
            {result.fault_presence ? "Yes" : "No"}
          </span>
        </div>
        {result.fault_presence && <>
          <div style={{ fontSize: 12 }}>
            <span style={{ color: C.muted }}>Active fault time:</span>{" "}
            <span style={{ color: C.orange, fontWeight: 600 }}>{result.active_fault_pct}%</span>
          </div>
          <div style={{ fontSize: 11, color: C.dim, fontFamily: "monospace" }}>
            <span style={{ color: C.muted }}>Fault window:</span> {formatFaultWindow()}
          </div>
        </>}
      </div>
    </div>

    {/* Aggregated Faults Summary */}
    {hasAggregated && <div style={{
      display: "flex", gap: 12, marginBottom: 16, padding: "12px 16px",
      borderRadius: 10, background: "rgba(255,255,255,0.02)", border: `1px solid ${C.border}`,
    }}>
      <div style={{ textAlign: "center" }}>
        <div style={{ fontSize: 20, fontWeight: 800, color: C.red }}>{aggregated.root_causes || 0}</div>
        <div style={{ fontSize: 9, color: C.muted, letterSpacing: 1, textTransform: "uppercase" }}>Root Causes</div>
      </div>
      <div style={{ width: 1, background: C.border }} />
      <div style={{ textAlign: "center" }}>
        <div style={{ fontSize: 20, fontWeight: 800, color: C.orange }}>{aggregated.cascades || 0}</div>
        <div style={{ fontSize: 9, color: C.muted, letterSpacing: 1, textTransform: "uppercase" }}>Cascades</div>
      </div>
      <div style={{ width: 1, background: C.border }} />
      <div style={{ textAlign: "center" }}>
        <div style={{ fontSize: 20, fontWeight: 800, color: C.amber }}>{aggregated.independent || 0}</div>
        <div style={{ fontSize: 9, color: C.muted, letterSpacing: 1, textTransform: "uppercase" }}>Independent</div>
      </div>
      <div style={{ width: 1, background: C.border }} />
      <div style={{ textAlign: "center" }}>
        <div style={{ fontSize: 20, fontWeight: 800, color: C.text }}>{aggregated.total_faults || 0}</div>
        <div style={{ fontSize: 9, color: C.muted, letterSpacing: 1, textTransform: "uppercase" }}>Total Points</div>
      </div>
    </div>}

    {/* Hierarchical Fault Cards */}
    {hasAggregated && <>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
        <div style={{ fontSize: 10, color: C.red, letterSpacing: 2, textTransform: "uppercase", fontWeight: 700 }}>
          Subsystem Faults
        </div>
      </div>
      {aggregated.subsystem_faults.map((f, i) => {
        // Build fault evidence lookup from result
        const faultEvidenceLookup = {};
        if (result.fault_evidence) {
          result.fault_evidence.forEach(e => {
            faultEvidenceLookup[e.pair] = e;
          });
        }
        return (
          <AggregatedFaultCard
            key={i}
            fault={f}
            expanded={!!expandedFaults[i]}
            onToggle={() => toggleExpand(i)}
            faultEvidence={faultEvidenceLookup}
          />
        );
      })}
    </>}

    {/* Pairs summary */}
    <div style={{ fontSize: 10, color: C.muted, letterSpacing: 2, textTransform: "uppercase", marginTop: hasAggregated ? 20 : 0, marginBottom: 8 }}>
      Sensor Pair Details
    </div>
    <div style={{ display: "flex", flexWrap: "wrap", gap: 10, marginBottom: 16 }}>
      {(result.pairs_summary || []).map((p, i) => (
        <div key={i} style={{
          padding: "10px 14px", borderRadius: 10, flex: "1 1 180px",
          background: p.status === "FAULT" ? "rgba(255,59,48,0.06)" : "rgba(48,209,88,0.04)",
          border: `1px solid ${p.status === "FAULT" ? "rgba(255,59,48,0.15)" : "rgba(48,209,88,0.12)"}`,
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span style={{ fontSize: 13, fontWeight: 600 }}>{p.name}</span>
            <Dot status={p.status} size={8} />
          </div>
          <div style={{ fontSize: 11, color: C.dim, marginTop: 4 }}>
            {p.fault_count} faults / {p.ok_count + p.fault_count} ticks ({p.fault_rate}%)
          </div>
        </div>
      ))}
    </div>

    {/* Individual Faults (collapsed by default if aggregated view exists) */}
    {result.faults && result.faults.length > 0 && !hasAggregated && <>
      <div style={{ fontSize: 10, color: C.red, letterSpacing: 2, textTransform: "uppercase", marginBottom: 8, fontWeight: 700 }}>
        Faults Detected
      </div>
      {result.faults.map((f, i) => (
        <div key={i} style={{
          display: "flex", gap: 10, alignItems: "center", padding: "10px 14px", marginBottom: 4,
          borderRadius: 8, background: "rgba(255,59,48,0.04)", border: "1px solid rgba(255,59,48,0.1)",
        }}>
          <Dot status={f.severity} size={8} />
          <div style={{ flex: 1 }}>
            <span style={{ fontSize: 13, fontWeight: 600, color: C.text }}>{f.pair}</span>
            <span style={{ fontSize: 11, color: C.muted, marginLeft: 8 }}>{f.group}</span>
          </div>
          <span style={{ fontSize: 12, color: C.red }}>{formatDiagnosis(f.diagnosis, f.pair)}</span>
        </div>
      ))}
    </>}

    {/* Latest ticks timeline */}
    {result.latest_ticks && result.latest_ticks.length > 0 && <>
      <div style={{ fontSize: 10, color: C.muted, letterSpacing: 2, textTransform: "uppercase", marginTop: 16, marginBottom: 8 }}>
        Latest Readings
      </div>
      <div style={{ maxHeight: 300, overflowY: "auto" }}>
        {result.latest_ticks.map((tick, ti) => (
          <div key={ti} style={{
            display: "flex", gap: 8, alignItems: "center", padding: "6px 10px",
            borderBottom: `1px solid ${C.border}`,
          }}>
            <span style={{ fontSize: 11, color: C.muted, fontFamily: "monospace", width: 30 }}>#{tick.timestamp.toFixed(0)}</span>
            <Dot status={tick.system_status} size={6} />
            <div style={{ display: "flex", gap: 12, flex: 1 }}>
              {tick.pairs.map((p, pi) => {
                const unit = p.unit || detectUnit(p.name);
                const formatVal = (v) => {
                  if (v === null || v === undefined) return "—";
                  return typeof v === "number" ? v.toFixed(1) : v;
                };
                return (
                  <div key={pi} style={{ display: "flex", alignItems: "center", gap: 4 }}>
                    <Dot status={p.status} size={5} />
                    <span style={{ fontSize: 10, color: p.status === "FAULT" ? C.red : C.dim }}>
                      {p.name.split(" ").slice(-1)}: {p.val_a !== null ? (
                        p.pair_type === "cmd_pos"
                          ? `${formatVal(p.val_a)}→${formatVal(p.val_b)}%`
                          : `${formatVal(p.val_a)}/${formatVal(p.val_b)}${unit}`
                      ) : "—"}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </>}
  </Card>;
}

/* ── Main App ── */
export default function SensorGuardApp() {
  const [user, setUser] = useState(null);
  const [buildings, setBuildings] = useState([]);
  const [selectedId, setSelectedId] = useState(null);
  const [building, setBuilding] = useState(null);
  const [analysisResult, setAnalysisResult] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [msg, setMsg] = useState("");
  const [deleteTarget, setDeleteTarget] = useState(null); // { id, name } or null
  const [deleting, setDeleting] = useState(false);
  const [reportStatus, setReportStatus] = useState(""); // "", "generating", "done", "error"
  const [reportError, setReportError] = useState("");

  const loadBuildings = useCallback(async () => {
    try {
      const list = await api("/api/buildings");
      setBuildings(list);
    } catch (e) { console.error(e); }
  }, []);

  const loadBuilding = useCallback(async (id) => {
    try {
      const b = await api(`/api/buildings/${id}`);
      setBuilding(b);
    } catch (e) { console.error(e); }
  }, []);

  useEffect(() => { if (user) loadBuildings(); }, [user, loadBuildings]);

  useEffect(() => {
    // always clear previous building state on change
    setBuilding(null);
    setAnalysisResult(null);
    setMsg("");

    if (!selectedId) return;

    const controller = new AbortController();

    api(`/api/buildings/${selectedId}`, { signal: controller.signal })
      .then((b) => setBuilding(b))
      .catch((e) => {
        if (e.name !== "AbortError") console.error(e);
      });

    return () => controller.abort();
  }, [selectedId]);

  const createBuilding = async (name) => {
    await api("/api/buildings", { method: "POST", body: JSON.stringify({ name }) });
    await loadBuildings();
  };

  const deleteBuilding = async () => {
    if (!deleteTarget) return;
    setDeleting(true);
    try {
      await api(`/api/buildings/${deleteTarget.id}`, { method: "DELETE" });
      if (selectedId === deleteTarget.id) {
        setSelectedId(null);
        setBuilding(null);
        setAnalysisResult(null);
      }
      await loadBuildings();
      setMsg("Building deleted.");
    } catch (e) { setMsg("Error: " + e.message); }
    setDeleting(false);
    setDeleteTarget(null);
  };

  const saveConfig = async (pairs, instanceCol) => {
    await api(`/api/buildings/${selectedId}/config`, {
      method: "PUT",
      body: JSON.stringify({ pairs, instance_col: instanceCol || null })
    });
    setMsg("Configuration saved!"); setTimeout(() => setMsg(""), 2000);
    await loadBuilding(selectedId);
  };

  const generateReport = async () => {
    if (!selectedId) return;
    setReportStatus("generating");
    setReportError("");
    try {
      const now = new Date();
      const end = now.toISOString().split("T")[0] + "T23:59:59";
      const start = new Date(now.getTime() - 90 * 86400000).toISOString().split("T")[0] + "T00:00:00";
      // Pass analysis_id if we have a recent analysis result
      const body = { period_start: start, period_end: end };
      if (analysisResult && analysisResult.analysis_id) {
        body.analysis_id = analysisResult.analysis_id;
      }
      const { report_id } = await api(`/api/buildings/${selectedId}/reports`, {
        method: "POST",
        body: JSON.stringify(body),
      });
      // Poll until ready
      while (true) {
        await new Promise(r => setTimeout(r, 1500));
        const meta = await api(`/api/reports/${report_id}`);
        if (meta.status === "completed") {
          // Download the PDF
          const h = {};
          if (store.token) h["Authorization"] = `Bearer ${store.token}`;
          const res = await fetch(`${API}/api/reports/${report_id}/download`, { headers: h });
          if (!res.ok) throw new Error("Download failed");
          const blob = await res.blob();
          const url = URL.createObjectURL(blob);
          const a = document.createElement("a");
          a.href = url;
          a.download = `sensorguard-report-${report_id.slice(0, 8)}.pdf`;
          a.click();
          URL.revokeObjectURL(url);
          setReportStatus("done");
          setTimeout(() => setReportStatus(""), 3000);
          break;
        } else if (meta.status === "failed") {
          throw new Error(meta.error_message || "Report generation failed");
        }
      }
    } catch (e) {
      setReportStatus("error");
      setReportError(e.message);
      setTimeout(() => { setReportStatus(""); setReportError(""); }, 5000);
    }
  };

  const uploadAndAnalyze = async (file) => {
    setUploading(true); setAnalysisResult(null);
    try {
      const form = new FormData();
      form.append("file", file);
      const { job_id } = await api(`/api/buildings/${selectedId}/analyze`, { method: "POST", body: form });

      // Poll until complete or failed
      while (true) {
        await new Promise(r => setTimeout(r, 2000));
        const job = await api(`/api/jobs/${job_id}`);
        if (job.status === "complete") {
          setAnalysisResult(job.result);
          localStorage.setItem(`sg_config_collapsed_${selectedId}`, "true");
          await loadBuildings();
          await loadBuilding(selectedId);
          break;
        } else if (job.status === "failed") {
          throw new Error(job.error || "Analysis failed");
        }
        // else queued/running — keep polling
      }
    } catch (e) { setMsg("Error: " + e.message); }
    setUploading(false);
  };

  if (!user) return <div style={{ background: C.bg, minHeight: "100vh", fontFamily: "SF Pro Display, -apple-system, sans-serif", color: C.text }}>
    <AuthScreen onAuth={setUser} />
  </div>;

  return <div style={{ background: C.bg, minHeight: "100vh", fontFamily: "SF Pro Display, -apple-system, sans-serif", color: C.text }}>
    <style>{`
      @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&display=swap');
      * { box-sizing: border-box; margin: 0; padding: 0; }
      ::-webkit-scrollbar { width: 4px; }
      ::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.1); border-radius: 2px; }
      select { appearance: none; cursor: pointer; }
    `}</style>

    {/* Header */}
    <div style={{
      display: "flex", justifyContent: "space-between", alignItems: "center",
      padding: "14px 24px", borderBottom: `1px solid ${C.border}`, background: "rgba(255,255,255,0.02)",
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <div style={{ width: 28, height: 28, borderRadius: 7, background: `linear-gradient(135deg, ${C.green}, ${C.blue})`,
          display: "flex", alignItems: "center", justifyContent: "center", fontSize: 12, fontWeight: 800, color: "#fff" }}>S</div>
        <span style={{ fontSize: 15, fontWeight: 700, letterSpacing: -0.3 }}>SensorGuard</span>
        <span style={{ fontSize: 10, color: C.muted, letterSpacing: 1.5, textTransform: "uppercase", marginLeft: 4 }}>Dashboard</span>
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        {msg && <span style={{ fontSize: 12, color: C.green, fontWeight: 600 }}>{msg}</span>}
        <span style={{ fontSize: 12, color: C.dim }}>{user.email}</span>
        <span style={{ fontSize: 12, color: C.muted, cursor: "pointer" }} onClick={() => { store.token = null; setUser(null); }}>Logout</span>
      </div>
    </div>

    <div style={{ display: "flex", height: "calc(100vh - 53px)" }}>
      {/* Sidebar */}
      <div style={{ width: 260, flexShrink: 0, padding: 16, borderRight: `1px solid ${C.border}`, overflowY: "auto" }}>
        {/* Stats */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 16 }}>
          <Card style={{ padding: 12, textAlign: "center" }}>
            <div style={{ fontSize: 24, fontWeight: 800, color: C.green }}>{buildings.filter(b => b.status === "healthy").length}</div>
            <div style={{ fontSize: 9, color: C.muted, letterSpacing: 1, textTransform: "uppercase" }}>Healthy</div>
          </Card>
          <Card style={{ padding: 12, textAlign: "center" }}>
            <div style={{ fontSize: 24, fontWeight: 800, color: C.red }}>{buildings.filter(b => b.status === "fault").length}</div>
            <div style={{ fontSize: 9, color: C.muted, letterSpacing: 1, textTransform: "uppercase" }}>Faulted</div>
          </Card>
        </div>
        <BuildingList
          buildings={buildings}
          selected={selectedId}
          onSelect={(id) => {
            setSelectedId(id);
            setBuilding(null);
            setAnalysisResult(null);
            setMsg("");
          }}
          onCreate={createBuilding}
          onDelete={(b) => setDeleteTarget({ id: b.id, name: b.name })}
        />
      </div>

      {/* Main content */}
      <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
        {!selectedId ? (
          <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100%", color: C.muted }}>
            <div style={{ textAlign: "center" }}>
              <div style={{ fontSize: 48, marginBottom: 16 }}>🏢</div>
              <div style={{ fontSize: 16 }}>Select a building or create one to start</div>
            </div>
          </div>
        ) : building ? (<>
          {/* Building header */}
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 20 }}>
            <div>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <h2 style={{ fontSize: 22, fontWeight: 700, letterSpacing: -0.5, margin: 0 }}>{building.name}</h2>
                <Dot status={building.active_faults?.length > 0 ? "fault" : "ok"} size={10} />
              </div>
              <div style={{ fontSize: 12, color: C.dim, marginTop: 4 }}>
                {building.sensor_config?.length || 0} pairs configured · {building.active_faults?.length || 0} active faults
              </div>
            </div>
            <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
              <span onClick={() => building && setDeleteTarget({ id: building.id, name: building.name })} style={{
                padding: "10px 16px", borderRadius: 10, fontSize: 13, fontWeight: 600,
                background: "rgba(255,59,48,0.08)", color: C.red, border: "1px solid rgba(255,59,48,0.15)",
                cursor: building ? "pointer" : "not-allowed",
                opacity: building ? 1 : 0.4,
              }}>Delete ({building.name})</span>
              <span onClick={reportStatus === "generating" ? undefined : generateReport} style={{
                padding: "10px 16px", borderRadius: 10, fontSize: 13, fontWeight: 600,
                background: reportStatus === "done" ? "rgba(48,209,88,0.12)" : "rgba(191,90,242,0.1)",
                color: reportStatus === "done" ? C.green : reportStatus === "error" ? C.red : C.purple,
                border: `1px solid ${reportStatus === "done" ? "rgba(48,209,88,0.25)" : "rgba(191,90,242,0.2)"}`,
                cursor: reportStatus === "generating" ? "wait" : "pointer",
                opacity: reportStatus === "generating" ? 0.6 : 1,
              }}>
                {reportStatus === "generating" ? "Generating PDF..." : reportStatus === "done" ? "Report Downloaded" : reportStatus === "error" ? "Export Failed" : "Export PDF Report"}
              </span>
              {reportError && <span style={{ fontSize: 11, color: C.red, maxWidth: 160 }}>{reportError}</span>}
              <label style={{
                padding: "10px 20px", borderRadius: 10, cursor: uploading ? "wait" : "pointer",
                background: `linear-gradient(135deg, ${C.green}, ${C.blue})`, color: "#fff",
                fontSize: 13, fontWeight: 700, opacity: uploading ? 0.5 : 1,
              }}>
                {uploading ? "Analyzing..." : "Upload CSV & Analyze"}
                <input type="file" accept=".csv" hidden disabled={uploading || !(building.sensor_config?.length > 0)}
                  onChange={e => e.target.files[0] && uploadAndAnalyze(e.target.files[0])} />
            </label>
            </div>
          </div>

          {/* Active faults */}
          {building.active_faults?.length > 0 && <Card style={{
            marginBottom: 16, background: "rgba(255,59,48,0.04)", border: "1px solid rgba(255,59,48,0.12)",
          }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: C.red, letterSpacing: 1.5, textTransform: "uppercase", marginBottom: 10 }}>
              Active Faults
            </div>
            {building.active_faults.map(f => (
              <div key={f.id} style={{ display: "flex", gap: 10, alignItems: "center", padding: "8px 0",
                borderBottom: `1px solid rgba(255,59,48,0.06)` }}>
                <Dot status={f.severity} size={7} />
                <span style={{ fontSize: 13, fontWeight: 600, flex: 1 }}>{f.pair_name}</span>
                <span style={{ fontSize: 12, color: C.red }}>{formatDiagnosis(f.diagnosis, f.pair_name)}</span>
              </div>
            ))}
          </Card>}

          {/* Config editor */}
          <ConfigEditor building={building} onSave={saveConfig} />

          {/* Analysis results */}
          <AnalysisResults result={analysisResult} />
        </>) : <div style={{ color: C.dim }}>Loading...</div>}
      </div>
    </div>

    {deleteTarget && <ConfirmModal
      title={`Delete building '${deleteTarget.name}'?`}
      message="This will delete its history and alerts. This cannot be undone."
      confirming={deleting}
      onCancel={() => setDeleteTarget(null)}
      onConfirm={deleteBuilding}
    />}
  </div>;
}
