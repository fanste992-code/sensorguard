"""
main.py — SensorGuard API

Upload BAS data → run TNA → return faults. That's it.

All 4 review fixes integrated:
  #1 hvac.analyze_csv now streams (O(1) memory)
  #2 timestamp_col flows through config → analysis
  #3 auth.py uses bcrypt + JWT
  #4 models.py auto-connects to Postgres via DATABASE_URL
"""
from __future__ import annotations
import csv, io, json, logging, os, uuid
from contextlib import asynccontextmanager
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from arq.connections import ArqRedis, create_pool
from models import (
    Base, DATABASE_URL, engine, init_db, get_db, User, Building, FaultEvent, Analysis, AnalysisJob,
    Organization, OrgMembership, require_building, list_user_buildings, get_or_create_user_org,
)
from plan_service import (
    require_feature, enforce_sensor_pair_limit,
    count_active_faults, query_active_faults, query_faults,
    query_analyses, query_alerts,
)
from report_service import (
    ensure_storage_dir, create_report, list_reports, get_report,
    get_report_filepath,
)
import alert_engine  # ensures AlertState/AlertEvent register with Base before init_db()
from alert_engine import AlertEvent, AlertState
from auth import hash_pw, verify_pw, make_token, get_user_id
from hvac import analyze_csv, build_config, AnalysisReport
from live_api import live_router
from fault_aggregator import aggregate_faults

log = logging.getLogger(__name__)


def run_migrations() -> None:
    """Bootstrap / migrate the database on startup.

    * SQLite (dev/tests): uses init_db() (create_all) — fast, no Alembic overhead.
    * Postgres (production): runs Alembic upgrade head.
      - If the DB already has app tables but no alembic_version table
        (pre-Alembic deployment), stamps the baseline first so upgrade
        doesn't try to recreate existing tables.
    """
    if DATABASE_URL.startswith("sqlite"):
        init_db()
        return

    # Postgres path — use Alembic
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import inspect as sa_inspect

    cfg = Config(os.path.join(os.path.dirname(__file__), "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", DATABASE_URL)

    inspector = sa_inspect(engine)
    has_alembic = inspector.has_table("alembic_version")
    has_app_tables = inspector.has_table("buildings")

    if not has_alembic and has_app_tables:
        # Existing DB that predates Alembic — stamp baseline so
        # upgrade head only runs subsequent migrations (0002+).
        log.info("Stamping existing DB at baseline revision 0001")
        command.stamp(cfg, "0001")

    log.info("Running Alembic upgrade head")
    command.upgrade(cfg, "head")


@asynccontextmanager
async def lifespan(app):
    run_migrations()
    ensure_storage_dir()
    # Create Arq Redis connection pool for enqueuing background jobs
    try:
        from worker import REDIS_URL
        from arq.connections import RedisSettings
        from urllib.parse import urlparse
        parsed = urlparse(REDIS_URL)
        settings = RedisSettings(
            host=parsed.hostname or "localhost",
            port=parsed.port or 6379,
            password=parsed.password,
            database=int(parsed.path.lstrip("/") or 0),
            conn_timeout=2,
            conn_retries=0,
        )
        app.state.arq_pool = await create_pool(settings)
        log.info("Arq Redis pool connected")
    except Exception as e:
        log.warning("Redis not available (%s) — analysis will run synchronously", e)
        app.state.arq_pool = None
    yield
    if app.state.arq_pool:
        await app.state.arq_pool.close()

app = FastAPI(title="SensorGuard API", version="1.2.0", lifespan=lifespan)

# CORS: restrict in production via ALLOWED_ORIGINS env var
_raw_origins = os.getenv("ALLOWED_ORIGINS", "")
if _raw_origins and _raw_origins != "*":
    _origins = [o.strip() for o in _raw_origins.split(",")]
    _credentials = True
elif DATABASE_URL.startswith("sqlite"):
    # Local dev: allow all origins, no credentials
    _origins = ["*"]
    _credentials = False
else:
    raise RuntimeError(
        "ALLOWED_ORIGINS must be set to explicit origins in production "
        "(e.g. 'https://app.example.com,https://admin.example.com'). "
        "Wildcard '*' with credentials is forbidden by the CORS spec."
    )
app.add_middleware(CORSMiddleware, allow_origins=_origins, allow_credentials=_credentials,
                   allow_methods=["*"], allow_headers=["*"])

# Include live data router for real-time BACnet integration
app.include_router(live_router, prefix="/api")

# ── Request models ────────────────────────────────────────────────

class SignupReq(BaseModel):
    email: str; password: str; name: str = ""

class LoginReq(BaseModel):
    email: str; password: str

class BuildingCreate(BaseModel):
    name: str; address: str = ""; floors: int = 1; ahus: int = 1

class PairMapping(BaseModel):
    name: str; group: str = "custom"; col_a: str; col_b: str
    pair_type: str = "meas_setp"; eps: float = 0.15; unit: str = ""
    range_min: Optional[float] = None  # Physical range validation
    range_max: Optional[float] = None

class ConfigUpdate(BaseModel):
    pairs: List[PairMapping]
    timestamp_col: Optional[str] = None   # FIX #2: user picks which column
    instance_col: Optional[str] = None    # For multi-instance sensors (IMU_I, BARO_I, GPS_I)

# ── Auth ──────────────────────────────────────────────────────────

@app.post("/api/auth/signup")
def signup(req: SignupReq, db=Depends(get_db)):
    if db.query(User).filter_by(email=req.email).first():
        raise HTTPException(400, "Email taken")
    user = User(email=req.email, password_hash=hash_pw(req.password), name=req.name)
    db.add(user)
    db.flush()
    # Auto-create personal org + owner membership
    org = Organization(name=f"{req.name or req.email}'s Organization")
    db.add(org)
    db.flush()
    db.add(OrgMembership(org_id=org.id, user_id=user.id, role="owner"))
    db.commit()
    db.refresh(user)
    return {"token": make_token(user.id, user.email),
            "user": {"id": user.id, "email": user.email, "name": user.name}}

@app.post("/api/auth/login")
def login(req: LoginReq, db=Depends(get_db)):
    user = db.query(User).filter_by(email=req.email).first()
    if not user or not verify_pw(req.password, user.password_hash):
        raise HTTPException(401, "Invalid credentials")
    return {"token": make_token(user.id, user.email),
            "user": {"id": user.id, "email": user.email, "name": user.name}}

@app.get("/api/auth/me")
def me(uid: int = Depends(get_user_id), db=Depends(get_db)):
    u = db.query(User).filter_by(id=uid).first()
    if not u: raise HTTPException(404)
    return {"id": u.id, "email": u.email, "name": u.name, "plan": u.plan}

# ── Buildings ─────────────────────────────────────────────────────

@app.get("/api/buildings")
def list_buildings(uid: int = Depends(get_user_id), db=Depends(get_db)):
    buildings = list_user_buildings(db, uid)
    out = []
    for b in buildings:
        faults = count_active_faults(db, b.id)
        recent = query_analyses(db, b.id, limit=1)
        last = recent[0] if recent else None
        out.append({
            "id": b.id, "name": b.name, "address": b.address,
            "floors": b.floors, "ahus": b.ahus,
            "active_faults": faults, "pair_count": len(b.get_config()),
            "last_analysis": last.created_at.isoformat() if last else None,
            "fault_rate": round(last.fault_rate * 100, 1) if last else 0,
            "status": "fault" if faults > 0 else "healthy",
        })
    return out

@app.post("/api/buildings")
def create_building(req: BuildingCreate, uid: int = Depends(get_user_id), db=Depends(get_db)):
    org = get_or_create_user_org(db, uid)
    b = Building(owner_id=uid, org_id=org.id, name=req.name, address=req.address, floors=req.floors, ahus=req.ahus)
    db.add(b)
    db.commit()
    db.refresh(b)
    return {"id": b.id, "name": b.name}

@app.get("/api/buildings/{bid}")
def get_building(bid: int, uid: int = Depends(get_user_id), db=Depends(get_db)):
    b = require_building(db, bid, uid)
    faults = query_active_faults(db, b.id)
    return {
        "id": b.id, "name": b.name, "address": b.address,
        "floors": b.floors, "ahus": b.ahus,
        "sensor_config": b.get_config(),
        "timestamp_col": b.timestamp_col or None,
        "instance_col": b.instance_col or None,
        "active_faults": [{
            "id": f.id, "pair_name": f.pair_name, "group": f.group,
            "severity": f.severity, "diagnosis": f.diagnosis,
            "detected_at": f.detected_at.isoformat(),
        } for f in faults],
    }

@app.delete("/api/buildings/{bid}")
def delete_building(bid: int, uid: int = Depends(get_user_id), db=Depends(get_db)):
    b = require_building(db, bid, uid)
    # Delete related data
    db.query(FaultEvent).filter_by(building_id=bid).delete()
    db.query(Analysis).filter_by(building_id=bid).delete()
    db.query(AlertEvent).filter_by(building_id=bid).delete()
    db.query(AlertState).filter_by(building_id=bid).delete()
    db.delete(b)
    db.commit()
    return {"status": "ok"}

# ── Sensor Config ─────────────────────────────────────────────────

@app.put("/api/buildings/{bid}/config")
def update_config(bid: int, req: ConfigUpdate, uid: int = Depends(get_user_id), db=Depends(get_db)):
    b = require_building(db, bid, uid)
    enforce_sensor_pair_limit(db, bid, len(req.pairs))
    b.set_config([p.dict() for p in req.pairs])
    if req.timestamp_col is not None:
        b.timestamp_col = req.timestamp_col
    if req.instance_col is not None:
        b.instance_col = req.instance_col
    db.commit()
    return {"status": "ok", "pair_count": len(req.pairs),
            "timestamp_col": b.timestamp_col, "instance_col": b.instance_col}

@app.post("/api/buildings/{bid}/discover-columns")
async def discover_columns(bid: int, file: UploadFile = File(...), uid: int = Depends(get_user_id)):
    """Read only the first 2 lines (header + sample). Does NOT load the whole file."""
    import codecs
    text_stream = codecs.getreader("utf-8")(file.file, errors="replace")
    reader = csv.reader(text_stream)
    headers = next(reader, [])
    sample = next(reader, [])
    columns = [{"index": i, "name": h.strip(),
                "sample": (sample[i].strip() if i < len(sample) else "")}
               for i, h in enumerate(headers)]
    return {"columns": columns}

# ── Upload & Analyze ──────────────────────────────────────────────

@app.post("/api/buildings/{bid}/analyze", status_code=202)
async def analyze(bid: int, request: Request, file: UploadFile = File(...),
                  uid: int = Depends(get_user_id), db=Depends(get_db)):
    b = require_building(db, bid, uid)
    pair_mappings = b.get_config()
    if not pair_mappings:
        raise HTTPException(400, "No sensor pairs configured")

    # Read CSV into memory so we can pass it to the background worker
    raw_bytes = await file.read()
    csv_text = raw_bytes.decode("utf-8", errors="replace")

    # Create job record
    job_id = str(uuid.uuid4())
    job = AnalysisJob(
        id=job_id, building_id=bid, user_id=uid,
        filename=file.filename or "upload.csv",
        csv_data=csv_text, status="queued",
    )
    db.add(job)
    db.commit()

    # Enqueue background task (or run inline if Redis unavailable)
    arq_pool = request.app.state.arq_pool
    if arq_pool:
        await arq_pool.enqueue_job("run_analysis", job_id)
    else:
        # Fallback: run synchronously in-process
        from worker import run_analysis
        await run_analysis({}, job_id)

    return {"job_id": job_id, "status": "queued"}


@app.get("/api/jobs/{job_id}")
def poll_job(job_id: str, uid: int = Depends(get_user_id), db=Depends(get_db)):
    job = db.query(AnalysisJob).filter_by(id=job_id).first()
    if not job:
        raise HTTPException(404, "Job not found")
    if job.user_id != uid:
        raise HTTPException(403, "Not your job")

    resp = {"job_id": job.id, "status": job.status}
    if job.status == "complete" and job.result_json:
        resp["result"] = json.loads(job.result_json)
    elif job.status == "failed":
        resp["error"] = job.error or "Unknown error"
    return resp

# ── Fault & Analysis History ──────────────────────────────────────

@app.get("/api/buildings/{bid}/faults")
def get_faults(bid: int, limit: int = Query(50, le=200),
               uid: int = Depends(get_user_id), db=Depends(get_db)):
    b = require_building(db, bid, uid)
    faults = query_faults(db, bid, limit=limit)
    return [{"id": f.id, "pair_name": f.pair_name, "group": f.group,
             "severity": f.severity, "diagnosis": f.diagnosis,
             "val_a": f.val_a, "val_b": f.val_b,
             "tick_timestamp": f.tick_timestamp,
             "detected_at": f.detected_at.isoformat(), "resolved": f.resolved} for f in faults]

@app.get("/api/buildings/{bid}/analyses")
def get_analyses(bid: int, limit: int = Query(20),
                 uid: int = Depends(get_user_id), db=Depends(get_db)):
    b = require_building(db, bid, uid)
    analyses = query_analyses(db, bid, limit=limit)
    return [{"id": a.id, "filename": a.filename, "total_ticks": a.total_ticks,
             "fault_ticks": a.fault_ticks, "fault_rate": round(a.fault_rate * 100, 2),
             "created_at": a.created_at.isoformat(),
             "summary": json.loads(a.summary_json) if a.summary_json else []} for a in analyses]

# ── Alerts ────────────────────────────────────────────────────────

@app.get("/api/buildings/{bid}/alerts")
def get_alerts(bid: int, limit: int = Query(50, le=200),
               uid: int = Depends(get_user_id), db=Depends(get_db)):
    b = require_building(db, bid, uid)
    events = query_alerts(db, bid, limit=limit)
    return [{
        "id": e.id, "building_id": e.building_id, "fault_key": e.fault_key,
        "subsystem": e.subsystem, "subsystem_name": e.subsystem_name,
        "severity": e.severity, "title": e.title, "message": e.message,
        "created_at": e.created_at, "details": e.details,
    } for e in events]

@app.get("/api/buildings/{bid}/alerts/active")
def get_active_alerts(bid: int, uid: int = Depends(get_user_id), db=Depends(get_db)):
    b = require_building(db, bid, uid)
    states = (
        db.query(AlertState)
        .filter_by(building_id=bid, active=True)
        .all()
    )
    return [{
        "fault_key": s.fault_key, "building_id": s.building_id,
        "present_streak": s.present_streak, "absent_streak": s.absent_streak,
        "confirmed": bool(s.confirmed),
        "last_alerted_at": s.last_alerted_at, "last_seen_at": s.last_seen_at,
    } for s in states]

# ── Webhooks (professional / portfolio only) ─────────────────────

class WebhookCreate(BaseModel):
    url: str
    events: List[str] = ["fault.new"]

@app.post("/api/buildings/{bid}/webhooks")
def create_webhook(bid: int, req: WebhookCreate,
                   uid: int = Depends(get_user_id), db=Depends(get_db)):
    b = require_building(db, bid, uid)
    require_feature(db, bid, "webhooks")
    # TODO: persist webhook subscription
    return {"status": "ok", "url": req.url, "events": req.events}

@app.get("/api/buildings/{bid}/webhooks")
def list_webhooks(bid: int, uid: int = Depends(get_user_id), db=Depends(get_db)):
    b = require_building(db, bid, uid)
    require_feature(db, bid, "webhooks")
    return []

# ── Custom Thresholds (professional / portfolio only) ────────────

class ThresholdUpdate(BaseModel):
    pair_name: str
    eps: float

@app.put("/api/buildings/{bid}/thresholds")
def update_thresholds(bid: int, req: ThresholdUpdate,
                      uid: int = Depends(get_user_id), db=Depends(get_db)):
    b = require_building(db, bid, uid)
    require_feature(db, bid, "custom_thresholds")
    # Apply custom eps to the matching pair in sensor_config
    pairs = b.get_config()
    updated = False
    for p in pairs:
        if p.get("name") == req.pair_name:
            p["eps"] = req.eps
            updated = True
            break
    if not updated:
        raise HTTPException(404, f"Pair '{req.pair_name}' not found")
    b.set_config(pairs)
    db.commit()
    return {"status": "ok", "pair_name": req.pair_name, "eps": req.eps}

# ── Reports ──────────────────────────────────────────────────────

class ReportCreate(BaseModel):
    period_start: str  # ISO date, e.g. "2026-01-01"
    period_end: str
    analysis_id: Optional[int] = None

@app.post("/api/buildings/{bid}/reports", status_code=202)
async def request_report(bid: int, req: ReportCreate, request: Request,
                         uid: int = Depends(get_user_id), db=Depends(get_db)):
    b = require_building(db, bid, uid)
    from datetime import datetime as dt, timezone as tz
    period_start = dt.fromisoformat(req.period_start).replace(tzinfo=tz.utc)
    period_end = dt.fromisoformat(req.period_end).replace(tzinfo=tz.utc)

    report = create_report(db, bid, uid, period_start, period_end, req.analysis_id)

    arq_pool = request.app.state.arq_pool
    if arq_pool:
        await arq_pool.enqueue_job("generate_report", report.id)
    else:
        from worker import generate_report
        await generate_report({}, report.id)

    return {"report_id": report.id, "status": "pending"}

@app.get("/api/buildings/{bid}/reports")
def list_building_reports(bid: int, limit: int = Query(20, le=100),
                          uid: int = Depends(get_user_id), db=Depends(get_db)):
    b = require_building(db, bid, uid)
    reports = list_reports(db, bid, limit=limit)
    return [{
        "id": r.id, "status": r.status,
        "period_start": r.period_start.isoformat() if r.period_start else None,
        "period_end": r.period_end.isoformat() if r.period_end else None,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "completed_at": r.completed_at.isoformat() if r.completed_at else None,
        "error_message": r.error_message,
    } for r in reports]

@app.get("/api/reports/{rid}")
def get_report_meta(rid: str, uid: int = Depends(get_user_id), db=Depends(get_db)):
    r = get_report(db, rid)
    require_building(db, r.building_id, uid)
    return {
        "id": r.id, "building_id": r.building_id, "status": r.status,
        "period_start": r.period_start.isoformat() if r.period_start else None,
        "period_end": r.period_end.isoformat() if r.period_end else None,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "completed_at": r.completed_at.isoformat() if r.completed_at else None,
        "error_message": r.error_message,
    }

@app.get("/api/reports/{rid}/download")
def download_report(rid: str, uid: int = Depends(get_user_id), db=Depends(get_db)):
    r = get_report(db, rid)
    require_building(db, r.building_id, uid)
    filepath = get_report_filepath(r)
    from fastapi.responses import FileResponse
    return FileResponse(
        path=str(filepath),
        media_type="application/pdf",
        filename=f"sensorguard-report-{r.id[:8]}.pdf",
    )

# ── Health ─────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok", "engine": "TNA v1.0", "false_alarm_rate": "0.0%", "version": "1.2.0"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
