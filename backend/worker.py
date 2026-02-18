"""
worker.py — Arq background worker for SensorGuard analysis jobs.

Start with:  cd backend && arq worker.WorkerSettings
"""
from __future__ import annotations

import io
import json
import logging
import os
import traceback
from datetime import datetime, timezone
from urllib.parse import urlparse

from arq import cron
from arq.connections import RedisSettings

from models import SessionLocal, Building, FaultEvent, Analysis, AnalysisJob
import alert_engine
from hvac import analyze_csv, build_config
from fault_aggregator import aggregate_faults

log = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


def _parse_redis_url(url: str) -> RedisSettings:
    """Convert a redis:// URL into arq RedisSettings."""
    parsed = urlparse(url)
    return RedisSettings(
        host=parsed.hostname or "localhost",
        port=parsed.port or 6379,
        password=parsed.password,
        database=int(parsed.path.lstrip("/") or 0),
    )


async def run_analysis(ctx: dict, job_id: str) -> None:
    """Main analysis task — mirrors the old synchronous analyze endpoint."""
    db = SessionLocal()
    try:
        job = db.query(AnalysisJob).filter_by(id=job_id).first()
        if not job:
            log.error("AnalysisJob %s not found", job_id)
            return

        job.status = "running"
        db.commit()

        bid = job.building_id
        b = db.query(Building).filter_by(id=bid).first()
        if not b:
            job.status = "failed"
            job.error = "Building not found"
            db.commit()
            return

        pair_mappings = b.get_config()
        if not pair_mappings:
            job.status = "failed"
            job.error = "No sensor pairs configured"
            db.commit()
            return

        config = build_config(
            b.name, pair_mappings,
            timestamp_col=b.timestamp_col or None,
            instance_col=b.instance_col or None,
        )

        # Stream CSV from stored text data
        text_stream = io.StringIO(job.csv_data)
        report = analyze_csv(text_stream, config)

        # Resolve old faults
        db.query(FaultEvent).filter_by(building_id=bid, resolved=False).update({"resolved": True})

        # Derive data time window from all CSV rows
        data_start_ts = None
        data_end_ts = None
        if report.data_ts_min and report.data_ts_min > 100:
            data_start_ts = datetime.fromtimestamp(report.data_ts_min, tz=timezone.utc)
        if report.data_ts_max and report.data_ts_max > 100:
            data_end_ts = datetime.fromtimestamp(report.data_ts_max, tz=timezone.utc)

        # Build coverage JSON
        coverage = {
            "csv_columns": report.csv_columns or [],
            "expected_columns": report.expected_columns or [],
            "missing_columns": report.missing_columns or [],
        }

        # Save analysis
        analysis = Analysis(
            building_id=bid, user_id=job.user_id, filename=job.filename,
            source="csv",
            total_ticks=report.total_ticks, fault_ticks=report.fault_ticks,
            fault_rate=report.fault_rate,
            summary_json=json.dumps(report.pairs_summary),
            coverage_json=json.dumps(coverage),
            data_start_ts=data_start_ts,
            data_end_ts=data_end_ts,
        )
        db.add(analysis)
        db.flush()

        # Save unique faults
        seen = set()
        new_faults = []
        for fault in report.faults:
            key = fault["pair"]
            if key not in seen:
                seen.add(key)
                fe = FaultEvent(
                    building_id=bid, analysis_id=analysis.id,
                    pair_name=fault["pair"], group=fault["group"],
                    severity=fault["severity"], diagnosis=fault["diagnosis"],
                    val_a=fault.get("val_a"), val_b=fault.get("val_b"),
                    tick_timestamp=fault.get("timestamp"),
                )
                db.add(fe)
                new_faults.append(fe)
        db.commit()

        # Aggregate faults by subsystem
        all_pair_results = []
        if report.timeline:
            latest_tick = report.timeline[-1]
            for p in latest_tick.pairs:
                pair_dict = p.to_dict()
                pair_dict["group"] = p.group
                all_pair_results.append(pair_dict)

        aggregated = aggregate_faults(all_pair_results)

        # Enrich aggregated faults with evidence
        fault_evidence_by_pair = {e["pair"]: e for e in (report.fault_evidence or [])}
        if aggregated.get("subsystem_faults"):
            for sf in aggregated["subsystem_faults"]:
                if sf.get("primary_fault"):
                    pair_name = sf["primary_fault"].get("name")
                    if pair_name and pair_name in fault_evidence_by_pair:
                        sf["primary_fault"]["evidence"] = fault_evidence_by_pair[pair_name]
                for cascade in sf.get("cascades", []):
                    pair_name = cascade.get("name") or cascade.get("pair")
                    if pair_name and pair_name in fault_evidence_by_pair:
                        cascade["evidence"] = fault_evidence_by_pair[pair_name]

        # Run alert engine
        now_iso = datetime.now(timezone.utc).isoformat()
        fired_alerts = alert_engine.update(
            db, building_id=bid,
            subsystem_faults=aggregated.get("subsystem_faults", []),
            now=now_iso,
        )
        db.commit()

        # Build result JSON (same shape as old endpoint)
        result = {
            "analysis_id": analysis.id,
            "filename": job.filename,
            "total_ticks": report.total_ticks,
            "fault_ticks": report.fault_ticks,
            "fault_rate": round(report.fault_rate * 100, 2),
            "pairs_summary": report.pairs_summary,
            "faults_found": len(new_faults),
            "faults": [
                {"pair": f.pair_name, "group": f.group,
                 "severity": f.severity, "diagnosis": f.diagnosis}
                for f in new_faults
            ],
            "latest_ticks": [
                {"timestamp": t.timestamp, "system_status": t.system_status,
                 "pairs": [p.to_dict() for p in t.pairs]}
                for t in report.timeline
            ],
            "aggregated_faults": aggregated,
            "fired_alerts": fired_alerts,
            "fault_presence": report.fault_presence,
            "active_fault_pct": report.active_fault_pct,
            "first_fault_tick": report.first_fault_tick,
            "last_fault_tick": report.last_fault_tick,
            "first_fault_time": report.first_fault_time,
            "last_fault_time": report.last_fault_time,
            "fault_evidence": report.fault_evidence or [],
        }

        job.analysis_id = analysis.id
        job.result_json = json.dumps(result, default=str)
        job.csv_data = None  # free memory
        job.status = "complete"
        job.completed_at = datetime.now(timezone.utc)
        db.commit()

    except Exception:
        log.exception("run_analysis failed for job %s", job_id)
        db.rollback()
        try:
            job = db.query(AnalysisJob).filter_by(id=job_id).first()
            if job:
                job.status = "failed"
                job.error = traceback.format_exc()[-2000:]
                job.csv_data = None
                job.completed_at = datetime.now(timezone.utc)
                db.commit()
        except Exception:
            log.exception("Failed to mark job %s as failed", job_id)
    finally:
        db.close()


async def generate_report(ctx: dict, report_id: str) -> None:
    """Generate a PDF report via report_service."""
    from report_service import generate_report_pdf
    db = SessionLocal()
    try:
        generate_report_pdf(db, report_id)
    except Exception:
        log.exception("generate_report failed for %s", report_id)
        db.rollback()
    finally:
        db.close()


async def cleanup_old_jobs(ctx: dict) -> None:
    """Cron: delete completed/failed jobs older than 24 hours."""
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    db = SessionLocal()
    try:
        db.query(AnalysisJob).filter(
            AnalysisJob.status.in_(["complete", "failed"]),
            AnalysisJob.completed_at < cutoff,
        ).delete(synchronize_session=False)
        db.commit()
    finally:
        db.close()


class WorkerSettings:
    functions = [run_analysis, generate_report]
    cron_jobs = [cron(cleanup_old_jobs, hour=3, minute=0)]
    max_jobs = 4
    job_timeout = 600
    redis_settings = _parse_redis_url(REDIS_URL)
