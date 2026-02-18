"""
report_service.py — Business logic for on-demand PDF report generation.

All report logic is centralised here.  Route handlers should call these
functions instead of implementing business logic inline.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, cast, Date

from models import Building, Report, Analysis, FaultEvent
from plan_service import (
    get_effective_building_plan,
    get_plan_config,
    get_retention_cutoff,
    require_building_access,
    query_faults,
    PLAN_CONFIGS,
)

REPORT_STORAGE_ROOT = os.getenv("REPORT_STORAGE_ROOT", "./var/reports")


def ensure_storage_dir():
    """Create the report storage root if it doesn't exist."""
    Path(REPORT_STORAGE_ROOT).mkdir(parents=True, exist_ok=True)


# ── Plan enforcement ────────────────────────────────────────────

STARTER_DAILY_REPORT_LIMIT = 3


def _count_reports_today(db: Session, building_id: int) -> int:
    """Count reports created today for a building."""
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    return (
        db.query(Report)
        .filter(
            Report.building_id == building_id,
            Report.created_at >= today_start,
        )
        .count()
    )


def enforce_report_limits(db: Session, building_id: int, period_start: datetime):
    """Raise 403 if plan limits prevent report creation.

    Checks:
    1. Starter plan: max 3 reports per building per day.
    2. Retention window: period_start must be within plan's retention window.
    """
    plan_name = get_effective_building_plan(db, building_id)
    cfg = get_plan_config(plan_name)

    # Daily limit for starter
    if plan_name == "starter":
        count = _count_reports_today(db, building_id)
        if count >= STARTER_DAILY_REPORT_LIMIT:
            raise HTTPException(
                403,
                f"Starter plan allows max {STARTER_DAILY_REPORT_LIMIT} reports "
                f"per building per day (used {count})",
            )

    # Retention window
    cutoff = get_retention_cutoff(db, building_id)
    if period_start < cutoff:
        raise HTTPException(
            403,
            f"Report period_start ({period_start.date()}) is outside the "
            f"{cfg.retention_days}-day retention window for the {plan_name} plan",
        )


# ── Report CRUD ─────────────────────────────────────────────────

def create_report(
    db: Session,
    building_id: int,
    user_id: int,
    period_start: datetime,
    period_end: datetime,
    analysis_id: Optional[int] = None,
) -> Report:
    """Create a pending Report row after enforcing plan limits.

    Returns the Report so the caller can enqueue the generation job.
    """
    enforce_report_limits(db, building_id, period_start)

    report = Report(
        id=str(uuid.uuid4()),
        building_id=building_id,
        user_id=user_id,
        analysis_id=analysis_id,
        period_start=period_start,
        period_end=period_end,
        status="pending",
    )
    db.add(report)
    db.commit()
    return report


def list_reports(db: Session, building_id: int, limit: int = 20) -> list[Report]:
    """Return reports for a building, newest first."""
    return (
        db.query(Report)
        .filter_by(building_id=building_id)
        .order_by(Report.created_at.desc())
        .limit(limit)
        .all()
    )


def get_report(db: Session, report_id: str) -> Report:
    """Return a Report or raise 404."""
    r = db.query(Report).filter_by(id=report_id).first()
    if not r:
        raise HTTPException(404, "Report not found")
    return r


def get_report_filepath(report: Report) -> Path:
    """Return the absolute path to a completed report's PDF file."""
    if report.status != "completed" or not report.file_relpath:
        raise HTTPException(400, "Report is not ready for download")
    path = Path(REPORT_STORAGE_ROOT) / report.file_relpath
    if not path.exists():
        raise HTTPException(404, "Report file not found on disk")
    # Prevent path traversal
    resolved = path.resolve()
    root_resolved = Path(REPORT_STORAGE_ROOT).resolve()
    if not str(resolved).startswith(str(root_resolved)):
        raise HTTPException(403, "Invalid report path")
    return resolved


# ── PDF generation (called by worker) ──────────────────────────

TEMPLATE_PATH = Path(__file__).parent / "report_template.html"


def generate_report_pdf(db: Session, report_id: str) -> None:
    """Render HTML → PDF for a report.  Called from ARQ worker task."""
    report = db.query(Report).filter_by(id=report_id).first()
    if not report:
        return

    report.status = "running"
    db.commit()

    try:
        building = db.query(Building).filter_by(id=report.building_id).first()

        # Resolve analysis: explicit > most recent for building
        analysis = None
        if report.analysis_id:
            analysis = db.query(Analysis).filter_by(id=report.analysis_id).first()
        if not analysis:
            analysis = (
                db.query(Analysis)
                .filter_by(building_id=report.building_id)
                .order_by(Analysis.created_at.desc())
                .first()
            )

        # Gather faults: by analysis_id (preferred) or time window (fallback)
        if analysis:
            faults = (
                db.query(FaultEvent)
                .filter_by(building_id=report.building_id, analysis_id=analysis.id)
                .order_by(FaultEvent.detected_at.desc())
                .all()
            )
        else:
            faults = (
                db.query(FaultEvent)
                .filter(
                    FaultEvent.building_id == report.building_id,
                    FaultEvent.detected_at >= report.period_start,
                    FaultEvent.detected_at <= report.period_end,
                )
                .order_by(FaultEvent.detected_at.desc())
                .all()
            )

        # Render HTML
        html = _render_html(building, report, faults, analysis)

        # Convert to PDF
        import io as _io
        from xhtml2pdf import pisa
        pdf_buffer = _io.BytesIO()
        pisa_status = pisa.CreatePDF(html, dest=pdf_buffer)
        if pisa_status.err:
            raise RuntimeError(f"xhtml2pdf error: {pisa_status.err}")
        pdf_bytes = pdf_buffer.getvalue()

        # Save file
        org_id = building.org_id or 0
        rel_dir = f"{org_id}/{report.building_id}"
        abs_dir = Path(REPORT_STORAGE_ROOT) / rel_dir
        abs_dir.mkdir(parents=True, exist_ok=True)

        rel_path = f"{rel_dir}/{report.id}.pdf"
        abs_path = Path(REPORT_STORAGE_ROOT) / rel_path
        abs_path.write_bytes(pdf_bytes)

        report.file_relpath = rel_path
        report.status = "completed"
        report.completed_at = datetime.now(timezone.utc)
        db.commit()

    except Exception as exc:
        db.rollback()
        report = db.query(Report).filter_by(id=report_id).first()
        if report:
            report.status = "failed"
            report.error_message = str(exc)[:2000]
            report.completed_at = datetime.now(timezone.utc)
            db.commit()


def _render_html(building, report, faults, analysis) -> str:
    """Render the report HTML from template."""
    import json as _json
    from jinja2 import Template

    template_str = TEMPLATE_PATH.read_text(encoding="utf-8")
    tmpl = Template(template_str)

    fault_rows = []
    for f in faults:
        fault_rows.append({
            "pair_name": f.pair_name,
            "group": f.group,
            "severity": f.severity,
            "diagnosis": f.diagnosis or "",
            "detected_at": f.detected_at.strftime("%Y-%m-%d %H:%M") if f.detected_at else "",
        })

    # Data window from analysis run (CSV timestamps)
    data_start = None
    data_end = None
    pairs_summary = []
    coverage = {}
    if analysis:
        if analysis.data_start_ts:
            data_start = analysis.data_start_ts.strftime("%Y-%m-%d %H:%M")
        if analysis.data_end_ts:
            data_end = analysis.data_end_ts.strftime("%Y-%m-%d %H:%M")
        # Parse pairs_summary for coverage table
        try:
            pairs_summary = _json.loads(analysis.summary_json) if analysis.summary_json else []
        except (ValueError, TypeError):
            pairs_summary = []
        # Parse coverage stats
        try:
            coverage = _json.loads(analysis.coverage_json) if analysis.coverage_json else {}
        except (ValueError, TypeError):
            coverage = {}

    # Build fault lookup for max_delta enrichment
    fault_lookup = {}
    for f in faults:
        if f.pair_name not in fault_lookup:
            fault_lookup[f.pair_name] = f

    # Enrich pairs_summary with max_delta from fault evidence
    for ps in pairs_summary:
        fe = fault_lookup.get(ps.get("name"))
        if fe and fe.val_a is not None and fe.val_b is not None:
            ps["max_delta"] = round(abs(fe.val_a - fe.val_b), 2)
        else:
            ps["max_delta"] = None

    return tmpl.render(
        building_name=building.name if building else "Unknown",
        building_address=building.address if building else "",
        report_id=report.id,
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        total_faults=len(faults),
        faults=fault_rows,
        analysis=analysis,
        data_start=data_start,
        data_end=data_end,
        pairs_summary=pairs_summary,
        csv_columns=coverage.get("csv_columns", []),
        expected_columns=coverage.get("expected_columns", []),
        missing_columns=coverage.get("missing_columns", []),
    )
