"""Baseline — create all application tables.

Revision ID: 0001
Revises: None
Create Date: 2026-02-18
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── users ────────────────────────────────────────────────────
    op.create_table(
        "users",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("email", sa.String(255), unique=True, nullable=False),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("name", sa.String(255), server_default=""),
        sa.Column("plan", sa.String(50), server_default="trial"),
        sa.Column("created_at", sa.DateTime),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=True)

    # ── buildings ────────────────────────────────────────────────
    op.create_table(
        "buildings",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("address", sa.String(500), server_default=""),
        sa.Column("floors", sa.Integer, server_default="1"),
        sa.Column("ahus", sa.Integer, server_default="1"),
        sa.Column("sensor_config", sa.Text, server_default="[]"),
        sa.Column("timestamp_col", sa.String(100), server_default=""),
        sa.Column("instance_col", sa.String(100), server_default=""),
        sa.Column("bacnet_config", sa.Text, server_default="{}"),
        sa.Column("created_at", sa.DateTime),
    )

    # ── analyses ─────────────────────────────────────────────────
    op.create_table(
        "analyses",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("building_id", sa.Integer, sa.ForeignKey("buildings.id"), nullable=False),
        sa.Column("filename", sa.String(255), server_default=""),
        sa.Column("total_ticks", sa.Integer, server_default="0"),
        sa.Column("fault_ticks", sa.Integer, server_default="0"),
        sa.Column("fault_rate", sa.Float, server_default="0.0"),
        sa.Column("summary_json", sa.Text, server_default="[]"),
        sa.Column("created_at", sa.DateTime),
    )

    # ── fault_events ─────────────────────────────────────────────
    op.create_table(
        "fault_events",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("building_id", sa.Integer, sa.ForeignKey("buildings.id"), nullable=False),
        sa.Column("analysis_id", sa.Integer, sa.ForeignKey("analyses.id"), nullable=True),
        sa.Column("pair_name", sa.String(255), nullable=False),
        sa.Column("group", sa.String(50), nullable=False),
        sa.Column("severity", sa.String(50), nullable=False),
        sa.Column("diagnosis", sa.Text, nullable=True),
        sa.Column("val_a", sa.Float, nullable=True),
        sa.Column("val_b", sa.Float, nullable=True),
        sa.Column("tick_timestamp", sa.Float, nullable=True),
        sa.Column("detected_at", sa.DateTime),
        sa.Column("resolved", sa.Boolean, server_default="0"),
    )
    op.create_index("ix_fault_bldg_time", "fault_events", ["building_id", "detected_at"])

    # ── alert_state ──────────────────────────────────────────────
    op.create_table(
        "alert_state",
        sa.Column("fault_key", sa.String(512), primary_key=True),
        sa.Column("building_id", sa.Integer, sa.ForeignKey("buildings.id"), nullable=False),
        sa.Column("present_streak", sa.Integer, server_default="0"),
        sa.Column("absent_streak", sa.Integer, server_default="0"),
        sa.Column("active", sa.Boolean, nullable=False, server_default="0"),
        sa.Column("confirmed", sa.Boolean, nullable=False, server_default="0"),
        sa.Column("last_alerted_at", sa.String(64), server_default=""),
        sa.Column("last_seen_at", sa.String(64), server_default=""),
    )
    op.create_index("ix_alert_state_building_id", "alert_state", ["building_id"])

    # ── alert_events ─────────────────────────────────────────────
    op.create_table(
        "alert_events",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("building_id", sa.Integer, sa.ForeignKey("buildings.id"), nullable=False),
        sa.Column("fault_key", sa.String(512), nullable=False),
        sa.Column("subsystem", sa.String(64), nullable=False, server_default=""),
        sa.Column("subsystem_name", sa.String(128), nullable=False, server_default=""),
        sa.Column("severity", sa.String(32), nullable=False),
        sa.Column("title", sa.String(512), nullable=False),
        sa.Column("message", sa.Text, server_default=""),
        sa.Column("details", sa.Text, server_default="{}"),
        sa.Column("created_at", sa.String(64), nullable=False),
    )
    op.create_index("ix_alert_evt_bldg", "alert_events", ["building_id", "created_at"])


def downgrade() -> None:
    op.drop_table("alert_events")
    op.drop_table("alert_state")
    op.drop_table("fault_events")
    op.drop_table("analyses")
    op.drop_table("buildings")
    op.drop_table("users")
