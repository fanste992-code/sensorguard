"""Add analysis_jobs table for background job queue.

Revision ID: 0003
Revises: 0002
Create Date: 2026-02-18
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "analysis_jobs",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("building_id", sa.Integer, sa.ForeignKey("buildings.id"), nullable=False),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("status", sa.String(20), server_default="queued"),
        sa.Column("filename", sa.String(255), server_default=""),
        sa.Column("csv_data", sa.Text, nullable=True),
        sa.Column("result_json", sa.Text, nullable=True),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("analysis_id", sa.Integer, sa.ForeignKey("analyses.id"), nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=True),
        sa.Column("completed_at", sa.DateTime, nullable=True),
    )


def downgrade() -> None:
    op.drop_table("analysis_jobs")
