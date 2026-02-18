"""Add reports table for on-demand PDF generation.

Revision ID: 0006
Revises: 0005
Create Date: 2026-02-18
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0006"
down_revision: Union[str, None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "reports",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("building_id", sa.Integer, sa.ForeignKey("buildings.id"), nullable=False),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("analysis_id", sa.Integer, sa.ForeignKey("analyses.id"), nullable=True),
        sa.Column("period_start", sa.DateTime, nullable=False),
        sa.Column("period_end", sa.DateTime, nullable=False),
        sa.Column("status", sa.String(20), server_default="pending"),
        sa.Column("file_relpath", sa.String(500), nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=True),
        sa.Column("completed_at", sa.DateTime, nullable=True),
    )
    op.create_index("ix_report_bldg_created", "reports", ["building_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_report_bldg_created", table_name="reports")
    op.drop_table("reports")
