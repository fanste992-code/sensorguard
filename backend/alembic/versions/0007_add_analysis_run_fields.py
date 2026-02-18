"""Add analysis run fields: user_id, source, data_start_ts, data_end_ts.

Revision ID: 0007
Revises: 0006
Create Date: 2026-02-18
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0007"
down_revision: Union[str, None] = "0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("analyses", sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=True))
    op.add_column("analyses", sa.Column("source", sa.String(20), server_default="csv"))
    op.add_column("analyses", sa.Column("coverage_json", sa.Text, server_default="{}"))
    op.add_column("analyses", sa.Column("data_start_ts", sa.DateTime, nullable=True))
    op.add_column("analyses", sa.Column("data_end_ts", sa.DateTime, nullable=True))
    op.create_index("ix_analysis_bldg_created", "analyses", ["building_id", "created_at"])
    op.create_index("ix_analysis_bldg_data_start", "analyses", ["building_id", "data_start_ts"])


def downgrade() -> None:
    op.drop_index("ix_analysis_bldg_data_start", table_name="analyses")
    op.drop_index("ix_analysis_bldg_created", table_name="analyses")
    op.drop_column("analyses", "data_end_ts")
    op.drop_column("analyses", "data_start_ts")
    op.drop_column("analyses", "source")
    op.drop_column("analyses", "user_id")
