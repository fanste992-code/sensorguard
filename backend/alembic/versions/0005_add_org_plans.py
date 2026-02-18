"""Add org_plans table for portfolio contracts.

Revision ID: 0005
Revises: 0004
Create Date: 2026-02-18
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "org_plans",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("org_id", sa.Integer, sa.ForeignKey("organizations.id"), unique=True, nullable=False),
        sa.Column("plan", sa.String(50), server_default="portfolio"),
        sa.Column("status", sa.String(50), server_default="trialing"),
        sa.Column("min_buildings", sa.Integer, server_default="10"),
        sa.Column("invoice_billing", sa.Boolean, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime, nullable=True),
        sa.Column("activated_at", sa.DateTime, nullable=True),
        sa.Column("cancelled_at", sa.DateTime, nullable=True),
    )


def downgrade() -> None:
    op.drop_table("org_plans")
