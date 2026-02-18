"""Migrate alert tables building_id from VARCHAR to INTEGER FK.

Revision ID: 0002
Revises: 0001
Create Date: 2026-02-18

Uses batch_alter_table for SQLite compatibility (SQLite cannot ALTER COLUMN).
On Postgres the batch wrapper falls through to normal ALTER TABLE statements.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # alert_state: building_id VARCHAR -> INTEGER with FK
    with op.batch_alter_table("alert_state", schema=None) as batch_op:
        batch_op.alter_column(
            "building_id",
            existing_type=sa.String(512),
            type_=sa.Integer,
            existing_nullable=False,
            postgresql_using="building_id::integer",
        )
        batch_op.create_foreign_key(
            "fk_alert_state_building_id",
            "buildings",
            ["building_id"],
            ["id"],
        )

    # alert_events: building_id VARCHAR -> INTEGER with FK
    with op.batch_alter_table("alert_events", schema=None) as batch_op:
        batch_op.alter_column(
            "building_id",
            existing_type=sa.String(512),
            type_=sa.Integer,
            existing_nullable=False,
            postgresql_using="building_id::integer",
        )
        batch_op.create_foreign_key(
            "fk_alert_events_building_id",
            "buildings",
            ["building_id"],
            ["id"],
        )


def downgrade() -> None:
    with op.batch_alter_table("alert_events", schema=None) as batch_op:
        batch_op.drop_constraint("fk_alert_events_building_id", type_="foreignkey")
        batch_op.alter_column(
            "building_id",
            existing_type=sa.Integer,
            type_=sa.String(512),
            existing_nullable=False,
        )

    with op.batch_alter_table("alert_state", schema=None) as batch_op:
        batch_op.drop_constraint("fk_alert_state_building_id", type_="foreignkey")
        batch_op.alter_column(
            "building_id",
            existing_type=sa.Integer,
            type_=sa.String(512),
            existing_nullable=False,
        )
