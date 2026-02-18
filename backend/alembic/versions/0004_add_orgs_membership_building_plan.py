"""Add organizations, org_memberships, building_plans tables + org_id on buildings.

Revision ID: 0004
Revises: 0003
Create Date: 2026-02-18
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create organizations table
    op.create_table(
        "organizations",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=True),
    )

    # 2. Create org_memberships table
    op.create_table(
        "org_memberships",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("org_id", sa.Integer, sa.ForeignKey("organizations.id"), nullable=False),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("role", sa.String(50), server_default="member"),
        sa.Column("created_at", sa.DateTime, nullable=True),
        sa.UniqueConstraint("org_id", "user_id", name="uq_org_user"),
    )
    op.create_index("ix_orgmember_user", "org_memberships", ["user_id"])

    # 3. Add org_id column to buildings (nullable for existing rows)
    op.add_column(
        "buildings",
        sa.Column("org_id", sa.Integer, sa.ForeignKey("organizations.id"), nullable=True),
    )

    # 4. Create building_plans table
    op.create_table(
        "building_plans",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("building_id", sa.Integer, sa.ForeignKey("buildings.id"), unique=True, nullable=False),
        sa.Column("plan", sa.String(50), server_default="starter"),
        sa.Column("status", sa.String(50), server_default="trialing"),
        sa.Column("created_at", sa.DateTime, nullable=True),
    )

    # 5. Data migration: create a personal org for each existing user
    #    and assign their buildings to it
    conn = op.get_bind()

    users_t = sa.table("users", sa.column("id", sa.Integer), sa.column("email", sa.String))
    orgs_t = sa.table("organizations", sa.column("id", sa.Integer), sa.column("name", sa.String))
    members_t = sa.table(
        "org_memberships",
        sa.column("id", sa.Integer),
        sa.column("org_id", sa.Integer),
        sa.column("user_id", sa.Integer),
        sa.column("role", sa.String),
    )
    buildings_t = sa.table(
        "buildings",
        sa.column("id", sa.Integer),
        sa.column("owner_id", sa.Integer),
        sa.column("org_id", sa.Integer),
    )

    for user in conn.execute(sa.select(users_t.c.id, users_t.c.email)):
        # Insert personal org
        result = conn.execute(
            orgs_t.insert().values(name=f"{user.email}'s Organization")
        )
        org_id = result.inserted_primary_key[0]

        # Insert owner membership
        conn.execute(
            members_t.insert().values(org_id=org_id, user_id=user.id, role="owner")
        )

        # Update all buildings owned by this user
        conn.execute(
            buildings_t.update()
            .where(buildings_t.c.owner_id == user.id)
            .values(org_id=org_id)
        )


def downgrade() -> None:
    op.drop_table("building_plans")
    op.drop_column("buildings", "org_id")
    op.drop_index("ix_orgmember_user", table_name="org_memberships")
    op.drop_table("org_memberships")
    op.drop_table("organizations")
