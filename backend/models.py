"""
models.py — FIX #4: Postgres-ready with SQLite fallback.

SQLite for local dev, Postgres in production via DATABASE_URL env var.
Added: timestamp_col field on Building for FIX #2 support.
"""
import enum as _enum
import json
import os
from datetime import datetime, timezone
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Text, DateTime,
    Boolean, ForeignKey, Index, UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker, relationship
from fastapi import HTTPException


# ── Python enums (documentation/validation only, DB uses String(50)) ──

class OrgRole(_enum.Enum):
    owner = "owner"
    admin = "admin"
    member = "member"

class BuildingPlanTier(_enum.Enum):
    starter = "starter"
    professional = "professional"

class BuildingPlanStatus(_enum.Enum):
    trialing = "trialing"
    active = "active"
    past_due = "past_due"

class OrgPlanTier(_enum.Enum):
    portfolio = "portfolio"

# FIX #4: Postgres in production, SQLite locally.
# Railway/Render/Supabase all set DATABASE_URL automatically.
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///sensorguard.db")

# Postgres on some PaaS uses postgres:// but SQLAlchemy needs postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, echo=False,
                       pool_pre_ping=True)  # reconnect on stale connections
SessionLocal = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    name = Column(String(255), default="")
    plan = Column(String(50), default="trial")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    buildings = relationship("Building", back_populates="owner")
    memberships = relationship("OrgMembership", back_populates="user")


class Organization(Base):
    __tablename__ = "organizations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    memberships = relationship("OrgMembership", back_populates="org")
    buildings = relationship("Building", back_populates="org")
    org_plan = relationship("OrgPlan", back_populates="org", uselist=False)


class OrgMembership(Base):
    __tablename__ = "org_memberships"
    id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(Integer, ForeignKey("organizations.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    role = Column(String(50), default="member")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    org = relationship("Organization", back_populates="memberships")
    user = relationship("User", back_populates="memberships")
    __table_args__ = (
        UniqueConstraint("org_id", "user_id", name="uq_org_user"),
        Index("ix_orgmember_user", "user_id"),
    )


class Building(Base):
    __tablename__ = "buildings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("organizations.id"), nullable=True)
    name = Column(String(255), nullable=False)
    address = Column(String(500), default="")
    floors = Column(Integer, default=1)
    ahus = Column(Integer, default=1)
    sensor_config = Column(Text, default="[]")    # JSON: list of pair mappings
    timestamp_col = Column(String(100), default="")  # FIX #2: which CSV column is the timestamp
    instance_col = Column(String(100), default="")   # For multi-instance sensors (IMU_I, BARO_I, GPS_I)
    bacnet_config = Column(Text, default="{}")       # JSON: BACnet point mappings
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    owner = relationship("User", back_populates="buildings")
    org = relationship("Organization", back_populates="buildings")
    plan_record = relationship("BuildingPlan", back_populates="building", uselist=False)
    fault_events = relationship("FaultEvent", back_populates="building")
    analyses = relationship("Analysis", back_populates="building")

    def get_config(self) -> list:
        try:
            return json.loads(self.sensor_config)
        except (json.JSONDecodeError, TypeError):
            return []

    def set_config(self, pairs: list):
        self.sensor_config = json.dumps(pairs)

    def get_bacnet_config(self) -> dict:
        try:
            cfg = json.loads(self.bacnet_config)
            return cfg if cfg else None
        except (json.JSONDecodeError, TypeError):
            return None

    def set_bacnet_config(self, config: dict):
        self.bacnet_config = json.dumps(config)


class FaultEvent(Base):
    __tablename__ = "fault_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    building_id = Column(Integer, ForeignKey("buildings.id"), nullable=False)
    analysis_id = Column(Integer, ForeignKey("analyses.id"), nullable=True)
    pair_name = Column(String(255), nullable=False)
    group = Column(String(50), nullable=False)
    severity = Column(String(50), nullable=False)
    diagnosis = Column(Text, nullable=True)
    val_a = Column(Float, nullable=True)
    val_b = Column(Float, nullable=True)
    tick_timestamp = Column(Float, nullable=True)  # actual BAS timestamp
    detected_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    resolved = Column(Boolean, default=False)
    building = relationship("Building", back_populates="fault_events")
    analysis = relationship("Analysis", back_populates="fault_events")
    __table_args__ = (Index("ix_fault_bldg_time", "building_id", "detected_at"),)


class Analysis(Base):
    __tablename__ = "analyses"
    id = Column(Integer, primary_key=True, autoincrement=True)
    building_id = Column(Integer, ForeignKey("buildings.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    filename = Column(String(255), default="")
    source = Column(String(20), default="csv")  # csv | api | bacnet
    total_ticks = Column(Integer, default=0)
    fault_ticks = Column(Integer, default=0)
    fault_rate = Column(Float, default=0.0)
    summary_json = Column(Text, default="[]")
    coverage_json = Column(Text, default="{}")  # CSV column coverage stats
    data_start_ts = Column(DateTime, nullable=True)  # earliest timestamp in the dataset
    data_end_ts = Column(DateTime, nullable=True)     # latest timestamp in the dataset
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    building = relationship("Building", back_populates="analyses")
    fault_events = relationship("FaultEvent", back_populates="analysis")
    __table_args__ = (
        Index("ix_analysis_bldg_created", "building_id", "created_at"),
        Index("ix_analysis_bldg_data_start", "building_id", "data_start_ts"),
    )


class AnalysisJob(Base):
    __tablename__ = "analysis_jobs"
    id = Column(String(64), primary_key=True)          # UUID
    building_id = Column(Integer, ForeignKey("buildings.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    status = Column(String(20), default="queued")      # queued | running | complete | failed
    filename = Column(String(255), default="")
    csv_data = Column(Text, nullable=True)             # stored temporarily, cleared on completion
    result_json = Column(Text, nullable=True)          # full response JSON
    error = Column(Text, nullable=True)
    analysis_id = Column(Integer, ForeignKey("analyses.id"), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime, nullable=True)


class BuildingPlan(Base):
    __tablename__ = "building_plans"
    id = Column(Integer, primary_key=True, autoincrement=True)
    building_id = Column(Integer, ForeignKey("buildings.id"), unique=True, nullable=False)
    plan = Column(String(50), default="starter")
    status = Column(String(50), default="trialing")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    building = relationship("Building", back_populates="plan_record")


class Report(Base):
    __tablename__ = "reports"
    id = Column(String(64), primary_key=True)  # UUID
    building_id = Column(Integer, ForeignKey("buildings.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    analysis_id = Column(Integer, ForeignKey("analyses.id"), nullable=True)
    period_start = Column(DateTime, nullable=False)
    period_end = Column(DateTime, nullable=False)
    status = Column(String(20), default="pending")  # pending | completed | failed
    file_relpath = Column(String(500), nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime, nullable=True)
    building = relationship("Building")
    __table_args__ = (Index("ix_report_bldg_created", "building_id", "created_at"),)


class OrgPlan(Base):
    __tablename__ = "org_plans"
    id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(Integer, ForeignKey("organizations.id"), unique=True, nullable=False)
    plan = Column(String(50), default="portfolio")
    status = Column(String(50), default="trialing")
    min_buildings = Column(Integer, default=10)
    invoice_billing = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    activated_at = Column(DateTime, nullable=True)
    cancelled_at = Column(DateTime, nullable=True)
    org = relationship("Organization", back_populates="org_plan")


def init_db():
    Base.metadata.create_all(engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Org-based access helpers ─────────────────────────────────────

def require_building(db, bid: int, uid: int) -> Building:
    """Return Building if user is a member of its org, else raise 404."""
    b = (
        db.query(Building)
        .join(OrgMembership, OrgMembership.org_id == Building.org_id)
        .filter(Building.id == bid, OrgMembership.user_id == uid)
        .first()
    )
    if not b:
        # Fallback: legacy owner_id check (buildings not yet migrated)
        b = db.query(Building).filter_by(id=bid, owner_id=uid).first()
    if not b:
        raise HTTPException(404, "Building not found")
    return b


def list_user_buildings(db, uid: int):
    """Return all buildings in any org the user belongs to."""
    buildings = (
        db.query(Building)
        .join(OrgMembership, OrgMembership.org_id == Building.org_id)
        .filter(OrgMembership.user_id == uid)
        .all()
    )
    if not buildings:
        # Fallback: legacy owner_id check
        buildings = db.query(Building).filter_by(owner_id=uid).all()
    return buildings


def get_or_create_user_org(db, uid: int) -> Organization:
    """Return first org where user is owner/admin, or create a personal org."""
    membership = (
        db.query(OrgMembership)
        .filter(
            OrgMembership.user_id == uid,
            OrgMembership.role.in_(["owner", "admin"]),
        )
        .first()
    )
    if membership:
        return membership.org

    user = db.query(User).filter_by(id=uid).first()
    org_name = f"{user.name or user.email}'s Organization"
    org = Organization(name=org_name)
    db.add(org)
    db.flush()
    mem = OrgMembership(org_id=org.id, user_id=uid, role="owner")
    db.add(mem)
    db.flush()
    return org
