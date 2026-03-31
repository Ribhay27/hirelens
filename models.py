"""
HireLens Database Layer
SQLAlchemy models + connection management for PostgreSQL
"""

import os
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Integer, String, Text, create_engine, text
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker

load_dotenv()


# ── Base ──────────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


# ── Models ────────────────────────────────────────────────────────────────────

class JobPosting(Base):
    """Raw job posting as scraped from the source."""
    __tablename__ = "job_postings"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    external_id     = Column(String(255), unique=True, nullable=False, index=True)
    source          = Column(String(50), nullable=False)          # "indeed" | "linkedin"
    title           = Column(String(500), nullable=False)
    company         = Column(String(255))
    location        = Column(String(255))
    is_remote       = Column(Boolean, default=False)
    salary_min      = Column(Float)
    salary_max      = Column(Float)
    salary_currency = Column(String(10), default="USD")
    description_raw = Column(Text)
    url             = Column(Text)
    posted_date     = Column(DateTime)
    scraped_at      = Column(DateTime, default=datetime.utcnow)
    is_processed    = Column(Boolean, default=False)

    processed = relationship("ProcessedJob", back_populates="posting", uselist=False)

    def __repr__(self):
        return f"<JobPosting {self.id}: {self.title} @ {self.company}>"


class ProcessedJob(Base):
    """Cleaned + NLP-enriched version of a job posting."""
    __tablename__ = "processed_jobs"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    posting_id      = Column(Integer, ForeignKey("job_postings.id"), unique=True)
    role_category   = Column(String(100), index=True)   # e.g. "Data Engineer"
    seniority       = Column(String(50))                # "Junior" / "Mid" / "Senior" / "Lead"
    skills          = Column(ARRAY(Text))               # ["Python", "SQL", ...]
    tools           = Column(ARRAY(Text))               # ["dbt", "Airflow", ...]
    description_clean = Column(Text)
    processed_at    = Column(DateTime, default=datetime.utcnow)

    posting = relationship("JobPosting", back_populates="processed")

    def __repr__(self):
        return f"<ProcessedJob {self.id}: {self.role_category}>"


class SkillTrend(Base):
    """Aggregated skill frequency snapshot (updated by pipeline runs)."""
    __tablename__ = "skill_trends"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    skill       = Column(String(255), nullable=False, index=True)
    category    = Column(String(100), index=True)       # role category filter
    count       = Column(Integer, default=0)
    snapshot_date = Column(DateTime, default=datetime.utcnow)


class PipelineRun(Base):
    """Audit log of each pipeline execution."""
    __tablename__ = "pipeline_runs"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    started_at      = Column(DateTime, default=datetime.utcnow)
    finished_at     = Column(DateTime)
    jobs_scraped    = Column(Integer, default=0)
    jobs_processed  = Column(Integer, default=0)
    errors          = Column(Integer, default=0)
    status          = Column(String(50), default="running")  # running/success/failed
    meta            = Column(JSONB)


# ── Connection ─────────────────────────────────────────────────────────────────

def get_engine(url: Optional[str] = None):
    db_url = url or os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hirelens")
    engine = create_engine(db_url, pool_pre_ping=True, pool_size=5, max_overflow=10)
    return engine


def get_session_factory(engine=None):
    if engine is None:
        engine = get_engine()
    return sessionmaker(bind=engine, expire_on_commit=False)


def init_db(engine=None):
    """Create all tables if they don't exist."""
    if engine is None:
        engine = get_engine()
    Base.metadata.create_all(engine)
    logger.info("Database tables initialised.")
    return engine


def get_db_session(engine=None) -> Session:
    """Return a new DB session (caller is responsible for closing)."""
    SessionLocal = get_session_factory(engine)
    return SessionLocal()


def check_connection(engine=None) -> bool:
    """Ping the database to verify connectivity."""
    try:
        eng = engine or get_engine()
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection OK.")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False
