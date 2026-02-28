"""SQLAlchemy models and database utilities for the API server."""

import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Generator

from sqlalchemy import (
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    sessionmaker,
)

from src.services.config import SETTINGS

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""


class Files(Base):
    """File tracking with aggregated metrics."""

    __tablename__ = "files"
    __table_args__ = (UniqueConstraint("bucket", "object_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    bucket: Mapped[str] = mapped_column(Text, nullable=False)
    object_name: Mapped[str] = mapped_column(Text, nullable=False)
    overall_status: Mapped[str] = mapped_column(
        String(50), nullable=False, default="pending"
    )
    total_computation_seconds: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.0
    )
    total_elapsed_seconds: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.0
    )
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=func.now(), onupdate=func.now()
    )


class JobExecutions(Base):
    """Step-level execution tracking."""

    __tablename__ = "job_executions"
    __table_args__ = (
        Index("idx_job_executions_file_id", "file_id"),
        Index("idx_job_executions_pipeline_run_id", "pipeline_run_id"),
        Index("idx_job_executions_status", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("files.id"), nullable=False
    )
    pipeline_run_id: Mapped[str] = mapped_column(Text, nullable=False)
    step_name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="pending")
    started_at: Mapped[datetime | None] = mapped_column(nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    computation_time_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=func.now(), onupdate=func.now()
    )


class AnalyticalResults(Base):
    """Analytical output tracking."""

    __tablename__ = "analytical_results"
    __table_args__ = (
        Index("idx_analytical_results_result_type", "result_type"),
        Index("idx_analytical_results_job_execution_id", "job_execution_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_execution_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("job_executions.id"), nullable=False
    )
    result_type: Mapped[str] = mapped_column(Text, nullable=False)
    summary_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    detail_s3_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    computation_time_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=func.now()
    )


_engine = None
_session_factory = None


def get_engine(database_url: str = SETTINGS.DATABASE_URL):
    """Create or return the SQLAlchemy engine.

    Args:
        database_url: Postgres connection string.

    Returns:
        SQLAlchemy Engine instance.
    """
    global _engine
    if _engine is None:
        _engine = create_engine(database_url)
    return _engine


def get_session_factory(database_url: str = SETTINGS.DATABASE_URL) -> sessionmaker:
    """Create or return the session factory.

    Args:
        database_url: Postgres connection string.

    Returns:
        SQLAlchemy sessionmaker instance.
    """
    global _session_factory
    if _session_factory is None:
        engine = get_engine(database_url=database_url)
        _session_factory = sessionmaker(bind=engine)
    return _session_factory


@contextmanager
def get_session(
    database_url: str = SETTINGS.DATABASE_URL,
) -> Generator[Session, None, None]:
    """Provide a transactional database session as a context manager.

    Args:
        database_url: Postgres connection string.

    Yields:
        Active SQLAlchemy Session.
    """
    factory = get_session_factory(database_url=database_url)
    session = factory()
    try:
        yield session
    finally:
        session.close()


def init_schema(database_url: str = SETTINGS.DATABASE_URL) -> None:
    """Create all tables and indexes if they do not exist.

    Args:
        database_url: Postgres connection string.
    """
    engine = get_engine(database_url=database_url)
    Base.metadata.create_all(bind=engine)
    logger.info("database schema initialized")


def reset_globals() -> None:
    """Reset module-level engine and session factory.

    Used in tests to allow re-initialization with different database URLs.
    """
    global _engine, _session_factory
    _engine = None
    _session_factory = None


if __name__ == "__main__":
    init_schema()
    with get_session() as session:
        result = session.execute(func.count(Files.id).select())
        print(f"Files count: {result.scalar()}")
