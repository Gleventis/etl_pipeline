"""Shared test fixtures for translator tests."""

import pytest

from src.services.db import get_connection, init_db


@pytest.fixture
def conn():
    """Yield a psycopg connection with initialized schema and post-test cleanup."""
    with get_connection() as connection:
        init_db(conn=connection)
        with connection.cursor() as cur:
            cur.execute(query="DELETE FROM translator_runs")
        connection.commit()
        yield connection
        with connection.cursor() as cur:
            cur.execute(query="DELETE FROM translator_runs")
        connection.commit()
