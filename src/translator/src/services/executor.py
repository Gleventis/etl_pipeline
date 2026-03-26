"""Background executor for translator pipeline runs.

Sequences downstream calls (collect → analyze → aggregate),
updating run phase in Postgres at each step.
"""

import logging
from uuid import UUID

from src.services.db import get_connection, update_run
from src.services.http_client import call_aggregator, call_collector, call_scheduler
from src.services.parser import ParsedDSL

logger = logging.getLogger(__name__)


def execute_run(run_id: UUID, parsed: ParsedDSL) -> None:
    """Execute a parsed DSL pipeline in sequence.

    Updates the run phase in Postgres before each downstream call.
    On failure, sets phase to 'failed' with the error message.
    On success, sets phase to 'completed'.

    Args:
        run_id: UUID of the run record.
        parsed: Parsed DSL with optional collect/analyze/aggregate commands.
    """
    try:
        with get_connection() as conn:
            if parsed.collect is not None:
                update_run(conn=conn, run_id=run_id, phase="collecting")
                call_collector(cmd=parsed.collect)
                logger.info("collect completed: run_id=%s", run_id)

            if parsed.analyze is not None:
                update_run(conn=conn, run_id=run_id, phase="analyzing")
                call_scheduler(cmd=parsed.analyze)
                logger.info("analyze completed: run_id=%s", run_id)

            if parsed.aggregate is not None:
                update_run(conn=conn, run_id=run_id, phase="aggregating")
                result = call_aggregator(cmd=parsed.aggregate)
                if not result:
                    update_run(
                        conn=conn,
                        run_id=run_id,
                        phase="failed",
                        error="412 Precondition Failed: aggregator returned no data",
                    )
                    return
                logger.info("aggregate completed: run_id=%s", run_id)

            update_run(conn=conn, run_id=run_id, phase="completed")
            logger.info("run completed: run_id=%s", run_id)
    except Exception as e:
        logger.error("run failed: run_id=%s, error=%s", run_id, e)
        with get_connection() as conn:
            update_run(conn=conn, run_id=run_id, phase="failed", error=str(e))


if __name__ == "__main__":
    from src.services.parser import parse_dsl

    sample_dsl = '{"collect": {"year": 2024, "month": 1, "taxi_type": "yellow"}}'
    parsed_sample = parse_dsl(dsl=sample_dsl)
    print(f"Would execute: {parsed_sample.model_dump()}")
