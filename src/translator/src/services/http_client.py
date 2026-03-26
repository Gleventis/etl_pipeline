"""HTTP client for calling downstream services.

Provides functions to call the data collector, scheduler, and aggregator
services. Each function raises on non-2xx responses.
"""

import logging

import httpx

from src.services.config import SETTINGS
from src.services.parser import AggregateCommand, AnalyzeCommand, CollectCommand

logger = logging.getLogger(__name__)


def call_collector(cmd: CollectCommand) -> dict:
    """Call data collector to download files.

    Args:
        cmd: Parsed collect command from DSL.

    Returns:
        Response JSON from the collector.

    Raises:
        httpx.HTTPStatusError: On non-2xx response.
    """
    with httpx.Client(base_url=SETTINGS.COLLECTOR_URL, verify=False) as client:
        logger.info("calling collector: taxi_type=%s", cmd.taxi_type)
        response = client.post(
            url="/collector/collect",
            json=cmd.model_dump(),
            timeout=SETTINGS.HTTP_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()


def call_scheduler(cmd: AnalyzeCommand) -> dict:
    """Call scheduler to start analytical pipeline.

    Args:
        cmd: Parsed analyze command from DSL.

    Returns:
        Response JSON from the scheduler.

    Raises:
        httpx.HTTPStatusError: On non-2xx response.
    """
    with httpx.Client(base_url=SETTINGS.SCHEDULER_URL, verify=False) as client:
        logger.info(
            "calling scheduler: bucket=%s, objects=%d", cmd.bucket, len(cmd.objects)
        )
        response = client.post(
            url="/scheduler/schedule",
            json=cmd.model_dump(),
            timeout=SETTINGS.HTTP_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()


def call_aggregator(cmd: AggregateCommand) -> dict:
    """Call aggregator to fetch aggregated results.

    Args:
        cmd: Parsed aggregate command from DSL.

    Returns:
        Response JSON from the aggregator.

    Raises:
        httpx.HTTPStatusError: On non-2xx response.
    """
    with httpx.Client(base_url=SETTINGS.AGGREGATOR_URL, verify=False) as client:
        logger.info("calling aggregator: endpoint=%s", cmd.endpoint)
        response = client.get(
            url=f"/aggregations/{cmd.endpoint}",
            params=cmd.params,
            timeout=SETTINGS.HTTP_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()


if __name__ == "__main__":
    sample_collect = CollectCommand(year=2024, month=1, taxi_type="yellow")
    print(f"Collector payload: {sample_collect.model_dump()}")

    sample_analyze = AnalyzeCommand(
        bucket="data-collector", objects=["yellow/2024-01.parquet"]
    )
    print(f"Scheduler payload: {sample_analyze.model_dump()}")

    sample_aggregate = AggregateCommand(
        endpoint="descriptive-stats", params={"taxi_type": "yellow"}
    )
    print(f"Aggregator endpoint: /aggregations/{sample_aggregate.endpoint}")
    print(f"Aggregator params: {sample_aggregate.params}")
