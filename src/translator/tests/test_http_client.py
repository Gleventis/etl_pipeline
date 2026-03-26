"""Tests for the HTTP client module."""

import json

import httpx
import pytest

from src.services.http_client import call_aggregator, call_collector, call_scheduler
from src.services.parser import AggregateCommand, AnalyzeCommand, CollectCommand

_original_init = httpx.Client.__init__


def _make_transport(
    status_code: int = 200,
    json_body: dict | None = None,
) -> httpx.MockTransport:
    """Create a mock transport that returns a fixed response."""
    body = json.dumps(json_body or {}).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=status_code,
            content=body,
            headers={"content-type": "application/json"},
            request=request,
        )

    return httpx.MockTransport(handler=handler)


def _capture_transport(
    status_code: int = 200,
    json_body: dict | None = None,
) -> tuple[httpx.MockTransport, list[httpx.Request]]:
    """Create a mock transport that captures requests."""
    body = json.dumps(json_body or {}).encode()
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            status_code=status_code,
            content=body,
            headers={"content-type": "application/json"},
            request=request,
        )

    return httpx.MockTransport(handler=handler), captured


def _patch_client(
    monkeypatch: pytest.MonkeyPatch,
    transport: httpx.MockTransport,
) -> None:
    """Monkeypatch httpx.Client.__init__ to inject a mock transport.

    Pops 'verify' to avoid conflict with the custom transport.
    """

    def patched_init(self: httpx.Client, **kwargs: object) -> None:
        kwargs.pop("verify", None)
        kwargs["transport"] = transport
        _original_init(self, **kwargs)

    monkeypatch.setattr(httpx.Client, "__init__", patched_init)


class TestCallCollector:
    """Tests for call_collector."""

    def test_sends_post_to_collector(self, monkeypatch: pytest.MonkeyPatch) -> None:
        transport, captured = _capture_transport(
            json_body={"successes": [], "failures": []}
        )
        _patch_client(monkeypatch=monkeypatch, transport=transport)

        cmd = CollectCommand(year=2024, month=1, taxi_type="yellow")
        result = call_collector(cmd=cmd)

        assert len(captured) == 1
        assert captured[0].method == "POST"
        assert captured[0].url.path == "/collector/collect"
        body = json.loads(captured[0].content)
        assert body["year"] == 2024
        assert body["month"] == 1
        assert body["taxi_type"] == "yellow"
        assert result == {"successes": [], "failures": []}

    def test_sends_year_range(self, monkeypatch: pytest.MonkeyPatch) -> None:
        transport, captured = _capture_transport(json_body={"successes": []})
        _patch_client(monkeypatch=monkeypatch, transport=transport)

        cmd = CollectCommand(
            year={"from": 2023, "to": 2024}, month=1, taxi_type="yellow"
        )
        call_collector(cmd=cmd)

        body = json.loads(captured[0].content)
        assert body["year"] == {"from": 2023, "to": 2024}

    def test_raises_on_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        transport = _make_transport(status_code=500)
        _patch_client(monkeypatch=monkeypatch, transport=transport)

        cmd = CollectCommand(year=2024, month=1, taxi_type="yellow")
        with pytest.raises(httpx.HTTPStatusError):
            call_collector(cmd=cmd)


class TestCallScheduler:
    """Tests for call_scheduler."""

    def test_sends_post_to_scheduler(self, monkeypatch: pytest.MonkeyPatch) -> None:
        transport, captured = _capture_transport(json_body={"files": []})
        _patch_client(monkeypatch=monkeypatch, transport=transport)

        cmd = AnalyzeCommand(
            bucket="data-collector", objects=["yellow/2024-01.parquet"]
        )
        result = call_scheduler(cmd=cmd)

        assert len(captured) == 1
        assert captured[0].method == "POST"
        assert captured[0].url.path == "/scheduler/schedule"
        body = json.loads(captured[0].content)
        assert body["bucket"] == "data-collector"
        assert body["objects"] == ["yellow/2024-01.parquet"]
        assert body["skip_checkpoints"] == []
        assert result == {"files": []}

    def test_sends_skip_checkpoints(self, monkeypatch: pytest.MonkeyPatch) -> None:
        transport, captured = _capture_transport(json_body={"files": []})
        _patch_client(monkeypatch=monkeypatch, transport=transport)

        cmd = AnalyzeCommand(
            bucket="b",
            objects=["o"],
            skip_checkpoints=["descriptive_statistics", "temporal_analysis"],
        )
        call_scheduler(cmd=cmd)

        body = json.loads(captured[0].content)
        assert body["skip_checkpoints"] == [
            "descriptive_statistics",
            "temporal_analysis",
        ]

    def test_raises_on_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        transport = _make_transport(status_code=502)
        _patch_client(monkeypatch=monkeypatch, transport=transport)

        cmd = AnalyzeCommand(bucket="b", objects=["o"])
        with pytest.raises(httpx.HTTPStatusError):
            call_scheduler(cmd=cmd)


class TestCallAggregator:
    """Tests for call_aggregator."""

    def test_sends_get_to_aggregator(self, monkeypatch: pytest.MonkeyPatch) -> None:
        transport, captured = _capture_transport(
            json_body={"file_count": 5, "filters_applied": {}}
        )
        _patch_client(monkeypatch=monkeypatch, transport=transport)

        cmd = AggregateCommand(endpoint="descriptive-stats")
        result = call_aggregator(cmd=cmd)

        assert len(captured) == 1
        assert captured[0].method == "GET"
        assert captured[0].url.path == "/aggregations/descriptive-stats"
        assert result == {"file_count": 5, "filters_applied": {}}

    def test_sends_query_params(self, monkeypatch: pytest.MonkeyPatch) -> None:
        transport, captured = _capture_transport(json_body={})
        _patch_client(monkeypatch=monkeypatch, transport=transport)

        cmd = AggregateCommand(
            endpoint="taxi-comparison",
            params={"start_year": "2024", "taxi_type": "yellow"},
        )
        call_aggregator(cmd=cmd)

        url = captured[0].url
        assert url.path == "/aggregations/taxi-comparison"
        assert dict(url.params) == {"start_year": "2024", "taxi_type": "yellow"}

    def test_sends_empty_params(self, monkeypatch: pytest.MonkeyPatch) -> None:
        transport, captured = _capture_transport(json_body={})
        _patch_client(monkeypatch=monkeypatch, transport=transport)

        cmd = AggregateCommand(endpoint="data-quality")
        call_aggregator(cmd=cmd)

        assert captured[0].url.path == "/aggregations/data-quality"
        assert not dict(captured[0].url.params)

    def test_raises_on_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        transport = _make_transport(status_code=404)
        _patch_client(monkeypatch=monkeypatch, transport=transport)

        cmd = AggregateCommand(endpoint="nonexistent")
        with pytest.raises(httpx.HTTPStatusError):
            call_aggregator(cmd=cmd)
