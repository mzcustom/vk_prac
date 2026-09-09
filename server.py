#!/usr/bin/env python3
"""Deterministic mock upstream APIs for the Reliable Data Pipeline take-home.
"""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
from collections import defaultdict, deque
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent
FIXTURES = json.loads((ROOT / "fixtures.json").read_text(encoding="utf-8"))

SOURCE_A_PAGE_SIZE = 2
SOURCE_B_PAGE_SIZE = 2
SOURCE_C_MAX_PAGE_SIZE = 2
SOURCE_C_RATE_LIMIT = 2
SOURCE_C_WINDOW_SECONDS = 1.0

BASE_LATENCY = {
    "source_a": 0.08,
    "source_b": 0.12,
    "source_c": 0.06,
}


class MockState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        with getattr(self, "lock", threading.Lock()):
            self.started_at = time.monotonic()
            self.source_b_attempts: dict[str, int] = defaultdict(int)
            self.source_c_requests: deque[float] = deque()
            self.total_requests = 0


STATE = MockState()


def scenario() -> str:
    return os.getenv("MOCK_SCENARIO", "standard").strip().lower()


def latency_multiplier() -> float:
    if scenario() == "slow":
        return 4.0
    try:
        return max(0.0, float(os.getenv("LATENCY_MULTIPLIER", "1.0")))
    except ValueError:
        return 1.0


def sleep_for(source: str) -> None:
    time.sleep(BASE_LATENCY[source] * latency_multiplier())


class Handler(BaseHTTPRequestHandler):
    server_version = "ReliablePipelineMock/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep logs useful but compact for candidates.
        print(f"[{self.log_date_time_string()}] {self.address_string()} {fmt % args}")

    def _json(self, status: int, payload: Any, headers: dict[str, str] | None = None) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _bad_request(self, message: str) -> None:
        self._json(HTTPStatus.BAD_REQUEST, {"error": "bad_request", "message": message})

    def _record_request(self) -> None:
        with STATE.lock:
            STATE.total_requests += 1

    def do_GET(self) -> None:  # noqa: N802
        self._record_request()
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query, keep_blank_values=True)

        if parsed.path == "/health":
            self._json(HTTPStatus.OK, {"status": "ok", "scenario": scenario()})
            return

        if parsed.path == "/source-a/products":
            self._source_a(query)
            return

        if parsed.path == "/source-b/products":
            self._source_b(query)
            return

        if parsed.path == "/source-c/products":
            self._source_c(query)
            return

        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        self._record_request()
        parsed = urlparse(self.path)

        if parsed.path == "/admin/reset":
            STATE.reset()
            self._json(HTTPStatus.OK, {"status": "reset"})
            return

        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def _source_a(self, query: dict[str, list[str]]) -> None:
        sleep_for("source_a")
        raw_page = query.get("page", ["1"])[0]
        try:
            page = int(raw_page)
        except ValueError:
            self._bad_request("page must be an integer")
            return
        if page < 1:
            self._bad_request("page must be >= 1")
            return

        items = FIXTURES["source_a"]
        total_pages = (len(items) + SOURCE_A_PAGE_SIZE - 1) // SOURCE_A_PAGE_SIZE
        start = (page - 1) * SOURCE_A_PAGE_SIZE
        end = start + SOURCE_A_PAGE_SIZE
        self._json(
            HTTPStatus.OK,
            {
                "page": page,
                "total_pages": total_pages,
                "products": items[start:end],
            },
        )

    def _source_b(self, query: dict[str, list[str]]) -> None:
        sleep_for("source_b")

        sc = scenario()
        if sc == "source-b-down":
            self._json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "upstream_unavailable", "message": "Source B is unavailable in this scenario."},
                {"Retry-After": "1"},
            )
            return

        cursor = query.get("cursor", [""])[0]
        cursor_to_page = {"": 0, "cursor-2": 1, "cursor-3": 2}
        if cursor not in cursor_to_page:
            self._bad_request("unknown cursor")
            return

        # In the standard scenario, selected pages fail transiently before succeeding.
        # This is deterministic and re-armed by POST /admin/reset.
        if sc not in {"no-failures", "bad-data-heavy"}:
            with STATE.lock:
                STATE.source_b_attempts[cursor] += 1
                attempt = STATE.source_b_attempts[cursor]

            failure_budget = {"cursor-2": 1, "cursor-3": 2}.get(cursor, 0)
            if attempt <= failure_budget:
                status = HTTPStatus.SERVICE_UNAVAILABLE if cursor == "cursor-2" else HTTPStatus.BAD_GATEWAY
                self._json(
                    status,
                    {
                        "error": "transient_upstream_failure",
                        "message": "Temporary Source B failure. Retrying later may succeed.",
                    },
                    {"Retry-After": "1"},
                )
                return

        page = cursor_to_page[cursor]
        start = page * SOURCE_B_PAGE_SIZE
        end = start + SOURCE_B_PAGE_SIZE
        items = [dict(x) for x in FIXTURES["source_b"][start:end]]

        if sc == "bad-data-heavy" and items:
            items[0]["amount_cents"] = None
            items[0].pop("department", None)

        next_cursor = None if page >= 2 else f"cursor-{page + 2}"
        self._json(HTTPStatus.OK, {"items": items, "next_cursor": next_cursor})

    def _source_c(self, query: dict[str, list[str]]) -> None:
        sleep_for("source_c")

        try:
            offset = int(query.get("offset", ["0"])[0])
            limit = int(query.get("limit", [str(SOURCE_C_MAX_PAGE_SIZE)])[0])
        except ValueError:
            self._bad_request("offset and limit must be integers")
            return

        if offset < 0:
            self._bad_request("offset must be >= 0")
            return
        if limit < 1 or limit > SOURCE_C_MAX_PAGE_SIZE:
            self._bad_request(f"limit must be between 1 and {SOURCE_C_MAX_PAGE_SIZE}")
            return

        now = time.monotonic()
        with STATE.lock:
            while STATE.source_c_requests and now - STATE.source_c_requests[0] >= SOURCE_C_WINDOW_SECONDS:
                STATE.source_c_requests.popleft()

            if len(STATE.source_c_requests) >= SOURCE_C_RATE_LIMIT:
                retry_after = max(1, int(SOURCE_C_WINDOW_SECONDS - (now - STATE.source_c_requests[0]) + 0.999))
                self._json(
                    HTTPStatus.TOO_MANY_REQUESTS,
                    {
                        "error": "rate_limited",
                        "message": f"Source C allows at most {SOURCE_C_RATE_LIMIT} requests per second.",
                    },
                    {
                        "Retry-After": str(retry_after),
                        "X-RateLimit-Limit": str(SOURCE_C_RATE_LIMIT),
                        "X-RateLimit-Window": "1",
                    },
                )
                return

            STATE.source_c_requests.append(now)

        items = FIXTURES["source_c"]
        page = items[offset : offset + limit]
        next_offset = offset + limit if offset + limit < len(items) else None
        self._json(
            HTTPStatus.OK,
            {
                "data": page,
                "next_offset": next_offset,
                "max_page_size": SOURCE_C_MAX_PAGE_SIZE,
            },
            {
                "X-RateLimit-Limit": str(SOURCE_C_RATE_LIMIT),
                "X-RateLimit-Window": "1",
            },
        )


def make_server(host: str, port: int) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), Handler)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Reliable Data Pipeline mock API service")
    parser.add_argument("--host", default=os.getenv("HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8080")))
    args = parser.parse_args()

    server = make_server(args.host, args.port)
    print(f"Mock API listening on http://{args.host}:{args.port} (scenario={scenario()})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
