"""Shared infrastructure for the three target services (photos, payments, identity).

Each service has its own business API (uploads, charges, logins), but they share:
  - The ServiceState dataclass (pool / memory / circuit-breaker model)
  - Prometheus metrics (request rate, latency, gauges for pool/memory)
  - Structured JSON logging (later shipped to Loki via Promtail)
  - Admin endpoints used by L1 runbooks: /admin/{crash,restart,scale,circuit_breaker,memory_bump}
  - Back-compat endpoints used by the L1 platform's existing tools:
      /healthz       - liveness probe
      /metrics       - JSON metrics snapshot (unchanged contract)
      /metrics/prom  - Prometheus exposition format (NEW)
      /logs          - JSON log lines (unchanged contract)
      /topology      - upstream/downstream dependencies

We deliberately keep both /metrics (JSON) and /metrics/prom (Prometheus) so the
existing L1 tools continue to work while Phase 2 migrates them to Prometheus.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable

import structlog
from dotenv import load_dotenv

# Load .env so GCP_LOG_PROJECT_ID and other vars are available when the flask
# services are started directly (uvicorn flask_app.photos:app ...). When running
# inside Docker, env vars come from docker-compose environment: blocks instead.
load_dotenv(override=False)

# GCP Cloud Logging — optional, degrades gracefully when not installed or configured.
try:
    from google.cloud import logging as gcp_logging
    _GCP_LOGGING_AVAILABLE = True
except ImportError:
    _GCP_LOGGING_AVAILABLE = False
from fastapi import APIRouter, Request, Response
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# ---------- Service state model (preserves the contract the L1 tools use) ----------

@dataclass
class ServiceState:
    """In-memory state for a single service instance.

    Fields mirror the JSON shape the L1 platform's `query_metrics` tool expects,
    so existing tools keep working unchanged.
    """

    service: str
    healthy: bool = True
    pool_used: int = 45
    pool_max: int = 200
    p99_ms: int = 120
    memory_mb: int = 340
    memory_limit_mb: int = 512
    circuit_breaker_open: bool = False
    tls_cert_expired: bool = False
    last_crash_at: float | None = None
    last_restart_at: float | None = None
    # Rolling log buffer of recent events (latest first)
    recent_logs: deque[str] = field(default_factory=lambda: deque(maxlen=200))
    # Injected by build_app_skeleton so add_log also ships to Loki + GCP
    _logger: Any = field(default=None, init=False, repr=False, compare=False)

    def add_log(self, level: str, message: str, **kwargs: Any) -> None:
        ts = time.strftime("%H:%M:%S")
        suffix = "  " + " ".join(f"{k}={v}" for k, v in kwargs.items()) if kwargs else ""
        self.recent_logs.appendleft(f"{ts} {level} {message}{suffix}")
        # Also emit through structlog so the entry reaches stdout → Promtail → Loki → GCP
        if self._logger is not None:
            try:
                log_fn = getattr(self._logger, level.lower(), self._logger.info)
                log_fn(message, **kwargs)
            except Exception:
                pass

    def current_failure_mode(self) -> str:
        if not self.healthy:
            if self.tls_cert_expired:
                return "tls_failure"
            if self.pool_used >= self.pool_max:
                return "pool_exhaustion"
            if self.memory_mb >= self.memory_limit_mb:
                return "oom"
            if self.p99_ms > 1500:
                return "high_latency"
            return "unhealthy"
        return "healthy"


# ---------- Topology (preserved from the old fake_service) ----------

TOPOLOGY: dict[str, dict[str, list[str]]] = {
    "photos":   {"upstream": ["photo-metadata-db"],         "downstream": ["cdn", "analytics"]},
    "payments": {"upstream": ["payments-db", "fraud-svc"],   "downstream": ["ledger", "notifications"]},
    "identity": {"upstream": ["identity-db", "session-cache"],"downstream": ["audit-svc"]},
}


# ---------- Prometheus metrics ----------

def make_metrics(service_name: str) -> dict[str, Any]:
    """Create a dedicated Prometheus registry + metric handles for one service.

    Each running container has its own registry, so labels are simple."""
    registry = CollectorRegistry()

    request_count = Counter(
        "l1demo_requests_total",
        "Total HTTP requests served",
        ["service", "method", "endpoint", "status"],
        registry=registry,
    )
    request_latency = Histogram(
        "l1demo_request_latency_seconds",
        "Request latency in seconds",
        ["service", "endpoint"],
        buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
        registry=registry,
    )
    # Business-level counters
    business_op = Counter(
        "l1demo_business_ops_total",
        "Total business operations (uploads, charges, logins, ...)",
        ["service", "op", "outcome"],
        registry=registry,
    )
    # Gauges that the diagnostic / verification tools read
    pool_used     = Gauge("l1demo_pool_used",          "Connection pool in use",   ["service"], registry=registry)
    pool_max      = Gauge("l1demo_pool_max",           "Connection pool capacity", ["service"], registry=registry)
    p99_ms_g      = Gauge("l1demo_p99_ms",             "Recent P99 latency (ms)",  ["service"], registry=registry)
    memory_mb_g   = Gauge("l1demo_memory_mb",          "Memory used (MB)",         ["service"], registry=registry)
    memory_limit_g= Gauge("l1demo_memory_limit_mb",    "Memory limit (MB)",        ["service"], registry=registry)
    healthy_g     = Gauge("l1demo_service_healthy",    "1 if healthy, 0 if not",   ["service"], registry=registry)
    cb_open_g     = Gauge("l1demo_circuit_breaker_open","1 if circuit breaker open",["service"],registry=registry)
    inflight_g    = Gauge("l1demo_inflight_requests",  "Requests currently inflight",["service"],registry=registry)

    return {
        "registry": registry,
        "request_count": request_count,
        "request_latency": request_latency,
        "business_op": business_op,
        "pool_used": pool_used,
        "pool_max": pool_max,
        "p99_ms": p99_ms_g,
        "memory_mb": memory_mb_g,
        "memory_limit_mb": memory_limit_g,
        "healthy": healthy_g,
        "circuit_breaker_open": cb_open_g,
        "inflight": inflight_g,
    }


def sync_gauges(metrics: dict[str, Any], state: ServiceState) -> None:
    """Push current ServiceState into Prometheus gauges before exposition."""
    svc = state.service
    metrics["pool_used"].labels(service=svc).set(state.pool_used)
    metrics["pool_max"].labels(service=svc).set(state.pool_max)
    metrics["p99_ms"].labels(service=svc).set(state.p99_ms)
    metrics["memory_mb"].labels(service=svc).set(state.memory_mb)
    metrics["memory_limit_mb"].labels(service=svc).set(state.memory_limit_mb)
    metrics["healthy"].labels(service=svc).set(1 if state.healthy else 0)
    metrics["circuit_breaker_open"].labels(service=svc).set(1 if state.circuit_breaker_open else 0)


# ---------- Structured logging ----------

_SEVERITY_MAP = {
    "debug": "DEBUG", "info": "INFO", "warning": "WARNING",
    "warn": "WARNING", "error": "ERROR", "critical": "CRITICAL", "fatal": "CRITICAL",
}


def _make_gcp_processor(service_name: str, project_id: str):
    """Build a structlog processor that ships structured entries to GCP Cloud Logging.

    Inserted BEFORE JSONRenderer so it receives the event dict (not a JSON string),
    giving GCP structured fields instead of a raw string blob. Swallows all errors
    so a GCP outage never crashes the service.
    """
    client = gcp_logging.Client(project=project_id)
    gcp_logger = client.logger(f"sre-hig-{service_name}")

    def _processor(_logger, _method, event_dict: dict) -> dict:
        severity = _SEVERITY_MAP.get(str(event_dict.get("level", "info")).lower(), "INFO")
        try:
            gcp_logger.log_struct(dict(event_dict), severity=severity)
        except Exception as exc:
            # Print once per unique error type so repeated failures don't flood stdout.
            _key = type(exc).__name__
            if _key not in _processor._seen:
                _processor._seen.add(_key)
                print(f"[gcp-logging] write failed ({_key}): {exc}", file=sys.stderr, flush=True)
        return event_dict

    _processor._seen: set = set()

    return _processor


def configure_logging(service_name: str) -> structlog.stdlib.BoundLogger:
    """JSON logs to stdout + GCP Cloud Logging when GCP_LOG_PROJECT_ID is set.

    GCP processor is inserted before JSONRenderer so GCP receives structured
    fields. Stdout path is unchanged for local development and Promtail.
    """
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)

    processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.dict_tracebacks,
        structlog.processors.EventRenamer("message"),
    ]

    # Accept GCP_LOG_PROJECT_ID or fall back to PROJECT_ID / GOOGLE_CLOUD_PROJECT
    # (same project already used for Vertex AI) so no extra config is needed.
    gcp_project_id = (
        os.environ.get("GCP_LOG_PROJECT_ID")
        or os.environ.get("PROJECT_ID")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or ""
    )
    if gcp_project_id and _GCP_LOGGING_AVAILABLE:
        try:
            processors.append(_make_gcp_processor(service_name, gcp_project_id))
        except Exception as exc:
            print(f"[gcp-logging] init failed, using local logs only: {exc}", file=sys.stderr, flush=True)

    processors.append(structlog.processors.JSONRenderer())

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger().bind(service=service_name)


# ---------- Routers: admin + status (shared across all 3 services) ----------

def make_admin_router(state: ServiceState, metrics: dict[str, Any], logger) -> APIRouter:
    """Endpoints L1 runbooks hit to remediate (or this demo hits to inject failures)."""
    router = APIRouter(prefix="/admin", tags=["admin"])

    @router.post("/crash")
    async def crash(payload: dict | None = None):
        mode = (payload or {}).get("mode", "auto")
        if mode == "auto":
            mode = {"photos": "pool", "payments": "latency", "identity": "oom"}.get(state.service, "pool")
        if mode == "pool":
            state.healthy = False
            state.pool_used = state.pool_max
            state.p99_ms = 2400
            state.add_log("ERROR", "connection acquire timeout", reason="pool_exhausted")
        elif mode == "latency":
            state.healthy = False
            state.p99_ms = 1820
            state.add_log("WARN", "slow downstream call to metadata-db", latency_ms=1820)
        elif mode == "oom":
            state.healthy = False
            state.memory_mb = state.memory_limit_mb
            state.add_log("FATAL", "OOMKilled signal received")
        elif mode == "tls_failure":
            state.healthy = False
            state.tls_cert_expired = True
            state.p99_ms = 2800
            state.memory_mb = 340   # reset to baseline — TLS failure doesn't cause OOM
            state.pool_used = 45    # reset to baseline — TLS fails before DB connections
            state.add_log("CRITICAL", "ssl.SSLCertVerificationError: certificate has expired (_ssl.c:1129)",
                          cert_subject="CN=identity.internal",
                          cert_expiry="2026-06-15T03:42:00Z",
                          issuer="Let's Encrypt Authority X3")
            state.add_log("ERROR", "TLS handshake failed",
                          endpoint="/auth/login",
                          error="ERR_CERT_DATE_INVALID",
                          client_ip="10.0.1.42",
                          cert_expiry="2026-06-15T03:42:00Z")
            state.add_log("ERROR", "TLS handshake failed",
                          endpoint="/auth/session",
                          error="ERR_CERT_DATE_INVALID",
                          client_ip="10.0.1.55")
            state.add_log("ERROR", "certificate verify failed: certificate has expired",
                          error_code=20,
                          depth=0,
                          reason="certificate_expired",
                          issuer="Let's Encrypt Authority X3")
            state.add_log("ERROR", "Failed to establish secure connection to identity service",
                          reason="CERTIFICATE_VERIFY_FAILED",
                          endpoint="https://identity.internal:443",
                          retry_attempt=3)
            state.add_log("WARNING", "downstream auth requests failing — identity TLS endpoint unreachable",
                          affected_services="payments,photos",
                          downstream_errors="401 Unauthorized",
                          tls_error="ERR_CERT_DATE_INVALID")
        state.last_crash_at = time.time()
        logger.warning("admin_crash", mode=mode)
        return {"crashed": True, "mode": mode, "state": _state_snapshot(state)}

    @router.post("/restart")
    async def restart():
        # Reset to baseline (pool_max sticks if it was scaled up — operators expect that)
        state.healthy = True
        state.pool_used = 45
        state.p99_ms = 120
        state.memory_mb = 340
        state.circuit_breaker_open = False
        state.tls_cert_expired = False
        state.last_restart_at = time.time()
        # Mimic a real restart losing in-memory state — clear the log buffer.
        # Logs from before the restart still live in Loki (24h retention) but
        # the direct /logs endpoint returns only post-restart entries.
        state.recent_logs.clear()
        state.add_log("INFO", "service restarted", reason="admin_restart")
        logger.info("admin_restart")
        return {"restarted": True, "state": _state_snapshot(state)}

    @router.post("/scale")
    async def scale():
        old = state.pool_max
        state.pool_max = max(state.pool_max * 2, 400)
        state.pool_used = int(state.pool_used * 0.4)
        state.add_log("INFO", "connection pool scaled", pool_max_old=old, pool_max_new=state.pool_max)
        logger.info("admin_scale", pool_max_old=old, pool_max_new=state.pool_max)
        return {"scaled": True, "pool_max": state.pool_max}

    @router.post("/circuit_breaker")
    async def circuit_breaker():
        state.circuit_breaker_open = True
        state.p99_ms = 280
        state.add_log("INFO", "circuit breaker opened, failing fast on downstream")
        logger.info("admin_circuit_breaker", state="open")
        return {"circuit_breaker": "open"}

    @router.post("/memory_bump")
    async def memory_bump():
        old = state.memory_limit_mb
        state.memory_limit_mb = state.memory_limit_mb * 2
        state.memory_mb = int(state.memory_mb * 0.6)
        state.add_log("INFO", "memory limit increased", memory_limit_old=old, memory_limit_new=state.memory_limit_mb)
        logger.info("admin_memory_bump", memory_limit_old=old, memory_limit_new=state.memory_limit_mb)
        return {"memory_limit_mb": state.memory_limit_mb}

    # ---- Phase 5: gradual degradation injectors ----
    # Real services don't fail instantly; they degrade. These endpoints kick off
    # background coroutines that mutate state over a 20-30s window so dashboards
    # show realistic ramp-up patterns.

    @router.post("/inject/slow_leak")
    async def inject_slow_leak(duration_seconds: int = 30):
        """Pool_used climbs linearly over `duration_seconds` until saturated."""
        asyncio.create_task(_slow_pool_leak(state, logger, duration_seconds))
        state.add_log("INFO", "slow_leak injection started", duration_s=duration_seconds)
        logger.info("inject_slow_leak", duration_seconds=duration_seconds)
        return {"injected": "slow_leak", "duration_seconds": duration_seconds,
                "service": state.service}

    @router.post("/inject/latency_climb")
    async def inject_latency_climb(duration_seconds: int = 20):
        """p99 latency climbs linearly until past SLO."""
        asyncio.create_task(_latency_climb(state, logger, duration_seconds))
        state.add_log("INFO", "latency_climb injection started")
        logger.info("inject_latency_climb", duration_seconds=duration_seconds)
        return {"injected": "latency_climb", "duration_seconds": duration_seconds,
                "service": state.service}

    @router.post("/inject/memory_pressure")
    async def inject_memory_pressure(duration_seconds: int = 25):
        """Memory_mb climbs until OOMKilled."""
        asyncio.create_task(_memory_pressure(state, logger, duration_seconds))
        state.add_log("INFO", "memory_pressure injection started")
        logger.info("inject_memory_pressure", duration_seconds=duration_seconds)
        return {"injected": "memory_pressure", "duration_seconds": duration_seconds,
                "service": state.service}

    return router


# ---------- Gradual-degradation worker coroutines (Phase 5) ----------

async def _slow_pool_leak(state: ServiceState, logger, duration_seconds: int) -> None:
    """Linear climb of pool_used to pool_max over `duration_seconds`, then ERROR burst."""
    initial = state.pool_used
    target = state.pool_max
    delta = max(target - initial, 1)
    steps = max(duration_seconds, 1)
    step_size = max(delta // steps, 1)
    for i in range(steps):
        await asyncio.sleep(1)
        state.pool_used = min(state.pool_used + step_size, state.pool_max)
        # Latency climbs proportionally
        state.p99_ms = min(int(state.p99_ms + (2400 - 120) / steps), 2400)
        if state.pool_used >= state.pool_max:
            state.healthy = False
            state.add_log("ERROR", "connection acquire timeout", iteration=i)
            logger.error("pool_exhausted_after_leak")
            for _ in range(3):       # emit a few errors so dashboards spike
                state.add_log("ERROR", "connection acquire timeout")
            break


async def _latency_climb(state: ServiceState, logger, duration_seconds: int) -> None:
    """p99 climbs linearly to above SLO over `duration_seconds`."""
    initial = state.p99_ms
    target = 1820
    steps = max(duration_seconds, 1)
    step = max((target - initial) // steps, 1)
    for i in range(steps):
        await asyncio.sleep(1)
        state.p99_ms = min(state.p99_ms + step, target)
        if state.p99_ms >= 1500 and state.healthy:
            state.healthy = False
            state.add_log("WARN", "slow downstream call to metadata-db",
                          latency_ms=state.p99_ms)
            logger.warning("latency_threshold_breached", p99_ms=state.p99_ms)


async def _memory_pressure(state: ServiceState, logger, duration_seconds: int) -> None:
    """memory_mb climbs to memory_limit_mb over `duration_seconds`, then OOM."""
    initial = state.memory_mb
    target = state.memory_limit_mb
    steps = max(duration_seconds, 1)
    step = max((target - initial) // steps, 1)
    for i in range(steps):
        await asyncio.sleep(1)
        state.memory_mb = min(state.memory_mb + step, state.memory_limit_mb)
        if state.memory_mb >= state.memory_limit_mb:
            state.healthy = False
            state.add_log("FATAL", "OOMKilled signal received", iteration=i)
            logger.critical("oom_killed")
            for _ in range(3):
                state.add_log("FATAL", "OOMKilled signal received")
            break


def make_status_router(state: ServiceState, metrics: dict[str, Any]) -> APIRouter:
    """The four endpoints the L1 platform's tools already hit. Contract preserved."""
    router = APIRouter(tags=["status"])

    @router.get("/healthz")
    async def healthz():
        if state.healthy:
            return {"status": "ok", "service": state.service}
        return Response(content='{"status":"unhealthy"}', status_code=503, media_type="application/json")

    @router.get("/metrics")
    async def metrics_json():
        """Legacy JSON endpoint - exact same shape the existing L1 tools read."""
        return _state_snapshot(state)

    @router.get("/metrics/prom")
    async def metrics_prometheus():
        """Prometheus exposition format - what Phase 2's prometheus_query tool will hit."""
        sync_gauges(metrics, state)
        body = generate_latest(metrics["registry"])
        return Response(content=body, media_type=CONTENT_TYPE_LATEST)

    @router.get("/logs")
    async def logs():
        """Legacy JSON log endpoint - exact same shape the existing L1 tools read."""
        return {
            "service": state.service,
            "lines": list(state.recent_logs),
            "mode": state.current_failure_mode(),
        }

    @router.get("/topology")
    async def topology():
        return TOPOLOGY.get(state.service, {"upstream": [], "downstream": []})

    return router


# ---------- Request middleware (metrics + structured logging) ----------

def install_request_middleware(app, state: ServiceState, metrics: dict[str, Any], logger) -> None:
    """Counts requests, observes latencies, increments inflight, logs in JSON."""

    @app.middleware("http")
    async def _middleware(request: Request, call_next: Callable):
        # Skip metric noise on the scrape endpoint itself
        if request.url.path == "/metrics/prom":
            return await call_next(request)

        metrics["inflight"].labels(service=state.service).inc()
        start = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            elapsed = time.perf_counter() - start
            metrics["inflight"].labels(service=state.service).dec()
            metrics["request_count"].labels(
                service=state.service,
                method=request.method,
                endpoint=request.url.path,
                status=str(status_code),
            ).inc()
            metrics["request_latency"].labels(
                service=state.service, endpoint=request.url.path
            ).observe(elapsed)
            logger.info(
                "request",
                method=request.method,
                path=request.url.path,
                status=status_code,
                latency_ms=int(elapsed * 1000),
            )


# ---------- Helpers ----------

def _state_snapshot(state: ServiceState) -> dict[str, Any]:
    """Exact shape the existing L1 query_metrics tool expects."""
    return {
        "service": state.service,
        "healthy": state.healthy,
        "pool_used": state.pool_used,
        "pool_max": state.pool_max,
        "p99_ms": state.p99_ms,
        "memory_mb": state.memory_mb,
        "memory_limit_mb": state.memory_limit_mb,
        "circuit_breaker_open": state.circuit_breaker_open,
        "tls_cert_expired": state.tls_cert_expired,
        "failure_mode": state.current_failure_mode(),
    }


def build_app_skeleton(service_name: str):
    """Convenience: build the shared bits (state, metrics, logger, routers).

    Returns (state, metrics, logger, admin_router, status_router) ready to be
    wired into a per-service FastAPI app along with its business endpoints.
    """
    from fastapi import FastAPI  # local import keeps the module importable without fastapi

    state = ServiceState(service=service_name)
    metrics = make_metrics(service_name)
    logger = configure_logging(service_name)
    state._logger = logger  # wire logger so add_log emits to Loki + GCP
    admin_router = make_admin_router(state, metrics, logger)
    status_router = make_status_router(state, metrics)
    return state, metrics, logger, admin_router, status_router


def env_service_name(default: str) -> str:
    return os.environ.get("SERVICE_NAME", default)
