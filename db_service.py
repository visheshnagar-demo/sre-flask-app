"""Mock PostgreSQL target service — a thin FastAPI wrapper around a real Postgres.

It exposes the SAME interface contract as the other mock services
(/admin/crash, /admin/restart, /healthz, /metrics, /logs) so the L1 platform
treats it identically — but every action is REAL SQL against the `postgres`
docker-compose service. The database remediation agent fixes the database via
direct SQL (psycopg3); this wrapper only injects failures and reports health.

v1 failure mode: connection_exhaustion — open and hold a batch of
idle-in-transaction sessions until the database is near its connection ceiling.

Run:  uvicorn flask_app.db_service:app --host 0.0.0.0 --port 5004
"""
from __future__ import annotations

import json
import os
import threading
import time
from collections import deque

import psycopg  # psycopg3
from fastapi import FastAPI, Response

SERVICE = os.environ.get("SERVICE_NAME", "db-service")
PG_DSN = os.environ.get("PG_DSN", "postgresql://sre_user:sre_pass@localhost:5432/sre_demo")

# How many connections to hold on a connection-exhaustion injection. Kept below
# max_connections so the remediation agent (a superuser) can still connect via a
# reserved slot to run the fix — exactly how a DBA keeps an admin connection.
HOLD_CONNECTIONS = int(os.environ.get("HOLD_CONNECTIONS", "44"))
# /healthz reports unhealthy once total connections reach this fraction of max.
HEALTHY_MAX_RATIO = float(os.environ.get("DB_HEALTHY_MAX_RATIO", "0.8"))

app = FastAPI(title=f"{SERVICE} (mock postgres wrapper)", version="0.1.0")

_held: list = []                 # held psycopg connections — the injected leak
_lock = threading.Lock()
_logs: deque[str] = deque(maxlen=200)


def _log(level: str, msg: str, **kw) -> None:
    ts = time.strftime("%H:%M:%S")
    suffix = ("  " + " ".join(f"{k}={v}" for k, v in kw.items())) if kw else ""
    _logs.appendleft(f"{ts} {level} {msg}{suffix}")


def _admin_conn():
    """Short-lived admin connection for health / metrics / cleanup queries."""
    return psycopg.connect(PG_DSN, connect_timeout=5, autocommit=True)


def _conn_stats() -> dict:
    with _admin_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT state, count(*) FROM pg_stat_activity "
            "WHERE datname = current_database() GROUP BY state"
        )
        by_state = {(r[0] or "unknown"): int(r[1]) for r in cur.fetchall()}
        cur.execute("SELECT current_setting('max_connections')::int")
        max_conn = int(cur.fetchone()[0])
    return {"by_state": by_state, "total": sum(by_state.values()), "max_connections": max_conn}


@app.post("/admin/crash")
def crash(mode: str = "connection_exhaustion"):
    """Inject a failure. v1 supports mode=connection_exhaustion."""
    if mode != "connection_exhaustion":
        return {"ok": False, "error": f"unsupported mode '{mode}' (v1 supports connection_exhaustion)"}
    opened = 0
    with _lock:
        for _ in range(HOLD_CONNECTIONS):
            try:
                c = psycopg.connect(PG_DSN, connect_timeout=5)  # autocommit=False (default)
                cur = c.cursor()
                cur.execute("SELECT 1")   # starts a transaction → leaves it 'idle in transaction'
                _held.append(c)
                opened += 1
            except Exception as exc:
                _log("ERROR", "could not open hold connection", error=str(exc))
                break
    _log("ERROR", "connection exhaustion injected", held=opened)
    return {"ok": True, "mode": mode, "held_connections": opened}


@app.post("/admin/restart")
def restart():
    """Release held connections and terminate any lingering idle sessions."""
    closed = 0
    with _lock:
        for c in _held:
            try:
                c.close()
                closed += 1
            except Exception:
                pass
        _held.clear()
    try:
        with _admin_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE pid <> pg_backend_pid() AND datname = current_database() "
                "AND state IN ('idle','idle in transaction','idle in transaction (aborted)')"
            )
    except Exception as exc:
        _log("WARNING", "restart cleanup failed", error=str(exc))
    _log("INFO", "service restarted — held connections released", closed=closed)
    return {"ok": True, "closed_connections": closed}


@app.get("/healthz")
def healthz():
    """Real-SQL liveness: healthy only when connection usage is below threshold."""
    try:
        stats = _conn_stats()
    except Exception:
        return Response(content='{"healthy": false, "service": "%s", "error": "cannot connect"}' % SERVICE,
                        status_code=503, media_type="application/json")
    limit = stats["max_connections"] * HEALTHY_MAX_RATIO
    healthy = stats["total"] < limit
    body = {
        "healthy": healthy,
        "service": SERVICE,
        "total_connections": stats["total"],
        "max_connections": stats["max_connections"],
        "threshold": int(limit),
    }
    if healthy:
        return body
    return Response(content=json.dumps(body), status_code=503, media_type="application/json")


@app.get("/metrics")
def metrics():
    """Real-SQL connection metrics snapshot."""
    try:
        stats = _conn_stats()
    except Exception as exc:
        return {"service": SERVICE, "error": str(exc)}
    limit = stats["max_connections"] * HEALTHY_MAX_RATIO
    return {
        "service": SERVICE,
        "total_connections": stats["total"],
        "max_connections": stats["max_connections"],
        "by_state": stats["by_state"],
        "held_by_wrapper": len(_held),
        "healthy": stats["total"] < limit,
    }


@app.get("/logs")
def logs():
    return {"service": SERVICE, "lines": list(_logs)}


@app.get("/")
def root():
    return {
        "service": SERVICE,
        "endpoints": [
            "/admin/crash?mode=connection_exhaustion", "/admin/restart",
            "/healthz", "/metrics", "/logs",
        ],
    }
