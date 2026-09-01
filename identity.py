"""AppleID-style identity / auth backend service.

Real business endpoints simulating login, session validation, logout. Failures
cascade exactly like a real service would:
  - OOM           -> logins fail with 500
  - high latency  -> session validation slow, auth flows time out
  - pool          -> login attempts queue and time out

Run with:    uvicorn services.identity:app --host 0.0.0.0 --port 5003
"""
import asyncio
import secrets
import time
from typing import Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from flask_app.common import build_app_skeleton, env_service_name, install_request_middleware

SERVICE = env_service_name("identity")
state, metrics, logger, admin_router, status_router = build_app_skeleton(SERVICE)

app = FastAPI(title=f"{SERVICE}-api (mock)", version="0.1.0")
app.include_router(admin_router)
app.include_router(status_router)
install_request_middleware(app, state, metrics, logger)


# ---------- In-memory data ----------
# (passwords accepted as long as username is non-empty — it's a mock)
SESSIONS: dict[str, dict[str, Any]] = {}        # token -> {user_id, expires_at, ...}
USER_SESSIONS: dict[str, list[str]] = {}        # user_id -> [token, ...]


# ---------- Models ----------

class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=128)
    password: str = Field(..., min_length=1, max_length=256)
    device_id: str | None = None


class LoginResponse(BaseModel):
    session_token: str
    user_id: str
    expires_at: float


class SessionInfo(BaseModel):
    valid: bool
    user_id: str | None = None
    expires_at: float | None = None


# ---------- Business endpoints ----------

@app.post("/auth/login", response_model=LoginResponse)
async def login(req: LoginRequest):
    """AppleID-style login. OOM kills it; pool exhaustion times it out."""
    await _apply_simulated_latency()

    if state.memory_mb >= state.memory_limit_mb:
        metrics["business_op"].labels(service=SERVICE, op="login", outcome="oom").inc()
        state.add_log("FATAL", "OOMKilled during auth", username=req.username)
        raise HTTPException(500, "out of memory")

    if state.pool_used >= state.pool_max:
        metrics["business_op"].labels(service=SERVICE, op="login", outcome="pool_exhausted").inc()
        state.add_log("ERROR", "identity-db pool exhausted", username=req.username)
        raise HTTPException(503, "identity-db pool exhausted")

    if state.circuit_breaker_open:
        metrics["business_op"].labels(service=SERVICE, op="login", outcome="rejected_cb").inc()
        raise HTTPException(503, "circuit breaker open")

    token = secrets.token_urlsafe(32)
    user_id = f"u-{req.username}"
    expires_at = time.time() + 24 * 3600       # 24h session
    SESSIONS[token] = {
        "user_id": user_id, "expires_at": expires_at,
        "issued_at": time.time(), "device_id": req.device_id,
    }
    USER_SESSIONS.setdefault(user_id, []).append(token)
    state.pool_used = min(state.pool_used + 1, state.pool_max)
    metrics["business_op"].labels(service=SERVICE, op="login", outcome="ok").inc()
    return LoginResponse(session_token=token, user_id=user_id, expires_at=expires_at)


@app.get("/auth/session/{token}", response_model=SessionInfo)
async def get_session(token: str):
    """Validate a session token."""
    await _apply_simulated_latency()
    sess = SESSIONS.get(token)
    if not sess:
        metrics["business_op"].labels(service=SERVICE, op="session", outcome="not_found").inc()
        return SessionInfo(valid=False)
    if sess["expires_at"] < time.time():
        metrics["business_op"].labels(service=SERVICE, op="session", outcome="expired").inc()
        return SessionInfo(valid=False, user_id=sess["user_id"], expires_at=sess["expires_at"])
    metrics["business_op"].labels(service=SERVICE, op="session", outcome="ok").inc()
    return SessionInfo(valid=True, user_id=sess["user_id"], expires_at=sess["expires_at"])


@app.post("/auth/logout")
async def logout(payload: dict):
    """Revoke a session token."""
    await _apply_simulated_latency()
    token = payload.get("session_token", "")
    if not token or token not in SESSIONS:
        raise HTTPException(404, "session not found")
    user_id = SESSIONS[token]["user_id"]
    del SESSIONS[token]
    USER_SESSIONS.get(user_id, []).remove(token) if token in USER_SESSIONS.get(user_id, []) else None
    state.pool_used = max(state.pool_used - 1, 0)
    metrics["business_op"].labels(service=SERVICE, op="logout", outcome="ok").inc()
    return {"logged_out": True}


# ---------- Helpers ----------

async def _apply_simulated_latency():
    if not state.healthy and state.p99_ms > 1500:
        await asyncio.sleep(min(state.p99_ms / 1000.0, 3.0))


@app.get("/")
def root():
    return {
        "service": SERVICE,
        "endpoints": [
            "/auth/login", "/auth/session/{token}", "/auth/logout",
            "/healthz", "/metrics", "/metrics/prom", "/logs", "/topology",
            "/admin/crash", "/admin/restart", "/admin/scale",
            "/admin/circuit_breaker", "/admin/memory_bump",
        ],
    }
