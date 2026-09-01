"""iCloud-Photos-style backend service.

Real business endpoints simulating photo upload + retrieval. Failures (when
inflicted via /admin/crash) cascade into business-API behaviour:
  - pool exhaustion  -> uploads return 503, latency spikes
  - high latency     -> all endpoints slow
  - oom              -> random 500s

Run with:    uvicorn services.photos:app --host 0.0.0.0 --port 5001
"""
import asyncio
import time
import uuid
from typing import Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from flask_app.common import build_app_skeleton, env_service_name, install_request_middleware

SERVICE = env_service_name("photos")
state, metrics, logger, admin_router, status_router = build_app_skeleton(SERVICE)

app = FastAPI(title=f"{SERVICE}-api (mock)", version="0.1.0")
app.include_router(admin_router)
app.include_router(status_router)
install_request_middleware(app, state, metrics, logger)


# ---------- In-memory data ----------
PHOTOS: dict[str, dict[str, Any]] = {}
USER_PHOTOS: dict[str, list[str]] = {}


# ---------- Request / response models ----------
class UploadRequest(BaseModel):
    user_id: str = Field(..., description="Apple ID")
    filename: str
    size_bytes: int = Field(..., ge=1, le=200_000_000)


class UploadResponse(BaseModel):
    photo_id: str
    status: str
    cdn_url: str


# ---------- Business endpoints ----------

@app.post("/photos/upload", response_model=UploadResponse)
async def upload(req: UploadRequest):
    """Receive a photo upload. Honours the simulated failure modes."""
    await _apply_simulated_latency()
    if state.circuit_breaker_open:
        metrics["business_op"].labels(service=SERVICE, op="upload", outcome="rejected_cb").inc()
        raise HTTPException(503, "circuit breaker open")
    if state.pool_used >= state.pool_max:
        metrics["business_op"].labels(service=SERVICE, op="upload", outcome="pool_exhausted").inc()
        state.add_log("ERROR", "connection acquire timeout", user_id=req.user_id)
        raise HTTPException(503, "connection acquire timeout")
    if state.memory_mb >= state.memory_limit_mb:
        metrics["business_op"].labels(service=SERVICE, op="upload", outcome="oom").inc()
        raise HTTPException(500, "out of memory")

    photo_id = f"ph-{uuid.uuid4().hex[:12]}"
    PHOTOS[photo_id] = {
        "photo_id": photo_id, "user_id": req.user_id, "filename": req.filename,
        "size_bytes": req.size_bytes, "uploaded_at": time.time(),
    }
    USER_PHOTOS.setdefault(req.user_id, []).append(photo_id)
    # Each upload consumes a pool slot for a moment (simulates connection use)
    state.pool_used = min(state.pool_used + 1, state.pool_max)
    metrics["business_op"].labels(service=SERVICE, op="upload", outcome="ok").inc()
    return UploadResponse(photo_id=photo_id, status="uploaded",
                          cdn_url=f"https://cdn.photos.local/{photo_id}")


@app.get("/photos/{photo_id}")
async def get_photo(photo_id: str):
    """Fetch a photo's metadata."""
    await _apply_simulated_latency()
    if photo_id not in PHOTOS:
        metrics["business_op"].labels(service=SERVICE, op="get", outcome="not_found").inc()
        raise HTTPException(404, "photo not found")
    metrics["business_op"].labels(service=SERVICE, op="get", outcome="ok").inc()
    return PHOTOS[photo_id]


@app.get("/photos/user/{user_id}")
async def list_user_photos(user_id: str, limit: int = 50):
    """List a user's photos (most recent first)."""
    await _apply_simulated_latency()
    photo_ids = USER_PHOTOS.get(user_id, [])
    items = [PHOTOS[pid] for pid in photo_ids[-limit:]][::-1]
    metrics["business_op"].labels(service=SERVICE, op="list", outcome="ok").inc()
    return {"user_id": user_id, "count": len(items), "photos": items}


@app.delete("/photos/{photo_id}")
async def delete_photo(photo_id: str):
    """Delete a photo (and free its pool slot)."""
    await _apply_simulated_latency()
    if photo_id not in PHOTOS:
        raise HTTPException(404, "photo not found")
    photo = PHOTOS.pop(photo_id)
    USER_PHOTOS.get(photo["user_id"], []).remove(photo_id)
    state.pool_used = max(state.pool_used - 1, 0)
    metrics["business_op"].labels(service=SERVICE, op="delete", outcome="ok").inc()
    return {"deleted": True, "photo_id": photo_id}


# ---------- Helpers ----------

async def _apply_simulated_latency():
    """When in degraded states, real wall-clock latency happens for callers."""
    if not state.healthy and state.p99_ms > 1500:
        await asyncio.sleep(min(state.p99_ms / 1000.0, 3.0))


@app.get("/")
def root():
    return {
        "service": SERVICE,
        "endpoints": [
            "/photos/upload", "/photos/{photo_id}", "/photos/user/{user_id}",
            "/healthz", "/metrics", "/metrics/prom", "/logs", "/topology",
            "/admin/crash", "/admin/restart", "/admin/scale",
            "/admin/circuit_breaker", "/admin/memory_bump",
        ],
    }
