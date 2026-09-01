"""Payments backend service.

Real business endpoints simulating payment charges + refunds. Failures cascade
into business-API behaviour exactly like a real service would degrade.

Run with:    uvicorn services.payments:app --host 0.0.0.0 --port 5002
"""
import asyncio
import time
import uuid
from typing import Any, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from flask_app.common import build_app_skeleton, env_service_name, install_request_middleware

SERVICE = env_service_name("payments")
state, metrics, logger, admin_router, status_router = build_app_skeleton(SERVICE)

app = FastAPI(title=f"{SERVICE}-api (mock)", version="0.1.0")
app.include_router(admin_router)
app.include_router(status_router)
install_request_middleware(app, state, metrics, logger)


# ---------- In-memory data ----------
PAYMENTS: dict[str, dict[str, Any]] = {}
IDEMPOTENCY_INDEX: dict[str, str] = {}      # idempotency_key -> payment_id


# ---------- Models ----------

class ChargeRequest(BaseModel):
    user_id: str
    amount_cents: int = Field(..., ge=1, le=1_000_000)
    currency: str = "USD"
    idempotency_key: Optional[str] = None
    description: Optional[str] = None


class ChargeResponse(BaseModel):
    payment_id: str
    status: str            # captured | declined | pending | failed
    amount_cents: int


# ---------- Business endpoints ----------

@app.post("/payments/charge", response_model=ChargeResponse)
async def charge(req: ChargeRequest):
    """Initiate a payment. Honours idempotency keys + simulated failure modes."""
    await _apply_simulated_latency()

    # Idempotency check
    if req.idempotency_key and req.idempotency_key in IDEMPOTENCY_INDEX:
        existing = PAYMENTS[IDEMPOTENCY_INDEX[req.idempotency_key]]
        metrics["business_op"].labels(service=SERVICE, op="charge", outcome="idempotent_replay").inc()
        return ChargeResponse(payment_id=existing["payment_id"], status=existing["status"],
                              amount_cents=existing["amount_cents"])

    if state.circuit_breaker_open:
        metrics["business_op"].labels(service=SERVICE, op="charge", outcome="rejected_cb").inc()
        raise HTTPException(503, "circuit breaker open - payment processor unreachable")

    if state.pool_used >= state.pool_max:
        metrics["business_op"].labels(service=SERVICE, op="charge", outcome="pool_exhausted").inc()
        state.add_log("ERROR", "payments-db pool exhausted", user_id=req.user_id)
        raise HTTPException(503, "payments database pool exhausted")

    if not state.healthy and state.p99_ms > 1500:
        # High-latency mode — some charges intermittently fail
        if (time.time_ns() % 4) == 0:
            metrics["business_op"].labels(service=SERVICE, op="charge", outcome="timeout").inc()
            state.add_log("ERROR", "timeout calling payment-processor")
            raise HTTPException(504, "payment processor timeout")

    payment_id = f"pay-{uuid.uuid4().hex[:12]}"
    record = {
        "payment_id": payment_id, "user_id": req.user_id,
        "amount_cents": req.amount_cents, "currency": req.currency,
        "status": "captured", "created_at": time.time(),
        "description": req.description,
    }
    PAYMENTS[payment_id] = record
    if req.idempotency_key:
        IDEMPOTENCY_INDEX[req.idempotency_key] = payment_id
    state.pool_used = min(state.pool_used + 1, state.pool_max)
    metrics["business_op"].labels(service=SERVICE, op="charge", outcome="captured").inc()
    return ChargeResponse(payment_id=payment_id, status="captured", amount_cents=req.amount_cents)


@app.get("/payments/{payment_id}")
async def get_payment(payment_id: str):
    """Fetch a payment's status."""
    await _apply_simulated_latency()
    if payment_id not in PAYMENTS:
        metrics["business_op"].labels(service=SERVICE, op="get", outcome="not_found").inc()
        raise HTTPException(404, "payment not found")
    metrics["business_op"].labels(service=SERVICE, op="get", outcome="ok").inc()
    return PAYMENTS[payment_id]


@app.post("/payments/{payment_id}/refund")
async def refund(payment_id: str):
    """Refund a previously-captured payment."""
    await _apply_simulated_latency()
    if payment_id not in PAYMENTS:
        raise HTTPException(404, "payment not found")
    p = PAYMENTS[payment_id]
    if p["status"] != "captured":
        raise HTTPException(409, f"cannot refund payment in status '{p['status']}'")
    p["status"] = "refunded"
    p["refunded_at"] = time.time()
    state.pool_used = max(state.pool_used - 1, 0)
    metrics["business_op"].labels(service=SERVICE, op="refund", outcome="ok").inc()
    return {"refunded": True, "payment_id": payment_id}


# ---------- Helpers ----------

async def _apply_simulated_latency():
    if not state.healthy and state.p99_ms > 1500:
        await asyncio.sleep(min(state.p99_ms / 1000.0, 3.0))


@app.get("/")
def root():
    return {
        "service": SERVICE,
        "endpoints": [
            "/payments/charge", "/payments/{id}", "/payments/{id}/refund",
            "/healthz", "/metrics", "/metrics/prom", "/logs", "/topology",
            "/admin/crash", "/admin/restart", "/admin/scale",
            "/admin/circuit_breaker", "/admin/memory_bump",
        ],
    }
