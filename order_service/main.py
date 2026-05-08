"""
order_service/main.py - run from inside order_service\ folder:
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

import os, sys, time, uuid, logging
from typing import List, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import grpc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import payment_pb2, payment_pb2_grpc
import inventory_pb2, inventory_pb2_grpc

from circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from vector_clock import VectorClock

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("order_service")

PAYMENT_ADDR   = os.getenv("PAYMENT_ADDR",   "localhost:50052")
INVENTORY_ADDR = os.getenv("INVENTORY_ADDR", "localhost:50053")
PROCESSES      = ["order_service", "payment_service", "inventory_service"]

_vc = VectorClock("order_service", PROCESSES)

_payment_cb = CircuitBreaker(name="payment-cb", failure_threshold=5, recovery_timeout=30.0, half_open_max_calls=1,
    fallback_function=lambda **kw: {"transaction_id": "", "status": "DEGRADED", "message": "Payment queued — circuit open", "vector_clock": {}})

_inventory_cb = CircuitBreaker(name="inventory-cb", failure_threshold=5, recovery_timeout=30.0, half_open_max_calls=1,
    fallback_function=lambda **kw: {"available": False, "stock_level": -1, "vector_clock": {}})

_orders: Dict[str, dict] = {}
app = FastAPI(title="Order Service", version="1.0.0")

class OrderItem(BaseModel):
    product_id: str
    quantity:   int
    unit_price: float

class PlaceOrderRequest(BaseModel):
    customer_id: str
    items:       List[OrderItem]

def _vc_to_payment_proto(vc):
    return [payment_pb2.VectorClockEntry(process_id=p, counter=c) for p, c in vc.items()]

def _vc_to_inventory_proto(vc):
    return [inventory_pb2.VectorClockEntry(process_id=p, counter=c) for p, c in vc.items()]

def _vc_from_proto(entries):
    return {e.process_id: e.counter for e in entries}

def _grpc_check_stock(product_id, quantity, vc):
    with grpc.insecure_channel(INVENTORY_ADDR) as ch:
        resp = inventory_pb2_grpc.InventoryServiceStub(ch).CheckStock(
            inventory_pb2.CheckStockRequest(product_id=product_id, quantity=quantity, vector_clock=_vc_to_inventory_proto(vc)), timeout=5.0)
    return {"available": resp.available, "stock_level": resp.stock_level, "vector_clock": _vc_from_proto(resp.vector_clock)}

def _grpc_compute_payment(order_id, amount, customer_id, vc):
    with grpc.insecure_channel(PAYMENT_ADDR) as ch:
        resp = payment_pb2_grpc.PaymentServiceStub(ch).Compute(
            payment_pb2.ComputeRequest(order_id=order_id, amount=amount, customer_id=customer_id, vector_clock=_vc_to_payment_proto(vc)), timeout=5.0)
    return {"transaction_id": resp.transaction_id, "status": resp.status, "message": resp.message, "vector_clock": _vc_from_proto(resp.vector_clock)}

@app.get("/health")
def health():
    return {"service": "order_service", "status": "UP",
            "circuit_breakers": {"payment": _payment_cb.status(), "inventory": _inventory_cb.status()},
            "vector_clock": _vc.snapshot()}

@app.post("/orders", status_code=201)
def place_order(body: PlaceOrderRequest):
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    vc = _vc.send()
    total = 0.0
    for item in body.items:
        try:
            result = _inventory_cb.call(_grpc_check_stock, product_id=item.product_id, quantity=item.quantity, vc=vc)
        except CircuitBreakerOpenError as e:
            raise HTTPException(status_code=503, detail=str(e))
        if result.get("vector_clock"):
            vc = _vc.receive(result["vector_clock"])
        if not result.get("available", False):
            raise HTTPException(status_code=409, detail=f"Insufficient stock for {item.product_id}")
        total += item.unit_price * item.quantity
    try:
        pay = _payment_cb.call(_grpc_compute_payment, order_id=order_id, amount=total, customer_id=body.customer_id, vc=vc)
    except CircuitBreakerOpenError as e:
        raise HTTPException(status_code=503, detail=str(e))
    if pay.get("vector_clock"):
        vc = _vc.receive(pay["vector_clock"])
    status = pay.get("status", "FAILED")
    _orders[order_id] = {"customer_id": body.customer_id, "items": [i.dict() for i in body.items],
        "status": status, "total_amount": round(total, 2), "transaction_id": pay.get("transaction_id", ""),
        "message": pay.get("message", ""), "created_at": int(time.time()), "vector_clock": vc}
    return {"order_id": order_id, "status": status, "total_amount": round(total, 2),
            "transaction_id": pay.get("transaction_id", ""), "message": pay.get("message", ""), "vector_clock": vc}

@app.get("/orders/{order_id}")
def get_order(order_id: str):
    order = _orders.get(order_id)
    if not order:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    return {"order_id": order_id, **order}

@app.get("/orders")
def list_orders():
    return {"orders": list(_orders.values()), "total": len(_orders)}

@app.get("/cb/status")
def cb_status():
    return {"payment_cb": _payment_cb.status(), "inventory_cb": _inventory_cb.status()}

@app.post("/cb/reset")
def cb_reset():
    _payment_cb.reset()
    _inventory_cb.reset()
    return {"message": "All circuit breakers reset"}
