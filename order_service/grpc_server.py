"""
order_service/grpc_server.py
gRPC server for OrderService — orchestrates Payment + Inventory via gRPC stubs.
Circuit Breaker + Vector Clock injected on every outgoing call.
Generated stubs assume you ran protoc for all 3 protos.
"""

import time
import uuid
import logging
import grpc
from concurrent import futures

import order_pb2
import order_pb2_grpc

# Stubs for downstream services
import payment_pb2
import payment_pb2_grpc
import inventory_pb2
import inventory_pb2_grpc

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from vector_clock import VectorClock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("OrderService")

# ─── Service discovery (override via env vars) ────────────────────────────────
PAYMENT_ADDR   = os.getenv("PAYMENT_ADDR",   "localhost:50052")
INVENTORY_ADDR = os.getenv("INVENTORY_ADDR", "localhost:50053")

PROCESSES = ["order_service", "payment_service", "inventory_service"]

# ─── Shared state ─────────────────────────────────────────────────────────────

_vector_clock = VectorClock("order_service", PROCESSES)

_payment_cb = CircuitBreaker(
    name="payment-cb",
    failure_threshold=5,
    recovery_timeout=30.0,
    half_open_max_calls=1,
    fallback_function=lambda *a, **kw: {
        "transaction_id": "",
        "status": "DEGRADED",
        "message": "Payment queued (CB open)",
    },
)

_inventory_cb = CircuitBreaker(
    name="inventory-cb",
    failure_threshold=5,
    recovery_timeout=30.0,
    half_open_max_calls=1,
    fallback_function=lambda *a, **kw: {
        "available": False,
        "stock_level": -1,
        "message": "Inventory unavailable (CB open)",
    },
)

# Simple in-memory order store
_orders: dict = {}

# ─── Downstream gRPC callers ──────────────────────────────────────────────────

def _call_payment(order_id: str, amount: float, customer_id: str, vc: dict) -> dict:
    with grpc.insecure_channel(PAYMENT_ADDR) as ch:
        stub = payment_pb2_grpc.PaymentServiceStub(ch)
        req = payment_pb2.ComputeRequest(
            order_id=order_id,
            amount=amount,
            customer_id=customer_id,
            vector_clock=[
                payment_pb2.VectorClockEntry(process_id=pid, counter=cnt)
                for pid, cnt in vc.items()
            ],
        )
        resp = stub.Compute(req, timeout=5.0)
    return {
        "transaction_id": resp.transaction_id,
        "status":         resp.status,
        "message":        resp.message,
        "vector_clock":   {e.process_id: e.counter for e in resp.vector_clock},
    }


def _call_check_stock(product_id: str, quantity: int, vc: dict) -> dict:
    with grpc.insecure_channel(INVENTORY_ADDR) as ch:
        stub = inventory_pb2_grpc.InventoryServiceStub(ch)
        req = inventory_pb2.CheckStockRequest(
            product_id=product_id,
            quantity=quantity,
            vector_clock=[
                inventory_pb2.VectorClockEntry(process_id=pid, counter=cnt)
                for pid, cnt in vc.items()
            ],
        )
        resp = stub.CheckStock(req, timeout=5.0)
    return {
        "available":    resp.available,
        "stock_level":  resp.stock_level,
        "vector_clock": {e.process_id: e.counter for e in resp.vector_clock},
    }

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _vc_from_proto(entries) -> dict:
    return {e.process_id: e.counter for e in entries}


def _vc_to_proto(vc: dict):
    return [
        order_pb2.VectorClockEntry(process_id=pid, counter=cnt)
        for pid, cnt in vc.items()
    ]

# ─── Servicer ─────────────────────────────────────────────────────────────────

class OrderServicer(order_pb2_grpc.OrderServiceServicer):

    def PlaceOrder(self, request, context):
        order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        logger.info(f"PlaceOrder | order_id={order_id} customer={request.customer_id}")

        # 1. Tick vector clock for this internal event
        vc = _vector_clock.send()

        # 2. Check stock for every item (via CB)
        total = 0.0
        for item in request.items:
            stock_result = _inventory_cb.call(
                _call_check_stock,
                product_id=item.product_id,
                quantity=item.quantity,
                vc=vc,
            )
            if not stock_result.get("available", False):
                logger.warning(f"Stock unavailable for {item.product_id}")
                return order_pb2.PlaceOrderResponse(
                    order_id=order_id,
                    status="FAILED",
                    total_amount=0.0,
                    vector_clock=_vc_to_proto(vc),
                )
            # Merge returned VC
            returned_vc = stock_result.get("vector_clock", {})
            if returned_vc:
                vc = _vector_clock.receive(returned_vc)

            total += item.unit_price * item.quantity

        # 3. Charge payment (via CB)
        pay_result = _payment_cb.call(
            _call_payment,
            order_id=order_id,
            amount=total,
            customer_id=request.customer_id,
            vc=vc,
        )

        # Merge payment VC
        pay_vc = pay_result.get("vector_clock", {})
        if pay_vc:
            vc = _vector_clock.receive(pay_vc)

        status = pay_result.get("status", "FAILED")

        # 4. Persist order
        _orders[order_id] = {
            "customer_id":    request.customer_id,
            "status":         status,
            "total_amount":   total,
            "transaction_id": pay_result.get("transaction_id", ""),
            "created_at":     int(time.time()),
        }

        logger.info(
            f"PlaceOrder DONE | order_id={order_id} status={status} "
            f"total={total} vc={vc}"
        )

        return order_pb2.PlaceOrderResponse(
            order_id=order_id,
            status=status,
            total_amount=total,
            vector_clock=_vc_to_proto(vc),
        )

    def GetOrder(self, request, context):
        order = _orders.get(request.order_id)
        if order is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Order {request.order_id} not found")
            return order_pb2.GetOrderResponse()

        return order_pb2.GetOrderResponse(
            order_id=request.order_id,
            customer_id=order["customer_id"],
            status=order["status"],
            total_amount=order["total_amount"],
            created_at=order["created_at"],
        )

    def CancelOrder(self, request, context):
        order = _orders.get(request.order_id)
        if order is None:
            return order_pb2.CancelOrderResponse(
                success=False,
                message=f"Order {request.order_id} not found",
            )
        order["status"] = "CANCELLED"
        logger.info(f"CancelOrder | order_id={request.order_id} reason={request.reason}")
        return order_pb2.CancelOrderResponse(
            success=True,
            message=f"Order {request.order_id} cancelled",
        )


# ─── Server entry-point ───────────────────────────────────────────────────────

def serve(port: int = 50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_pb2_grpc.add_OrderServiceServicer_to_server(OrderServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"OrderService gRPC server listening on port {port}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down OrderService...")
        server.stop(grace=5)


if __name__ == "__main__":
    serve()
