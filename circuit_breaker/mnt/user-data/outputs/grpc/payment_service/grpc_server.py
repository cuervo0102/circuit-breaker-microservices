import time
import random
import logging
import threading
import grpc
from concurrent import futures

import payment_pb2
import payment_pb2_grpc

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from circuit_breaker import CircuitBreaker, CircuitBreakerOpenError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("PaymentService")


_crashed = False         
_crash_lock = threading.Lock()


def _payment_logic(order_id: str, amount: float, customer_id: str) -> dict:
  
    with _crash_lock:
        if _crashed:
            raise ConnectionError("Payment processor is DOWN (forced crash)")

    time.sleep(random.uniform(0.05, 0.20))

    if random.random() < 0.2:
        raise TimeoutError(f"Payment gateway timeout for order {order_id}")

    tx_id = f"TXN-{order_id}-{int(time.time())}"
    logger.info(f"Payment OK | order={order_id} amount={amount} tx={tx_id}")
    return {"transaction_id": tx_id, "status": "SUCCESS", "message": "Payment processed"}


def _payment_fallback(order_id: str = "?", amount: float = 0, customer_id: str = "?") -> dict:
    logger.warning(f"Payment FALLBACK | order={order_id}")
    return {
        "transaction_id": "",
        "status": "DEGRADED",
        "message": "Payment queued — service temporarily unavailable",
    }


_cb = CircuitBreaker(
    name="payment-cb",
    failure_threshold=5,
    recovery_timeout=30.0,
    half_open_max_calls=1,
    fallback_function=_payment_fallback,
)



def _vc_from_proto(entries) -> dict:
    return {e.process_id: e.counter for e in entries}


def _vc_to_proto(vc: dict):
    return [
        payment_pb2.VectorClockEntry(process_id=pid, counter=cnt)
        for pid, cnt in vc.items()
    ]



class PaymentServicer(payment_pb2_grpc.PaymentServiceServicer):

    def Compute(self, request, context):
        logger.info(
            f"RPC Compute | order={request.order_id} "
            f"amount={request.amount} vc={_vc_from_proto(request.vector_clock)}"
        )

        result = _cb.call(
            _payment_logic,
            order_id=request.order_id,
            amount=request.amount,
            customer_id=request.customer_id,
        )

        vc = _vc_from_proto(request.vector_clock)
        vc["payment_service"] = vc.get("payment_service", 0) + 1

        return payment_pb2.ComputeResponse(
            transaction_id=result["transaction_id"],
            status=result["status"],
            message=result["message"],
            vector_clock=_vc_to_proto(vc),
        )

    def Crash(self, request, context):
        global _crashed
        with _crash_lock:
            _crashed = True
        logger.error(f"[PaymentService] CRASHED — reason: {request.reason}")
        return payment_pb2.CrashResponse(ok=True, message="Service marked as crashed")

    def Recover(self, request, context):
        global _crashed
        with _crash_lock:
            _crashed = False
        _cb.reset()
        logger.info("[PaymentService] RECOVERED — circuit reset to CLOSED")
        return payment_pb2.RecoverResponse(ok=True, message="Service recovered")

    def Health(self, request, context):
        with _crash_lock:
            crashed = _crashed
        status = "DOWN" if crashed else _cb.state.value
        return payment_pb2.HealthResponse(status=status)



def serve(port: int = 50052):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    payment_pb2_grpc.add_PaymentServiceServicer_to_server(PaymentServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"PaymentService gRPC server listening on port {port}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down PaymentService...")
        server.stop(grace=5)


if __name__ == "__main__":
    serve()
