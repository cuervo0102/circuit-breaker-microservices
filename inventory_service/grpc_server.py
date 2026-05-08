"""
inventory_service/grpc_server.py
gRPC server for InventoryService — stock check + reservation with simulated latency.
Generated stubs assume you ran:
    python -m grpc_tools.protoc -I../protos --python_out=. --grpc_python_out=. ../protos/inventory.proto
"""

import time
import random
import logging
import threading
import grpc
from concurrent import futures
from collections import defaultdict

import inventory_pb2
import inventory_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("InventoryService")

# ─── In-memory stock store ────────────────────────────────────────────────────

_INITIAL_STOCK = {
    "PROD-001": 100,
    "PROD-002": 50,
    "PROD-003": 200,
}

_stock: dict = defaultdict(lambda: 10, _INITIAL_STOCK)   # default 10 for unknown products
_reservations: dict = {}                                  # reservation_id → {product, qty}
_lock = threading.Lock()

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _vc_from_proto(entries) -> dict:
    return {e.process_id: e.counter for e in entries}


def _vc_to_proto(vc: dict, pb2_module):
    return [
        pb2_module.VectorClockEntry(process_id=pid, counter=cnt)
        for pid, cnt in vc.items()
    ]

# ─── Servicer ─────────────────────────────────────────────────────────────────

class InventoryServicer(inventory_pb2_grpc.InventoryServiceServicer):

    def CheckStock(self, request, context):
        # Simulate latency 30–150 ms
        time.sleep(random.uniform(0.03, 0.15))

        with _lock:
            level = _stock[request.product_id]
            available = level >= request.quantity

        logger.info(
            f"CheckStock | product={request.product_id} "
            f"requested={request.quantity} stock={level} available={available}"
        )

        vc = _vc_from_proto(request.vector_clock)
        vc["inventory_service"] = vc.get("inventory_service", 0) + 1

        return inventory_pb2.CheckStockResponse(
            available=available,
            stock_level=level,
            vector_clock=_vc_to_proto(vc, inventory_pb2),
        )

    def ReserveStock(self, request, context):
        time.sleep(random.uniform(0.05, 0.20))

        res_id = f"RES-{request.order_id}-{request.product_id}-{int(time.time())}"

        with _lock:
            level = _stock[request.product_id]
            if level < request.quantity:
                logger.warning(
                    f"ReserveStock FAILED | product={request.product_id} "
                    f"wanted={request.quantity} have={level}"
                )
                vc = _vc_from_proto(request.vector_clock)
                vc["inventory_service"] = vc.get("inventory_service", 0) + 1
                return inventory_pb2.ReserveStockResponse(
                    success=False,
                    reservation_id="",
                    message=f"Insufficient stock: {level} < {request.quantity}",
                    vector_clock=_vc_to_proto(vc, inventory_pb2),
                )

            _stock[request.product_id] -= request.quantity
            _reservations[res_id] = {
                "product_id": request.product_id,
                "quantity":   request.quantity,
            }

        logger.info(
            f"ReserveStock OK | product={request.product_id} "
            f"qty={request.quantity} res_id={res_id}"
        )

        vc = _vc_from_proto(request.vector_clock)
        vc["inventory_service"] = vc.get("inventory_service", 0) + 1

        return inventory_pb2.ReserveStockResponse(
            success=True,
            reservation_id=res_id,
            message="Stock reserved",
            vector_clock=_vc_to_proto(vc, inventory_pb2),
        )

    def ReleaseStock(self, request, context):
        with _lock:
            res = _reservations.pop(request.reservation_id, None)
            if res is None:
                return inventory_pb2.ReleaseStockResponse(
                    success=False,
                    message=f"Reservation {request.reservation_id} not found",
                )
            _stock[res["product_id"]] += res["quantity"]

        logger.info(
            f"ReleaseStock | res_id={request.reservation_id} "
            f"reason={request.reason} stock_restored={res['quantity']}"
        )
        return inventory_pb2.ReleaseStockResponse(success=True, message="Stock released")

    def Health(self, request, context):
        return inventory_pb2.HealthResponse(status="UP")


# ─── Server entry-point ───────────────────────────────────────────────────────

def serve(port: int = 50053):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(InventoryServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"InventoryService gRPC server listening on port {port}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down InventoryService...")
        server.stop(grace=5)


if __name__ == "__main__":
    serve()
