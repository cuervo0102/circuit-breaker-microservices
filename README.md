# Circuit Breaker Microservices

Distributed system with 3 microservices communicating via gRPC, implementing Circuit Breaker + Vector Clock + FIFO ordering patterns.

## Project Structure

```
circuit-breaker-microservices/
├── circuit_breaker.py        # Circuit Breaker (3 states: CLOSED/OPEN/HALF-OPEN)
├── vector_clock.py           # Vector Clock (causal ordering)
├── fifo_buffer.py            # FIFO Buffer (message ordering)
├── requirements.txt
├── protos/
│   ├── order.proto
│   ├── payment.proto
│   └── inventory.proto
├── order_service/            # Service A — FastAPI + gRPC orchestrator (port 8000 / 50051)
│   ├── main.py               ← FastAPI REST gateway (Siham)
│   └── grpc_server.py        ← gRPC server (Siham)
├── payment_service/          # Service B — gRPC payment processor (port 50052)
│   ├── grpc_server.py        ← done (Siham)
│   └── main.py               ← FastAPI /compute /crash /recover (Houda)
└── inventory_service/        # Service C — gRPC stock manager (port 50053)
    ├── grpc_server.py        ← done (Siham)
    └── main.py               ← FastAPI /stock (Houda)
```

## Setup (everyone does this once)

```powershell
# 1. Create and activate virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1

# 2. Install dependencies
pip install -r requirements.txt

# 3. Generate gRPC stubs (IMPORTANT — run from root folder)
python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/order.proto ./protos/payment.proto ./protos/inventory.proto

# 4. Copy stubs into each service folder
Copy-Item *_pb2*.py order_service\
Copy-Item *_pb2*.py payment_service\
Copy-Item *_pb2*.py inventory_service\

# 5. Copy shared modules
Copy-Item circuit_breaker.py order_service\
Copy-Item circuit_breaker.py payment_service\
Copy-Item circuit_breaker.py inventory_service\
Copy-Item vector_clock.py order_service\
Copy-Item fifo_buffer.py order_service\
```

## Running the Services (3 separate terminals)

**Terminal 1 — Inventory Service (port 50053):**
```powershell
cd C:\...\circuit-breaker-microservices
.\venv\Scripts\Activate.ps1
python inventory_service\grpc_server.py
```

**Terminal 2 — Payment Service (port 50052):**
```powershell
cd C:\...\circuit-breaker-microservices
.\venv\Scripts\Activate.ps1
python payment_service\grpc_server.py
```

**Terminal 3 — Order Service FastAPI (port 8000):**
```powershell
cd C:\...\circuit-breaker-microservices\order_service
.\venv\Scripts\Activate.ps1
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Testing

```powershell
# Health check
curl http://localhost:8000/health

# Place an order
curl -X POST http://localhost:8000/orders -H "Content-Type: application/json" -d "{\"customer_id\": \"CUST-001\", \"items\": [{\"product_id\": \"PROD-001\", \"quantity\": 2, \"unit_price\": 49.99}]}"

# List orders
curl http://localhost:8000/orders

# Circuit breaker status
curl http://localhost:8000/cb/status

# Reset circuit breakers
curl -X POST http://localhost:8000/cb/reset
```

## API Endpoints (order_service)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Service health + CB status + vector clock |
| POST | `/orders` | Place a new order |
| GET | `/orders` | List all orders |
| GET | `/orders/{id}` | Get order by ID |
| DELETE | `/orders/{id}` | Cancel an order |
| GET | `/cb/status` | Circuit breaker metrics |
| POST | `/cb/reset` | Reset all circuit breakers to CLOSED |

## Team Tasks

| Member | Task | Files |
|--------|------|-------|
| **Siham** ✅ | Circuit Breaker, gRPC, Vector Clock, FIFO, Order Service | Done |
| **Houda** 🔲 | Payment + Inventory FastAPI | `payment_service/main.py`, `inventory_service/main.py` |
| **Hafsa** 🔲 | Tests | `test_circuit_breaker.py`, `test_vector_clock.py`, locust |
| **Salma** 🔲 | Docs + diagrams | `README` additions, architecture diagrams |

## Ports

| Service | gRPC | FastAPI |
|---------|------|---------|
| order_service | 50051 | 8000 |
| payment_service | 50052 | 8001 (Houda) |
| inventory_service | 50053 | 8002 (Houda) |

## Notes

- The `*_pb2*.py` files are **generated** — do not edit them manually
- Always run `protoc` from the **root** folder
- Always run `uvicorn` from **inside** the service folder (`cd order_service`)
- Circuit breaker opens after **5 failures**, recovers after **30 seconds**
- Payment service has a **20% random failure rate** by design (for CB demo)
