import time
import threading
import logging
from enum import Enum
from dataclasses import dataclass, field
from typing import Callable, Any, Optional
 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("CircuitBreaker")
 

class CBState(Enum):
   
    CLOSED    = "CLOSED"     
    OPEN      = "OPEN"        
    HALF_OPEN = "HALF-OPEN"   
 
 
@dataclass
class CBMetrics:
   
    total_calls:       int   = 0
    successful_calls:  int   = 0
    failed_calls:      int   = 0
    fallback_calls:    int   = 0
    state_changes:     int   = 0
    latencies_ms:      list  = field(default_factory=list)
 
    def average_latency(self) -> float:
        if not self.latencies_ms:
            return 0.0
        return round(sum(self.latencies_ms) / len(self.latencies_ms), 2)
 
    def error_rate(self) -> float:
        if self.total_calls == 0:
            return 0.0
        return round((self.failed_calls / self.total_calls) * 100, 2)
 
    def to_dict(self) -> dict:
        return {
            "total_calls":      self.total_calls,
            "successful_calls": self.successful_calls,
            "failed_calls":     self.failed_calls,
            "fallback_calls":   self.fallback_calls,
            "state_changes":    self.state_changes,
            "error_rate_%":     self.error_rate(),
            "avg_latency_ms":   self.average_latency(),
        }
 
 
class CircuitBreaker:
   
    def __init__(
        self,
        name: str = "circuit-breaker",
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 1,
        fallback_function: Optional[Callable] = None,
    ):
        self.name                = name
        self.failure_threshold   = failure_threshold
        self.recovery_timeout    = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.fallback_function   = fallback_function
 
        self._state              = CBState.CLOSED
        self._failure_count      = 0
        self._half_open_calls    = 0
        self._last_failure_time  = None
 
        self.metrics = CBMetrics()
 
        self._lock = threading.Lock()
 
        logger.info(f"[{self.name}] Circuit Breaker initialized | "
                    f"threshold={failure_threshold} | timeout={recovery_timeout}s")
 
    @property
    def state(self) -> CBState:
        return self._state
 
    @property
    def failure_count(self) -> int:
        return self._failure_count
 
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute `func` through the Circuit Breaker.
 
        - If CLOSED    → run normally, track success/failure
        - If OPEN      → check timeout; if elapsed go HALF-OPEN, else return fallback
        - If HALF-OPEN → allow one probe; on success go CLOSED, on fail go OPEN
 
        Returns the result of `func` or the fallback response.
        Raises CircuitBreakerOpenError if no fallback defined and CB is OPEN.
        """
        with self._lock:
            self._maybe_transition_to_half_open()
 
            if self._state == CBState.OPEN:
                return self._handle_open_state()
 
            if self._state == CBState.HALF_OPEN:
                if self._half_open_calls >= self.half_open_max_calls:
                    return self._handle_open_state()
                self._half_open_calls += 1
 
        start_time = time.monotonic()
        try:
            result = func(*args, **kwargs)
            elapsed_ms = (time.monotonic() - start_time) * 1000
 
            with self._lock:
                self._on_success(elapsed_ms)
 
            return result
 
        except Exception as exc:
            elapsed_ms = (time.monotonic() - start_time) * 1000
 
            with self._lock:
                self._on_failure(elapsed_ms, exc)
 
            if self.fallback_function:
                self.metrics.fallback_calls += 1
                logger.warning(f"[{self.name}] Returning fallback after exception: {exc}")
                return self.fallback_function(*args, **kwargs)
 
            raise
 
    def _maybe_transition_to_half_open(self):
        
        if (
            self._state == CBState.OPEN
            and self._last_failure_time is not None
            and (time.monotonic() - self._last_failure_time) >= self.recovery_timeout
        ):
            self._transition_to(CBState.HALF_OPEN)
            self._half_open_calls = 0
 
    def _on_success(self, elapsed_ms: float):
        self.metrics.total_calls      += 1
        self.metrics.successful_calls += 1
        self.metrics.latencies_ms.append(round(elapsed_ms, 2))
 
        if self._state == CBState.HALF_OPEN:
            logger.info(f"[{self.name}] Probe succeeded. Closing circuit.")
            self._transition_to(CBState.CLOSED)
 
        self._failure_count = 0
 
        logger.debug(f"[{self.name}] Call succeeded in {elapsed_ms:.1f}ms | "
                     f"state={self._state.value}")
 
    def _on_failure(self, elapsed_ms: float, exc: Exception):
        self.metrics.total_calls  += 1
        self.metrics.failed_calls += 1
        self.metrics.latencies_ms.append(round(elapsed_ms, 2))
        self._failure_count       += 1
        self._last_failure_time    = time.monotonic()
 
        logger.warning(f"[{self.name}] Call failed ({exc}) | "
                       f"failures={self._failure_count}/{self.failure_threshold} | "
                       f"state={self._state.value}")
 
        if self._state == CBState.HALF_OPEN:
            logger.warning(f"[{self.name}] Probe failed. Re-opening circuit.")
            self._transition_to(CBState.OPEN)
 
        elif (
            self._state == CBState.CLOSED
            and self._failure_count >= self.failure_threshold
        ):
            logger.error(f"[{self.name}] Threshold reached. Opening circuit.")
            self._transition_to(CBState.OPEN)
 
    def _handle_open_state(self) -> Any:
        self.metrics.total_calls    += 1
        self.metrics.fallback_calls += 1
 
        logger.info(f"[{self.name}] Circuit OPEN — returning fallback immediately")
 
        if self.fallback_function:
            return self.fallback_function()
 
        raise CircuitBreakerOpenError(
            f"Circuit '{self.name}' is OPEN. "
            f"Retry after {self.recovery_timeout}s."
        )
 
    def _transition_to(self, new_state: CBState):
        """Log and apply a state transition."""
        old_state = self._state
        self._state = new_state
        self.metrics.state_changes += 1
        logger.info(
            f"[{self.name}] State transition: "
            f"{old_state.value} → {new_state.value}"
        )
 
    def reset(self):
        with self._lock:
            self._failure_count     = 0
            self._half_open_calls   = 0
            self._last_failure_time = None
            self._transition_to(CBState.CLOSED)
        logger.info(f"[{self.name}] Circuit manually reset to CLOSED.")
 
    def force_open(self):
        with self._lock:
            self._transition_to(CBState.OPEN)
            self._last_failure_time = time.monotonic()
        logger.info(f"[{self.name}] Circuit manually forced to OPEN.")
 
    def status(self) -> dict:
       
        with self._lock:
            return {
                "name":              self.name,
                "state":             self._state.value,
                "failure_count":     self._failure_count,
                "failure_threshold": self.failure_threshold,
                "recovery_timeout":  self.recovery_timeout,
                "metrics":           self.metrics.to_dict(),
            }
 
    def __repr__(self):
        return (f"CircuitBreaker(name={self.name!r}, "
                f"state={self._state.value}, "
                f"failures={self._failure_count}/{self.failure_threshold})")
 
 

class CircuitBreakerOpenError(Exception):
   
    pass
 
 
if __name__ == "__main__":
    import random
 
 
    def fallback(*args, **kwargs):
        return {"status": "degraded", "message": "Payment service unavailable. Order queued."}
 
    cb = CircuitBreaker(
        name="payment-cb",
        failure_threshold=3,
        recovery_timeout=5.0,  
        fallback_function=fallback,
    )
 
    def flaky_payment_service(order_id: int):
        if random.random() < 0.7:   
            raise ConnectionError(f"Payment service timeout for order #{order_id}")
        return {"status": "ok", "order_id": order_id, "charged": True}
 
    for i in range(1, 11):
        result = cb.call(flaky_payment_service, order_id=i)
        print(f"Call {i:02d} | state={cb.state.value:9s} | result={result}")
 
    import json
    print(json.dumps(cb.status(), indent=2))
 
    time.sleep(5)
 
    result = cb.call(lambda: {"status": "ok", "recovered": True})
    print(f"Probe result: {result} | state={cb.state.value}")
 
    print(json.dumps(cb.status(), indent=2))