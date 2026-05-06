import threading
import logging
from typing import Dict, List, Optional
from copy import deepcopy

logger = logging.getLogger("VectorClock")


class VectorClock:
 

    def __init__(self, process_id: str, all_processes: List[str]):
        if process_id not in all_processes:
            raise ValueError(f"process_id '{process_id}' must be in all_processes list")

        self.process_id    = process_id
        self.all_processes = all_processes
        self._clock: Dict[str, int] = {p: 0 for p in all_processes}
        self._lock = threading.Lock()

        logger.info(f"[VectorClock] '{process_id}' initialized | "
                    f"processes={all_processes}")

    def tick(self) -> Dict[str, int]:
        
        with self._lock:
            self._clock[self.process_id] += 1
            snapshot = deepcopy(self._clock)
        logger.debug(f"[{self.process_id}] tick → {snapshot}")
        return snapshot

    def send(self) -> Dict[str, int]:
        
        with self._lock:
            self._clock[self.process_id] += 1
            timestamp = deepcopy(self._clock)
        logger.info(f"[{self.process_id}] send → timestamp={timestamp}")
        return timestamp

    def receive(self, received_clock: Dict[str, int]) -> Dict[str, int]:
        
        with self._lock:
            for process in self.all_processes:
                self._clock[process] = max(
                    self._clock.get(process, 0),
                    received_clock.get(process, 0)
                )
            self._clock[self.process_id] += 1
            snapshot = deepcopy(self._clock)

        logger.info(f"[{self.process_id}] receive ← {received_clock} → merged={snapshot}")
        return snapshot

    def happened_before(self, vc_a: Dict[str, int], vc_b: Dict[str, int]) -> bool:
       
        all_leq     = all(vc_a.get(p, 0) <= vc_b.get(p, 0) for p in self.all_processes)
        at_least_lt = any(vc_a.get(p, 0) <  vc_b.get(p, 0) for p in self.all_processes)
        return all_leq and at_least_lt

    def concurrent(self, vc_a: Dict[str, int], vc_b: Dict[str, int]) -> bool:
       
        return (
            not self.happened_before(vc_a, vc_b)
            and not self.happened_before(vc_b, vc_a)
        )

    def is_causally_ready(
        self,
        msg_clock: Dict[str, int],
        sender_id: str,
    ) -> bool:
        
        with self._lock:
            local = self._clock

            # Check sender's counter is exactly one ahead
            sender_ok = (
                msg_clock.get(sender_id, 0) == local.get(sender_id, 0) + 1
            )

            # Check all other processes
            others_ok = all(
                msg_clock.get(p, 0) <= local.get(p, 0)
                for p in self.all_processes
                if p != sender_id
            )

        ready = sender_ok and others_ok
        logger.debug(
            f"[{self.process_id}] causal_ready={ready} | "
            f"msg={msg_clock} sender={sender_id} local={local}"
        )
        return ready

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return deepcopy(self._clock)

    def reset(self):
        with self._lock:
            self._clock = {p: 0 for p in self.all_processes}

    def to_list(self) -> List[int]:
        with self._lock:
            return [self._clock[p] for p in self.all_processes]

    @classmethod
    def from_list(
        cls,
        values: List[int],
        process_id: str,
        all_processes: List[str]
    ) -> "VectorClock":
        vc = cls(process_id, all_processes)
        vc._clock = {p: v for p, v in zip(all_processes, values)}
        return vc

    def __repr__(self):
        return f"VectorClock({self.process_id}, {self._clock})"



if __name__ == "__main__":
    print("   Vector Clock Demo — Causal Ordering")

    PROCESSES = ["order_service", "payment_service", "inventory_service"]

    vc_order     = VectorClock("order_service",     PROCESSES)
    vc_payment   = VectorClock("payment_service",   PROCESSES)
    vc_inventory = VectorClock("inventory_service", PROCESSES)

    print("Step 1: Customer places order (internal event) ")
    t1 = vc_order.tick()
    print(f"  order_service clock:     {t1}\n")

    print("Step 2: order_service → payment_service ")
    t2 = vc_order.send()
    print(f"  Sent with timestamp:     {t2}")
    t3 = vc_payment.receive(t2)
    print(f"  payment_service clock:   {t3}\n")

    print("Step 3: order_service → inventory_service ")
    t4 = vc_order.send()
    print(f"  Sent with timestamp:     {t4}")
    t5 = vc_inventory.receive(t4)
    print(f"  inventory_service clock: {t5}\n")

    print("Step 4: inventory_service → payment_service ")
    t6 = vc_inventory.send()
    print(f"  Sent with timestamp:     {t6}")
    t7 = vc_payment.receive(t6)
    print(f"  payment_service clock:   {t7}\n")

    print("Causal relationship checks ")
    print(f"  t2 happened-before t6? {vc_order.happened_before(t2, t6)}")
    print(f"  t4 happened-before t6? {vc_order.happened_before(t4, t6)}")
    print(f"  t2 concurrent with t4? {vc_order.concurrent(t2, t4)}")

    print(" Step 5: Causal delivery check ")
  
    fake_msg_clock = {"order_service": 3, "payment_service": 0, "inventory_service": 2}
    ready = vc_payment.is_causally_ready(fake_msg_clock, sender_id="order_service")
    print(f"  Message causally ready to deliver? {ready}")
    print(f"  (False = must buffer and wait — causal ordering enforced )")
