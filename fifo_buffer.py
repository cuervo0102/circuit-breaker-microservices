import threading
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("FIFOBuffer")

@dataclass
class Message:
   
    sender_id:    str             
    seq_number:   int              
    vector_clock: Dict[str, int]   
    payload:      Any              
    msg_id:       str = ""         

    def __post_init__(self):
        if not self.msg_id:
            self.msg_id = f"{self.sender_id}-seq{self.seq_number}"

    def __repr__(self):
        return (f"Message(from={self.sender_id}, seq={self.seq_number}, "
                f"vc={self.vector_clock}, id={self.msg_id})")


class FIFOSender:

    def __init__(self, sender_id: str):
        self.sender_id  = sender_id
        self._seq       = 0
        self._lock      = threading.Lock()

    def prepare(self, payload: Any, vector_clock: Dict[str, int]) -> Message:
        
        with self._lock:
            self._seq += 1
            seq = self._seq

        msg = Message(
            sender_id=self.sender_id,
            seq_number=seq,
            vector_clock=vector_clock,
            payload=payload,
        )
        logger.info(f"[FIFOSender:{self.sender_id}] prepared {msg}")
        return msg

    @property
    def current_seq(self) -> int:
        return self._seq


class FIFOBuffer:
  

    def __init__(self, receiver_id: str, deliver_callback: Callable[[Message], None]):
        self.receiver_id       = receiver_id
        self.deliver_callback  = deliver_callback

        self._seq_expected: Dict[str, int]         = defaultdict(lambda: 1)
        self._buffer:       Dict[str, List[Message]] = defaultdict(list)
        self._lock = threading.Lock()

        logger.info(f"[FIFOBuffer:{receiver_id}] initialized")

    def receive(self, msg: Message):
        
        to_deliver = []

        with self._lock:
            sender = msg.sender_id
            expected = self._seq_expected[sender]

            logger.info(
                f"[FIFOBuffer:{self.receiver_id}] received {msg} | "
                f"expected_seq={expected}"
            )

            if msg.seq_number < expected:
                logger.warning(
                    f"[FIFOBuffer:{self.receiver_id}] DUPLICATE discarded: "
                    f"seq={msg.seq_number} (expected {expected})"
                )
                return

            if msg.seq_number > expected:
                logger.info(
                    f"[FIFOBuffer:{self.receiver_id}] BUFFERED (out of order): "
                    f"seq={msg.seq_number} (expected {expected})"
                )
                self._buffer[sender].append(msg)
                self._buffer[sender].sort(key=lambda m: m.seq_number)
                return

            to_deliver.append(msg)
            self._seq_expected[sender] += 1

            while self._buffer[sender]:
                next_msg = self._buffer[sender][0]
                if next_msg.seq_number == self._seq_expected[sender]:
                    self._buffer[sender].pop(0)
                    to_deliver.append(next_msg)
                    self._seq_expected[sender] += 1
                else:
                    break

        for m in to_deliver:
            self._deliver(m)

    def _deliver(self, msg: Message):
        logger.info(
            f"[FIFOBuffer:{self.receiver_id}] DELIVERING seq={msg.seq_number} "
            f"from {msg.sender_id}"
        )
        self.deliver_callback(msg)

    def _drain_buffer(self, sender: str):
        pass

    def buffer_size(self, sender: Optional[str] = None) -> int:
        with self._lock:
            if sender:
                return len(self._buffer[sender])
            return sum(len(v) for v in self._buffer.values())

    def status(self) -> dict:
        with self._lock:
            total = sum(len(v) for v in self._buffer.values())
            return {
                "receiver":       self.receiver_id,
                "seq_expected":   dict(self._seq_expected),
                "buffered_msgs":  {k: len(v) for k, v in self._buffer.items()},
                "total_buffered": total,
            }



if __name__ == "__main__":
    print("FIFO Buffer Demo — Message Ordering")

    delivered = []

    def on_deliver(msg: Message):
        delivered.append(msg.seq_number)
        print(f"DELIVERED seq={msg.seq_number} | payload={msg.payload}")

    sender   = FIFOSender("order_service")
    receiver = FIFOBuffer("payment_service", deliver_callback=on_deliver)

    dummy_vc = {"order_service": 1, "payment_service": 0, "inventory_service": 0}

    print("Prepare 4 messages from sender:")
    m1 = sender.prepare({"order_id": 1, "amount": 100}, dummy_vc)
    m2 = sender.prepare({"order_id": 2, "amount": 200}, dummy_vc)
    m3 = sender.prepare({"order_id": 3, "amount": 300}, dummy_vc)
    m4 = sender.prepare({"order_id": 4, "amount": 400}, dummy_vc)

    print("\n Simulate out-of-order arrival: 1, 3, 2, 4 ")
    print("\nReceiving seq=1 (in order):")
    receiver.receive(m1)

    print("\nReceiving seq=3 (out of order — buffered):")
    receiver.receive(m3)
    print(f"  Buffer status: {receiver.status()}")

    print("\nReceiving seq=2 (fills the gap — 2 and 3 both delivered):")
    receiver.receive(m2)

    print("\nReceiving seq=4 (in order):")
    receiver.receive(m4)

    print(f"\n Final delivery order: {delivered} ")
    assert delivered == [1, 2, 3, 4], "FIFO ordering violated!"
    print("  FIFO ordering guaranteed ")

    print("\n Duplicate detection ")
    print("Receiving seq=2 again (duplicate):")
    receiver.receive(m2)

    print(f"\n Final buffer status ")
    import json
    print(json.dumps(receiver.status(), indent=2))
