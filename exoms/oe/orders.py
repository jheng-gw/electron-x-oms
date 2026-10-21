"""Order model and FIX status mapping."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

BUY, SELL = "1", "2"  # Side(54)


class OrdStatus(str, Enum):
    PENDING_NEW = "PendingNew"
    NEW = "New"
    PARTIALLY_FILLED = "PartiallyFilled"
    FILLED = "Filled"
    DONE_FOR_DAY = "DoneForDay"
    CANCELED = "Canceled"
    REJECTED = "Rejected"
    EXPIRED = "Expired"


TERMINAL = frozenset({OrdStatus.FILLED, OrdStatus.DONE_FOR_DAY, OrdStatus.CANCELED,
                      OrdStatus.REJECTED, OrdStatus.EXPIRED})

# OrdStatus(39) values ElectronX sends (it has no PendingCancel/PendingReplace)
FIX_ORD_STATUS = {
    "0": OrdStatus.NEW, "1": OrdStatus.PARTIALLY_FILLED, "2": OrdStatus.FILLED,
    "3": OrdStatus.DONE_FOR_DAY, "4": OrdStatus.CANCELED, "8": OrdStatus.REJECTED,
    "A": OrdStatus.PENDING_NEW, "C": OrdStatus.EXPIRED,
}


@dataclass(eq=False)  # identity semantics so orders can live in sets
class Order:
    cl_ord_id: str          # current ClOrdID; changes on every cancel/replace
    symbol: str
    side: str
    qty: int
    price: float
    manual: bool = False
    status: OrdStatus = OrdStatus.PENDING_NEW
    order_id: str = ""      # exchange OrderID(37)
    cum_qty: int = 0
    avg_px: float = 0.0
    text: str = ""          # last reject / cancel reason
    pending_cl_ord_id: str = ""  # ClOrdID of a cancel or replace in flight
    created_ns: int = 0
    history: list[str] = field(default_factory=list)  # every ClOrdID this order has used

    @property
    def is_live(self) -> bool:
        return self.status not in TERMINAL

    @property
    def is_pending(self) -> bool:
        """True while waiting on the exchange; don't send another change until it answers."""
        return self.status == OrdStatus.PENDING_NEW or bool(self.pending_cl_ord_id)

    @property
    def leaves_qty(self) -> int:
        return self.qty - self.cum_qty if self.is_live else 0

    @property
    def side_name(self) -> str:
        return "BUY" if self.side == BUY else "SELL"

    def __repr__(self) -> str:
        return (f"<{self.cl_ord_id} {self.side_name} {self.leaves_qty}/{self.qty} {self.symbol} "
                f"@ {self.price} {self.status.value}>")
