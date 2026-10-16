"""The hub's liveness: healthy while the spawner is connected, the latest server-time sample fresh
and the broker clock verified; its body carries the clock and the load either way."""

import dataclasses
from collections import Counter
from http import HTTPStatus

from mt5connector.server.hub_gate import ClockGate
from mt5connector.server.hub_requests import Pending


def health(
    gate: ClockGate, spawner: bool, pending: list[Pending], labels: dict[object, tuple[str, str]]
) -> tuple[HTTPStatus, dict[str, object]]:
    """The status and the body of /health; `labels` names each EA by its chart's role and symbol."""
    age_s = gate.sample_age_s()
    by_ea = Counter(pending_request.ea for pending_request in pending)
    result = {
        "spawner": spawner,
        "clock": None if gate.verification is None else dataclasses.asdict(gate.verification),
        "sample_age_s": None if age_s is None else round(age_s, 3),
        "bootstrap_missed": gate.bootstrap_missed,
        "in_flight": len(pending),
        "pending": [
            {"role": labels[ea][0], "symbol": labels[ea][1], "requests": count}
            for ea, count in by_ea.items()
        ],
    }
    if not spawner:
        error = "no spawner is connected"
    elif gate.verification is None:
        error = "the broker clock is not verified"
    elif not gate.is_fresh():
        error = "the latest server-time sample is not fresh"
    else:
        error = None
    if error is None:
        return HTTPStatus.OK, {"ok": True, "result": result}
    else:
        return HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": error, "result": result}
