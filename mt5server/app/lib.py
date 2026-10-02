import logging

from constants import MT5Timeframe

logger = logging.getLogger(__name__)


def get_timeframe(timeframe_str: str) -> MT5Timeframe:
    try:
        return MT5Timeframe[timeframe_str.upper()].value
    except KeyError:
        valid_timeframes = ", ".join([t.name for t in MT5Timeframe])
        raise ValueError(
            f"Invalid timeframe: '{timeframe_str}'. Valid options are: {valid_timeframes}."
        ) from None
