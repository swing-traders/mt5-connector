"""The image's health probe: exits 0 when the hub's /health answers HTTP 200, and otherwise prints
what it got and exits 1."""

from __future__ import annotations

import os
import sys
import urllib.error
import urllib.request


def main() -> int:
    url = f"http://127.0.0.1:{os.environ['MT5_HUB_PORT']}/health"
    try:
        with urllib.request.urlopen(url, timeout=4) as response:
            status = response.status
    except urllib.error.HTTPError as answered:
        status = answered.code
    except OSError as failure:
        print(f"{url}: {failure}")
        return 1
    if status == 200:
        return 0
    else:
        print(f"{url}: HTTP {status}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
