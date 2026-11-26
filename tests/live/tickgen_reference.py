"""The registered native BTC tester exports and native history witnesses."""

import json
from pathlib import Path

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/tickgen-btc"
REFERENCE = json.loads((FIXTURES / "manifest.json").read_text())
