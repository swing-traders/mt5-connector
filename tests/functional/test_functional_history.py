import csv
import gzip
import json

import pytest

from functional import history


def test_registered_native_exports_materialize_with_their_provenance(tmp_path):
    destination = tmp_path / "exports"
    reference = history.materialize(destination)
    assert reference["symbol"] == "BTCUSD.a"
    for week in reference["weeks"]:
        with (destination / week["job"] / "out/metadata.csv").open(newline="") as file:
            native = {row["key"]: row["value"] for row in csv.DictReader(file)}
        assert native["tester"] == "1"
        assert native["symbol"] == reference["symbol"]
        provenance = json.loads((destination / week["job"] / "out/provenance.json").read_text())
        assert provenance["native_every_tick_confirmed"]
        assert provenance["files"] == week["sha256"]
    with (destination / "floors/out/metadata.csv").open(newline="") as file:
        native = {row["key"]: row["value"] for row in csv.DictReader(file)}
    assert int(native["m1_floor_broker"]) == reference["floors"]["m1_broker"]
    assert int(native["quote_floor_broker"]) == reference["floors"]["quote_broker"]
    assert int(native["january_rows"]) == reference["floors"]["january_rows"]


def test_a_changed_native_export_fails_its_registered_checksum(tmp_path, monkeypatch):
    fixture = tmp_path / "fixture"
    (fixture / "week").mkdir(parents=True)
    (fixture / "manifest.json").write_text(
        json.dumps(
            {
                "weeks": [{"job": "week", "sha256": {"metadata.csv": "0" * 64}}],
                "native_floor_files": {},
            }
        )
    )
    (fixture / "week/metadata.csv.gz").write_bytes(gzip.compress(b"changed native data"))
    monkeypatch.setattr(history, "FIXTURES", fixture)
    with pytest.raises(ValueError, match="invalid checksum"):
        history.materialize(tmp_path / "exports")
