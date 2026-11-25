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
    witnesses = {}
    for role, job in reference["floor_reference_jobs"].items():
        output = destination / job / "out"
        provenance = json.loads((output / "provenance.json").read_text())
        assert provenance["native_every_tick_confirmed"]
        with (output / "bars.csv").open(newline="") as file:
            witnesses[role] = list(csv.DictReader(file))
    m1 = witnesses["m1"]
    first_pair = next(
        int(left["time"])
        for left, right in zip(m1, m1[1:], strict=False)
        if int(right["time"]) - int(left["time"]) == 60
    )
    assert first_pair == reference["floors"]["m1_broker"]
    first_spread = next(int(row["time"]) for row in witnesses["quote"] if int(row["spread"]) > 0)
    assert first_spread == reference["floors"]["quote_broker"]
    january = [
        row
        for row in witnesses["january"]
        if int(native["january_from_broker"])
        <= int(row["time"])
        <= int(native["january_to_broker"])
    ]
    assert len(january) == reference["floors"]["january_rows"]


def test_a_changed_native_export_fails_its_registered_checksum(tmp_path, monkeypatch):
    fixture = tmp_path / "fixture"
    (fixture / "week").mkdir(parents=True)
    (fixture / "manifest.json").write_text(
        json.dumps(
            {
                "weeks": [{"job": "week", "sha256": {"metadata.csv": "0" * 64}}],
                "floor_tester_jobs": [],
                "native_floor_files": {},
            }
        )
    )
    (fixture / "week/metadata.csv.gz").write_bytes(gzip.compress(b"changed native data"))
    monkeypatch.setattr(history, "FIXTURES", fixture)
    with pytest.raises(ValueError, match="invalid checksum"):
        history.materialize(tmp_path / "exports")
