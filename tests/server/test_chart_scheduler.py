"""Run the chart controller's MQL-compatible functions against a deterministic terminal double."""

import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
EA = ROOT / "packages/server/mt5connector/server/mql5/Expert/ticks.mq5"


def function(source, name):
    match = re.search(rf"^\w+ {name}\([^{{]*\)\n{{", source, re.M)
    start = match.start()
    end = match.end()
    depth = 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[start:end]


@pytest.fixture(scope="module")
def controller(tmp_path_factory):
    source = EA.read_text()
    globals_ = source[
        source.index("long g_openedCharts[];") : source.index("// The publisher's outcome log")
    ]
    body = source[source.index("string Marker(") : source.index("void CloseOwnChart(")]
    body += (
        "\n"
        + source[source.index("void QueueChartFrame(") : source.index("void OnDuplicateFrame(")]
    )
    body += "\n" + function(source, "OnTimer")
    declarations = re.findall(r"^(\w+ \w+\([^{]*?\))\n{", body, re.M)
    code = globals_ + "\n" + ";\n".join(declarations) + ";\n" + body
    code = re.sub(r"\b(const )?(\w+) &(\w+)\[\]", r"\1std::vector<\2>& \3", code)
    code = re.sub(r"\b(\w+) (\w+)\[\];", r"std::vector<\1> \2;", code)
    directory = tmp_path_factory.mktemp("chart-controller")
    cpp = directory / "controller.cpp"
    cpp.write_text(
        (Path(__file__).with_name("chart_terminal.hpp")).read_text()
        + "\n"
        + code
        + "\n"
        + (Path(__file__).with_name("chart_scenarios.cpp")).read_text()
    )
    executable = directory / "controller"
    result = subprocess.run(
        ["g++", "-std=c++17", "-O0", str(cpp), "-o", str(executable)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return executable


@pytest.mark.parametrize(
    "scenario",
    ["opens", "stray", "budgets", "symbols", "teardown", "capacity", "sightings", "rollback"],
)
def test_chart_pass(controller, scenario):
    result = subprocess.run([str(controller), scenario], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
