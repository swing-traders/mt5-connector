"""Reject source drift before compiling the native busy-build probe."""

import pytest
from test_history_build_busy import instrument

SOURCE = """void SendBusy(
#property script_show_inputs
input int size;
ArraySize(rows)
ArraySize(g_history)
k < na + nb;
"""


@pytest.mark.parametrize(
    "anchor",
    [
        "void SendBusy(",
        "#property script_show_inputs",
        "\ninput ",
        "ArraySize(",
        "ArraySize(g_history)",
        "k < na + nb;",
    ],
)
def test_a_missing_probe_edit_fails_at_instrumentation(anchor):
    broken = SOURCE.replace(anchor, "reformatted")
    with pytest.raises(AssertionError, match="matched no source"):
        instrument(broken)


@pytest.mark.parametrize(
    "anchor", ["void SendBusy(", "#property script_show_inputs", "k < na + nb;"]
)
def test_an_ambiguous_unique_probe_edit_fails_at_instrumentation(anchor):
    with pytest.raises(AssertionError, match="matched 2 times"):
        instrument(SOURCE + anchor)


def test_probe_edits_leave_the_request_registry_on_the_native_array_size():
    assert instrument(SOURCE) == """void UnusedSendBusy(

int size;
BuildSize(rows)
ArraySize(g_history)
k < na + nb && BuildStep();
"""
