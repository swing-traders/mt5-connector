"""Make skipped certification cases fail with their names and reasons."""

import pytest


def fail_skip(report):
    if report.skipped:
        report.outcome = "failed"
        report.longrepr = f"Certification forbids a skip: {report.nodeid}\n{report.longrepr}"
        if hasattr(report, "wasxfail"):
            del report.wasxfail


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    result = yield
    fail_skip(result.get_result())


@pytest.hookimpl(tryfirst=True)
def pytest_collectreport(report):
    fail_skip(report)
