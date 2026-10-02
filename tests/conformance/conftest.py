import pytest
from mt5_wheel import PackageSurface, fetch_wheel, read_surface


@pytest.fixture(scope="session")
def package_surface(request) -> PackageSurface:
    return read_surface(fetch_wheel(request.config.cache.mkdir("mt5-wheel")))
