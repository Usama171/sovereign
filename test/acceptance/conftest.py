import os

import pytest


@pytest.fixture
def skip_if_not_worker_v2():
    if os.environ.get("SOVEREIGN_WORKER_V2_ENABLED", "0") != "1":
        pytest.skip("worker v2 not enabled")
