import pytest

from drools.ruleset import RulesetCollection


@pytest.fixture(autouse=True)
def cleanup_engine():
    """Ensure RulesetCollection engine is cleaned up after each test.

    This prevents tests from interfering with each other by ensuring
    the engine is shutdown between tests. This is especially important
    when mixing HA and non-HA tests.
    """
    yield
    # Cleanup after each test
    if RulesetCollection.engine is not None:
        try:
            RulesetCollection.shutdown()
        except Exception:
            # Ignore errors during cleanup
            pass
