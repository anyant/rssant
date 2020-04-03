import asyncio
import pytest
import rssant_common.django_setup  # noqa:F401


@pytest.fixture(scope="session")
def event_loop():
    # Fix pytest-asyncio ResourceWarning: unclosed socket
    loop = asyncio.get_event_loop()
    try:
        yield loop
    finally:
        loop.close()
