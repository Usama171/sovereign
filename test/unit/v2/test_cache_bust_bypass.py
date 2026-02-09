from unittest.mock import patch

import pytest
from sovereign.types import DiscoveryResponse
from sovereign.utils.mock import mock_discovery_request
from sovereign.v2.data.data_store import InMemoryDataStore
from sovereign.v2.data.repositories import DiscoveryEntryRepository
from sovereign.v2.data.worker_queue import InMemoryQueue
from sovereign.v2.types import DiscoveryEntry
from sovereign.v2.web import wait_for_discovery_response
from starlette_context import context


@pytest.fixture(scope="function")
def data_store() -> InMemoryDataStore:
    return InMemoryDataStore()


@pytest.fixture(scope="function")
def queue() -> InMemoryQueue:
    return InMemoryQueue()


@pytest.fixture
def mock_response() -> DiscoveryResponse:
    return DiscoveryResponse(
        version_info="test_version_123",
        resources=[{"@type": "test", "name": "test_resource"}],
    )


@pytest.mark.asyncio
async def test_cache_bust_renders_inline(
    data_store: InMemoryDataStore,
    queue: InMemoryQueue,
    mock_response: DiscoveryResponse,
):
    """When cache_bust is set in metadata, render inline without persisting."""
    request = mock_discovery_request(
        resource_type="clusters",
        metadata={"cache_bust": "1234567890"},
    )

    with (
        patch("sovereign.v2.web.get_data_store_web", return_value=data_store),
        patch("sovereign.v2.web.get_queue", return_value=queue),
        patch(
            "sovereign.v2.web.render_template_to_response",
            return_value=mock_response,
        ) as mock_render,
    ):
        result = await wait_for_discovery_response(request)

    # render_template_to_response should have been called
    mock_render.assert_called_once()

    # should return the rendered response
    assert result is not None
    assert result.version_info == "test_version_123"

    # no DiscoveryEntry should have been created
    discovery_entry_repo = DiscoveryEntryRepository(data_store)
    # The data store should have no discovery entries
    assert queue.is_empty(), "No render job should have been queued"

    # CACHE_XDS_HIT should be set to bypass
    assert context.data.get("CACHE_XDS_HIT") == "bypass"


@pytest.mark.asyncio
async def test_no_cache_bust_uses_normal_flow(
    data_store: InMemoryDataStore,
    queue: InMemoryQueue,
    mock_response: DiscoveryResponse,
):
    """Without cache_bust, the normal DB lookup + queue flow is used."""
    request = mock_discovery_request(
        resource_type="clusters",
        metadata={"auth": "test_auth"},
    )

    with (
        patch("sovereign.v2.web.get_data_store_web", return_value=data_store),
        patch("sovereign.v2.web.get_queue", return_value=queue),
        patch(
            "sovereign.v2.web.render_template_to_response",
        ) as mock_render,
        patch("sovereign.v2.web.config") as mock_config,
    ):
        mock_config.cache.hash_rules = []
        mock_config.cache.read_timeout = 0.1
        mock_config.cache.poll_interval_secs = 0.05
        result = await wait_for_discovery_response(request)

    # render_template_to_response should NOT have been called directly
    mock_render.assert_not_called()

    # A render job should have been queued (normal flow)
    assert not queue.is_empty(), "A render job should have been queued"


@pytest.mark.asyncio
async def test_empty_cache_bust_uses_normal_flow(
    data_store: InMemoryDataStore,
    queue: InMemoryQueue,
):
    """Empty string cache_bust is falsy, so normal flow is used."""
    request = mock_discovery_request(
        resource_type="clusters",
        metadata={"cache_bust": "", "auth": "test_auth"},
    )

    with (
        patch("sovereign.v2.web.get_data_store_web", return_value=data_store),
        patch("sovereign.v2.web.get_queue", return_value=queue),
        patch(
            "sovereign.v2.web.render_template_to_response",
        ) as mock_render,
        patch("sovereign.v2.web.config") as mock_config,
    ):
        mock_config.cache.hash_rules = []
        mock_config.cache.read_timeout = 0.1
        mock_config.cache.poll_interval_secs = 0.05
        result = await wait_for_discovery_response(request)

    # render_template_to_response should NOT have been called (empty string is falsy)
    mock_render.assert_not_called()

    # Normal flow should have been used
    assert not queue.is_empty(), "A render job should have been queued"


@pytest.mark.asyncio
async def test_cache_hit_sets_hit_in_context(
    data_store: InMemoryDataStore,
    queue: InMemoryQueue,
    mock_response: DiscoveryResponse,
):
    """When a cached DiscoveryEntry exists with a response, CACHE_XDS_HIT is 'hit'."""
    request = mock_discovery_request(
        resource_type="clusters",
        metadata={"auth": "test_auth"},
    )

    # Pre-populate a cached entry in the data store
    discovery_entry_repo = DiscoveryEntryRepository(data_store)

    with (
        patch("sovereign.v2.web.config") as mock_config,
    ):
        mock_config.cache.hash_rules = []
        request_hash = request.cache_key([])

    cached_entry = DiscoveryEntry(
        request_hash=request_hash,
        template="clusters",
        request=request,
        response=mock_response,
    )
    discovery_entry_repo.save(cached_entry)

    with (
        patch("sovereign.v2.web.get_data_store_web", return_value=data_store),
        patch("sovereign.v2.web.get_queue", return_value=queue),
        patch("sovereign.v2.web.config") as mock_config,
    ):
        mock_config.cache.hash_rules = []
        result = await wait_for_discovery_response(request)

    assert result is not None
    assert result.version_info == "test_version_123"
    assert context.data.get("CACHE_XDS_HIT") == "hit"
    assert queue.is_empty(), "No render job should have been queued for a cache hit"
