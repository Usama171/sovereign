import pytest

from sovereign.v2.data.data_store import (
    CachingDataStore,
    DataType,
    InMemoryDataStore,
)
from sovereign.v2.types import Context, WorkerNode


def _make_context(
    name: str = "test_ctx",
    data: dict | None = None,
    data_hash: int = 12345,
) -> Context:
    return Context(
        name=name,
        data=data or {"key": "value"},
        data_hash=data_hash,
        last_refreshed_at=1000,
        refresh_after=2000,
    )


@pytest.fixture()
def inner():
    return InMemoryDataStore()


@pytest.fixture()
def store(inner):
    return CachingDataStore(inner)


class TestCachingDataStoreGet:
    def test_cache_miss_on_first_get(self, store, inner):
        ctx = _make_context()
        inner.set(DataType.Context, ctx.name, ctx)

        result = store.get(DataType.Context, "test_ctx")

        assert result is not None
        assert result.name == "test_ctx"
        assert result.data_hash == 12345

    def test_cache_hit_on_second_get(self, store, inner):
        ctx = _make_context()
        inner.set(DataType.Context, ctx.name, ctx)

        first = store.get(DataType.Context, "test_ctx")
        second = store.get(DataType.Context, "test_ctx")

        assert first is not None
        assert second is not None
        assert second.data_hash == first.data_hash

    def test_cache_hit_returns_deep_copy(self, store, inner):
        ctx = _make_context(data={"items": [1, 2, 3]})
        inner.set(DataType.Context, ctx.name, ctx)

        first = store.get(DataType.Context, "test_ctx")
        second = store.get(DataType.Context, "test_ctx")
        assert first is not second

        first.data["items"].append(999)
        third = store.get(DataType.Context, "test_ctx")
        assert 999 not in third.data["items"]

    def test_hash_change_triggers_refetch(self, store, inner):
        ctx = _make_context(data_hash=111)
        inner.set(DataType.Context, ctx.name, ctx)

        store.get(DataType.Context, "test_ctx")

        updated = _make_context(data={"key": "new_value"}, data_hash=222)
        inner.set(DataType.Context, updated.name, updated)

        result = store.get(DataType.Context, "test_ctx")
        assert result is not None
        assert result.data_hash == 222
        assert result.data == {"key": "new_value"}

    def test_eviction_when_inner_returns_none(self, store, inner):
        ctx = _make_context()
        inner.set(DataType.Context, ctx.name, ctx)
        store.get(DataType.Context, "test_ctx")

        # Remove from inner store
        inner.stores[DataType.Context].pop("test_ctx")

        result = store.get(DataType.Context, "test_ctx")
        assert result is None
        assert "test_ctx" not in store._cache

    def test_get_returns_none_for_missing_context(self, store):
        result = store.get(DataType.Context, "nonexistent")
        assert result is None
        assert "nonexistent" not in store._cache

    def test_multiple_contexts_cached_independently(self, store, inner):
        ctx_a = _make_context(name="ctx_a", data_hash=100)
        ctx_b = _make_context(name="ctx_b", data_hash=200)
        inner.set(DataType.Context, ctx_a.name, ctx_a)
        inner.set(DataType.Context, ctx_b.name, ctx_b)

        result_a = store.get(DataType.Context, "ctx_a")
        result_b = store.get(DataType.Context, "ctx_b")

        assert result_a.data_hash == 100
        assert result_b.data_hash == 200

    def test_non_context_types_bypass_cache(self, store, inner):
        """get() for non-Context types should delegate directly without caching."""
        node = WorkerNode(node_id="node-1", last_heartbeat=1000)
        inner.set(DataType.WorkerNode, "node-1", node)

        result = store.get(DataType.WorkerNode, "node-1")
        assert result is not None
        assert result.node_id == "node-1"
        assert len(store._cache) == 0


class TestCachingDataStoreSet:
    def test_set_warms_cache(self, store):
        ctx = _make_context()
        store.set(DataType.Context, ctx.name, ctx)

        assert "test_ctx" in store._cache
        assert store._cache["test_ctx"].data_hash == 12345

    def test_set_then_get_hits_cache(self, store):
        ctx = _make_context()
        store.set(DataType.Context, ctx.name, ctx)

        result = store.get(DataType.Context, "test_ctx")
        assert result is not None
        assert result.data_hash == ctx.data_hash

    def test_set_then_mutate_does_not_corrupt_cache(self, store):
        ctx = _make_context(data={"items": [1, 2, 3]})
        store.set(DataType.Context, ctx.name, ctx)
        ctx.data["items"].append(999)

        result = store.get(DataType.Context, "test_ctx")
        assert 999 not in result.data["items"]

    def test_set_non_context_does_not_cache(self, store):
        node = WorkerNode(node_id="node-1", last_heartbeat=1000)
        store.set(DataType.WorkerNode, "node-1", node)
        assert len(store._cache) == 0


class TestCachingDataStoreDelegation:
    def test_get_property_delegates(self, store, inner):
        ctx = _make_context()
        inner.set(DataType.Context, ctx.name, ctx)

        result = store.get_property(DataType.Context, "test_ctx", "data_hash")
        assert result == 12345

    def test_set_property_delegates(self, store, inner):
        ctx = _make_context()
        inner.set(DataType.Context, ctx.name, ctx)

        store.set_property(DataType.Context, "test_ctx", "refresh_after", 9999)
        result = inner.get_property(DataType.Context, "test_ctx", "refresh_after")
        assert result == 9999

    def test_set_property_invalidates_context_cache(self, store, inner):
        ctx = _make_context()
        store.set(DataType.Context, ctx.name, ctx)
        assert "test_ctx" in store._cache

        store.set_property(DataType.Context, "test_ctx", "refresh_after", 9999)
        assert "test_ctx" not in store._cache

    def test_set_property_does_not_invalidate_non_context_cache(self, store, inner):
        """set_property on non-Context types should not affect the context cache."""
        ctx = _make_context()
        store.set(DataType.Context, ctx.name, ctx)
        assert "test_ctx" in store._cache

        node = WorkerNode(node_id="node-1", last_heartbeat=1000)
        inner.set(DataType.WorkerNode, "node-1", node)
        store.set_property(DataType.WorkerNode, "node-1", "last_heartbeat", 2000)

        assert "test_ctx" in store._cache

    def test_migrate_delegates(self, store):
        assert store.migrate() is True
