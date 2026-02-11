import pytest

from sovereign.v2.data.data_store import (
    DataType,
    InMemoryDataStore,
)
from sovereign.v2.data.repositories import ContextRepository
from sovereign.v2.types import Context


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
def repo(inner):
    # Clear the class-level cache before each test
    ContextRepository._cache.clear()
    return ContextRepository(inner)


class TestContextRepositoryCacheGet:
    def test_cache_miss_on_first_get(self, repo, inner):
        ctx = _make_context()
        inner.set(DataType.Context, ctx.name, ctx)

        result = repo.get("test_ctx")

        assert result is not None
        assert result.name == "test_ctx"
        assert result.data_hash == 12345

    def test_cache_hit_on_second_get(self, repo, inner):
        ctx = _make_context()
        inner.set(DataType.Context, ctx.name, ctx)

        first = repo.get("test_ctx")
        second = repo.get("test_ctx")

        assert first is not None
        assert second is not None
        assert second.data_hash == first.data_hash

    def test_cache_hit_returns_deep_copy(self, repo, inner):
        ctx = _make_context(data={"items": [1, 2, 3]})
        inner.set(DataType.Context, ctx.name, ctx)

        first = repo.get("test_ctx")
        second = repo.get("test_ctx")
        assert first is not second

        first.data["items"].append(999)
        third = repo.get("test_ctx")
        assert 999 not in third.data["items"]

    def test_hash_change_triggers_refetch(self, repo, inner):
        ctx = _make_context(data_hash=111)
        inner.set(DataType.Context, ctx.name, ctx)

        repo.get("test_ctx")

        updated = _make_context(data={"key": "new_value"}, data_hash=222)
        inner.set(DataType.Context, updated.name, updated)

        result = repo.get("test_ctx")
        assert result is not None
        assert result.data_hash == 222
        assert result.data == {"key": "new_value"}

    def test_eviction_when_inner_returns_none(self, repo, inner):
        ctx = _make_context()
        inner.set(DataType.Context, ctx.name, ctx)
        repo.get("test_ctx")

        # Remove from inner store
        inner.stores[DataType.Context].pop("test_ctx")

        result = repo.get("test_ctx")
        assert result is None
        assert "test_ctx" not in ContextRepository._cache

    def test_get_returns_none_for_missing_context(self, repo):
        result = repo.get("nonexistent")
        assert result is None
        assert "nonexistent" not in ContextRepository._cache

    def test_multiple_contexts_cached_independently(self, repo, inner):
        ctx_a = _make_context(name="ctx_a", data_hash=100)
        ctx_b = _make_context(name="ctx_b", data_hash=200)
        inner.set(DataType.Context, ctx_a.name, ctx_a)
        inner.set(DataType.Context, ctx_b.name, ctx_b)

        result_a = repo.get("ctx_a")
        result_b = repo.get("ctx_b")

        assert result_a.data_hash == 100
        assert result_b.data_hash == 200


class TestContextRepositoryCacheSave:
    def test_save_warms_cache(self, repo):
        ctx = _make_context()
        repo.save(ctx)

        assert "test_ctx" in ContextRepository._cache
        assert ContextRepository._cache["test_ctx"].data_hash == 12345

    def test_save_then_get_hits_cache(self, repo):
        ctx = _make_context()
        repo.save(ctx)

        result = repo.get("test_ctx")
        assert result is not None
        assert result.data_hash == ctx.data_hash

    def test_save_then_mutate_does_not_corrupt_cache(self, repo):
        ctx = _make_context(data={"items": [1, 2, 3]})
        repo.save(ctx)
        ctx.data["items"].append(999)

        result = repo.get("test_ctx")
        assert 999 not in result.data["items"]


class TestContextRepositoryCacheInvalidation:
    def test_update_refresh_after_invalidates_cache(self, repo, inner):
        ctx = _make_context()
        repo.save(ctx)
        assert "test_ctx" in ContextRepository._cache

        repo.update_refresh_after("test_ctx", 9999)
        assert "test_ctx" not in ContextRepository._cache

    def test_get_hash_delegates_without_affecting_cache(self, repo, inner):
        ctx = _make_context()
        inner.set(DataType.Context, ctx.name, ctx)

        result = repo.get_hash("test_ctx")
        assert result == 12345


class TestContextRepositoryCacheSharedAcrossInstances:
    def test_cache_shared_across_instances(self, inner):
        ContextRepository._cache.clear()

        repo1 = ContextRepository(inner)
        repo2 = ContextRepository(inner)

        ctx = _make_context()
        repo1.save(ctx)

        # repo2 should see the cached value without hitting the store
        assert "test_ctx" in ContextRepository._cache
        result = repo2.get("test_ctx")
        assert result is not None
        assert result.data_hash == 12345
