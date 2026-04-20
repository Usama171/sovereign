import asyncio
import logging
import multiprocessing
import os
import threading
import time
from concurrent.futures import ProcessPoolExecutor

from structlog.typing import FilteringBoundLogger

from sovereign import config, logs, stats
from sovereign.types import DiscoveryRequest, DiscoveryResponse
from sovereign.v2.data.repositories import DiscoveryEntryRepository
from sovereign.v2.data.utils import get_data_store_web, get_queue
from sovereign.v2.jobs.render_discovery_job import render_template_to_response
from sovereign.v2.logging import get_named_logger
from sovereign.v2.types import DiscoveryEntry, RenderDiscoveryJob

_inline_render_pool: ProcessPoolExecutor | None = None
_inline_render_pool_lock = threading.Lock()
_inline_render_count: int = 0


def _get_inline_render_pool() -> ProcessPoolExecutor:
    """Lazily create a ProcessPoolExecutor for inline renders.

    Uses fork so child processes inherit loaded modules (fast startup),
    with an initialiser that resets DB connections so each child creates
    its own (safe after fork).

    Worker count is based on available CPUs (container-aware), capped at 7
    since there are only 7 templates to render in parallel.
    """
    global _inline_render_pool
    if _inline_render_pool is None:
        with _inline_render_pool_lock:
            if _inline_render_pool is None:
                from sovereign.server import get_available_cpus

                max_workers = min(get_available_cpus(), 7)
                _inline_render_pool = ProcessPoolExecutor(
                    max_workers=max_workers,
                    mp_context=multiprocessing.get_context("fork"),
                    initializer=_reset_inherited_connections,
                )
    assert _inline_render_pool is not None
    return _inline_render_pool


def _shutdown_inline_render_pool():
    """Shut down the pool. Caller must hold _inline_render_pool_lock."""
    global _inline_render_pool
    if _inline_render_pool is not None:
        _inline_render_pool.shutdown(wait=False)
        _inline_render_pool = None


def _reset_inherited_connections():
    """Called once per forked worker process. Resets DB state inherited from
    the parent so each subprocess creates its own connections."""
    from sovereign.v2.data import utils

    utils._data_store_web = None
    utils._context_repository_web = None


def _render_inline_in_subprocess(
    request: DiscoveryRequest,
    request_hash: str,
    node_id: str,
) -> DiscoveryResponse | None:
    """Runs in a subprocess via ProcessPoolExecutor.

    Each subprocess has its own GIL, so multiple calls run with true
    CPU parallelism. Creates its own ContextRepository (and therefore
    its own DB connection) on first use.
    """
    from sovereign.v2.data.utils import get_context_repository_web

    context_repository = get_context_repository_web()
    return render_template_to_response(
        request, request_hash, node_id, context_repository
    )


async def wait_for_discovery_response(
    request: DiscoveryRequest,
    render_inline: bool = False,
) -> DiscoveryResponse | None:
    # 1 - if render_inline is set, render inline without persisting
    # 2 - check if the entry already exists in the database with a non-empty response
    # 3 - if it does, return it
    # 4 - if it doesn't, enqueue a new job to render it
    # 5 - poll for up to CACHE_READ_TIMEOUT seconds, if we find a response, return it

    logger: FilteringBoundLogger = get_named_logger(
        f"{__name__}.{wait_for_discovery_response.__qualname__} ({__file__})",
        level=logging.DEBUG,
    ).bind(
        template=request.template.resource_type,
        process_id=os.getpid(),
        thread_id=threading.get_ident(),
    )

    data_store = get_data_store_web()

    request_hash = request.cache_key(
        config.cache.effective_hash_rules(request.resource_type)
    )
    logger = logger.bind(request_hash=request_hash)

    # render_inline: render inline without persisting to avoid unbounded growth
    sovereign_metadata = request.node.metadata.get("sovereign", {})
    if render_inline or sovereign_metadata.get("render_inline"):
        global _inline_render_count
        logger.info("Inline render requested")
        pool = _get_inline_render_pool()
        with _inline_render_pool_lock:
            _inline_render_count += 1
        try:
            response = await asyncio.get_running_loop().run_in_executor(
                pool,
                _render_inline_in_subprocess,
                request,
                request_hash,
                "inline",
            )
        except Exception:
            logs.access_logger.queue_log_fields(XDS_RESPONSE_SOURCE="inline")
            raise
        finally:
            with _inline_render_pool_lock:
                _inline_render_count -= 1
                if _inline_render_count == 0:
                    _shutdown_inline_render_pool()
        stats.increment(
            "v2.worker.discovery_response",
            tags=[
                f"template:{request.template.resource_type}",
                "result:success" if response else "result:error",
                "source:inline",
            ],
        )
        logs.access_logger.queue_log_fields(XDS_RESPONSE_SOURCE="inline")
        return response

    logger.debug("Starting lookup for discovery response")

    discovery_entry_repository = DiscoveryEntryRepository(data_store)

    queue = get_queue()

    discovery_entry = discovery_entry_repository.get(request_hash)

    if not discovery_entry:
        logger.debug(
            "No existing discovery entry found, creating new entry and enqueuing job"
        )

        # we need to save this request to the database
        discovery_entry = DiscoveryEntry(
            request_hash=request_hash,
            template=request.template.resource_type,
            request=request,
            response=None,
            last_requested_at=int(time.time()),
        )
        discovery_entry_repository.save(discovery_entry)

    # Update last_requested_at to track when this request hash was last requested
    discovery_entry_repository.update_last_requested_at(request_hash)

    if discovery_entry.response:
        logger = logger.bind(last_rendered_at=discovery_entry.last_rendered_at)
        logger.debug("Returning cached response immediately")
        stats.increment(
            "v2.worker.discovery_response",
            tags=[
                f"template:{request.template.resource_type}",
                "result:success",
                "source:from_db",
            ],
        )
        logs.access_logger.queue_log_fields(XDS_RESPONSE_SOURCE="immediately")
        return discovery_entry.response

    # enqueue a job to render this discovery request
    job = RenderDiscoveryJob(request_hash=request_hash)
    queue.put(job)

    # wait for up to CACHE_READ_TIMEOUT seconds for the response to be populated
    logger.debug(
        "Polling for response",
        timeout=config.cache.read_timeout,
        poll_interval=config.cache.poll_interval_secs,
    )

    start_time = asyncio.get_event_loop().time()
    attempts = 0

    while (
        asyncio.get_event_loop().time() - start_time
    ) < config.cache.read_timeout and (
        discovery_entry is None or discovery_entry.response is None
    ):
        attempts += 1
        discovery_entry = discovery_entry_repository.get(request_hash)
        if discovery_entry is None:
            logger.error("No discovery entry found while polling for response")
            return None
        await asyncio.sleep(config.cache.poll_interval_secs)

    elapsed_time = asyncio.get_event_loop().time() - start_time

    if discovery_entry.response:
        logger.debug(
            "Response received after polling",
            attempts=attempts,
            elapsed_time=elapsed_time,
        )

        stats.increment(
            "v2.worker.discovery_response",
            tags=[
                f"template:{request.template.resource_type}",
                "result:success",
                "source:after_polling",
            ],
        )
        logs.access_logger.queue_log_fields(XDS_RESPONSE_SOURCE="after_polling")
    else:
        logger.error(
            "Timeout waiting for response", attempts=attempts, elapsed_time=elapsed_time
        )

        stats.increment(
            "v2.worker.discovery_response",
            tags=[f"template:{request.template.resource_type}", "result:timed_out"],
        )

    return discovery_entry.response
