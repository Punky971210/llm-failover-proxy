from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.config import load_config, ProxyConfig
from app.logger import setup_logger
from app.proxy import FailoverProxy

logger = setup_logger("llm-failover")

# ── globals (set during lifespan) ────────────────────────────────────

_config: ProxyConfig | None = None
_proxy: FailoverProxy | None = None
_recovery_task: asyncio.Task | None = None


async def _recovery_loop(proxy: FailoverProxy):
    """Background task: periodically probe degraded providers."""
    cb = proxy._cb
    cfg = proxy._config.circuit_breaker
    if not cfg.enabled:
        return
    client = httpx.AsyncClient(timeout=httpx.Timeout(10.0))

    while True:
        await asyncio.sleep(cfg.recovery_interval_seconds)
        degraded = cb.get_degraded_list()
        if not degraded:
            continue

        for state in degraded:
            if not cb.needs_recovery_probe(state.name):
                continue
            provider_cfg = next(
                (p for p in proxy._config.sorted_providers if p.name == state.name),
                None,
            )
            if not provider_cfg:
                continue
            cb.mark_recovery_attempted(state.name)
            try:
                url = provider_cfg.api_base.rstrip("/") + cfg.probe_path
                resp = await client.get(url, headers={"Authorization": f"Bearer {provider_cfg.api_key}"})
                if resp.is_success:
                    cb.record_success(state.name)
                    logger.info(
                        "[Recovery] %s 已恢复 cb=%s",
                        state.name, cb.summary(),
                    )
                else:
                    logger.info(
                        "[Recovery] %s 仍未恢复 HTTP %d cb=%s",
                        state.name, resp.status_code, cb.summary(),
                    )
            except Exception as exc:
                logger.info(
                    "[Recovery] %s 探测失败: %s cb=%s",
                    state.name, exc, cb.summary(),
                )

    await client.aclose()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _config, _proxy, _recovery_task
    config_path = os.environ.get(
        "PROXY_CONFIG",
        str(Path(__file__).resolve().parent.parent / "config.yaml"),
    )
    logger.info("Loading config from %s", config_path)
    _config = load_config(config_path)
    _proxy = FailoverProxy(_config)
    # start background recovery probe
    if _config.circuit_breaker.enabled:
        _recovery_task = asyncio.create_task(_recovery_loop(_proxy))
        logger.info(
            "Recovery probe started: interval=%ds threshold=%d",
            _config.circuit_breaker.recovery_interval_seconds,
            _config.circuit_breaker.failure_threshold,
        )
    logger.info(
        "Proxy ready: %d providers, listening on %s:%d",
        len(_config.providers),
        _config.host, _config.port,
    )
    yield
    if _recovery_task is not None:
        _recovery_task.cancel()
    await _proxy.close()
    logger.info("Proxy shut down")


app = FastAPI(
    title="LLM Failover Proxy",
    version="0.1.0",
    lifespan=lifespan,
)


# ── routes ───────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    if not _config or not _proxy:
        return {"status": "not ready"}
    cb = _proxy._cb
    return {
        "status": "ok",
        "providers": [p.name for p in _config.sorted_providers],
        "circuit_breaker": {
            "enabled": _config.circuit_breaker.enabled,
            "summary": cb.summary(),
        },
        "soft_trigger": {
            "enabled": _config.soft_trigger.enabled,
            "ttft_threshold_ms": _config.soft_trigger.ttft_threshold_ms,
            "tpot_threshold_ms": _config.soft_trigger.tpot_threshold_ms,
            "throughput_threshold_tokens_per_sec": _config.soft_trigger.throughput_threshold_tokens_per_sec,
        },
    }


@app.get("/v1/models")
async def list_models():
    """Return all models the proxy may resolve, deduplicated."""
    if not _config:
        return {"object": "list", "data": []}
    seen: set[str] = set()
    models: list[dict] = []
    for p in _config.sorted_providers:
        for incoming in p.model_map:
            if incoming not in seen:
                seen.add(incoming)
                models.append({"id": incoming, "object": "model", "created": 0, "owned_by": p.name})
    return {"object": "list", "data": models}


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    if _proxy is None:
        return JSONResponse(status_code=503, content={"error": "proxy not ready"})

    body = await request.json()
    is_stream = body.get("stream", False)

    if is_stream:
        # Progressive flush mode: proxy internally buffers small batches
        # (50 chunks / 48 KB) and yields them with event loop yields.
        # Using StreamingResponse means Jiuwen receives data progressively,
        # preventing CPU starvation in its event loop.
        return StreamingResponse(
            _proxy.chat_completions_stream(body),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
    result = await _proxy.chat_completions(body)
    if "error" in result:
        return JSONResponse(status_code=502, content=result)
    return result


# ── direct entry point (python -m app.main) ────────────────────────

if __name__ == "__main__":
    import uvicorn
    # Load config once to get host/port, then pass them to uvicorn.
    # The lifespan will reload the same config — acceptable for direct-run.
    cfg_path = os.environ.get("PROXY_CONFIG", str(Path(__file__).resolve().parent.parent / "config.yaml"))
    cfg = load_config(cfg_path)
    uvicorn.run(
        "app.main:app",
        host=cfg.host,
        port=cfg.port,
        log_level=cfg.log_level,
        reload=False,
    )
