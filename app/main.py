from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.config import load_config, ProxyConfig
from app.admin_api import broadcast_event
from app.logger import setup_logger
from app.proxy import FailoverProxy, heuristic_difficulty, _cb_snapshot, _cb_snapshot

logger = setup_logger("llm-failover")

# ── globals (set during lifespan) ────────────────────────────────────

_config: ProxyConfig | None = None
_proxy: FailoverProxy | None = None
_recovery_task: asyncio.Task | None = None
_startup_probe_results: dict[str, dict] = {}
_probe_cache: dict[str, tuple[float, dict]] = {}  # name -> (timestamp, result)


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
                    broadcast_event("recovery", {
                        "provider": state.name,
                        "status": "UP",
                    })
                    broadcast_event("provider_status", {
                        "providers": _cb_snapshot(cb),
                    })
                else:
                    logger.info(
                        "[Recovery] %s 仍未恢复 HTTP %d cb=%s",
                        state.name, resp.status_code, cb.summary(),
                    )
                    broadcast_event("recovery", {
                        "provider": state.name,
                        "status": "DOWN",
                        "error": f"HTTP {resp.status_code}",
                    })
            except Exception as exc:
                logger.info(
                    "[Recovery] %s 探测失败: %s cb=%s",
                    state.name, exc, cb.summary(),
                )

    await client.aclose()


async def _startup_probe_all(config: ProxyConfig):
    """后台：启动时对所有 provider 做连通性探测，不阻塞启动。"""
    async def _probe_single(provider) -> dict:
        t0 = time.monotonic()
        try:
            url = provider.api_base.rstrip("/") + config.circuit_breaker.probe_path
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
                resp = await client.get(
                    url,
                    headers={"Authorization": f"Bearer {provider.api_key}"},
                )
                latency = (time.monotonic() - t0) * 1000
                if resp.is_success:
                    return {"status": "ok", "latency_ms": round(latency, 1), "error": None}
                return {"status": "error", "latency_ms": round(latency, 1), "error": f"HTTP {resp.status_code}"}
        except Exception as exc:
            return {"status": "error", "latency_ms": None, "error": str(exc)}

    import time as _time
    time = _time  # noqa: F811

    probes = []
    for provider in config.sorted_providers:
        probes.append(_probe_single(provider))
    results = await asyncio.gather(*probes, return_exceptions=True)

    global _startup_probe_results
    for provider, result in zip(config.sorted_providers, results):
        if isinstance(result, Exception):
            _startup_probe_results[provider.name] = {"status": "error", "error": str(result), "latency_ms": None}
        else:
            _startup_probe_results[provider.name] = result
            _probe_cache[provider.name] = (time.monotonic(), result)


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

    # 初始化管理面板
    from app.admin_api import init_admin, set_admin_key
    init_admin(_config, _proxy, _startup_probe_results)
    if _config.admin_key:
        set_admin_key(_config.admin_key)
        logger.info("Admin panel authentication enabled")

    # 后台启动探测
    asyncio.create_task(_startup_probe_all(_config))

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

# ── 管理面板静态文件 ────────────────────────────────────────────────

_static_dir = Path(__file__).resolve().parent / "static"
_static_dir.mkdir(exist_ok=True)
app.mount("/admin/static", StaticFiles(directory=str(_static_dir)), name="admin_static")

# ── 管理面板 API ─────────────────────────────────────────────────────

from app.admin_api import router as admin_router
app.include_router(admin_router)


@app.get("/admin")
@app.get("/admin/{path:path}")
async def admin_spa():
    """Serve the admin SPA — all /admin/* paths return the HTML."""
    spa_path = _static_dir / "admin.html"
    if not spa_path.exists():
        return JSONResponse(status_code=404, content={"error": "admin.html not found"})
    from starlette.responses import HTMLResponse
    return HTMLResponse(spa_path.read_text(encoding="utf-8"))


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
        "startup_probe": _startup_probe_results,
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


# ── A2: result store endpoints ──────────────────────────────────


class ConsumeBody(BaseModel):
    ids: list[int] = []


@app.get("/results/{session_id}")
async def get_results(session_id: str):
    """Return unconsumed LLM results for a session. Used by frontend after WS reconnect."""
    if not _proxy or not hasattr(_proxy, "_result_store"):
        return {"results": []}
    return {"results": _proxy._result_store.get_unconsumed(session_id)}


@app.post("/results/consume")
async def consume_results(body: ConsumeBody):
    """Mark result records as consumed (frontend has processed them)."""
    if _proxy and hasattr(_proxy, "_result_store"):
        _proxy._result_store.mark_consumed(body.ids)
    return {"ok": True}


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    if _proxy is None:
        return JSONResponse(status_code=503, content={"error": "proxy not ready"})

    body = await request.json()
    is_stream = body.get("stream", False)

    if is_stream:
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
        from app.admin_api import sanitize_error_message
        result["error"]["message"] = sanitize_error_message(result["error"]["message"])
        return JSONResponse(status_code=502, content=result)
    return result


# ── 方案 C: 启发式难度估计端点 ──────────────────────────────────


class DifficultyInput(BaseModel):
    prompt: str


@app.post("/v1/difficulty")
async def estimate_difficulty(body: DifficultyInput):
    """返回 prompt 的启发式难度分数（无需 GPU，纯文本特征分析）。"""
    return heuristic_difficulty(body.prompt)


# ── direct entry point (python -m app.main) ────────────────────────

if __name__ == "__main__":
    import uvicorn
    cfg_path = os.environ.get("PROXY_CONFIG", str(Path(__file__).resolve().parent.parent / "config.yaml"))
    cfg = load_config(cfg_path)
    import time as _time
    time = _time  # noqa
    uvicorn.run(
        "app.main:app",
        host=cfg.host,
        port=cfg.port,
        log_level=cfg.log_level,
        reload=False,
    )
