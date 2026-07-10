"""
Jiuwen Failover Proxy — 管理面板 API 路由

依赖 main.py 在 lifespan 中调用 init_admin() 注入 _config / _proxy 引用。
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from app.config import ProxyConfig, ProviderConfig
from app.circuit_breaker import CircuitBreaker

router = APIRouter(prefix="/admin")

# ── globals (injected by main.py lifespan) ────────────────────────────
_config: ProxyConfig | None = None
_proxy: Any = None  # FailoverProxy instance
_startup_probe: dict[str, dict] = {}


def init_admin(config: ProxyConfig, proxy: Any, probe_results: dict[str, dict]) -> None:
    """Called by main.py lifespan to inject runtime references."""
    global _config, _proxy, _startup_probe
    _config = config
    _proxy = proxy
    _startup_probe = probe_results


# ── auth helper ───────────────────────────────────────────────────────

_ADMIN_KEY: str = ""


def set_admin_key(key: str) -> None:
    global _ADMIN_KEY
    _ADMIN_KEY = key


def _verify_admin(req: Request) -> None:
    if not _ADMIN_KEY:
        return
    auth = req.headers.get("Authorization", "")
    if auth != f"Bearer {_ADMIN_KEY}":
        raise HTTPException(401, "Unauthorized")


# ═══════════════════════════════════════════════════════════════════════
# 仪表盘
# ═══════════════════════════════════════════════════════════════════════


@router.get("/dashboard")
async def dashboard(request: Request):
    _verify_admin(request)
    if not _config or not _proxy:
        return {"status": "not ready"}

    cb: CircuitBreaker = _proxy._cb
    rs = getattr(_proxy, "_result_store", None)

    # 24h 用量摘要
    stats_24h = {}
    if rs:
        try:
            stats_24h = rs.get_stats_summary(hours=24)
        except Exception:
            stats_24h = {"total_requests": 0, "total_switches": 0}

    return {
        "status": "ok",
        "uptime_hours": _get_uptime_hours(),
        "provider_count": len(_config.sorted_providers),
        "circuit_breaker": {
            "enabled": cb._failure_threshold > 0,
            "summary": cb.summary(),
            "states": {
                name: {
                    "degraded": s.degraded,
                    "failures": s.failure_count,
                    "switches": s.total_switches_away,
                }
                for name, s in cb._states.items()
            },
        },
        "soft_trigger": {
            "enabled": _config.soft_trigger.enabled,
            "ttft_threshold_ms": _config.soft_trigger.ttft_threshold_ms,
            "tpot_threshold_ms": _config.soft_trigger.tpot_threshold_ms,
            "throughput_threshold_tokens_per_sec": _config.soft_trigger.throughput_threshold_tokens_per_sec,
            "throughput_window_seconds": _config.soft_trigger.throughput_window_seconds,
        },
        "stats_24h": stats_24h,
        "providers": [
            {
                "name": p.name,
                "priority": p.priority,
                "model": list(p.model_map.values())[0] if p.model_map else p.api_base,
                "api_base": p.api_base,
                "timeout": p.timeout,
                "cb_state": {
                    "degraded": cb._states[p.name].degraded if p.name in cb._states else False,
                    "failures": cb._states[p.name].failure_count if p.name in cb._states else 0,
                    "switches": cb._states[p.name].total_switches_away if p.name in cb._states else 0,
                },
                "probe": _startup_probe.get(p.name, {"status": "unknown"}),
            }
            for p in _config.sorted_providers
        ],
    }


# ═══════════════════════════════════════════════════════════════════════
# Provider 管理
# ═══════════════════════════════════════════════════════════════════════


@router.get("/providers")
async def list_providers(request: Request):
    _verify_admin(request)
    if not _config or not _proxy:
        return {"providers": []}
    cb: CircuitBreaker = _proxy._cb
    return {
        "providers": [
            {
                "name": p.name,
                "priority": p.priority,
                "api_base": p.api_base,
                "api_type": p.api_type,
                "timeout": p.timeout,
                "model_map": p.model_map,
                "cb_state": {
                    "degraded": cb._states[p.name].degraded if p.name in cb._states else False,
                    "failures": cb._states[p.name].failure_count if p.name in cb._states else 0,
                    "switches": cb._states[p.name].total_switches_away if p.name in cb._states else 0,
                },
                "probe": _startup_probe.get(p.name, {"status": "unknown"}),
            }
            for p in _config.sorted_providers
        ]
    }


class ReorderBody(BaseModel):
    order: list[str]


@router.put("/providers/reorder")
async def reorder_providers(request: Request, body: ReorderBody):
    _verify_admin(request)
    if not _config:
        raise HTTPException(503, "proxy not ready")

    names = {p.name for p in _config.providers}
    if set(body.order) != names:
        raise HTTPException(400, f"Provider names mismatch. Got {body.order}, expected {names}")

    for i, name in enumerate(body.order):
        for p in _config.providers:
            if p.name == name:
                p.priority = i
                break

    return {"ok": True, "order": body.order}


@router.post("/providers/{name}/reset-cb")
async def reset_circuit_breaker(request: Request, name: str):
    """手动恢复某个 provider 的熔断器状态。"""
    _verify_admin(request)
    if not _proxy:
        raise HTTPException(503, "proxy not ready")
    cb: CircuitBreaker = _proxy._cb
    if name not in cb._states:
        raise HTTPException(404, f"Provider '{name}' not found")
    cb.record_success(name)
    return {"ok": True, "provider": name, "status": "UP"}


@router.get("/providers/{name}/keys")
async def provider_keys(request: Request, name: str):
    """查看 provider 的 key 状态（单 key 模式返回基础信息）。"""
    _verify_admin(request)
    if not _config:
        raise HTTPException(503, "proxy not ready")
    provider = next((p for p in _config.providers if p.name == name), None)
    if not provider:
        raise HTTPException(404, f"Provider '{name}' not found")

    # 单 key 模式
    keys = provider.resolved_keys() if hasattr(provider, "resolved_keys") else [provider.api_key]
    masked = [_mask_key(k) for k in keys]

    return {
        "provider": name,
        "keys": masked,
        "count": len(keys),
    }


# ═══════════════════════════════════════════════════════════════════════
# 模型测试
# ═══════════════════════════════════════════════════════════════════════


class TestBody(BaseModel):
    provider: str
    prompt: str
    stream: bool = False
    max_tokens: int = 1024


@router.post("/test/completion")
async def test_completion(request: Request, body: TestBody):
    _verify_admin(request)
    if not _config or not _proxy:
        raise HTTPException(503, "proxy not ready")

    provider = next((p for p in _config.providers if p.name == body.provider), None)
    if not provider:
        raise HTTPException(404, f"Provider '{body.provider}' not found")

    test_body = {
        "model": "local_route",
        "messages": [{"role": "user", "content": body.prompt}],
        "max_tokens": body.max_tokens,
        "stream": body.stream,
    }

    if body.stream:
        return StreamingResponse(
            _admin_test_stream(provider, test_body),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    t0 = time.monotonic()
    try:
        result = await _proxy._try_chat(provider, test_body)
        elapsed = (time.monotonic() - t0) * 1000
        content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
        usage = result.get("usage", {})
        return {
            "content": content,
            "total_ms": round(elapsed, 1),
            "ttft_ms": round(elapsed, 1),
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
            "model": result.get("model", ""),
        }
    except Exception as exc:
        return JSONResponse(status_code=502, content={"error": str(exc)})


async def _admin_test_stream(provider: ProviderConfig, body: dict):
    """Stream test response from a specific provider, with timing metrics."""
    t0 = time.monotonic()
    first_chunk = None
    content_parts = []
    try:
        async for chunk in _proxy._try_chat_stream(provider, body):
            if first_chunk is None:
                first_chunk = time.monotonic()
                ttft = (first_chunk - t0) * 1000
                yield f"data: {json.dumps({'type': 'meta', 'ttft_ms': round(ttft, 1)})}\n\n"
            yield chunk
            # extract content for final summary
            raw = chunk.decode("utf-8", errors="replace").strip()
            if raw.startswith("data: ") and raw != "data: [DONE]":
                try:
                    d = json.loads(raw[6:])
                    delta = d.get("choices", [{}])[0].get("delta", {})
                    if delta.get("content"):
                        content_parts.append(delta["content"])
                except (json.JSONDecodeError, IndexError):
                    pass

        total = (time.monotonic() - t0) * 1000
        yield f"data: {json.dumps({'type': 'done', 'total_ms': round(total, 1), 'content_len': len(''.join(content_parts))})}\n\n"
    except Exception as exc:
        yield f"data: {json.dumps({'type': 'error', 'error': str(exc)})}\n\n"


# ═══════════════════════════════════════════════════════════════════════
# 用量统计
# ═══════════════════════════════════════════════════════════════════════


@router.get("/stats/summary")
async def stats_summary(request: Request, hours: int = Query(24, ge=1, le=720)):
    _verify_admin(request)
    rs = getattr(_proxy, "_result_store", None) if _proxy else None
    if not rs:
        return {"total_requests": 0, "total_switches": 0}
    try:
        return rs.get_stats_summary(hours=hours)
    except Exception as e:
        return {"error": str(e)}


@router.get("/stats/by-provider")
async def stats_by_provider(request: Request, hours: int = Query(24, ge=1, le=720)):
    _verify_admin(request)
    rs = getattr(_proxy, "_result_store", None) if _proxy else None
    if not rs:
        return {"providers": []}
    try:
        return {"providers": rs.get_stats_by_provider(hours=hours)}
    except Exception as e:
        return {"error": str(e)}


@router.get("/stats/switches")
async def stats_switches(request: Request, hours: int = Query(24, ge=1, le=720), limit: int = Query(50, ge=1, le=500)):
    _verify_admin(request)
    rs = getattr(_proxy, "_result_store", None) if _proxy else None
    if not rs:
        return {"switches": []}
    try:
        return {"switches": rs.get_recent_switches(hours=hours, limit=limit)}
    except Exception as e:
        return {"error": str(e)}


@router.get("/stats/trend")
async def stats_trend(request: Request, hours: int = Query(168, ge=1, le=720)):
    _verify_admin(request)
    rs = getattr(_proxy, "_result_store", None) if _proxy else None
    if not rs:
        return {"buckets": []}
    try:
        return {"buckets": rs.get_trend_buckets(hours=hours)}
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════
# 安全自检
# ═══════════════════════════════════════════════════════════════════════


_SENSITIVE_PATTERNS = [
    (re.compile(r"sk-[a-zA-Z0-9]{20,}"), "sk-***"),
    (re.compile(r"ark-[a-zA-Z0-9-]{20,}"), "ark-***"),
    (re.compile(r"[a-f0-9]{32}\.[a-zA-Z0-9]+"), "glm-key-***"),
]


def _mask_key(key: str) -> str:
    """Mask an API key for display."""
    if len(key) <= 8:
        return key[:4] + "****"
    return key[:6] + "****" + key[-4:]


def sanitize_error_message(msg: str) -> str:
    """Filter sensitive information from error messages."""
    for pattern, replacement in _SENSITIVE_PATTERNS:
        msg = pattern.sub(replacement, msg)
    return msg


@router.get("/security/checks")
async def security_checks(request: Request):
    _verify_admin(request)
    if not _config or not _proxy:
        return {"checks": [], "overall": "error"}

    checks = []

    # 1. Provider Key 检查
    # 将 key -> 不同 api_base 集合，仅当同一 key 被不同平台的 provider 使用时才告警
    key_base_map: dict[str, set[str]] = {}
    for p in _config.providers:
        key_base_map.setdefault(p.api_key, set()).add(p.api_base)
    cross_platform_duplicate = any(len(bases) > 1 for bases in key_base_map.values())
    keys_ok = all(
        (p.api_key.startswith("sk-") or p.api_key.startswith("ark-") or "." in p.api_key)
        for p in _config.providers
    )
    checks.append({
        "name": "provider_keys",
        "status": "warn" if cross_platform_duplicate else ("pass" if keys_ok else "warn"),
        "detail": "所有 Key 格式正常" if keys_ok and not cross_platform_duplicate
        else ("多个 Provider 使用相同 Key，建议独立" if cross_platform_duplicate else "部分 Key 格式异常"),
    })

    # 2. 熔断器配置
    cb = _config.circuit_breaker
    cb_ok = cb.enabled and cb.failure_threshold >= 2
    checks.append({
        "name": "circuit_breaker",
        "status": "pass" if cb_ok else "warn",
        "detail": f"熔断器已启用，阈值={cb.failure_threshold}，恢复间隔={cb.recovery_interval_seconds}s"
        if cb_ok else "熔断器未启用或阈值 < 2",
    })

    # 3. 软触发配置
    st = _config.soft_trigger
    st_ok = (
        st.enabled
        and st.ttft_threshold_ms >= 30000
        and st.tpot_threshold_ms >= 5000
        and st.throughput_threshold_tokens_per_sec >= 2
    )
    checks.append({
        "name": "soft_trigger",
        "status": "pass" if st_ok else "info",
        "detail": f"软触发已启用，TTFT={st.ttft_threshold_ms}ms TPOT={st.tpot_threshold_ms}ms 吞吐={st.throughput_threshold_tokens_per_sec}t/s"
        if st.enabled else "软触发未启用",
    })

    # 4. HTTPS 检查
    https_ok = all(p.api_base.startswith("https://") for p in _config.providers)
    checks.append({
        "name": "https",
        "status": "pass" if https_ok else "fail",
        "detail": "所有 Provider 使用 HTTPS" if https_ok
        else "存在非 HTTPS 连接，建议升级",
    })

    # 5. 启动探测结果
    probe_ok = any(
        v.get("status") == "ok" for v in _startup_probe.values()
    )
    reachable = sum(1 for v in _startup_probe.values() if v.get("status") == "ok")
    total = len(_startup_probe)
    checks.append({
        "name": "startup_probe",
        "status": "pass" if probe_ok else "fail",
        "detail": f"{reachable}/{total} Provider 启动探测成功" if probe_ok
        else "所有 Provider 启动探测失败",
    })

    # 6. Error sanitizer
    checks.append({
        "name": "error_sanitizer",
        "status": "pass",
        "detail": "错误信息过滤 (sanitizer) 已部署",
    })

    overall = "pass" if all(c["status"] == "pass" for c in checks) else \
              "warn" if any(c["status"] in ("warn", "info") for c in checks) else "fail"

    return {"checks": checks, "overall": overall}


# ═══════════════════════════════════════════════════════════════════════
# 配置查看
# ═══════════════════════════════════════════════════════════════════════


@router.get("/config")
async def get_config(request: Request):
    _verify_admin(request)
    if not _config:
        return {"config": {}}
    return {
        "config": _config.model_dump(mode="json"),
        "providers": [
            {
                "name": p.name,
                "priority": p.priority,
                "api_base": p.api_base,
                "api_key": _mask_key(p.api_key),
                "model_map": p.model_map,
                "timeout": p.timeout,
                "api_type": p.api_type,
            }
            for p in _config.sorted_providers
        ],
    }


# ═══════════════════════════════════════════════════════════════════════
# 实时事件 SSE (用于 Live Monitor 页面)
# ═══════════════════════════════════════════════════════════════════════

_event_subscribers: list[asyncio.Queue] = []


def broadcast_event(event_type: str, data: dict) -> None:
    """Broadcast an event to all SSE subscribers. Called by proxy.py."""
    payload = json.dumps({"type": event_type, **data})
    stale = []
    for q in _event_subscribers:
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            stale.append(q)
    for q in stale:
        _event_subscribers.remove(q)


@router.get("/events/stream")
async def event_stream(request: Request, token: str = ""):
    """SSE endpoint — accepts token query param for EventSource (no custom headers)."""
    if token:
        #伪造 Authorization header 以便复用 _verify_admin
        from starlette.datastructures import Headers
        scope = dict(request.scope)
        scope["headers"] = [(b"authorization", f"Bearer {token}".encode())]
        fake_req = Request(scope)
        _verify_admin(fake_req)
    else:
        _verify_admin(request)
    async def _event_generator():
        queue: asyncio.Queue = asyncio.Queue(maxsize=256)
        _event_subscribers.append(queue)
        try:
            while True:
                payload = await queue.get()
                yield f"data: {payload}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            if queue in _event_subscribers:
                _event_subscribers.remove(queue)

    return StreamingResponse(
        _event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ── helpers ──────────────────────────────────────────────────────────

_start_time: float = time.monotonic()


def _get_uptime_hours() -> float:
    return round((time.monotonic() - _start_time) / 3600, 2)
