from __future__ import annotations

import asyncio
import json
import re
import time
from collections import deque
from typing import AsyncIterator, Optional

import httpx

from app.config import ProviderConfig, ProxyConfig
from app.circuit_breaker import CircuitBreaker
from app.result_store import ResultStore
from app.admin_api import broadcast_event
from app.converter import (
    openai_to_anthropic_request,
    anthropic_to_openai_response,
    AnthropicStreamConverter,
)
from app.logger import setup_logger

logger = setup_logger("llm-failover")

# HTTP statuses that are considered "retryable" — we'll try the next provider.
RETRYABLE_STATUSES = {408, 429, 500, 502, 503, 504}


def _is_retryable(exc: Exception) -> bool:
    """Connection / timeout / DNS / read errors are retryable."""
    return isinstance(exc, (
        httpx.ConnectError,
        httpx.ConnectTimeout,
        httpx.ReadError,         # 1M 上下文长响应时连接中断
        httpx.ReadTimeout,
        httpx.WriteTimeout,
        httpx.PoolTimeout,
        httpx.RemoteProtocolError,
    ))


class ProviderFailed(Exception):
    """Raised when a provider fails (status or connection)."""
    def __init__(self, provider_name: str, reason: str, status_code: int = 0):
        self.provider_name = provider_name
        self.reason = reason
        self.status_code = status_code
        super().__init__(f"[{provider_name}] {reason}")


class SoftTriggerSwitch(Exception):
    """Raised when soft metrics (TTFT/TPOT/throughput) indicate degradation."""
    def __init__(self, provider_name: str, reason: str, metrics: dict):
        self.provider_name = provider_name
        self.reason = reason
        self.metrics = metrics
        super().__init__(f"[{provider_name}] {reason} (metrics={metrics})")


# ── SSE helpers ─────────────────────────────────────────────────────────

_RE_SSE_DATA = re.compile(r"^data: (.+)$", re.MULTILINE)


def _rewrite_sse_chunk(raw: bytes, proxy_id: str, proxy_model: str) -> bytes:
    """Rewrite ``id`` and ``model`` fields in SSE data chunks.

    Non-JSON lines (e.g. ``data: [DONE]``) are passed through unchanged.
    If JSON parsing fails for a line, it is passed through as-is.
    """
    def _rewrite_line(m: re.Match) -> str:
        payload = m.group(1)
        if payload.strip() == "[DONE]":
            return m.group(0)
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError:
            return m.group(0)
        # rewrite id
        if "id" in obj:
            obj["id"] = proxy_id
        # rewrite model at top level
        if "model" in obj:
            obj["model"] = proxy_model
        # rewrite model inside choices[i] if present
        choices = obj.get("choices")
        if choices and isinstance(choices, list):
            for ch in choices:
                if isinstance(ch, dict) and "model" in ch:
                    ch["model"] = proxy_model
        return "data: " + json.dumps(obj, ensure_ascii=False)

    text = raw.decode("utf-8", errors="replace")
    rewritten = _RE_SSE_DATA.sub(_rewrite_line, text)
    return rewritten.encode("utf-8")


# ── Heartbeat / idle timeout ───────────────────────────────────────────

async def _aiter_with_heartbeat(
    upstream: AsyncIterator[bytes],
    *,
    provider_name: str,
    heartbeat_interval: float = 3.0,
    idle_soft_trigger_ms: int = 12000,
    t0: float | None = None,
) -> AsyncIterator[bytes]:
    """Wrap an SSE byte steam with keepalive heartbeats and idle failover.

    During idle periods (upstream not sending data), emits SSE comment
    lines (``: keepalive\\n\\n``) every *heartbeat_interval* seconds to
    keep the downstream connection alive.

    If no meaningful data arrives within *idle_soft_trigger_ms* from the
    start (i.e. TTFT violation during upstream's thinking phase), raises
    :class:`SoftTriggerSwitch`.

    Set *idle_soft_trigger_ms* to **0** to disable TTFT checking —
    useful for second-level wrappers (e.g. after the Anthropic converter)
    where the upstream level already covers TTFT detection.
    """
    has_data = False
    start = t0 if t0 is not None else time.monotonic()

    while True:
        try:
            chunk = await asyncio.wait_for(
                upstream.__anext__(),
                timeout=heartbeat_interval,
            )
            if chunk and chunk.strip():
                has_data = True
            yield chunk
        except StopAsyncIteration:
            return
        except asyncio.TimeoutError:
            elapsed_ms = (time.monotonic() - start) * 1000
            if idle_soft_trigger_ms > 0 and not has_data and elapsed_ms > idle_soft_trigger_ms:
                raise SoftTriggerSwitch(
                    provider_name,
                    f"空闲超时: 等待首 chunk {elapsed_ms:.0f}ms > {idle_soft_trigger_ms}ms",
                    {"ttft_s": round(elapsed_ms / 1000, 2)},
                )
            yield b": keepalive\n\n"


# ── Sliding window metrics ──────────────────────────────────────────────

class SlidingMetrics:
    """Real-time stream quality metrics for soft trigger detection."""

    def __init__(self, throughput_window: float = 5.0):
        self._chunk_times: deque[float] = deque()
        self._chunk_sizes: deque[int] = deque()
        self._timestamps: deque[float] = deque()
        self._throughput_window = throughput_window
        self._total_chars = 0
        self._total_chunks = 0
        self._first_chunk_time: float | None = None
        self._start_time = time.monotonic()

    def record_chunk(self, text_len: int) -> None:
        now = time.monotonic()
        self._chunk_times.append(now)
        self._chunk_sizes.append(text_len)
        self._timestamps.append(now)
        self._total_chars += text_len
        self._total_chunks += 1
        if self._first_chunk_time is None:
            self._first_chunk_time = now

        # keep window trim (last 10 entries for TPOT)
        while len(self._chunk_times) > 10:
            self._chunk_times.popleft()
            self._chunk_sizes.popleft()
        # keep throughput window
        window_start = now - self._throughput_window
        while self._timestamps and self._timestamps[0] < window_start:
            self._timestamps.popleft()

    @property
    def ttft(self) -> float | None:
        """Time to first token in seconds, or None if no chunk yet."""
        if self._first_chunk_time is None:
            return None
        return self._first_chunk_time - self._start_time

    @property
    def avg_tpot(self) -> float | None:
        """Average time per token across recent chunks (seconds)."""
        n = len(self._chunk_times)
        if n < 2:
            return None
        elapsed = self._chunk_times[-1] - self._chunk_times[0]
        if elapsed <= 0:
            return None
        # sum sizes except first chunk (which is often empty reasoning header)
        total_chars = 0
        for i in range(1, n):
            total_chars += self._chunk_sizes[i]
        if total_chars <= 0:
            return None
        return elapsed / total_chars

    @property
    def tokens_per_sec(self) -> float | None:
        """Recent throughput in tokens/s based on last 5s window."""
        n = len(self._timestamps)
        if n < 2:
            return None
        span = self._timestamps[-1] - self._timestamps[0]
        if span <= 0:
            return None
        return n / span  # chunks/sec ≈ tokens/sec

    def snapshot(self) -> dict:
        return {
            "ttft_s": round(self.ttft, 2) if self.ttft is not None else None,
            "avg_tpot_s": round(self.avg_tpot, 3) if self.avg_tpot is not None else None,
            "tokens_per_sec": round(self.tokens_per_sec, 1) if self.tokens_per_sec is not None else None,
            "total_chunks": self._total_chunks,
            "total_chars": self._total_chars,
        }


def _has_tool_call(raw_chunk: bytes) -> bool:
    """检测 SSE chunk 中是否包含 tool_call 指令"""
    try:
        text = raw_chunk.decode("utf-8", errors="replace").strip()
        if not text.startswith("data: ") or text == "data: [DONE]":
            return False
        data = json.loads(text[6:])
        choices = data.get("choices", [])
        if choices and isinstance(choices, list):
            delta = choices[0].get("delta", {})
            if delta.get("tool_calls"):
                return True
    except (json.JSONDecodeError, IndexError, TypeError):
        pass
    return False


# ── Main proxy ──────────────────────────────────────────────────────────

class FailoverProxy:
    """Proxies chat/completions requests across providers with failover."""

    def __init__(self, config: ProxyConfig) -> None:
        self._config = config
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(180.0))
        self._cb = CircuitBreaker(
            failure_threshold=config.circuit_breaker.failure_threshold,
            recovery_interval=config.circuit_breaker.recovery_interval_seconds,
        )
        # A2: persistent result store for frontend recovery
        self._result_store = ResultStore()
        # register all providers
        for p in config.sorted_providers:
            self._cb.register(p.name)

    async def close(self) -> None:
        await self._client.aclose()

    # ── non-streaming ──────────────────────────────────────────────────

    async def chat_completions(self, body: dict) -> dict:
        """Non-streaming chat completions with failover + circuit breaker + model rewrite."""
        last_error: Optional[Exception] = None
        providers_tried: list[str] = []
        _t0 = time.monotonic()

        for provider in self._config.sorted_providers:
            if self._cb.is_degraded(provider.name):
                logger.info("[Switch] 跳过已降级 provider=%s", provider.name)
                continue
            providers_tried.append(provider.name)
            try:
                result = await self._try_chat(provider, body)
                self._cb.record_success(provider.name)
                # rewrite model to proxy model name
                if "model" in result:
                    result["model"] = body.get("model", "local_route")
                # P0: 记录用量统计
                elapsed_ms = int((time.monotonic() - _t0) * 1000)
                usage = result.get("usage", {})
                self._result_store.log_request(
                    provider=provider.name,
                    model=body.get("model", "local_route"),
                    prompt_tokens=usage.get("prompt_tokens", 0),
                    completion_tokens=usage.get("completion_tokens", 0),
                    duration_ms=elapsed_ms,
                )
                broadcast_event("provider_status", {
                    "providers": _cb_snapshot(self._cb),
                })
                logger.info(
                    "[Switch] 完成 provider=%s cb=%s",
                    provider.name, self._cb.summary(),
                )
                return result
            except ProviderFailed as exc:
                deg = self._cb.record_failure(provider.name)
                self._cb.mark_switched_away(provider.name)
                self._result_store.log_switch(
                    from_provider=provider.name,
                    trigger_type="hard",
                    reason=exc.reason,
                )
                broadcast_event("switch", {
                    "from_provider": provider.name,
                    "trigger_type": "hard",
                    "reason": exc.reason,
                })
                logger.warning(
                    "[Switch] 切换 provider=%s -> next reason=\"%s\" degraded=%s cb=%s",
                    exc.provider_name, exc.reason, deg, self._cb.summary(),
                )
                last_error = exc
                continue
            except SoftTriggerSwitch as exc:
                deg = self._cb.record_failure(provider.name)
                self._cb.mark_switched_away(provider.name)
                self._result_store.log_switch(
                    from_provider=provider.name,
                    trigger_type="soft",
                    reason=exc.reason,
                )
                broadcast_event("switch", {
                    "from_provider": provider.name,
                    "trigger_type": "soft",
                    "reason": exc.reason,
                })
                logger.warning(
                    "[Switch] 软触发切换 provider=%s -> next reason=\"%s\" metrics=%s degraded=%s cb=%s",
                    exc.provider_name, exc.reason, exc.metrics, deg, self._cb.summary(),
                )
                last_error = exc
                continue

        logger.error(
            "[Switch] 所有 provider 已耗尽 tried=%s cb=%s",
            providers_tried, self._cb.summary(),
        )
        return {
            "error": {
                "message": f"All providers failed. Last: {last_error}",
                "type": "proxy_failover_exhausted",
                "providers_tried": providers_tried,
            }
        }

    # ── streaming ──────────────────────────────────────────────────────

    async def chat_completions_stream(
        self, body: dict,
    ) -> AsyncIterator[bytes]:
        """Streaming chat completions with failover + circuit breaker + soft trigger + SSE rewrite.

        Progressive flush mode: chunks are yielded in small batches (every 50 chunks
        or 48 KB, whichever comes first), with a yield to the event loop between
        batches. This prevents CPU starvation in Jiuwen's event loop when processing
        large responses (e.g. 1 MB / 955 chunks), while keeping the connection alive.
        """
        proxy_id = f"proxy_{int(time.time() * 1000)}_{id(body)}"
        proxy_model = body.get("model", "local_route")
        _t0 = time.monotonic()

        # Tune these constants for your workload:
        #   Smaller → more frequent flushes → less CPU starvation risk
        #   Larger  → fewer round-trips → higher throughput
        # P0: 密集工具调用会话优化 - 减少 agent loop 迭代约 75%
        FLUSH_CHUNKS = 200
        FLUSH_BYTES = 192 * 1024  # 192 KB

        last_error: Optional[Exception] = None
        providers_tried: list[str] = []

        for provider in self._config.sorted_providers:
            if self._cb.is_degraded(provider.name):
                logger.info("[Switch] 跳过已降级 provider=%s", provider.name)
                continue
            providers_tried.append(provider.name)
            try:
                # Progressive flush: buffer a small batch, then yield + yield event loop
                batch: list[bytes] = []
                batch_bytes = 0
                chunks_this_provider = 0
                total_bytes = 0

                # A2: accumulate full response text for result store
                full_response: list[str] = []

                async for raw_chunk in self._try_chat_stream(provider, body):
                    # SSE keepalive comment — flush current batch immediately,
                    # then yield the keepalive directly to keep proxy→Jiuwen
                    # connection alive during upstream idle periods.
                    if raw_chunk.startswith(b":"):
                        if batch:
                            for chunk in batch:
                                yield chunk
                            await asyncio.sleep(0)
                            batch.clear()
                            batch_bytes = 0
                        yield raw_chunk
                        await asyncio.sleep(0)
                        continue

                    # A2: extract content from raw SSE chunk for accumulation
                    try:
                        chunk_str = raw_chunk.decode("utf-8", errors="replace").strip()
                        if chunk_str.startswith("data: ") and chunk_str != "data: [DONE]":
                            _data = json.loads(chunk_str[6:])
                            _choices = _data.get("choices", [])
                            if _choices and isinstance(_choices, list):
                                _delta = _choices[0].get("delta", {})
                                _content = _delta.get("content", "") or ""
                                if _content:
                                    full_response.append(_content)
                    except (json.JSONDecodeError, IndexError):
                        pass

                    rewritten = _rewrite_sse_chunk(raw_chunk, proxy_id, proxy_model)

                    # ── Tool_call 即时通道 ──────────────────────────────
                    # 同类工具并行到达时不等待 content batch 积满，
                    # 立即 flush 当前 content + 直接 yield tool_call chunk。
                    if _has_tool_call(raw_chunk):
                        if batch:
                            for chunk in batch:
                                yield chunk
                            await asyncio.sleep(0)
                            batch.clear()
                            batch_bytes = 0
                        yield rewritten
                        await asyncio.sleep(0)
                        chunks_this_provider += 1
                        total_bytes += len(rewritten)
                        continue

                    # ── Content chunk → 积攒大 batch ─────────────────────
                    batch.append(rewritten)
                    batch_bytes += len(rewritten)
                    total_bytes += len(rewritten)

                    # Flush when batch reaches threshold
                    if len(batch) >= FLUSH_CHUNKS or batch_bytes >= FLUSH_BYTES:
                        for chunk in batch:
                            yield chunk
                        # Yield control to event loop so uvicorn can flush data
                        # to the TCP socket and Jiuwen can process the batch
                        await asyncio.sleep(0)
                        batch.clear()
                        batch_bytes = 0

                    chunks_this_provider += 1

                # Flush remaining chunks (last incomplete batch)
                for chunk in batch:
                    yield chunk
                await asyncio.sleep(0)

                # stream finished normally
                self._cb.record_success(provider.name)
                # P0: 统计 + 广播
                elapsed_ms = int((time.monotonic() - _t0) * 1000)
                self._result_store.log_request(
                    provider=provider.name,
                    model=proxy_model,
                    duration_ms=elapsed_ms,
                )
                broadcast_event("provider_status", {
                    "providers": _cb_snapshot(self._cb),
                })
                # A2: save full response to result store
                full_text = "".join(full_response)
                if full_text:
                    self._result_store.save(
                        session_id=body.get("session_id", "unknown"),
                        req_id=proxy_id,
                        model=proxy_model,
                        content=full_text,
                    )
                logger.info(
                    "[Switch] 流完成 provider=%s (%d chunks, %d bytes, %d chars saved) cb=%s",
                    provider.name, chunks_this_provider, total_bytes, len(full_text),
                    self._cb.summary(),
                )
                return

            except (ProviderFailed, SoftTriggerSwitch) as exc:
                self._cb.record_failure(provider.name)
                self._cb.mark_switched_away(provider.name)

                # P0: 记录切换事件 + 广播
                trigger_type = "soft" if isinstance(exc, SoftTriggerSwitch) else "hard"
                self._result_store.log_switch(
                    from_provider=provider.name,
                    trigger_type=trigger_type,
                    reason=exc.reason,
                )
                broadcast_event("switch", {
                    "from_provider": provider.name,
                    "trigger_type": trigger_type,
                    "reason": exc.reason,
                })

                if isinstance(exc, SoftTriggerSwitch):
                    logger.warning(
                        "[Switch] 软触发流切换 provider=%s -> next reason=\"%s\" metrics=%s cb=%s",
                        exc.provider_name, exc.reason, exc.metrics, self._cb.summary(),
                    )
                else:
                    logger.warning(
                        "[Switch] 流切换 provider=%s -> next reason=\"%s\" cb=%s",
                        exc.provider_name, exc.reason, self._cb.summary(),
                    )
                last_error = exc
                continue

        # All providers exhausted
        logger.error(
            "[Switch] 所有 provider 流耗尽 tried=%s cb=%s",
            providers_tried, self._cb.summary(),
        )
        error_body = {
            "error": {
                "message": f"All providers failed. Last: {last_error}",
                "type": "proxy_failover_exhausted",
                "providers_tried": providers_tried,
            }
        }
        yield b"data: " + json.dumps(error_body).encode() + b"\n\ndata: [DONE]\n\n"

    # ── single attempt: non-streaming ──────────────────────────────────

    # ── single attempt: Anthropic (实验性) ─────────────────────────

    async def _try_chat_anthropic(self, provider: ProviderConfig, body: dict) -> dict:
        """Non-streaming chat via Anthropic Messages API, converting on both ends."""
        mapped_model = provider.resolve_model(body.get("model", ""))
        anth_body = openai_to_anthropic_request({**body, "model": mapped_model})
        headers = {
            "x-api-key": provider.api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        }
        url = provider.api_base.rstrip("/") + "/messages"
        logger.info(
            "[Try-Anth] 非流式 provider=%s url=%s model=%s",
            provider.name, url, mapped_model,
        )
        t0 = time.monotonic()
        try:
            resp = await self._client.post(url, json=anth_body, headers=headers, timeout=provider.timeout)
            elapsed = time.monotonic() - t0
            if resp.is_success:
                raw = resp.json()
                openai_resp = anthropic_to_openai_response(raw, body.get("model", "local_route"))
                logger.info(
                    "[Try-Anth] 成功 provider=%s model=%s %.1fs",
                    provider.name, mapped_model, elapsed,
                )
                return openai_resp
            if resp.status_code in RETRYABLE_STATUSES:
                raise ProviderFailed(provider.name, f"HTTP {resp.status_code}: {resp.text[:200]}", resp.status_code)
            logger.error(
                "[Try-Anth] 非可重试错误 provider=%s HTTP %d: %.300s",
                provider.name, resp.status_code, resp.text,
            )
            resp.raise_for_status()
            return {}  # unreachable
        except ProviderFailed:
            raise
        except httpx.HTTPStatusError as exc:
            raise ProviderFailed(
                provider.name,
                f"HTTP {exc.response.status_code}: {exc.response.text[:200]}",
                exc.response.status_code,
            ) from exc
        except Exception as exc:
            if _is_retryable(exc):
                raise ProviderFailed(provider.name, f"{type(exc).__name__}: {exc}") from exc
            raise ProviderFailed(provider.name, f"{type(exc).__name__}: {exc}") from exc

    async def _try_chat(self, provider: ProviderConfig, body: dict) -> dict:
        if provider.api_type == "anthropic":
            return await self._try_chat_anthropic(provider, body)
        mapped_model = provider.resolve_model(body.get("model", ""))
        payload = {**body, "model": mapped_model}
        headers = {
            "Authorization": f"Bearer {provider.api_key}",
            "Content-Type": "application/json",
        }
        url = provider.api_base.rstrip("/") + "/chat/completions"
        logger.info(
            "[Try] 非流式 provider=%s url=%s model=%s",
            provider.name, url, mapped_model,
        )
        t0 = time.monotonic()
        try:
            resp = await self._client.post(url, json=payload, headers=headers, timeout=provider.timeout)
            elapsed = time.monotonic() - t0
            if resp.is_success:
                logger.info(
                    "[Try] 成功 provider=%s model=%s %.1fs",
                    provider.name, mapped_model, elapsed,
                )
                return resp.json()
            if resp.status_code in RETRYABLE_STATUSES:
                raise ProviderFailed(provider.name, f"HTTP {resp.status_code}: {resp.text[:200]}", resp.status_code)
            logger.error(
                "[Try] 非可重试错误 provider=%s HTTP %d: %.300s",
                provider.name, resp.status_code, resp.text,
            )
            resp.raise_for_status()
            return resp.json()  # unreachable
        except ProviderFailed:
            raise
        except httpx.HTTPStatusError as exc:
            raise ProviderFailed(
                provider.name,
                f"HTTP {exc.response.status_code}: {exc.response.text[:200]}",
                exc.response.status_code,
            ) from exc
        except Exception as exc:
            if _is_retryable(exc):
                raise ProviderFailed(provider.name, f"{type(exc).__name__}: {exc}") from exc
            raise ProviderFailed(provider.name, f"{type(exc).__name__}: {exc}") from exc

    # ── single attempt: streaming Anthropic (实验性) ───────────────

    async def _try_chat_stream_anthropic(
        self, provider: ProviderConfig, body: dict,
    ) -> AsyncIterator[bytes]:
        """Streaming chat via Anthropic Messages API with on-the-fly protocol conversion."""
        mapped_model = provider.resolve_model(body.get("model", ""))
        anth_body = openai_to_anthropic_request({**body, "model": mapped_model, "stream": True})
        headers = {
            "x-api-key": provider.api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        }
        url = provider.api_base.rstrip("/") + "/messages"
        logger.info(
            "[Try-Anth] 流式 provider=%s url=%s model=%s",
            provider.name, url, mapped_model,
        )

        soft_cfg = self._config.soft_trigger
        converter = AnthropicStreamConverter(body.get("model", "local_route"))
        t0 = time.monotonic()
        first_chunk_seen = False

        try:
            async with self._client.stream("POST", url, json=anth_body, headers=headers, timeout=provider.timeout) as resp:
                if not resp.is_success:
                    body_text = await resp.aread()
                    raise ProviderFailed(
                        provider.name,
                        f"HTTP {resp.status_code}: {body_text[:200]}",
                        resp.status_code,
                    )

                connect_elapsed = time.monotonic() - t0
                logger.info(
                    "[Try-Anth] 流已连接 provider=%s model=%s %.1fs",
                    provider.name, mapped_model, connect_elapsed,
                )

                # Level 1 heartbeat: upstream raw bytes — TTFT detection + keepalive
                upstream_hb = _aiter_with_heartbeat(
                    resp.aiter_bytes(),
                    provider_name=provider.name,
                    t0=t0,
                    heartbeat_interval=3.0,
                    idle_soft_trigger_ms=soft_cfg.ttft_threshold_ms if soft_cfg.enabled else 0,
                )
                # Level 2 heartbeat: converter output — keepalive only (TTFT already upstream)
                converter_hb = _aiter_with_heartbeat(
                    converter.convert(upstream_hb),
                    provider_name=provider.name,
                    heartbeat_interval=3.0,
                    idle_soft_trigger_ms=0,  # TTFT handled at level 1
                )

                async for openai_chunk in converter_hb:
                    yield openai_chunk

                    if not first_chunk_seen:
                        first_chunk_seen = True
                        # TTFT check — estimate from converter's first emitted chunk
                        if soft_cfg.enabled:
                            ttft_s = time.monotonic() - t0
                            ttft_ms = ttft_s * 1000
                            if ttft_ms > soft_cfg.ttft_threshold_ms:
                                raise SoftTriggerSwitch(
                                    provider.name,
                                    f"TTFT={ttft_ms:.0f}ms > {soft_cfg.ttft_threshold_ms}ms",
                                    {"ttft_s": round(ttft_s, 2)},
                                )

                    # per-chunk soft trigger (track via first_chunk_seen as proxy for progress)
                    if soft_cfg.enabled and first_chunk_seen:
                        elapsed_since_start = time.monotonic() - t0
                        if elapsed_since_start >= soft_cfg.throughput_window_seconds:
                            # check if we're still receiving data recently
                            pass  # Anthropic stream health is harder to measure — defer to circuit breaker

        except (ProviderFailed, SoftTriggerSwitch):
            raise
        except Exception as exc:
            if _is_retryable(exc):
                raise ProviderFailed(provider.name, f"{type(exc).__name__}: {exc}") from exc
            raise ProviderFailed(provider.name, f"{type(exc).__name__}: {exc}") from exc

    # ── single attempt: streaming ──────────────────────────────────────

    async def _try_chat_stream(self, provider: ProviderConfig, body: dict) -> AsyncIterator[bytes]:
        if provider.api_type == "anthropic":
            async for chunk in self._try_chat_stream_anthropic(provider, body):
                yield chunk
            return
        mapped_model = provider.resolve_model(body.get("model", ""))
        payload = {**body, "model": mapped_model, "stream": True}
        headers = {
            "Authorization": f"Bearer {provider.api_key}",
            "Content-Type": "application/json",
        }
        url = provider.api_base.rstrip("/") + "/chat/completions"
        logger.info(
            "[Try] 流式 provider=%s url=%s model=%s",
            provider.name, url, mapped_model,
        )

        soft_cfg = self._config.soft_trigger
        metrics = SlidingMetrics(throughput_window=soft_cfg.throughput_window_seconds)
        t0 = time.monotonic()
        first_chunk_seen = False

        try:
            async with self._client.stream("POST", url, json=payload, headers=headers, timeout=provider.timeout) as resp:
                if not resp.is_success:
                    body_text = await resp.aread()
                    raise ProviderFailed(
                        provider.name,
                        f"HTTP {resp.status_code}: {body_text[:200]}",
                        resp.status_code,
                    )

                connect_elapsed = time.monotonic() - t0
                logger.info(
                    "[Try] 流已连接 provider=%s model=%s %.1fs",
                    provider.name, mapped_model, connect_elapsed,
                )

                async for raw_chunk in _aiter_with_heartbeat(
                    resp.aiter_bytes(),
                    provider_name=provider.name,
                    t0=t0,
                    heartbeat_interval=3.0,
                    idle_soft_trigger_ms=soft_cfg.ttft_threshold_ms if soft_cfg.enabled else 0,
                ):
                    chunk_str = raw_chunk.decode("utf-8", errors="replace").strip()

                    # pass through SSE comments (keepalive), non-data lines and [DONE]
                    if chunk_str.startswith(":") or not chunk_str.startswith("data: ") or chunk_str == "data: [DONE]":
                        yield raw_chunk
                        continue

                    # extract text length from delta for metrics
                    text_len = 0
                    try:
                        json_str = chunk_str[6:]  # strip "data: "
                        data = json.loads(json_str)
                        choices = data.get("choices", [])
                        if choices and isinstance(choices, list):
                            delta = choices[0].get("delta", {})
                            text_len = len(delta.get("content", "") or "")
                    except json.JSONDecodeError:
                        pass

                    metrics.record_chunk(text_len)
                    yield raw_chunk

                    if not first_chunk_seen:
                        first_chunk_seen = True
                        # TTFT check after first chunk
                        if soft_cfg.enabled and metrics.ttft is not None:
                            ttft_ms = metrics.ttft * 1000
                            if ttft_ms > soft_cfg.ttft_threshold_ms:
                                snap = metrics.snapshot()
                                raise SoftTriggerSwitch(
                                    provider.name,
                                    f"TTFT={ttft_ms:.0f}ms > {soft_cfg.ttft_threshold_ms}ms",
                                    snap,
                                )

                    # per-chunk soft trigger checks (TPOT, throughput)
                    if soft_cfg.enabled and metrics._total_chunks >= 3:
                        snap = metrics.snapshot()

                        # TPOT check
                        tpot_ms = (metrics.avg_tpot or 0) * 1000
                        if tpot_ms > soft_cfg.tpot_threshold_ms:
                            raise SoftTriggerSwitch(
                                provider.name,
                                f"TPOT={tpot_ms:.0f}ms > {soft_cfg.tpot_threshold_ms}ms",
                                snap,
                            )

                        # throughput check
                        tps = metrics.tokens_per_sec or 999
                        elapsed_since_start = time.monotonic() - t0
                        if (
                            tps < soft_cfg.throughput_threshold_tokens_per_sec
                            and elapsed_since_start >= soft_cfg.throughput_window_seconds
                        ):
                            raise SoftTriggerSwitch(
                                provider.name,
                                f"吞吐={tps:.1f} t/s < {soft_cfg.throughput_threshold_tokens_per_sec} t/s (持续{elapsed_since_start:.0f}s)",
                                snap,
                            )

                # stream ended normally
                elapsed = time.monotonic() - t0
                logger.info(
                    "[Try] 流正常结束 provider=%s model=%s %.1fs",
                    provider.name, mapped_model, elapsed,
                )

        except (ProviderFailed, SoftTriggerSwitch):
            raise
        except Exception as exc:
            if _is_retryable(exc):
                raise ProviderFailed(provider.name, f"{type(exc).__name__}: {exc}") from exc
            raise ProviderFailed(provider.name, f"{type(exc).__name__}: {exc}") from exc


# ── 方案 C: 启发式难度估计 ────────────────────────────────────────

# 常见高难度关键词
_HIGH_DIFFICULTY_KEYWORDS = [
    "如何", "为什么", "分析", "比较", "解释", "总结", "推导",
    "how", "why", "analyze", "compare", "explain", "summarize", "derive",
    "根因", "原理", "机制", "影响", "关系", "区别", "优缺点",
]


def heuristic_difficulty(prompt: str) -> dict:
    """无模型 forward pass 的启发式难度估计。

    用于方案 C 快速路由决策。proxy 侧无需 GPU，根据 prompt 文本特征
    估算难度分数，供客户端（Jiuwen）或 proxy 内部路由参考。

    Returns:
        dict: {difficulty, len_term, kw_factor, struct_factor, special_factor}
    """
    # 长度因子 — 长文本通常更复杂
    len_term = min(len(prompt) / 500.0, 1.0)

    # 句子结构因子 — 长句多说明逻辑链条长
    sentences = [s for s in re.split(r'[.!?。！？\n]+', prompt) if s.strip()]
    avg_sentence_len = sum(len(s) for s in sentences) / max(len(sentences), 1)
    struct_factor = min(avg_sentence_len / 200.0, 1.0)

    # 关键词因子 — 高难度关键词命中率
    prompt_lower = prompt.lower()
    kw_hits = sum(1 for kw in _HIGH_DIFFICULTY_KEYWORDS if kw in prompt_lower)
    kw_factor = min(kw_hits / 4.0, 1.0)

    # 代码/数学块因子 — 含代码块或数学表达通常难度更高
    has_code_block = bool(re.search(r'```|def |class |function |import ', prompt))
    has_math = bool(re.search(r'[+\-*/=<>≤≥≠±∑∫√∂Δλ]', prompt))
    special_factor = 0.3 if has_code_block or has_math else 0.0

    difficulty = 0.30 * len_term + 0.15 * struct_factor + 0.35 * kw_factor + 0.20 * special_factor
    difficulty = max(0.0, min(difficulty, 1.0))

    return {
        "difficulty": round(difficulty, 3),
        "len_term": round(len_term, 3),
        "kw_factor": round(kw_factor, 3),
        "struct_factor": round(struct_factor, 3),
        "special_factor": round(special_factor, 3),
    }


def _cb_snapshot(cb: CircuitBreaker) -> list[dict]:
    """Snapshot CB states for SSE provider_status broadcast."""
    return [
        {
            "name": name,
            "degraded": state.degraded,
            "failures": state.failure_count,
            "switches": state.total_switches_away,
        }
        for name, state in cb._states.items()
    ]
