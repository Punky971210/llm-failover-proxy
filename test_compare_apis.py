"""
DeepSeek OpenAI vs Anthropic API 裸延迟对比 + 翻译层模拟
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field

import httpx

DEEPSEEK_KEY = "sk-your-deepseek-api-key-here"

# ── 测试用 prompt（中等长度，含工具调用） ─────────────────────
SYSTEM = "You are a helpful assistant."
USER = "用中文回答：请简述量子计算和经典计算的核心区别，并给出三个实际应用场景。"

SYSTEM_TOOL = "You are a helpful assistant with access to tools."
USER_TOOL = "北京和上海的天气怎么样？帮我查一下。"

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "获取指定城市的天气信息",
            "parameters": {
                "type": "object",
                "properties": {
                    "city": {"type": "string", "description": "城市名"},
                    "date": {"type": "string", "description": "日期"},
                },
                "required": ["city"],
            },
        },
    }
]

ANTHROPIC_URL = "https://api.deepseek.com/anthropic/v1/messages"
OPENAI_URL = "https://api.deepseek.com/chat/completions"

# ── 测试用例参数 ──────────────────────────────────────────────
TEST_ITERATIONS = 3  # 每项测 3 次取均值


@dataclass
class TestResult:
    name: str
    success: bool = False
    ttft_ms: float = 0.0        # time to first token
    total_ms: float = 0.0       # total request time
    output_tokens: int = 0
    tokens_per_sec: float = 0.0
    error: str = ""
    detail: str = ""


def _parse_anthropic_sse(lines):
    """Parse Anthropic SSE format: event:xxx line then data:xxx line."""
    events = []
    current_event = None
    for line in lines:
        if line.startswith("event: "):
            current_event = line[7:].strip()
        elif line.startswith("data: ") and current_event:
            payload = line[6:]
            try:
                evt = json.loads(payload)
                evt["_event"] = current_event
                events.append(evt)
            except json.JSONDecodeError:
                pass
            current_event = None
    return events


# ═══════════════════════════════════════════════════════════════
# 1. OpenAI 协议（直连）
# ═══════════════════════════════════════════════════════════════
def test_openai_nonstream() -> TestResult:
    r = TestResult(name="OpenAI non-stream")
    try:
        t0 = time.perf_counter()
        with httpx.Client(timeout=180) as c:
            resp = c.post(
                OPENAI_URL,
                headers={
                    "Authorization": f"Bearer {DEEPSEEK_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "deepseek-v4-flash",
                    "messages": [
                        {"role": "system", "content": SYSTEM},
                        {"role": "user", "content": USER},
                    ],
                    "stream": False,
                    "max_tokens": 1024,
                },
            )
        t_total = time.perf_counter() - t0
        data = resp.json()
        r.success = True
        r.total_ms = round(t_total * 1000)
        r.output_tokens = data["usage"]["completion_tokens"]
        t_tokens = r.total_ms / 1000
        r.tokens_per_sec = round(r.output_tokens / t_tokens, 1) if t_tokens > 0 else 0
        r.ttft_ms = r.total_ms  # non-stream 只有一个 total
        r.detail = data["choices"][0]["message"]["content"][:80]
    except Exception as e:
        r.error = str(e)
    return r


def test_openai_stream() -> TestResult:
    r = TestResult(name="OpenAI stream")
    try:
        t0 = time.perf_counter()
        ttft = None
        content_chunks = []
        with httpx.Client(timeout=180) as c:
            with c.stream(
                "POST",
                OPENAI_URL,
                headers={
                    "Authorization": f"Bearer {DEEPSEEK_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "deepseek-v4-flash",
                    "messages": [
                        {"role": "system", "content": SYSTEM},
                        {"role": "user", "content": USER},
                    ],
                    "stream": True,
                    "max_tokens": 1024,
                },
            ) as resp:
                for line in resp.iter_lines():
                    if not line.startswith("data: "):
                        continue
                    payload = line[6:]
                    if payload == "[DONE]":
                        break
                    chunk = json.loads(payload)
                    delta = chunk.get("choices", [{}])[0].get("delta", {})
                    if delta.get("content"):
                        if ttft is None:
                            ttft = time.perf_counter() - t0
                        content_chunks.append(delta["content"])
        t_total = time.perf_counter() - t0
        full_content = "".join(content_chunks)
        r.success = True
        r.ttft_ms = round(ttft * 1000) if ttft else 0
        r.total_ms = round(t_total * 1000)
        r.output_tokens = len(full_content)  # 近似
        t_gen = t_total - (ttft or 0)
        r.tokens_per_sec = round(r.output_tokens / t_gen, 1) if t_gen > 0 else 0
        r.detail = full_content[:80]
    except Exception as e:
        r.error = str(e)
    return r


# ═══════════════════════════════════════════════════════════════
# 2. Anthropic 协议（直连）
# ═══════════════════════════════════════════════════════════════
def test_anthropic_nonstream() -> TestResult:
    r = TestResult(name="Anthropic non-stream")
    try:
        t0 = time.perf_counter()
        with httpx.Client(timeout=180) as c:
            resp = c.post(
                ANTHROPIC_URL,
                headers={
                    "x-api-key": DEEPSEEK_KEY,
                    "Content-Type": "application/json",
                    "anthropic-version": "2023-06-01",
                },
                json={
                    "model": "deepseek-v4-flash",
                    "max_tokens": 1024,
                    "system": SYSTEM,
                    "messages": [
                        {"role": "user", "content": USER},
                    ],
                },
            )
        t_total = time.perf_counter() - t0
        data = resp.json()
        r.success = True
        r.total_ms = round(t_total * 1000)
        # Anthropic 响应格式：content 是 array of blocks (含 thinking + text)
        content_text = ""
        has_thinking = False
        for block in data.get("content", []):
            if block.get("type") == "text":
                content_text += block.get("text", "")
            elif block.get("type") == "thinking":
                has_thinking = True
        r.output_tokens = data.get("usage", {}).get("output_tokens", len(content_text))
        t_tokens = r.total_ms / 1000
        r.tokens_per_sec = round(r.output_tokens / t_tokens, 1) if t_tokens > 0 else 0
        r.ttft_ms = r.total_ms
        r.detail = f"{'[thinking] ' if has_thinking else ''}{content_text[:80]}"
    except Exception as e:
        r.error = str(e)
        if hasattr(e, "response") and e.response is not None:
            try:
                r.detail = e.response.text[:200]
            except Exception:
                pass
    return r


def test_anthropic_stream() -> TestResult:
    r = TestResult(name="Anthropic stream")
    try:
        t0 = time.perf_counter()
        ttft = None
        content_text = ""
        has_thinking = False
        with httpx.Client(timeout=180) as c:
            with c.stream(
                "POST",
                ANTHROPIC_URL,
                headers={
                    "x-api-key": DEEPSEEK_KEY,
                    "Content-Type": "application/json",
                    "anthropic-version": "2023-06-01",
                },
                json={
                    "model": "deepseek-v4-flash",
                    "max_tokens": 1024,
                    "system": SYSTEM,
                    "messages": [
                        {"role": "user", "content": USER},
                    ],
                    "stream": True,
                },
            ) as resp:
                lines = list(resp.iter_lines())
                events = _parse_anthropic_sse(lines)
                for evt in events:
                    if evt.get("type") == "content_block_delta":
                        delta = evt.get("delta", {})
                        if delta.get("type") == "text_delta":
                            text = delta.get("text", "")
                            if text:
                                if ttft is None:
                                    ttft = time.perf_counter() - t0
                                content_text += text
                        elif delta.get("type") == "thinking_delta":
                            has_thinking = True
                    elif evt.get("type") == "content_block_start":
                        cb = evt.get("content_block", {})
                        if cb.get("type") == "thinking":
                            has_thinking = True
        t_total = time.perf_counter() - t0
        r.success = True
        r.ttft_ms = round(ttft * 1000) if ttft else 0
        r.total_ms = round(t_total * 1000)
        r.output_tokens = len(content_text)
        t_gen = t_total - (ttft or 0)
        r.tokens_per_sec = round(r.output_tokens / t_gen, 1) if t_gen > 0 else 0
        r.detail = f"{'[thinking] ' if has_thinking else ''}{content_text[:80]}"
    except Exception as e:
        r.error = str(e)
    return r


def test_anthropic_tool_call() -> TestResult:
    """测试 Anthropic 协议的工具调用能力"""
    r = TestResult(name="Anthropic tool call")
    try:
        t0 = time.perf_counter()
        with httpx.Client(timeout=180) as c:
            resp = c.post(
                ANTHROPIC_URL,
                headers={
                    "x-api-key": DEEPSEEK_KEY,
                    "Content-Type": "application/json",
                    "anthropic-version": "2023-06-01",
                },
                json={
                    "model": "deepseek-v4-flash",
                    "max_tokens": 1024,
                    "system": SYSTEM_TOOL,
                    "messages": [{"role": "user", "content": USER_TOOL}],
                    "tools": [
                        {
                            "name": "get_weather",
                            "description": "获取指定城市的天气信息",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "city": {"type": "string"},
                                    "date": {"type": "string"},
                                },
                                "required": ["city"],
                            },
                        }
                    ],
                },
            )
        t_total = time.perf_counter() - t0
        data = resp.json()
        r.success = True
        r.total_ms = round(t_total * 1000)
        r.ttft_ms = r.total_ms
        # 提取工具调用信息
        tool_uses = []
        text_parts = []
        has_thinking = False
        for block in data.get("content", []):
            if block.get("type") == "text":
                text_parts.append(block.get("text", ""))
            elif block.get("type") == "tool_use":
                tool_uses.append(f"{block['name']}({json.dumps(block['input'], ensure_ascii=False)})")
            elif block.get("type") == "thinking":
                has_thinking = True
        r.detail = f"{'[thinking] ' if has_thinking else ''}"
        r.detail += " | ".join(text_parts[:1] + tool_uses[:2])[:120]
        r.output_tokens = data.get("usage", {}).get("output_tokens", 0)
        t_tokens = r.total_ms / 1000
        r.tokens_per_sec = round(r.output_tokens / t_tokens, 1) if t_tokens > 0 else 0
    except Exception as e:
        r.error = str(e)
        if hasattr(e, "response") and e.response is not None:
            try:
                r.detail = e.response.text[:300]
            except Exception:
                pass
    return r


# ═══════════════════════════════════════════════════════════════
# 3. 模拟翻译层：OpenAI → Anthropic → OpenAI
# ═══════════════════════════════════════════════════════════════
def _openai_to_anthropic(messages: list, tools: list | None, system: str) -> dict:
    """将 OpenAI messages 转为 Anthropic Messages API 格式"""
    anon_messages = []
    for m in messages:
        if m.get("role") == "system":
            continue  # system 已被提取到顶层
        role = m["role"]
        content = m.get("content", "")
        if isinstance(content, list):
            anon_content = content
        else:
            anon_content = [{"type": "text", "text": content}]

        if role == "assistant" and m.get("tool_calls"):
            anon_content = [{"type": "text", "text": content or ""}]
            for tc in m["tool_calls"]:
                anon_content.append({
                    "type": "tool_use",
                    "id": tc["id"],
                    "name": tc["function"]["name"],
                    "input": json.loads(tc["function"]["arguments"]),
                })
        elif role == "tool":
            anon_content = [{
                "type": "tool_result",
                "tool_use_id": m["tool_call_id"],
                "content": m["content"],
            }]
            role = "user"
        anon_messages.append({"role": role, "content": anon_content})

    body = {
        "model": "deepseek-v4-flash",
        "max_tokens": 1024,
        "system": system,
        "messages": anon_messages,
    }
    if tools:
        anon_tools = []
        for t in tools:
            fn = t.get("function", t)
            anon_tools.append({
                "name": fn["name"],
                "description": fn.get("description", ""),
                "input_schema": fn["parameters"],
            })
        body["tools"] = anon_tools
    return body


def _anthropic_to_openai_chunk(evt: dict) -> dict | None:
    """将单个 Anthropic SSE 事件转换为 OpenAI SSE chunk"""
    if evt.get("type") == "content_block_delta":
        delta = evt.get("delta", {})
        if delta.get("type") == "text_delta":
            return {
                "choices": [{
                    "index": 0,
                    "delta": {"content": delta.get("text", "")},
                    "finish_reason": None,
                }]
            }
    return None


def test_translation_layer() -> TestResult:
    """模拟翻译层完整链：OpenAI req → Anthropic req → 直连 → 转回 OpenAI chunk"""
    r = TestResult(name="Translation layer (Anthropic)")
    try:
        t0 = time.perf_counter()
        ttft = None
        content_chunks = []

        # 1. 将 OpenAI 格式的请求转为 Anthropic 格式
        openai_msgs = [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": USER},
        ]
        anon_body = _openai_to_anthropic(openai_msgs, None, SYSTEM)

        t_convert_start = time.perf_counter()
        # 2. 以 Anthropic 协议发送
        with httpx.Client(timeout=180) as c:
            with c.stream(
                "POST",
                ANTHROPIC_URL,
                headers={
                    "x-api-key": DEEPSEEK_KEY,
                    "Content-Type": "application/json",
                    "anthropic-version": "2023-06-01",
                },
                json=anon_body,
            ) as resp:
                lines = list(resp.iter_lines())
                events = _parse_anthropic_sse(lines)
                for evt in events:
                    # 3. 将 Anthropic chunk 转回 OpenAI chunk
                    oai_chunk = _anthropic_to_openai_chunk(evt)
                    if oai_chunk:
                        delta = oai_chunk["choices"][0]["delta"]
                        if delta.get("content"):
                            if ttft is None:
                                ttft = time.perf_counter() - t0
                                r.detail += f"conv_cost={round((ttft - t_convert_start)*1000)}ms "
                            content_chunks.append(delta["content"])

        t_total = time.perf_counter() - t0
        full_content = "".join(content_chunks)
        r.success = True
        r.ttft_ms = round(ttft * 1000) if ttft else 0
        r.total_ms = round(t_total * 1000)
        r.output_tokens = len(full_content)
        t_gen = t_total - (ttft or 0)
        r.tokens_per_sec = round(r.output_tokens / t_gen, 1) if t_gen > 0 else 0
        r.detail = full_content[:80] + "..." if full_content else "no content"
    except Exception as e:
        r.error = str(e)
    return r


# ═══════════════════════════════════════════════════════════════
# 运行测试
# ═══════════════════════════════════════════════════════════════
def run_battery(name: str, tests: list[tuple[str, callable]]):
    print(f"\n{'='*70}")
    print(f"  {name}")
    print(f"{'='*70}")
    for label, fn in tests:
        results = []
        for i in range(TEST_ITERATIONS):
            time.sleep(0.5)
            r = fn()
            results.append(r)
            status = "OK" if r.success else "FAIL"
            print(f"  [{status}] {label} (#{i+1}): {r.total_ms}ms total"
                  f"{f', TTFT={r.ttft_ms}ms' if r.ttft_ms else ''}"
                  f"{f', {r.tokens_per_sec} t/s' if r.tokens_per_sec else ''}"
                  f"{'  ERR: ' + r.error[:60] if not r.success else ''}")

        ok = [r for r in results if r.success]
        if ok:
            avg_total = sum(r.total_ms for r in ok) / len(ok)
            avg_ttft = sum(r.ttft_ms for r in ok) / len(ok)
            avg_tps = sum(r.tokens_per_sec for r in ok) / len(ok)
            print(f"  >>> AVG({len(ok)} ok): {avg_total:.0f}ms total, "
                  f"TTFT={avg_ttft:.0f}ms, {avg_tps:.1f} t/s")
        else:
            print(f"  >>> ALL FAILED")


if __name__ == "__main__":
    print(f"Testing DeepSeek API endpoints ({TEST_ITERATIONS} iterations each)")

    # OpenAI 协议
    run_battery("OpenAI Protocol (直连)", [
        ("OpenAI non-stream", test_openai_nonstream),
        ("OpenAI stream", test_openai_stream),
    ])

    # Anthropic 协议
    run_battery("Anthropic Protocol (直连)", [
        ("Anthropic non-stream", test_anthropic_nonstream),
        ("Anthropic stream", test_anthropic_stream),
        ("Anthropic tool call", test_anthropic_tool_call),
    ])

    # 翻译层
    run_battery("Translation Layer (OpenAI->Anthropic)", [
        ("Translated stream", test_translation_layer),
    ])

    print("\nDone.")
