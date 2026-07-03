"""
DeepSeek OpenAI ↔ Anthropic 接口基准测速。

测试项：
  1. DeepSeek OpenAI  API 直连（基线）
  2. DeepSeek Anthropic API 直连（原生协议）
  3. 经代理 Anthropic 路径（含协议转换层）

每个测试模式分别测非流式 + 流式。
"""

from __future__ import annotations

import json
import time
import statistics
from typing import Any

import httpx

# ── 配置（使用前替换为真实 key） ──────────────────────────────────
API_KEY = "sk-your-deepseek-api-key-here"
MODEL = "deepseek-v4-flash"
PROXY_BASE = "http://localhost:8000/v1"
TEST_MESSAGES = [
    {"role": "user", "content": "写一篇 500 字左右的短文，主题是人工智能对教育的影响。要求逻辑清晰，有具体例子。"}
]

NUM_RUNS = 5       # 每种模式重复次数
STREAM = False     # 先测非流式

# ── 测速函数 ──────────────────────────────────────────────────────

def _fmt(ms: float) -> str:
    if ms >= 1000:
        return f"{ms/1000:.2f}s"
    return f"{ms:.0f}ms"


def test_openai_direct(client: httpx.Client) -> dict:
    """直接调用 DeepSeek OpenAI /chat/completions（基线）。"""
    url = "https://api.deepseek.com/chat/completions"
    body = {
        "model": MODEL,
        "messages": TEST_MESSAGES,
        "max_tokens": 1024,
        "stream": False,
    }
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

    t0 = time.monotonic()
    resp = client.post(url, json=body, headers=headers, timeout=180)
    ttfb = (time.monotonic() - t0) * 1000

    data = resp.json()
    elapsed = (time.monotonic() - t0) * 1000

    usage = data.get("usage", {})
    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
    output_tokens = usage.get("completion_tokens", 0) or max(len(content) // 2, 1)

    return {
        "ttfb_ms": ttfb,
        "total_ms": elapsed,
        "output_chars": len(content),
        "output_tokens": output_tokens,
        "chars_per_sec": len(content) / (elapsed / 1000) if elapsed > 0 else 0,
        "tokens_per_sec": output_tokens / (elapsed / 1000) if elapsed > 0 else 0,
        "content_preview": content[:80],
    }


def test_anthropic_direct(client: httpx.Client) -> dict:
    """直接调用 DeepSeek Anthropic /v1/messages（原生协议）。"""
    url = "https://api.deepseek.com/v1/messages"
    body = {
        "model": MODEL,
        "max_tokens": 1024,
        "messages": TEST_MESSAGES,
        "stream": False,
    }
    headers = {
        "x-api-key": API_KEY,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }

    t0 = time.monotonic()
    resp = client.post(url, json=body, headers=headers, timeout=180)
    ttfb = (time.monotonic() - t0) * 1000

    if resp.status_code != 200:
        print(f"    [WARN] Anthropic direct HTTP {resp.status_code}: {resp.text[:300]}")
        return {
            "ttfb_ms": ttfb,
            "total_ms": ttfb,
            "output_chars": 0,
            "output_tokens": 1,
            "chars_per_sec": 0,
            "tokens_per_sec": 0,
            "content_preview": f"[HTTP {resp.status_code}]",
        }

    data = resp.json()
    elapsed = (time.monotonic() - t0) * 1000

    # Parse Anthropic response
    content_blocks = data.get("content", [])
    text = "".join(b.get("text", "") for b in content_blocks if b.get("type") == "text")
    usage = data.get("usage", {})
    output_tokens = usage.get("output_tokens", 0) or max(len(text) // 2, 1)

    return {
        "ttfb_ms": ttfb,
        "total_ms": elapsed,
        "output_chars": len(text),
        "output_tokens": output_tokens,
        "chars_per_sec": len(text) / (elapsed / 1000) if elapsed > 0 else 0,
        "tokens_per_sec": output_tokens / (elapsed / 1000) if elapsed > 0 else 0,
        "content_preview": text[:80],
    }


def test_via_proxy(client: httpx.Client, provider_hint: str = "") -> dict:
    """经代理（走 Anthropic 转换路径）。"""
    url = f"{PROXY_BASE}/chat/completions"
    body = {
        "model": "local_route",
        "messages": TEST_MESSAGES,
        "max_tokens": 1024,
        "stream": False,
    }
    headers = {"Authorization": "Bearer any", "Content-Type": "application/json"}

    t0 = time.monotonic()
    resp = client.post(url, json=body, headers=headers, timeout=180)
    ttfb = (time.monotonic() - t0) * 1000

    data = resp.json()
    elapsed = (time.monotonic() - t0) * 1000

    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
    usage = data.get("usage", {})
    output_tokens = usage.get("completion_tokens", 0) or max(len(content) // 2, 1)

    return {
        "ttfb_ms": ttfb,
        "total_ms": elapsed,
        "output_chars": len(content),
        "output_tokens": output_tokens,
        "chars_per_sec": len(content) / (elapsed / 1000) if elapsed > 0 else 0,
        "tokens_per_sec": output_tokens / (elapsed / 1000) if elapsed > 0 else 0,
        "content_preview": content[:80],
    }


# ── 流式测速 ──────────────────────────────────────────────────────

async def test_stream_openai_direct(client: httpx.AsyncClient) -> dict:
    """流式：OpenAI 直连。"""
    url = "https://api.deepseek.com/chat/completions"
    body = {
        "model": MODEL,
        "messages": TEST_MESSAGES,
        "max_tokens": 1024,
        "stream": True,
    }
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

    t0 = time.monotonic()
    first_chunk_time = None
    chunk_count = 0
    total_chars = 0
    last_chunk_time = None

    async with client.stream("POST", url, json=body, headers=headers, timeout=180) as resp:
        async for raw in resp.aiter_bytes():
            if first_chunk_time is None:
                first_chunk_time = time.monotonic()
            last_chunk_time = time.monotonic()
            chunk_count += 1
            # Parse to count chars
            text = raw.decode("utf-8", errors="replace")
            for line in text.split("\n"):
                if line.startswith("data: ") and line != "data: [DONE]":
                    try:
                        data = json.loads(line[6:])
                        choices = data.get("choices", [])
                        if choices:
                            delta = choices[0].get("delta", {})
                            total_chars += len(delta.get("content", "") or "")
                    except json.JSONDecodeError:
                        pass

    total_elapsed = (last_chunk_time - t0) * 1000 if last_chunk_time else 0
    ttft = (first_chunk_time - t0) * 1000 if first_chunk_time else 0

    return {
        "ttft_ms": round(ttft, 1),
        "total_ms": round(total_elapsed, 1),
        "chunks": chunk_count,
        "output_chars": total_chars,
        "chars_per_sec": round(total_chars / (total_elapsed / 1000), 1) if total_elapsed > 0 else 0,
    }


async def test_stream_anthropic_direct(client: httpx.AsyncClient) -> dict:
    """流式：Anthropic 直连。"""
    url = "https://api.deepseek.com/v1/messages"
    body = {
        "model": MODEL,
        "max_tokens": 1024,
        "messages": TEST_MESSAGES,
        "stream": True,
    }
    headers = {
        "x-api-key": API_KEY,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }

    t0 = time.monotonic()
    first_chunk_time = None
    chunk_count = 0
    total_chars = 0
    last_chunk_time = None

    buffer = ""
    async with client.stream("POST", url, json=body, headers=headers, timeout=180) as resp:
        async for raw in resp.aiter_bytes():
            if first_chunk_time is None:
                first_chunk_time = time.monotonic()
            last_chunk_time = time.monotonic()
            chunk_count += 1

            buffer += raw.decode("utf-8", errors="replace")
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if line.startswith("data: "):
                    try:
                        data = json.loads(line[6:])
                        if data.get("type") == "content_block_delta":
                            delta = data.get("delta", {})
                            if delta.get("type") == "text_delta":
                                total_chars += len(delta.get("text", ""))
                    except json.JSONDecodeError:
                        pass

    total_elapsed = (last_chunk_time - t0) * 1000 if last_chunk_time else 0
    ttft = (first_chunk_time - t0) * 1000 if first_chunk_time else 0

    return {
        "ttft_ms": round(ttft, 1),
        "total_ms": round(total_elapsed, 1),
        "chunks": chunk_count,
        "output_chars": total_chars,
        "chars_per_sec": round(total_chars / (total_elapsed / 1000), 1) if total_elapsed > 0 else 0,
    }


async def test_stream_via_proxy(client: httpx.AsyncClient) -> dict:
    """流式：经代理（Anthropic 转换路径）。"""
    url = f"{PROXY_BASE}/chat/completions"
    body = {
        "model": "local_route",
        "messages": TEST_MESSAGES,
        "max_tokens": 1024,
        "stream": True,
    }
    headers = {"Authorization": "Bearer any", "Content-Type": "application/json"}

    t0 = time.monotonic()
    first_chunk_time = None
    chunk_count = 0
    total_chars = 0
    last_chunk_time = None

    async with client.stream("POST", url, json=body, headers=headers, timeout=180) as resp:
        async for raw in resp.aiter_bytes():
            if first_chunk_time is None:
                first_chunk_time = time.monotonic()
            last_chunk_time = time.monotonic()
            chunk_count += 1

            text = raw.decode("utf-8", errors="replace")
            for line in text.split("\n"):
                if line.startswith("data: ") and line != "data: [DONE]":
                    try:
                        data = json.loads(line[6:])
                        choices = data.get("choices", [])
                        if choices:
                            delta = choices[0].get("delta", {})
                            total_chars += len(delta.get("content", "") or "")
                    except json.JSONDecodeError:
                        pass

    total_elapsed = (last_chunk_time - t0) * 1000 if last_chunk_time else 0
    ttft = (first_chunk_time - t0) * 1000 if first_chunk_time else 0

    return {
        "ttft_ms": round(ttft, 1),
        "total_ms": round(total_elapsed, 1),
        "chunks": chunk_count,
        "output_chars": total_chars,
        "chars_per_sec": round(total_chars / (total_elapsed / 1000), 1) if total_elapsed > 0 else 0,
    }


# ── 主流程 ────────────────────────────────────────────────────────

def _print_table(label: str, results: list[dict]):
    avg = {k: statistics.mean([r[k] for r in results]) for k in results[0] if isinstance(results[0][k], (int, float))}
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(f"  运行次数: {len(results)}")
    for k in avg:
        vals = [r[k] for r in results]
        sd = statistics.stdev(vals) if len(vals) > 1 else 0
        if "ms" in k:
            print(f"  {k:20s}:  avg={_fmt(avg[k]):>10s}  min={_fmt(min(vals)):>10s}  max={_fmt(max(vals)):>10s}  sd={sd:.1f}")
        else:
            print(f"  {k:20s}:  avg={avg[k]:>10.1f}  min={min(vals):>10.1f}  max={max(vals):>10.1f}  sd={sd:.1f}")
    print()


def main():
    print("=" * 60)
    print("  DeepSeek OpenAI vs Anthropic 接口测速")
    print(f"  模型: {MODEL}  每模式 {NUM_RUNS} 次")
    print(f"  测试提示: \"{TEST_MESSAGES[0]['content'][:40]}...\"")
    print("=" * 60)

    # ── 非流式 ──────────────────────────────────────────────────
    with httpx.Client(timeout=httpx.Timeout(180.0)) as client:
        # Warm-up
        print("\n>>> Warm-up...")
        test_openai_direct(client)
        print("    Ready.")

        print("\n>>> [非流式] DeepSeek OpenAI 直连（基线）")
        openai_results = [test_openai_direct(client) for _ in range(NUM_RUNS)]
        _print_table("OpenAI 直连 (非流式)", openai_results)

        print(">>> [非流式] DeepSeek Anthropic 直连（原生协议）")
        anth_results = [test_anthropic_direct(client) for _ in range(NUM_RUNS)]
        _print_table("Anthropic 直连 (非流式)", anth_results)

        print(">>> [非流式] 经代理 — Anthropic 转换路径")
        proxy_results = [test_via_proxy(client) for _ in range(NUM_RUNS)]
        _print_table("代理转换 (非流式)", proxy_results)

    # ── 流式 ────────────────────────────────────────────────────
    import asyncio
    print("\n" + "=" * 60)
    print("  流式测试")
    print("=" * 60)

    async def run_stream_tests():
        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
            # Warm-up
            await test_stream_openai_direct(client)

            print("\n>>> [流式] DeepSeek OpenAI 直连")
            o_results = [await test_stream_openai_direct(client) for _ in range(NUM_RUNS)]
            _print_table("OpenAI 直连 (流式)", o_results)

            print(">>> [流式] DeepSeek Anthropic 直连")
            a_results = [await test_stream_anthropic_direct(client) for _ in range(NUM_RUNS)]
            _print_table("Anthropic 直连 (流式)", a_results)

            print(">>> [流式] 经代理 — Anthropic 转换路径")
            p_results = [await test_stream_via_proxy(client) for _ in range(NUM_RUNS)]
            _print_table("代理转换 (流式)", p_results)

    asyncio.run(run_stream_tests())

    print("\n" + "=" * 60)
    print("  测速完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
