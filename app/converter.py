"""
OpenAI <-> Anthropic 协议转换器（实验性模块）。

将 Jiuwen 发出的 OpenAI 格式请求转换为 Anthropic Messages API 格式
（用于 DeepSeek 的 Anthropic 兼容接口），并将响应转换回 OpenAI 格式。
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, AsyncIterator

# ── tool_call_id 双向映射（跨轮次对话需要） ──────────────────────────
# OpenAI call_xxx -> Anthropic toolu_xxx
_tc_fwd: dict[str, str] = {}
# Anthropic toolu_xxx -> OpenAI call_xxx
_tc_rev: dict[str, str] = {}


def _map_to_openai_tcid(anthropic_id: str) -> str:
    """Convert Anthropic tool_use_id -> OpenAI-style tool_call_id."""
    if anthropic_id in _tc_rev:
        return _tc_rev[anthropic_id]
    oid = f"call_{uuid.uuid4().hex[:12]}"
    _tc_fwd[oid] = anthropic_id
    _tc_rev[anthropic_id] = oid
    return oid


def _map_to_anthropic_tcid(openai_id: str) -> str:
    """Convert OpenAI tool_call_id -> Anthropic tool_use_id."""
    return _tc_fwd.get(openai_id, openai_id)


# ═══════════════════════════════════════════════════════════════════
# OpenAI Request -> Anthropic Request
# ═══════════════════════════════════════════════════════════════════

def openai_to_anthropic_request(body: dict) -> dict:
    """Convert OpenAI /v1/chat/completions body -> Anthropic /v1/messages body."""
    out: dict[str, Any] = {}

    # model
    out["model"] = body.get("model", "")

    # max_tokens (required in Anthropic API)
    out["max_tokens"] = body.get("max_tokens", 4096)

    # stream
    if body.get("stream"):
        out["stream"] = True

    # temperature / top_p / stop_sequences
    for k in ("temperature", "top_p"):
        if k in body:
            out[k] = body[k]
    stop = body.get("stop")
    if stop:
        out["stop_sequences"] = [stop] if isinstance(stop, str) else stop

    # system (extract from messages[0])
    messages = body.get("messages", [])
    system_parts: list[str] = []
    non_system_msgs: list[dict] = []
    for m in messages:
        if m.get("role") == "system":
            content = m.get("content", "")
            if isinstance(content, list):
                system_parts.extend(c.get("text", "") for c in content if c.get("type") == "text")
            else:
                system_parts.append(str(content))
        else:
            non_system_msgs.append(m)
    if system_parts:
        out["system"] = "\n".join(system_parts)

    # messages
    out["messages"] = _convert_messages(non_system_msgs)

    # tools
    if body.get("tools"):
        out["tools"] = [_convert_tool(t) for t in body["tools"]]

    # tool_choice（None = 不发送，走默认 auto）
    if "tool_choice" in body:
        converted = _convert_tool_choice(body["tool_choice"])
        if converted is not None:
            out["tool_choice"] = converted

    # reasoning_effort -> thinking
    if body.get("reasoning_effort") == "high":
        out["thinking"] = {"type": "enabled"}

    # metadata.user_id
    user = body.get("user")
    if user:
        out.setdefault("metadata", {})["user_id"] = str(user)

    return out


def _convert_messages(messages: list[dict]) -> list[dict]:
    """Convert OpenAI messages[] to Anthropic messages[]."""
    out: list[dict] = []
    for m in messages:
        role = m.get("role", "user")
        content = m.get("content", "")

        if role == "user":
            out.append({"role": "user", "content": _convert_user_content(content)})

        elif role == "assistant":
            anth_content: list[dict] = []
            # text content
            if content:
                if isinstance(content, str):
                    anth_content.append({"type": "text", "text": content})
                elif isinstance(content, list):
                    for block in content:
                        if isinstance(block, dict) and block.get("type") == "text":
                            anth_content.append({"type": "text", "text": block.get("text", "")})

            # tool_calls
            tool_calls = m.get("tool_calls", [])
            if isinstance(tool_calls, list):
                for tc in tool_calls:
                    if not isinstance(tc, dict):
                        continue
                    tc_id = tc.get("id", f"call_{uuid.uuid4().hex[:12]}")
                    func = tc.get("function", {})
                    try:
                        args = json.loads(func.get("arguments", "{}")) if isinstance(func.get("arguments"), str) else func.get("arguments", {})
                    except json.JSONDecodeError:
                        args = {}
                    anth_content.append({
                        "type": "tool_use",
                        "id": _map_to_anthropic_tcid(tc_id),
                        "name": func.get("name", ""),
                        "input": args,
                    })
            out.append({"role": "assistant", "content": anth_content})

        elif role == "tool":
            # tool_result -> user message with tool_result content block
            tool_use_id = _map_to_anthropic_tcid(m.get("tool_call_id", ""))
            result_content = content
            if isinstance(result_content, str):
                result_content = [{"type": "text", "text": result_content}]
            elif isinstance(result_content, list):
                result_content = [{"type": "text", "text": json.dumps(c) if not isinstance(c, str) else c} for c in result_content]
            out.append({
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": tool_use_id, "content": result_content}],
            })

    return out


def _convert_user_content(content: Any) -> str | list[dict]:
    """Convert OpenAI user message content to Anthropic format."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        blocks: list[dict] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            btype = block.get("type", "text")
            if btype == "text":
                blocks.append({"type": "text", "text": block.get("text", "")})
            elif btype == "image_url":
                # Anthropic format: type=image, source={type, media_type, data}
                image_url = block.get("image_url", {}).get("url", "")
                if image_url.startswith("data:"):
                    # base64 inline
                    try:
                        media_type, b64data = image_url.split(",", 1)[0][5:], image_url.split(",", 1)[1]
                    except (ValueError, IndexError):
                        media_type, b64data = "image/png", ""
                    blocks.append({
                        "type": "image",
                        "source": {"type": "base64", "media_type": media_type, "data": b64data},
                    })
                else:
                    # URL image — not all Anthropic endpoints support this, fallback to text
                    blocks.append({"type": "text", "text": f"[Image: {image_url}]"})
        return blocks
    return str(content)


def _convert_tool(t: dict) -> dict:
    """Convert OpenAI tool definition -> Anthropic tool definition."""
    func = t.get("function", t)
    return {
        "name": func.get("name", ""),
        "description": func.get("description", ""),
        "input_schema": func.get("parameters", {}),
    }


def _convert_tool_choice(tc: Any) -> dict | None:
    """Convert OpenAI tool_choice -> Anthropic tool_choice.

    DeepSeek Anthropic API 不接受字符串格式（如 ``"auto"``），
    统一转为对象格式。返回 ``None`` 表示不发送（默认 auto）。
    """
    if isinstance(tc, str):
        if tc == "auto":
            return None  # 不发送 = 默认 auto
        if tc == "none":
            return {"type": "none"}
        if tc == "any":
            return {"type": "any"}
        return None
    if isinstance(tc, dict):
        tc_type = tc.get("type", "auto")
        if tc_type == "function":
            name = tc.get("function", {}).get("name", "")
            return {"type": "tool", "name": name}
        return {"type": tc_type}
    return None


# ═══════════════════════════════════════════════════════════════════
# Anthropic Response -> OpenAI Response (non-streaming)
# ═══════════════════════════════════════════════════════════════════

_REASON_MAP = {
    "end_turn": "stop",
    "tool_use": "tool_calls",
    "max_tokens": "length",
    "stop_sequence": "stop",
}


def anthropic_to_openai_response(body: dict, proxy_model: str) -> dict:
    """Convert Anthropic /v1/messages response -> OpenAI /v1/chat/completions format."""
    content_blocks = body.get("content", [])

    # Extract text and tool_calls from content blocks
    text_parts: list[str] = []
    tool_calls_out: list[dict] = []
    for block in content_blocks:
        btype = block.get("type", "")
        if btype == "text":
            text_parts.append(block.get("text", ""))
        elif btype == "tool_use":
            tool_calls_out.append({
                "id": _map_to_openai_tcid(block.get("id", "")),
                "type": "function",
                "function": {
                    "name": block.get("name", ""),
                    "arguments": json.dumps(block.get("input", {}), ensure_ascii=False),
                },
            })
        # type=thinking — stripped (no OpenAI equivalent)

    finish_reason = _REASON_MAP.get(body.get("stop_reason", ""), "stop")

    usage = body.get("usage", {})
    return {
        "id": f"chatcmpl_{uuid.uuid4().hex[:12]}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": proxy_model,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "\n".join(text_parts) if text_parts else None,
                **({"tool_calls": tool_calls_out} if tool_calls_out else {}),
            },
            "finish_reason": finish_reason,
        }],
        "usage": {
            "prompt_tokens": usage.get("input_tokens", 0),
            "completion_tokens": usage.get("output_tokens", 0),
            "total_tokens": usage.get("input_tokens", 0) + usage.get("output_tokens", 0),
        },
    }


# ═══════════════════════════════════════════════════════════════════
# Anthropic SSE Stream -> OpenAI SSE Stream
# ═══════════════════════════════════════════════════════════════════

class AnthropicStreamConverter:
    """Reads Anthropic SSE events and yields OpenAI-format SSE bytes."""

    def __init__(self, proxy_model: str):
        self._openai_id = f"chatcmpl_{uuid.uuid4().hex[:12]}"
        self._created = int(time.time())
        self._model = proxy_model
        # Track content block types by index (to skip thinking blocks)
        self._block_types: dict[int, str] = {}
        self._finished = False

    async def convert(self, stream: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
        """Convert Anthropic SSE byte stream -> OpenAI SSE byte chunks."""
        buffer = ""
        current_event: str | None = None

        async for raw in stream:
            buffer += raw.decode("utf-8", errors="replace")

            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)

                if line.startswith("event: "):
                    current_event = line[7:]

                elif line.startswith("data: "):
                    payload = line[6:]
                    chunk = self._process_event(current_event, payload)
                    if chunk is not None:
                        yield chunk

                elif line == "":
                    current_event = None

        if not self._finished:
            yield b"data: [DONE]\n\n"

    def _process_event(self, event: str | None, data_str: str) -> bytes | None:
        """Process a single Anthropic SSE event -> OpenAI chunk or None."""
        try:
            data = json.loads(data_str)
        except json.JSONDecodeError:
            return None

        event_type = data.get("type", "")

        if event_type == "message_start":
            msg = data.get("message", {})
            usage = msg.get("usage", {})
            return self._make_chunk(
                delta={"role": "assistant", "content": ""},
                finish_reason=None,
                usage=usage,
            )

        elif event_type == "content_block_start":
            idx = data.get("index", 0)
            block = data.get("content_block", {})
            btype = block.get("type", "")
            self._block_types[idx] = btype

            if btype == "tool_use":
                return self._make_chunk(
                    delta={
                        "tool_calls": [{
                            "index": idx,
                            "id": _map_to_openai_tcid(block.get("id", "")),
                            "type": "function",
                            "function": {
                                "name": block.get("name", ""),
                                "arguments": json.dumps(block.get("input", {}), ensure_ascii=False),
                            },
                        }],
                    },
                    finish_reason=None,
                )
            return None

        elif event_type == "content_block_delta":
            idx = data.get("index", 0)
            # Skip thinking deltas
            if self._block_types.get(idx) in ("thinking", None):
                return None

            delta_data = data.get("delta", {})
            dtype = delta_data.get("type", "")

            if dtype == "text_delta":
                text = delta_data.get("text", "")
                return self._make_chunk(
                    delta={"content": text},
                    finish_reason=None,
                )

            return None

        elif event_type == "content_block_stop":
            return None

        elif event_type == "message_delta":
            delta = data.get("delta", {})
            stop_reason = delta.get("stop_reason")
            finish_reason = _REASON_MAP.get(stop_reason, "stop") if stop_reason else None
            usage = data.get("usage", {})
            return self._make_chunk(
                delta={},
                finish_reason=finish_reason if finish_reason != "stop" else "stop",
                usage=usage if usage else None,
            )

        elif event_type == "message_stop":
            self._finished = True
            return b"data: [DONE]\n\n"

        return None

    def _make_chunk(
        self,
        delta: dict,
        finish_reason: str | None,
        usage: dict | None = None,
    ) -> bytes:
        """Build an OpenAI SSE data chunk."""
        chunk = {
            "id": self._openai_id,
            "object": "chat.completion.chunk",
            "created": self._created,
            "model": self._model,
            "choices": [{
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }],
        }
        if usage:
            chunk["usage"] = {
                "prompt_tokens": usage.get("input_tokens", 0),
                "completion_tokens": usage.get("output_tokens", 0),
                "total_tokens": usage.get("input_tokens", 0) + usage.get("output_tokens", 0),
            }
        return b"data: " + json.dumps(chunk, ensure_ascii=False).encode("utf-8") + b"\n\n"
