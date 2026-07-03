from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class ProviderConfig(BaseModel):
    """A single LLM provider the proxy can failover to."""

    name: str
    api_base: str
    api_key: str
    api_type: str = "openai"
    model_map: dict[str, str] = Field(
        default_factory=dict,
        description="Map incoming model name -> provider-specific model name. "
        "Key 'default' is used when no explicit match.",
    )
    timeout: float = 60.0
    max_retries: int = 0
    priority: int = 0
    tags: list[str] = Field(default_factory=list)

    def resolve_model(self, incoming_model: str) -> str:
        """Map the incoming model name to this provider's model name."""
        if incoming_model in self.model_map:
            return self.model_map[incoming_model]
        if "default" in self.model_map:
            return self.model_map["default"]
        return incoming_model


class CircuitBreakerConfig(BaseModel):
    """熔断器配置"""

    enabled: bool = True
    failure_threshold: int = 3
    recovery_interval_seconds: float = 60.0
    probe_path: str = "/v1/models"


class SoftTriggerConfig(BaseModel):
    """软触发（流式降级检测）阈值配置

    所有阈值已按用户要求上调 50%：
    - TTFT:  8s  → 12s
    - TPOT: 500ms → 750ms
    - 吞吐: 10 tokens/s → 15 tokens/s
    """

    enabled: bool = True
    # 首 chunk 等待超时 (ms)
    ttft_threshold_ms: int = 12000
    # 每 token 生成时间滑动平均 (ms)
    tpot_threshold_ms: int = 750
    # 吞吐量低于此值持续 N 秒触发 (tokens/s)
    throughput_threshold_tokens_per_sec: int = 15
    # 吞吐量采样窗口 (秒)
    throughput_window_seconds: float = 5.0


class ProxyConfig(BaseModel):
    """Top-level proxy configuration."""

    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "info"
    providers: list[ProviderConfig] = Field(default_factory=list)
    upstream_api_key_header: str = "Authorization"
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
    soft_trigger: SoftTriggerConfig = Field(default_factory=SoftTriggerConfig)

    @property
    def sorted_providers(self) -> list[ProviderConfig]:
        """Providers ordered by priority (lowest first = highest priority)."""
        return sorted(self.providers, key=lambda p: p.priority)


def load_config(path: str | Path) -> ProxyConfig:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    raw: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not raw:
        raise ValueError(f"Empty config: {path}")

    # Environment variable substitution: ${VAR_NAME:-default}
    def _subst(value: Any) -> Any:
        if isinstance(value, str):
            import re

            def _replace(m: re.Match) -> str:
                var = m.group(1)
                default = m.group(2)
                return os.environ.get(var, default or "")
            return re.sub(r"\$\{([^}:]+)(?::-(.*?))?\}", _replace, value)
        if isinstance(value, dict):
            return {k: _subst(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_subst(v) for v in value]
        return value

    raw = _subst(raw)
    return ProxyConfig(**raw)
