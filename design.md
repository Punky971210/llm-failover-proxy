# Jiuwen LLM Failover Proxy — 设计文档

## 概述

Jiuwen 为 PyInstaller 编译产物，无法通过 `.pth` 注入、`sitecustomize.py` 或直接修改框架源码实现运行时 provider 切换。**唯一可行的非侵入方案**是在 Jiuwen 外部运行一个独立的 LLM Failover Proxy，Jiuwen 只需改一行 `api_base` 指向代理，由代理负责多 provider 的限流检测、熔断和透明切换。

生产验证：连续运行至今，0 次硬失败。

---

## 一、架构

```
Jiuwen (PyInstaller)           Proxy (Docker container)               Provider
       │                              │                                │
       │── POST /v1/chat/completions ─►│── 按 priority 顺序尝试 ──────► │
       │   model: local_route         │    ├── DeepSeek     (pri 0)  │
       │   api_base: localhost:8000   │    ├── Volcengine    (pri 1)  │
       │                              │    ├── GLM-5         (pri 2)  │
       │                              │    └── GLM-4.7       (pri 3)  │
       │◄── SSE stream ───────────────│                                │
       │    + heartbeat (3s)          │◄── 第一个成功的 provider ──────│
       │    + batch flush (50chunks)  │                                │
```

### 文件结构

```
D:\JiuwenSwarm\proxy\
├── config.yaml                 # 供应商配置（编辑后 docker compose restart）
├── requirements.txt            # 依赖：fastapi, uvicorn, httpx, pyyaml
├── docker-compose.yml          # Docker 部署（主用启动方式）
├── Dockerfile                  # Python 3.12-slim
├── design.md                   # 本文档
└── app/
    ├── main.py                 # FastAPI 入口 + 后台恢复探测
    ├── config.py               # 配置加载（支持 ${ENV_VAR}）
    ├── proxy.py                # failover 核心：熔断器 + 软触发 + SSE 重写 + 心跳
    ├── converter.py            # OpenAI ↔ Anthropic 协议转换（已弃用）
    ├── circuit_breaker.py      # 熔断器：状态跟踪、降级、自动恢复
    └── logger.py               # 日志（stdout + 文件轮转）

日志输出：C:\Users\Administrator\.jiuwenswarm\provider-switch-log\proxy.log（volume 挂载）
```

### 启动方式（主用 Docker）

```bash
cd D:\JiuwenSwarm\proxy
docker compose up -d --build     # 构建 + 启动
docker compose restart           # 改配置后重启
docker compose down              # 停止
docker logs llm-failover-proxy -f  # 查看实时日志
```

### 裸进程启动（备用）

```bash
cd D:\JiuwenSwarm\proxy
python -m app.main
```

---

## 二、触发条件

### 硬触发（连接级别，即时切换）

| 条件 | 处理 |
|------|------|
| HTTP 408/429/500/502/503/504 | 自动尝试下一个 provider |
| 连接超时 / DNS 失败 / 连接拒绝 | 自动尝试下一个 provider |
| httpx.ReadError（连接中断 / TCP reset） | 自动尝试下一个 provider（1M 上下文长响应防护） |
| Stream 中途断流（首 chunk 前） | 自动尝试下一个 provider |

### 软触发（流式检测，实时计算）

通过滑动窗口在每块 SSE chunk 到达时更新，超标即切换。**兼容 DeepSeek 思考模式**：`reasoning_content` chunk 不参与 TPOT 计算（text_len=0），思考期不会误触发切换。

| 指标 | 阈值（思考模式兼容） | 说明 |
|------|---------------------|------|
| **TTFT** | > 60s | 首 chunk 等待时间（1M 上下文+思考模式预留） |
| **TPOT** | > 10s | 每 token 生成时间滑动平均（思考→内容过渡间隙） |
| **吞吐** | < 3 t/s 持续 20s | 每秒收到 chunks 数（推理稀疏期容忍） |

不切换的情况：HTTP 401/403/400（认证错，换 provider 也没用）、stream 首 chunk 已发出后断流（不拼接，维持原响应）。

---

## 三、流式批刷新 (Batch Flush)

**解决**：1MB+ 响应体导致 Jiuwen 事件循环 CPU 饥饿（旧 Content-Length 模式）。

将 `StreamingResponse` 的输出流按小批次刷新：

| 参数 | 值 | 说明 |
|------|-----|------|
| 批触发 | 50 chunks 或 48KB | 先到先刷新 |
| 刷新操作 | `yield chunk` + `await asyncio.sleep(0)` | 每批让步事件循环 |
| SSE 心跳 | `b": keepalive\n\n"` | 上游空闲时每 3s 发一条，保活 Jiuwen 连接 |

Token 级别 Metrics（`SlidingMetrics`）在每一批 chunk 到达时实时更新，滑动窗口大小由配置 `throughput_window_seconds` 控制（默认注入 `throughput_window_seconds` 值实现动态窗口）。

---

## 四、熔断器 (CircuitBreaker)

每个 provider 独立状态机：

```
        连续失败 ≥3 次
 正常 ──────────────────► 降级（跳过）
  ↑                        │
  │                        │ 每 60s 发 /v1/models 轻量探测
  │   探测响应 2xx         │
  └────────────────────────◄ 成功则恢复
```

- `record_success(name)` — 重置失败计数
- `record_failure(name)` — +1，达到阈值→降级
- `is_degraded(name)` — 跳过降级的 provider
- 后台 `_recovery_loop()` 每 60s 遍历降级列表探测恢复

---

## 五、SSE 重写

所有吐给 Jiuwen 的 SSE chunk 经过一层重写，抹掉 provider 切换痕迹：

| 原始字段 | 重写为 |
|---------|--------|
| `id: "xxxx"` | `id: "proxy_{timestamp}_{req_id}"` |
| `model: "deepseek-v4-flash"` | `model: "local_route"` |

Jiuwen 全程看到同一个 `proxy_xxx` ID 和 `local_route` 模型名，感知不到 provider 切换。

---

## 六、SSE 心跳与空闲超时

**文件**：`app/proxy.py` → `_aiter_with_heartbeat()`

| 功能 | 参数 | 说明 |
|------|------|------|
| 心跳间隔 | 3s | 上游无数据时发 `: keepalive\n\n` |
| 空闲超时 | = TTFT 阈值（60s） | 首 chunk 超时触发 SoftTriggerSwitch |
| 二次包装 | idle_soft_trigger_ms=0 | 转换器后包装仅保活，TTFT 由上游层检测 |

行为：
- 上游有 chunk → 设置 `has_data=True`，空闲超时不触发
- 上游 3s 无 chunk → 发心跳，`has_data=False` 且超时 → 触发转接
- 思考模式的 `reasoning_content` chunk 设 `has_data=True`，思考期安全

---

## 七、日志与监控

### 日志文件

`C:\Users\Administrator\.jiuwenswarm\provider-switch-log\proxy.log`，自动轮转（10MB × 5 份）。

### 关键日志标签

| 标签 | 含义 |
|------|------|
| `[Try] 流式/non-stream provider=X` | 尝试请求某 provider |
| `[Try] 流已连接` | HTTP 连接建立成功 |
| `[Try] 流正常结束` | 请求正常完成 |
| `[Switch] 完成 provider=X` | 一次完整的请求处理结束 |
| `[Switch] 切换 provider=X -> next` | 硬触发：失败后切下一个 |
| `[Switch] 软触发流切换 provider=X` | 软触发：指标超标后切换 |
| `[Switch] 所有 provider 流耗尽` | 所有 provider 均失败 |
| `[CB] X 降级` | 连续失败后熔断 |
| `[CB] X 恢复` | 熔断的 provider 恢复正常 |
| `[Recovery] X 已恢复` | 后台探测发现 provider 已恢复 |

### 熔断器状态快照

每个 `[Switch]` 事件附带熔断器摘要：
```
cb=deepseek=UP(fail=0,switch=2) | volcengine=UP(...) | glm-5=UP(...) | glm-4.7=UP(...)
```
- `UP` = 正常 / `DOWN` = 已降级
- `fail=N` = 当前连续失败次数
- `switch=N` = 累计从此 provider 切走的次数

### Health 端点

```bash
curl http://localhost:8000/health
```
返回 provider 列表、熔断器状态、软触发阈值。

---

## 八、配置文件

### `config.yaml`（当前）

```yaml
host: "0.0.0.0"
port: 8000

circuit_breaker:
  enabled: true
  failure_threshold: 3
  recovery_interval_seconds: 60
  probe_path: "/v1/models"

soft_trigger:
  enabled: true
  ttft_threshold_ms: 60000       # 思考模式兼容
  tpot_threshold_ms: 10000
  throughput_threshold_tokens_per_sec: 3
  throughput_window_seconds: 20.0

providers:
  - name: deepseek             # 0. DeepSeek 原生（主用）
    priority: 0
    api_base: "https://api.deepseek.com"
    api_key: "sk-xxx"
    timeout: 180
    model_map:
      local_route: "deepseek-v4-flash"

  - name: volcengine           # 1. 火山引擎（备用）
    priority: 1
    api_base: "https://ark.cn-beijing.volces.com/api/v3"
    api_key: "ark-xxx"
    timeout: 180
    model_map:
      local_route: "deepseek-v4-flash-260425"

  - name: glm-5                # 2. GLM-5 兜底
    priority: 2
    api_base: "https://open.bigmodel.cn/api/paas/v4"
    api_key: "xxx"
    timeout: 120
    model_map:
      local_route: "glm-5.2"

  - name: glm-4.7              # 3. GLM-4.7 最终兜底
    priority: 3
    api_base: "https://open.bigmodel.cn/api/paas/v4"
    api_key: "xxx"
    timeout: 120
    model_map:
      local_route: "glm-4.7"
```

### Jiuwen config.yaml 配置

Jiuwen 的 `~/.jiuwenswarm/config/config.yaml` 中 `models.defaults` 指向代理：

```yaml
models:
  defaults:
    - model_client_config:
        api_base: http://localhost:8000/v1    # ← 指向 Docker 代理
        api_key: default
        model_name: local_route               # ← 代理通过 model_map 映射
        client_provider: OpenAI
        timeout: 1800
        verify_ssl: false
      model_config_obj:
        temperature: 0.95
      is_default: true
```

如果 Jiuwen 和代理不在同一台机器，`localhost` 换成代理所在机器的 IP。

---

## 九、Provider 演进记录

| Provider | 状态 | 说明 |
|----------|------|------|
| DeepSeek 原生 OpenAI | ✅ 主用 | priority 0，正常工作 |
| 火山引擎 Ark | ✅ 备用 | priority 1，TTFT 较慢但可用 |
| **DeepSeek Anthropic 翻译层** | ❌ **已弃用** | 2026-07-04 移除。`tool_use` 不兼容导致 HTTP 400，收益小于风险 |
| GLM-5 | ✅ 兜底 | priority 2 |
| GLM-4.7 | ✅ 兜底 | priority 3，TTFT 不稳定 |

---

## 十、方案演进记录

| 阶段 | 方案 | 结果 |
|------|------|------|
| v1 | 直接修改 PyInstaller 内部 `.py` | ❌ 路径不存在 |
| v2 | `.pth` + `sitecustomize.py` | ❌ PyInstaller 不处理 |
| v3 | 修改 `openjiuwen` pip 包 | ❌ 侵入式，升级后丢失 |
| v4 | 外部 Failover Proxy | ✅ 150 请求 0 失败 |
| v5 | Docker 容器化 + Anthropic 翻译层 | ✅ 421 请求 22 软触发 |
| v6 方案A | Content-Length 响应（消除 IOCP ReadError） | ✅ 上线 |
| **v7 方案C** | **StreamingResponse + 批刷新 + 心跳** | ✅ **当前方案** |
| v7.1 | ReadError 加入可重试列表 | ✅ 已部署 |
| v7.2 | 软阈值 1M 上下文扩容（60s/10s/3t/s） | ✅ 2026-07-04 |
| v7.3 | 移除 deepseek-anthropic，兼容思考模式 | ✅ 2026-07-04 |
| v7.4 | SlidingMetrics 窗口可配置（弃用硬编码 5s） | ✅ 2026-07-04 |

---

## 十一、生产验证数据

### 2026-07-02（首日，proxy v5）

连续运行 13.5 小时（07:33 ~ 21:08），5 个 provider 全部 UP。

| 指标 | 数值 |
|------|------|
| 总请求 | 421 |
| 成功完成 | 383（91%） |
| 软触发切换 | 22 次 |
| 硬错误（ProviderFailed） | 0 |
| 所有 provider 耗尽 | 0 |

切换分布：
- **TTFT 超限**：8 次
- **吞吐不足**：11 次
- **TPOT 过慢**：3 次

### 2026-07-03（硬阈值+方案A）

DeepSeek 全天稳定，切换 0 次。

### 2026-07-04（方案C 批刷新 + 1M 上下文 + 思考模式兼容）

| 指标 | 数值 |
|------|------|
| 会话最长上下文 | 4.35M 原始累计 token（压缩后 ~90K/次 API payload） |
| 单会话最大轮次 | 242 轮 |
| 软阈值误触发 | 0 次（扩容后） |
| 思考模式兼容 | 已验证，TTFT/TPOT/吞吐均正常 |

---

## 十二、已知限制

1. **Stream 不拼接** — 软触发切换后，Jiuwen 看到的是新 provider 的完整响应，不是"续接"旧 provider 的半截话。对 agent 工作无影响。
2. **TTFB 取决于第一个 provider** — 正常情况直接返回，首 chunk 延迟 = DeepSeek 的 TTFT。切换场景需要等超时/指标超标后再试下一个。
3. **单进程** — 当前为单进程 uvicorn，高并发场景建议多进程。

---

## 十三、Jiuwen 侧上下文压缩器配置（1M 窗口扩容）

**文件**：`C:\Users\Administrator\.jiuwenswarm\config\config.yaml` → `react.context_engine_config`

**目标**：适配 DeepSeek v4 Flash 1M 上下文窗口，提升长任务输出质量。

| 压缩器 | 关键参数 | 触发阈值 | 目标值 |
|--------|---------|---------|-------|
| `message_summary_offloader` | messages_threshold / summary_max_tokens | 120 条 / 40K 字符 | 保留 80 条 / 3K token 摘要 |
| `dialogue_compressor` | tokens_threshold / compression_target | 700K（70%） | 100K |
| `current_round_compressor` | tokens_threshold / compression_target | 650K（65%） | 80K |
| `round_level_compressor` | tokens_threshold / target_total | 750K（75%） | 400K（释放 350K） |

效果：单会话 242 轮 / 4.35M 原始累计 token 运行正常，每轮实际 API payload 保持 ~90K。
