# Jiuwen LLM Failover Proxy

Jiuwen 外部 Failover Proxy。Jiuwen（PyInstaller 编译产物）只需改一行 `api_base` 指向本代理，即可获得多 Provider 自动故障转移、熔断保护、软触发切换、SSE 心跳保活和完整的管理面板。

生产验证：连续运行 0 次硬失败。

---

## 目录

- [快速开始](#快速开始)
- [手动部署](#手动部署)
- [配置文件](#配置文件)
- [Jiuwen 侧配置](#jiuwen-侧配置)
- [deploy_local.bat 说明](#deploy_localbat-说明)
- [管理面板](#管理面板)
- [API 端点](#api-端点)
- [架构原理](#架构原理)
- [已知限制](#已知限制)
- [项目文件结构](#项目文件结构)

---

## 快速开始

```batch
cd .\proxy
deploy_local.bat
```

脚本自动执行：

1. 停用旧容器并删除旧镜像
2. 清理 Docker 构建缓存
3. 构建新镜像
4. 启动容器
5. 等待健康检查通过
6. 打开浏览器进入管理面板 `http://localhost:8000/admin`

> 首次构建需要几分钟（下载 Python 3.12-slim 基础镜像），后续构建秒级完成。

---

## 手动部署

### Docker（主用）

```bash
cd .\proxy

# 构建 + 启动
docker compose up -d --build

# 改配置后重启
docker compose restart

# 查看实时日志
docker logs llm-failover-proxy -f

# 停止
docker compose down
```

### 裸进程启动（备用）

```bash
cd .\proxy
python -m app.main
```

> 裸进程模式需要自行安装依赖：`pip install -r requirements.txt`

---

## 配置文件

`config.yaml` 是代理的唯一配置来源，编辑后需 `docker compose restart`。

```yaml
host: "0.0.0.0"
port: 8000
admin_key: "${PROXY_ADMIN_KEY:-admin123}"  # 管理面板认证（环境变量可覆盖）

circuit_breaker:
  enabled: true
  failure_threshold: 3                    # 连续失败 N 次后熔断
  recovery_interval_seconds: 60           # 每 N 秒尝试恢复熔断的 provider
  probe_path: "/v1/models"                # 探测请求路径（不消耗 token）

soft_trigger:
  enabled: true
  ttft_threshold_ms: 60000                # 首 chunk 等待超时（ms）
  tpot_threshold_ms: 10000                # 每 token 生成超时（ms）
  throughput_threshold_tokens_per_sec: 3  # 最低吞吐（t/s）
  throughput_window_seconds: 20.0         # 吞吐采样窗口（s）

providers:
  - name: deepseek                        # 提供商标识（用于日志/熔断器）
    priority: 0                           # 优先级（0=最高）
    api_base: "https://api.deepseek.com"  # 上游 API 地址
    api_key: "sk-xxx"                     # API Key
    timeout: 180                          # 请求超时（秒）
    model_map:
      local_route: "deepseek-v4-flash"    # Jiuwen→上游模型名映射

  - name: volcengine
    priority: 1
    api_base: "https://ark.cn-beijing.volces.com/api/v3"
    api_key: "ark-xxx"
    timeout: 180
    model_map:
      local_route: "deepseek-v4-flash-260425"

  - name: glm-5
    priority: 2
    api_base: "https://open.bigmodel.cn/api/paas/v4"
    api_key: "xxx"
    timeout: 120
    model_map:
      local_route: "glm-5.2"

  - name: glm-4.7
    priority: 3
    api_base: "https://open.bigmodel.cn/api/paas/v4"
    api_key: "xxx"
    timeout: 120
    model_map:
      local_route: "glm-4.7"
```

### 配置字段说明

| 字段 | 必填 | 说明 |
|------|------|------|
| `admin_key` | 否 | 面板认证密钥。置空=不认证。支持 `${ENV_VAR:-default}` 语法 |
| `circuit_breaker.enabled` | 否 | 是否启用熔断器，默认 `true` |
| `circuit_breaker.failure_threshold` | 否 | 连续失败次数阈值，默认 `3` |
| `circuit_breaker.recovery_interval_seconds` | 否 | 熔断恢复探测间隔，默认 `60` |
| `soft_trigger.enabled` | 否 | 是否启用软触发，默认 `true` |
| `soft_trigger.ttft_threshold_ms` | 否 | 首 chunk 等待超时，默认 `60000` |
| `soft_trigger.tpot_threshold_ms` | 否 | 每 token 生成超时，默认 `10000` |
| `soft_trigger.throughput_threshold_tokens_per_sec` | 否 | 最低吞吐，默认 `3` |
| `providers[].name` | 是 | 提供商标识 |
| `providers[].priority` | 是 | 优先级（小=高），代理按优先级升序尝试 |
| `providers[].api_base` | 是 | 上游 API 地址 |
| `providers[].api_key` | 是 | API Key |
| `providers[].timeout` | 否 | 请求超时（秒），默认 `60` |
| `providers[].model_map` | 推荐 | 模型名映射字典（Jiuwen 发 `local_route` → 替换为真实模型名） |

---

## Jiuwen 侧配置

在 Jiuwen 的 `~/.jiuwenswarm/config/config.yaml` 中修改 `models.defaults`：

```yaml
models:
  defaults:
    - model_client_config:
        api_base: http://localhost:8000/v1    # 指向 Docker 代理
        api_key: default                      # 代理不验证 key
        model_name: local_route               # 代理通过 model_map 映射
        client_provider: OpenAI
        timeout: 1800
        verify_ssl: false
      model_config_obj:
        temperature: 0.95
      is_default: true
```

> 如果 Jiuwen 和代理不在同一台机器，将 `localhost` 换成代理所在机器的 IP。

---

## deploy_local.bat 说明

### 作用

一键完成"构建 Docker 镜像 → 启动容器 → 等待就绪 → 打开管理面板"全流程，专为本地开发/测试设计。

### 认证处理

`deploy_local.bat` 会构建 Docker 镜像并启动容器，同时将 `PROXY_ADMIN_KEY` 环境变量设为空值从而关闭面板认证：

```
[停用旧容器] docker compose down
[删除旧镜像] docker rmi -f proxy-llm-failover:latest
[清理缓存]   docker builder prune
[构建]       docker compose build --build-arg APP_SRC_HASH=<timestamp>
[启动]       docker compose up -d
[等待健康]   每 3 秒探测一次 /health 端点
[打开浏览器]  http://localhost:8000/admin
```

### 系统要求

- Windows 10/11
- Docker Desktop（已启动）
- 脚本使用纯 ASCII 字符、CRLF 行尾，兼容中文 Windows 的 GBK 编码

### 常用命令

```batch
deploy_local.bat              # 构建 + 启动 + 打开面板

# 手动管理（不用脚本时）
docker compose restart        # 改配置后重启
docker compose down           # 停止
docker compose up -d          # 启动已有容器
docker compose up -d --build  # 强制重建
docker logs llm-failover-proxy -f  # 实时日志
```

---

## 管理面板

访问 `http://localhost:8000/admin`（无需额外启动进程）。

| 页面 | 功能 |
|------|------|
| **仪表盘** | 24h 总请求/切换次数、运行时长、熔断器状态、软触发阈值、最近切换事件 |
| **Provider** | 拖拽排序调整 failover 优先级、手动恢复熔断器、查看 API Key |
| **实时监控** | SSE 实时事件流（切换/熔断/恢复）、Provider 实时状态卡片 |
| **用量统计** | Chart.js 趋势图/饼图、Provider 详情表、切换事件表（24h/7d/30d） |
| **模型测试** | 选 Provider 发送测试请求、显示 TTFT/总耗时/输出字符（支持流式） |
| **安全** | 6 项自检：Key 重复/格式、熔断器配置、软触发配置、HTTPS、启动探测、错误过滤 |
| **配置** | Provider 配置卡片 + 原始 JSON 查看 |

---

## API 端点

### 代理端点

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/chat/completions` | OpenAI 兼容的 chat completions（流式/非流式） |
| GET | `/v1/models` | 可用模型列表 |
| POST | `/v1/difficulty` | 问题难度估算 |
| GET | `/results/{session_id}` | 按 session 查询缓存的 LLM 响应 |
| POST | `/results/consume` | 消费（读取后删除）缓存的 LLM 响应 |
| GET | `/health` | 健康检查（含熔断器状态） |

### 管理面板 API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/admin/dashboard` | 仪表盘汇总数据 |
| GET | `/admin/providers` | Provider 列表 + 熔断器状态 |
| PUT | `/admin/providers/reorder` | 拖拽排序持久化 |
| POST | `/admin/providers/{name}/reset-cb` | 手动恢复熔断器 |
| GET | `/admin/providers/{name}/keys` | 查看 API Key（掩码） |
| POST | `/admin/test/completion` | 模型测试（支持流式） |
| GET | `/admin/stats/summary` | 用量汇总 |
| GET | `/admin/stats/by-provider` | 各 Provider 用量详情 |
| GET | `/admin/stats/switches` | 切换事件历史 |
| GET | `/admin/stats/trend` | Chart.js 趋势数据 |
| GET | `/admin/security/checks` | 6 项安全自检 |
| GET | `/admin/config` | 当前配置查看 |
| GET | `/admin/events/stream` | SSE 实时事件流 |

---

## 架构原理

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

### 触发条件

- **硬触发** — HTTP 408/429/500/502/503/504、连接超时、DNS 失败、ReadError：即时切换下一个 provider
- **软触发** — 流式检测 TTFT（>60s）/ TPOT（>10s）/ 吞吐（<3 t/s 持续 20s）：性能下降时主动切换
- **不切换** — HTTP 401/403/400（认证错，换 provider 也没用）、首 chunk 已发出后断流

### 熔断器

```
        连续失败 ≥3 次
 正常 ──────────────────► 降级（跳过）
  ↑                        │
  │                        │ 每 60s 发 /v1/models 轻量探测
  │   探测响应 2xx         │
  └────────────────────────◄ 成功则恢复
```

### SSE 重写

抹掉 provider 切换痕迹：`id` 统一重写为 `proxy_{timestamp}_{req_id}`，`model` 统一重写为 `local_route`，Jiuwen 全程感知不到切换发生。

---

## 已知限制

1. **Stream 不拼接** — 软触发切换后，Jiuwen 看到的是新 provider 的完整响应，不是"续接"旧 provider 的半截话。对 agent 工作无影响。
2. **TTFB 取决于第一个 provider** — 正常情况直接返回，首 chunk 延迟 = 主用 provider 的 TTFT。切换场景需要等超时/指标超标后再试下一个。
3. **单进程** — 当前为单进程 uvicorn，高并发场景建议多进程。
4. **管理面板认证** — `admin_key` 仅在加载时读取（FastAPI lifespan），修改后需重启容器。本地部署建议置空，生产环境务必设强密码。

---

## 项目文件结构

```
.\proxy\
├── README.md                   # 本文档
├── config.yaml                 # 供应商配置（编辑后 docker compose restart）
├── deploy_local.bat            # 本地部署脚本（自动关闭面板认证 + 构建 + 启动）
├── requirements.txt            # 依赖：fastapi, uvicorn, httpx, pyyaml
├── docker-compose.yml          # Docker 部署（主用启动方式）
├── Dockerfile                  # Python 3.12-slim
└── app/
    ├── main.py                 # FastAPI 入口 + 后台恢复探测 + 管理面板注册
    ├── config.py               # 配置加载（支持 ${ENV_VAR}）
    ├── proxy.py                # failover 核心：熔断器 + 软触发 + SSE 重写 + 心跳
    ├── result_store.py         # LLM 响应结果 SQLite 存储 + 用量统计表
    ├── admin_api.py            # 管理面板 API 路由（14 端点）
    ├── converter.py            # OpenAI ↔ Anthropic 协议转换（已弃用）
    ├── circuit_breaker.py      # 熔断器：状态跟踪、降级、自动恢复
    ├── logger.py               # 日志（stdout + 文件轮转）
    └── static/                 # 管理面板前端（SPA）
        ├── admin.html          # 7 页面 SPA 骨架，Chart.js CDN
        ├── admin.css           # 深色主题样式系统
        └── admin.js            # 7 页面完整交互逻辑

日志输出：`./provider-switch-log/proxy.log`（volume 挂载，宿主机路径由 docker-compose.yml 中 volumes 配置决定）
```

---

## 参考

- [MoyuFamily/ai-relay](https://github.com/MoyuFamily/ai-relay) — Serverless AI API 网关，本项目的管理面板 UI 设计参考了其功能分区思路
