/**
 * LLM Failover Proxy — 管理面板前端
 *
 * 7 页面 SPA，依赖 Chart.js (CDN)。
 * 所有 API 调用走 fetchAPI() 自动附加 admin auth header。
 */

// ═══════════════════════════════════════════════════════════════════════════
// 全局状态
// ═══════════════════════════════════════════════════════════════════════════

let ADMIN_KEY = sessionStorage.getItem("admin_key") || "";
let currentPage = "dashboard";
let dragSrcIndex = -1;
let providerOrder = []; // current ordered names
let sse = null;
let testAbort = null;
let trendChart = null;
let providerChart = null;

// ═══════════════════════════════════════════════════════════════════════════
// 工具函数
// ═══════════════════════════════════════════════════════════════════════════

function getCSRFToken() {
  return "";
}

async function fetchAPI(path, opts = {}) {
  const headers = { ...opts.headers };
  if (ADMIN_KEY) {
    headers["Authorization"] = `Bearer ${ADMIN_KEY}`;
  }
  if (opts.body && typeof opts.body === "object" && !(opts.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(opts.body);
  }
  try {
    const res = await fetch(path, { ...opts, headers });
    if (res.status === 401) {
      ADMIN_KEY = "";
      sessionStorage.removeItem("admin_key");
      promptAuth();
      throw new Error("Unauthorized");
    }
    if (res.headers.get("content-type")?.includes("text/event-stream")) {
      return res; // raw response for stream handling
    }
    return await res.json();
  } catch (e) {
    if (e.message !== "Unauthorized") throw e;
    return null;
  }
}

function promptAuth() {
  if (ADMIN_KEY) return;
  const key = prompt("请输入管理面板密码 (admin_key):");
  if (key) {
    ADMIN_KEY = key;
    sessionStorage.setItem("admin_key", key);
    // 重新加载当前页面
    loadPage(currentPage);
  }
}

function $(id) { return document.getElementById(id); }

function showPage(name) {
  currentPage = name;
  document.querySelectorAll(".page").forEach((p) => p.classList.remove("active"));
  document.querySelectorAll(".nav-item").forEach((n) => n.classList.remove("active"));
  const page = $(`page-${name}`);
  if (page) page.classList.add("active");
  const navItem = document.querySelector(`.nav-item[data-page="${name}"]`);
  if (navItem) navItem.classList.add("active");
  loadPage(name);
}

function loadPage(name) {
  // 不在此校验 ADMIN_KEY——由 fetchAPI 在收到 401 时自动触发 promptAuth
  switch (name) {
    case "dashboard": refreshDashboard(); break;
    case "providers": refreshProviders(); break;
    case "monitor": connectSSE(); break;
    case "stats": refreshStats(); break;
    case "test": populateTestProviders(); break;
    case "security": refreshSecurity(); break;
    case "config": refreshConfig(); break;
  }
}

function fmtTime(ts) {
  if (!ts) return "-";
  const d = new Date(ts * 1000);
  return d.toLocaleString("zh-CN");
}

function fmtMs(ms) {
  if (ms == null) return "-";
  if (ms < 1000) return ms.toFixed(0) + " ms";
  return (ms / 1000).toFixed(2) + " s";
}

// ═══════════════════════════════════════════════════════════════════════════
// 导航
// ═══════════════════════════════════════════════════════════════════════════

document.addEventListener("DOMContentLoaded", () => {
  // 侧边栏导航
  document.querySelectorAll(".nav-item").forEach((item) => {
    item.addEventListener("click", () => {
      const page = item.dataset.page;
      if (page) showPage(page);
    });
  });

  // 认证后置：先尝试加载 API，若后端要求密码，fetchAPI 的 401 处理会自动触发 promptAuth
  refreshDashboard();
});

// ═══════════════════════════════════════════════════════════════════════════
// 仪表盘
// ═══════════════════════════════════════════════════════════════════════════

async function refreshDashboard() {
  try {
    const [data, switchesData] = await Promise.all([
      fetchAPI("/admin/dashboard"),
      fetchAPI("/admin/stats/switches?hours=24&limit=10"),
    ]);
    if (!data) return;

    // 指标卡片
    const s = data.stats_24h || {};
    $("metric-requests").textContent = s.total_requests ?? "-";
    $("metric-switches").textContent = s.total_switches ?? "-";
    $("metric-providers").textContent = data.provider_count ?? "-";
    $("metric-uptime").textContent = data.uptime_hours ? data.uptime_hours.toFixed(1) + " h" : "-";

    // 熔断器状态
    const cbList = $("cb-status-list");
    const states = data.circuit_breaker?.states || {};
    cbList.innerHTML = Object.entries(states)
      .map(([name, st]) => {
        const cls = st.degraded ? "down" : "up";
        const label = st.degraded ? "降级" : "正常";
        return `<div class="cb-item ${cls}">
          <strong>${name}</strong>
          <span>${label}</span>
          <span class="muted">(失败${st.failures}次)</span>
        </div>`;
      })
      .join("");

    // 软触发阈值
    const st = data.soft_trigger || {};
    $("soft-trigger-info").innerHTML = `
      <div class="param-item"><span class="param-key">TTFT 阈值</span><span class="param-val">${fmtMs(st.ttft_threshold_ms)}</span></div>
      <div class="param-item"><span class="param-key">TPOT 阈值</span><span class="param-val">${fmtMs(st.tpot_threshold_ms)}</span></div>
      <div class="param-item"><span class="param-key">吞吐阈值</span><span class="param-val">${st.throughput_threshold_tokens_per_sec ?? "-"} t/s</span></div>
      <div class="param-item"><span class="param-key">吞吐窗口</span><span class="param-val">${st.throughput_window_seconds ?? "-"} s</span></div>
    `;

    // 最近切换事件
    const switches = switchesData?.switches || [];
    const swTable = $("recent-switches");
    if (switches.length === 0) {
      swTable.innerHTML = '<p class="muted">暂无切换事件</p>';
    } else {
      swTable.innerHTML =
        "<table><thead><tr><th>时间</th><th>从</th><th>到</th><th>类型</th><th>原因</th></tr></thead><tbody>" +
        switches
          .map(
            (s) =>
              `<tr><td>${fmtTime(s.created_at)}</td><td>${s.from_provider}</td><td>${s.to_provider || "-"}</td><td>${s.trigger_type}</td><td>${s.reason || ""}</td></tr>`
          )
          .join("") +
        "</tbody></table>";
    }
  } catch (e) {
    console.error("dashboard error:", e);
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Provider 管理
// ═══════════════════════════════════════════════════════════════════════════

async function refreshProviders() {
  try {
    const data = await fetchAPI("/admin/providers");
    if (!data) return;
    providerOrder = data.providers.map((p) => p.name);
    renderProviderCards(data.providers);
  } catch (e) {
    console.error("providers error:", e);
  }
}

function renderProviderCards(providers) {
  const container = $("provider-list");
  container.innerHTML = providers
    .map(
      (p, idx) => `
      <div class="provider-card" draggable="true" data-index="${idx}" data-name="${p.name}">
        <div class="pc-header">
          <span class="pc-name">${p.name}</span>
          <span class="pc-priority">优先级 ${idx}</span>
        </div>
        <div class="pc-details">
          <span>⏱ ${p.timeout}s</span>
          <span>🔗 ${p.api_type}</span>
          <span class="${p.cb_state?.degraded ? 'dot-down' : 'dot-up'}">● ${p.cb_state?.degraded ? '降级' : '正常'}</span>
          <span>失败 ${p.cb_state?.failures || 0} 次</span>
          <span>探测: ${p.probe?.status || 'unknown'}</span>
        </div>
        <div class="pc-key-preview">${p.api_base}</div>
        <div class="pc-actions">
          <button class="btn btn-sm" onclick="resetCB('${p.name}')">恢复熔断器</button>
          <button class="btn btn-sm" onclick="viewKeys('${p.name}')">查看 Key</button>
        </div>
      </div>`
    )
    .join("");

  // 拖拽事件
  document.querySelectorAll(".provider-card").forEach((card) => {
    card.addEventListener("dragstart", onDragStart);
    card.addEventListener("dragover", onDragOver);
    card.addEventListener("dragenter", onDragEnter);
    card.addEventListener("dragleave", onDragLeave);
    card.addEventListener("drop", onDrop);
    card.addEventListener("dragend", onDragEnd);
  });

  $("btn-save-order").disabled = true;
}

function onDragStart(e) {
  dragSrcIndex = parseInt(e.target.dataset.index);
  e.target.classList.add("dragging");
  e.dataTransfer.effectAllowed = "move";
}

function onDragOver(e) {
  e.preventDefault();
  e.dataTransfer.dropEffect = "move";
}

function onDragEnter(e) {
  e.preventDefault();
  const card = e.target.closest(".provider-card");
  if (card) card.classList.add("drag-over");
}

function onDragLeave(e) {
  const card = e.target.closest(".provider-card");
  if (card) card.classList.remove("drag-over");
}

function onDrop(e) {
  e.preventDefault();
  const target = e.target.closest(".provider-card");
  if (!target) return;
  const targetIdx = parseInt(target.dataset.index);
  if (dragSrcIndex === targetIdx) return;

  // 重新排序 providerOrder
  const [moved] = providerOrder.splice(dragSrcIndex, 1);
  providerOrder.splice(targetIdx, 0, moved);

  // 本地重新渲染卡片（不开新请求）
  const cards = [...document.querySelectorAll(".provider-card")];
  const providerData = cards.map((c) => ({
    name: c.dataset.name,
    timeout: c.querySelector(".pc-details span:first-child")?.textContent?.replace("⏱ ", "").replace("s", "") || "?",
    api_type: c.querySelector(".pc-details span:nth-child(2)")?.textContent?.replace("🔗 ", "") || "?",
    cb_state: { degraded: c.querySelector(".dot-down") !== null, failures: 0 },
    probe: { status: c.querySelector(".pc-details span:nth-child(5)")?.textContent?.replace("探测: ", "") || "unknown" },
    api_base: c.querySelector(".pc-key-preview")?.textContent || "",
  }));
  const reordered = providerOrder.map((name) => providerData.find((p) => p.name === name)).filter(Boolean);
  renderProviderCards(reordered);
  // 标记需要保存
  $("btn-save-order").disabled = false;
}

function onDragEnd(e) {
  e.target.classList.remove("dragging");
  document.querySelectorAll(".provider-card").forEach((c) => c.classList.remove("drag-over"));
}

async function saveProviderOrder() {
  try {
    const res = await fetchAPI("/admin/providers/reorder", {
      method: "PUT",
      body: { order: providerOrder },
    });
    if (res && res.ok) {
      $("btn-save-order").disabled = true;
      refreshProviders();
    }
  } catch (e) {
    console.error("save order error:", e);
  }
}

async function resetCB(name) {
  try {
    await fetchAPI(`/admin/providers/${encodeURIComponent(name)}/reset-cb`, { method: "POST" });
    refreshProviders();
  } catch (e) {
    console.error("reset CB error:", e);
  }
}

async function viewKeys(name) {
  try {
    const data = await fetchAPI(`/admin/providers/${encodeURIComponent(name)}/keys`);
    if (data) {
      alert(
        `Provider: ${data.provider}\nKey 数量: ${data.count}\n\n${data.keys.join("\n")}`
      );
    }
  } catch (e) {
    console.error("view keys error:", e);
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// 实时监控 (SSE)
// ═══════════════════════════════════════════════════════════════════════════

function connectSSE() {
  if (sse) {
    sse.close();
    sse = null;
  }

  const statusEl = $("sse-status");
  statusEl.textContent = "连接中...";
  statusEl.className = "status-badge warn";

  const url = ADMIN_KEY
    ? `/admin/events/stream?token=${encodeURIComponent(ADMIN_KEY)}`
    : "/admin/events/stream";
  sse = new EventSource(url);

  sse.onopen = async () => {
    statusEl.textContent = "已连接";
    statusEl.className = "status-badge ok";
    $("event-log").innerHTML = '<p class="muted">等待事件...</p>';
    // 拉取初始状态填充 Provider 面板（避免空面板）
    try {
      const dash = await fetchAPI("/admin/dashboard");
      if (dash && dash.providers) {
        const initProviders = dash.providers.map((p) => ({
          name: p.name,
          degraded: p.cb_state?.degraded || false,
          failures: p.cb_state?.failures || 0,
          switches: p.cb_state?.switches || 0,
          latency_ms: p.probe?.latency_ms ?? null,
        }));
        renderMonitorProviders(initProviders);
      }
    } catch (e) {
      // 静默失败，等后续 SSE 事件自然填充
    }
  };

  sse.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      handleSSEEvent(data);
    } catch (e) {
      // ignore
    }
  };

  sse.onerror = () => {
    statusEl.textContent = "未连接";
    statusEl.className = "status-badge fail";
    sse.close();
    sse = null;
  };
}

function handleSSEEvent(data) {
  // 追加到事件日志
  const log = $("event-log");
  const time = new Date().toLocaleTimeString("zh-CN");

  const typeMap = {
    switch: "log-switch",
    cb_change: "log-cb",
    error: "log-error",
    recovery: "log-ok",
  };
  const cls = typeMap[data.type] || "";

  const entry = document.createElement("div");
  entry.className = `log-entry ${cls}`;
  entry.innerHTML = `<span class="log-time">[${time}]</span> [${data.type}] ${JSON.stringify(data)}`;
  log.appendChild(entry);

  // 自动滚动
  log.scrollTop = log.scrollHeight;

  // 更新 provider 状态
  if (data.type === "provider_status" && data.providers) {
    renderMonitorProviders(data.providers);
  }
}

function renderMonitorProviders(providers) {
  const container = $("monitor-providers");
  container.innerHTML = providers
    .map(
      (p) => `
      <div class="provider-card" style="cursor:default">
        <div class="pc-header">
          <span class="pc-name">${p.name}</span>
          <span class="${p.degraded ? 'dot-down' : 'dot-up'}">● ${p.degraded ? '降级' : '正常'}</span>
        </div>
        <div class="pc-details">
          <span>延迟: ${p.latency_ms != null ? p.latency_ms + 'ms' : '-'}</span>
          <span>失败: ${p.failures || 0}</span>
          <span>切换: ${p.switches || 0}</span>
        </div>
      </div>`
    )
    .join("");
}

// ═══════════════════════════════════════════════════════════════════════════
// 用量统计
// ═══════════════════════════════════════════════════════════════════════════

async function refreshStats() {
  const hours = parseInt($("stats-range")?.value || "168");

  try {
    // 并行获取数据
    const [summary, byProvider, switches, trend] = await Promise.all([
      fetchAPI(`/admin/stats/summary?hours=${hours}`),
      fetchAPI(`/admin/stats/by-provider?hours=${hours}`),
      fetchAPI(`/admin/stats/switches?hours=${hours}&limit=50`),
      fetchAPI(`/admin/stats/trend?hours=${hours}`),
    ]);

    // 指标卡片
    $("stat-requests").textContent = summary?.total_requests ?? "-";
    const totalTokens = (summary?.total_prompt_tokens || 0) + (summary?.total_completion_tokens || 0);
    $("stat-tokens").textContent = totalTokens ? totalTokens.toLocaleString() : "-";
    $("stat-avg-ms").textContent = summary?.avg_duration_ms ? fmtMs(summary.avg_duration_ms) : "-";
    $("stat-switches").textContent = summary?.total_switches ?? "-";

    // Provider 详情表
    renderProviderDetailTable(byProvider?.providers || []);
    // 切换事件表
    renderSwitchEventTable(switches?.switches || []);
    // 图表
    renderCharts(trend?.buckets || [], byProvider?.providers || []);
  } catch (e) {
    console.error("stats error:", e);
  }
}

function renderProviderDetailTable(providers) {
  const container = $("provider-detail-table");
  if (providers.length === 0) {
    container.innerHTML = '<p class="muted">暂无数据</p>';
    return;
  }
  container.innerHTML = `
    <table>
      <thead><tr><th>Provider</th><th>请求数</th><th>Prompt Tokens</th><th>Completion Tokens</th><th>平均耗时</th><th>切换次数</th></tr></thead>
      <tbody>
        ${providers
          .map(
            (p) =>
              `<tr>
                <td><strong>${p.provider}</strong></td>
                <td>${p.requests}</td>
                <td>${(p.prompt_tokens || 0).toLocaleString()}</td>
                <td>${(p.completion_tokens || 0).toLocaleString()}</td>
                <td>${fmtMs(p.avg_duration_ms)}</td>
                <td>${p.switches}</td>
              </tr>`
          )
          .join("")}
      </tbody>
    </table>`;
}

function renderSwitchEventTable(switches) {
  const container = $("switch-event-table");
  if (switches.length === 0) {
    container.innerHTML = '<p class="muted">暂无切换事件</p>';
    return;
  }
  container.innerHTML = `
    <table>
      <thead><tr><th>时间</th><th>从</th><th>到</th><th>触发类型</th><th>原因</th></tr></thead>
      <tbody>
        ${switches
          .map(
            (s) =>
              `<tr>
                <td>${fmtTime(s.created_at)}</td>
                <td>${s.from_provider}</td>
                <td>${s.to_provider || "-"}</td>
                <td>${s.trigger_type}</td>
                <td>${s.reason || ""}</td>
              </tr>`
          )
          .join("")}
      </tbody>
    </table>`;
}

function fmtDateLabel(ts) {
  if (!ts) return "-";
  const d = new Date(ts * 1000);
  // Show compact date+hour for single-day, date-only for multi-day
  return d.toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit" });
}

function renderCharts(buckets, providers) {
  // 趋势图
  const trendCtx = $("trend-chart")?.getContext("2d");
  if (trendCtx) {
    if (trendChart) trendChart.destroy();

    // 当数据点少于 3 个时，使用柱状图而非折线图，避免填充块拉伸
    const isSparse = buckets.length < 3;
    const labels = buckets.map((b) => fmtDateLabel(b.timestamp));
    const requests = buckets.map((b) => b.requests);
    const switches = buckets.map((b) => b.switches);

    trendChart = new Chart(trendCtx, {
      type: isSparse ? "bar" : "line",
      data: {
        labels,
        datasets: [
          {
            label: "请求数",
            data: requests,
            borderColor: "#3b82f6",
            backgroundColor: isSparse ? "rgba(59,130,246,0.6)" : "rgba(59,130,246,0.1)",
            fill: !isSparse,
            tension: 0.3,
            pointRadius: isSparse ? 0 : 2,
            borderWidth: 2,
          },
          {
            label: "切换次数",
            data: switches,
            borderColor: "#eab308",
            backgroundColor: "rgba(234,179,8,0.6)",
            fill: false,
            tension: 0.3,
            pointRadius: isSparse ? 0 : 2,
            borderWidth: 2,
            yAxisID: "y1",
            type: "line",
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        interaction: { intersect: false, mode: "index" },
        plugins: {
          legend: {
            labels: { color: "#8892a4" },
          },
        },
        scales: {
          x: {
            ticks: { color: "#5a6478", maxTicksLimit: 12, maxRotation: 45 },
            grid: { color: "rgba(42,51,80,0.3)" },
          },
          y: {
            beginAtZero: true,
            ticks: { color: "#5a6478" },
            grid: { color: "rgba(42,51,80,0.3)" },
          },
          y1: {
            beginAtZero: true,
            position: "right",
            ticks: { color: "#eab308" },
            grid: { display: false },
          },
        },
      },
    });
  }

  // Provider 分布图
  const provCtx = $("provider-chart")?.getContext("2d");
  if (provCtx) {
    if (providerChart) providerChart.destroy();

    const colors = ["#3b82f6", "#22c55e", "#eab308", "#f97316", "#ef4444", "#a855f7"];
    providerChart = new Chart(provCtx, {
      type: "doughnut",
      data: {
        labels: providers.map((p) => p.provider),
        datasets: [
          {
            data: providers.map((p) => p.requests),
            backgroundColor: colors.slice(0, providers.length),
            borderColor: "#1a1f2e",
            borderWidth: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
          legend: {
            position: "right",
            labels: { color: "#8892a4", padding: 12 },
          },
        },
      },
    });
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// 模型测试
// ═══════════════════════════════════════════════════════════════════════════

async function populateTestProviders() {
  try {
    const data = await fetchAPI("/admin/providers");
    if (!data) return;
    const sel = $("test-provider");
    sel.innerHTML = data.providers
      .map((p) => `<option value="${p.name}">${p.name}</option>`)
      .join("");
  } catch (e) {
    console.error("populate test providers error:", e);
  }
}

async function runTest() {
  const provider = $("test-provider").value;
  const prompt = $("test-prompt").value;
  const stream = $("test-stream").checked;
  const maxTokens = parseInt($("test-max-tokens").value) || 1024;

  if (!prompt) { alert("请输入 prompt"); return; }

  // 重置
  $("test-ttft").textContent = "-";
  $("test-total").textContent = "-";
  $("test-chars").textContent = "-";
  const respBox = $("test-response");
  respBox.innerHTML = "";

  $("btn-stop-test").disabled = false;
  const btn = event?.target;
  if (btn) btn.disabled = true;

  try {
    const headers = { "Content-Type": "application/json" };
    if (ADMIN_KEY) headers["Authorization"] = `Bearer ${ADMIN_KEY}`;

    testAbort = new AbortController();

    const res = await fetch("/admin/test/completion", {
      method: "POST",
      headers,
      body: JSON.stringify({ provider, prompt, stream, max_tokens: maxTokens }),
      signal: testAbort.signal,
    });

    if (!res.ok) {
      const errData = await res.json().catch(() => ({}));
      respBox.innerHTML = `<span class="log-error">错误: ${errData.error || res.statusText}</span>`;
      return;
    }

    if (stream) {
      await handleTestStream(res, respBox);
    } else {
      const data = await res.json();
      respBox.textContent = data.content || "(空响应)";
      $("test-ttft").textContent = fmtMs(data.ttft_ms);
      $("test-total").textContent = fmtMs(data.total_ms);
      $("test-chars").textContent = (data.content?.length || 0).toLocaleString();
    }
  } catch (e) {
    if (e.name !== "AbortError") {
      $("test-response").innerHTML = `<span class="log-error">错误: ${e.message}</span>`;
    }
  } finally {
    testAbort = null;
    $("btn-stop-test").disabled = true;
    if (btn) btn.disabled = false;
  }
}

async function handleTestStream(res, respBox) {
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let contentLen = 0;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || !trimmed.startsWith("data: ")) continue;
      const payload = trimmed.slice(6);
      if (payload === "[DONE]") continue;

      try {
        const data = JSON.parse(payload);

        if (data.type === "meta") {
          $("test-ttft").textContent = fmtMs(data.ttft_ms);
          continue;
        }
        if (data.type === "done") {
          $("test-total").textContent = fmtMs(data.total_ms);
          $("test-chars").textContent = (data.content_len || contentLen).toLocaleString();
          continue;
        }
        if (data.type === "error") {
          respBox.innerHTML += `\n[错误] ${data.error}`;
          continue;
        }

        // Standard SSE chunk
        const choices = data.choices || [];
        for (const c of choices) {
          const delta = c.delta || {};
          if (delta.content) {
            respBox.textContent += delta.content;
            contentLen += delta.content.length;
          }
        }
      } catch (e) {
        // skip unparseable lines
      }
    }
  }
}

function stopTest() {
  if (testAbort) {
    testAbort.abort();
    testAbort = null;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// 安全自检
// ═══════════════════════════════════════════════════════════════════════════

async function refreshSecurity() {
  try {
    const data = await fetchAPI("/admin/security/checks");
    if (!data) return;

    // 整体评级
    const overallEl = $("security-overall");
    overallEl.textContent = data.overall === "pass" ? "通过" : data.overall === "warn" ? "警告" : "失败";
    overallEl.className = `status-badge ${data.overall === "pass" ? "ok" : data.overall === "warn" ? "warn" : "fail"}`;

    // 检查项
    const container = $("security-checks");
    container.innerHTML = (data.checks || [])
      .map(
        (c) => `
        <div class="check-item">
          <div>
            <div class="check-name">${checkNameMap(c.name)}</div>
            <div class="check-detail">${c.detail || ""}</div>
          </div>
          <div class="check-status">
            <span class="status-badge ${c.status === "pass" ? "ok" : c.status === "warn" ? "warn" : c.status === "info" ? "neutral" : "fail"}">
              ${c.status === "pass" ? "通过" : c.status === "warn" ? "警告" : c.status === "info" ? "信息" : "失败"}
            </span>
          </div>
        </div>`
      )
      .join("");
  } catch (e) {
    console.error("security error:", e);
  }
}

function checkNameMap(key) {
  const names = {
    provider_keys: "Provider API Key 检查",
    circuit_breaker: "熔断器配置",
    soft_trigger: "软触发配置",
    https: "HTTPS 连接",
    startup_probe: "启动连通性探测",
    error_sanitizer: "错误信息过滤",
  };
  return names[key] || key;
}

// ═══════════════════════════════════════════════════════════════════════════
// 配置查看
// ═══════════════════════════════════════════════════════════════════════════

async function refreshConfig() {
  try {
    const data = await fetchAPI("/admin/config");
    if (!data) return;

    // Provider 配置卡片
    const container = $("config-providers");
    container.innerHTML = (data.providers || [])
      .map(
        (p) => `
        <div class="provider-card" style="cursor:default">
          <div class="pc-header">
            <span class="pc-name">${p.name}</span>
            <span class="pc-priority">优先级 ${p.priority}</span>
          </div>
          <div class="pc-details">
            <span>⏱ ${p.timeout}s</span>
            <span>🔗 ${p.api_type}</span>
          </div>
          <div class="pc-key-preview">${p.api_base}</div>
          <div class="pc-key-preview">Key: ${p.api_key}</div>
          <div class="pc-key-preview">模型映射: ${JSON.stringify(p.model_map)}</div>
        </div>`
      )
      .join("");

    // 原始 JSON
    $("config-raw").textContent = JSON.stringify(data.config, null, 2);
  } catch (e) {
    console.error("config error:", e);
  }
}
