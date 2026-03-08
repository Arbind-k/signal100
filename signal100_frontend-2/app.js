const STORAGE_KEY = "signal100_api_base_url";
const DEFAULT_API_BASE = "http://127.0.0.1:8000";

const state = {
  stocks: [],
};

const elements = {
  apiBaseUrl: document.getElementById("apiBaseUrl"),
  saveApiBtn: document.getElementById("saveApiBtn"),
  refreshPredictionsBtn: document.getElementById("refreshPredictionsBtn"),
  reloadMetricsBtn: document.getElementById("reloadMetricsBtn"),
  winnersList: document.getElementById("winnersList"),
  losersList: document.getElementById("losersList"),
  stocksTableBody: document.getElementById("stocksTableBody"),
  metricsContent: document.getElementById("metricsContent"),
  statusMessage: document.getElementById("statusMessage"),
  totalStocks: document.getElementById("totalStocks"),
  bestReturn: document.getElementById("bestReturn"),
  worstReturn: document.getElementById("worstReturn"),
  avgConfidence: document.getElementById("avgConfidence"),
  searchInput: document.getElementById("searchInput"),
  sortSelect: document.getElementById("sortSelect"),
};

function getApiBaseUrl() {
  return localStorage.getItem(STORAGE_KEY) || DEFAULT_API_BASE;
}

function setApiBaseUrl(url) {
  localStorage.setItem(STORAGE_KEY, url);
}

function formatCurrency(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(num);
}

function formatPercent(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "—";
  return `${(num * 100).toFixed(2)}%`;
}

function formatDecimal(value, digits = 4) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "—";
  return num.toFixed(digits);
}

function setStatus(message, isError = false) {
  elements.statusMessage.textContent = message;
  elements.statusMessage.className = isError ? "status" : "status muted";
  if (isError) {
    elements.statusMessage.style.color = "#fca5a5";
  } else {
    elements.statusMessage.style.color = "";
  }
}

async function apiFetch(path, options = {}) {
  const baseUrl = getApiBaseUrl().replace(/\/$/, "");
  const response = await fetch(`${baseUrl}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`API ${response.status}: ${text || response.statusText}`);
  }

  return response.json();
}

function createStockCard(stock) {
  const returnClass = Number(stock.predicted_return) >= 0 ? "positive" : "negative";
  return `
    <article class="stock-item">
      <div class="stock-meta">
        <span class="ticker">${stock.ticker}</span>
        <span class="small">Current: ${formatCurrency(stock.current_price)}</span>
        <span class="small">Predicted: ${formatCurrency(stock.predicted_price)}</span>
      </div>
      <div class="return ${returnClass}">${formatPercent(stock.predicted_return)}</div>
    </article>
  `;
}

function renderStocksTable() {
  const query = elements.searchInput.value.trim().toUpperCase();
  const sortValue = elements.sortSelect.value;

  let rows = [...state.stocks];

  if (query) {
    rows = rows.filter((row) => String(row.ticker).toUpperCase().includes(query));
  }

  rows.sort((a, b) => {
    switch (sortValue) {
      case "predicted_return_asc":
        return Number(a.predicted_return) - Number(b.predicted_return);
      case "ticker_asc":
        return String(a.ticker).localeCompare(String(b.ticker));
      case "ticker_desc":
        return String(b.ticker).localeCompare(String(a.ticker));
      case "confidence_desc":
        return Number(b.model_confidence) - Number(a.model_confidence);
      case "predicted_return_desc":
      default:
        return Number(b.predicted_return) - Number(a.predicted_return);
    }
  });

  elements.stocksTableBody.innerHTML = rows.map((stock) => `
    <tr>
      <td><strong>${stock.ticker}</strong></td>
      <td>${formatCurrency(stock.current_price)}</td>
      <td>${formatCurrency(stock.predicted_price)}</td>
      <td class="${Number(stock.predicted_return) >= 0 ? "return positive" : "return negative"}">${formatPercent(stock.predicted_return)}</td>
      <td>${formatDecimal(stock.model_mae, 3)}</td>
      <td>${formatDecimal(stock.model_confidence, 3)}</td>
    </tr>
  `).join("");

  setStatus(`Showing ${rows.length} stock predictions.`);
}

function renderSummary() {
  const rows = state.stocks;
  elements.totalStocks.textContent = rows.length ? rows.length : "0";

  if (!rows.length) {
    elements.bestReturn.textContent = "—";
    elements.worstReturn.textContent = "—";
    elements.avgConfidence.textContent = "—";
    return;
  }

  const returns = rows.map((x) => Number(x.predicted_return)).filter(Number.isFinite);
  const confidences = rows.map((x) => Number(x.model_confidence)).filter(Number.isFinite);

  const best = Math.max(...returns);
  const worst = Math.min(...returns);
  const avgConfidence = confidences.reduce((a, b) => a + b, 0) / confidences.length;

  elements.bestReturn.textContent = formatPercent(best);
  elements.worstReturn.textContent = formatPercent(worst);
  elements.avgConfidence.textContent = formatDecimal(avgConfidence, 3);
}

function renderMetrics(metrics) {
  const entries = [
    ["MAE", formatDecimal(metrics.mae, 4)],
    ["Baseline MAE", formatDecimal(metrics.baseline_mae, 4)],
    ["Improvement vs Baseline", `${formatDecimal(metrics.improvement_pct, 2)}%`],
    ["Training Rows", metrics.num_training_rows ?? "—"],
    ["Test Rows", metrics.num_test_rows ?? "—"],
    ["Tickers", metrics.num_tickers ?? "—"],
    ["Feature Count", metrics.feature_count ?? "—"],
  ];

  const importances = metrics.top_feature_importances || {};

  elements.metricsContent.innerHTML = `
    ${entries.map(([label, value]) => `
      <div class="metric-row">
        <span class="muted">${label}</span>
        <strong>${value}</strong>
      </div>
    `).join("")}
    <div>
      <h3>Top Feature Importances</h3>
      ${Object.entries(importances).map(([key, value]) => `
        <div class="metric-row">
          <span class="muted">${key}</span>
          <strong>${formatDecimal(value, 4)}</strong>
        </div>
      `).join("") || '<p class="muted">No feature importance data available.</p>'}
    </div>
  `;
}

async function loadStocks() {
  setStatus("Loading stock predictions...");
  const [stocks, winners, losers] = await Promise.all([
    apiFetch("/stocks"),
    apiFetch("/winners"),
    apiFetch("/losers"),
  ]);

  state.stocks = stocks;
  elements.winnersList.innerHTML = winners.map(createStockCard).join("");
  elements.losersList.innerHTML = losers.map(createStockCard).join("");
  renderStocksTable();
  renderSummary();
}

async function loadMetrics() {
  const metrics = await apiFetch("/model-metrics");
  renderMetrics(metrics);
}

async function loadAll() {
  try {
    await Promise.all([loadStocks(), loadMetrics()]);
  } catch (error) {
    console.error(error);
    setStatus(`Could not load backend data. ${error.message}`, true);
    elements.metricsContent.innerHTML = `<p class="muted">Could not load metrics. Check your API URL and make sure the FastAPI server is running.</p>`;
  }
}

async function refreshPredictions() {
  try {
    elements.refreshPredictionsBtn.disabled = true;
    elements.refreshPredictionsBtn.textContent = "Refreshing...";
    await apiFetch("/refresh", { method: "POST" });
    await loadAll();
  } catch (error) {
    console.error(error);
    setStatus(`Refresh failed. ${error.message}`, true);
  } finally {
    elements.refreshPredictionsBtn.disabled = false;
    elements.refreshPredictionsBtn.textContent = "Refresh Predictions";
  }
}

function bindEvents() {
  elements.saveApiBtn.addEventListener("click", () => {
    const url = elements.apiBaseUrl.value.trim() || DEFAULT_API_BASE;
    setApiBaseUrl(url);
    loadAll();
  });

  elements.reloadMetricsBtn.addEventListener("click", loadMetrics);
  elements.refreshPredictionsBtn.addEventListener("click", refreshPredictions);
  elements.searchInput.addEventListener("input", renderStocksTable);
  elements.sortSelect.addEventListener("change", renderStocksTable);
}

function init() {
  elements.apiBaseUrl.value = getApiBaseUrl();
  bindEvents();
  loadAll();
}

init();
