import { html, render } from "lit-html";
import { unsafeHTML } from "lit-html/directives/unsafe-html.js";
import { Marked } from "marked";
import hljs from "highlight.js";
import { formatBytes, truncate } from "./utils.js";

const marked = new Marked({
  renderer: {
    code(code, lang) {
      const language = hljs.getLanguage(lang) ? lang : "plaintext";
      return `<pre class="hljs language-${language}"><code>${hljs.highlight(code ?? "", { language }).value.trim()}</code></pre>`;
    }
  }
});

const loading = html`<div class="spinner-border text-primary" role="status"><span class="visually-hidden">Loading...</span></div>`;

function sanitizeDisplayText(value = "") {
  return String(value)
    .replace(/\bolli_household_id\b/gi, "audience_household_id")
    .replace(/\bneo_order_id\b/gi, "booking_order_id")
    .replace(/\bstreamx_delivery_pct\b/gi, "delivery_pct")
    .replace(/\bOlli Household ID\b/gi, "Audience Household ID")
    .replace(/\bNeo Order ID\b/gi, "Booking Order ID")
    .replace(/\bStreamx Delivery %\b/gi, "Delivery %")
    .replace(/\bDemo\s*Direct\b/gi, "Booking")
    .replace(/\bOlli\b/gi, "Audience Identity")
    .replace(/\bNEO\b/gi, "Planning")
    .replace(/\bNeo\b/gi, "Planning")
    .replace(/\bStreamX\b/gi, "Delivery");
}

function formatPrettyLabel(value = "") {
  return sanitizeDisplayText(String(value)
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b[a-z]/g, (char) => char.toUpperCase())
    .replace(/\bId\b/g, "ID")
    .replace(/\bUsd\b/g, "USD")
    .replace(/\bPct\b/g, "%")
    .replace(/\bCpm\b/g, "CPM")
    .replace(/\bRoi\b/g, "ROI")
    .replace(/\bScte35\b/gi, "SCTE-35"));
}

function formatPrettyScalar(value) {
  if (typeof value === "number") {
    return Number.isInteger(value)
      ? value.toLocaleString()
      : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  if (typeof value === "boolean") return value ? "Yes" : "No";
  return String(value ?? "");
}

function isPlainRecord(value) {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function tryParseStructuredContent(content = "") {
  if (typeof content !== "string") return null;
  const trimmed = content.trim();
  if (!trimmed || (!trimmed.startsWith("{") && !trimmed.startsWith("["))) return null;
  try {
    return JSON.parse(trimmed);
  } catch {
    return null;
  }
}

function getDetailCardTitle(item, index) {
  if (!isPlainRecord(item)) return `Item ${index + 1}`;
  return item.name
    || item.label
    || item.network
    || item.surface
    || item.topic
    || item.daypart
    || item.policy
    || item.country_code
    || `Item ${index + 1}`;
}

function getDetailCardBody(item, title) {
  if (!isPlainRecord(item)) return item;
  const next = { ...item };
  if (next.name === title) delete next.name;
  if (next.label === title) delete next.label;
  return next;
}

function renderPrettyValue(value) {
  if (value === null || value === undefined || value === "") {
    return html`<span class="detail-null">Not available</span>`;
  }

  if (typeof value === "string") {
    const text = sanitizeDisplayText(value).trim();
    return (text.includes("\n") || text.length > 96)
      ? html`<div class="detail-text-panel">${text}</div>`
      : html`<span>${text}</span>`;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return html`<span>${formatPrettyScalar(value)}</span>`;
  }

  if (Array.isArray(value)) {
    if (!value.length) return html`<span class="detail-null">No items</span>`;

    if (value.every((item) => item === null || item === undefined || typeof item !== "object")) {
      return html`
        <div class="detail-pill-row">
          ${value.map((item) => html`<span class="detail-pill">${formatPrettyScalar(item)}</span>`)}
        </div>
      `;
    }

    return html`
      <div class="detail-card-grid">
        ${value.map((item, index) => {
          const title = getDetailCardTitle(item, index);
          const body = getDetailCardBody(item, title);
          return html`
            <div class="detail-card">
              <div class="detail-card-title">${title}</div>
              ${renderPrettyValue(body)}
            </div>
          `;
        })}
      </div>
    `;
  }

  if (isPlainRecord(value)) {
    const entries = Object.entries(value);
    if (!entries.length) return html`<span class="detail-null">No fields</span>`;

    return html`
      <div class="detail-kv">
        ${entries.map(([key, itemValue]) => html`
          <div class="detail-kv-row">
            <div class="detail-kv-key">${formatPrettyLabel(key)}</div>
            <div class="detail-kv-value">${renderPrettyValue(itemValue)}</div>
          </div>
        `)}
      </div>
    `;
  }

  return html`<pre class="raw-data mb-0">${sanitizeDisplayText(String(value))}</pre>`;
}

function renderPrettySection(title, value) {
  if (value === undefined || value === null) return null;
  return html`
    <div class="detail-section">
      <div class="detail-section-label">${title}</div>
      ${renderPrettyValue(value)}
    </div>
  `;
}

function stripMarkdownToText(text = "") {
  return String(text || "")
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/`([^`]*)`/g, "$1")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/^#{1,6}\s*/gm, "")
    .replace(/^\s*[-*+]\s+/gm, "")
    .replace(/^\s*\d+\.\s+/gm, "")
    .replace(/[>*_~|]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function truncateCardText(text = "", max = 120) {
  const cleaned = stripMarkdownToText(text);
  if (!cleaned) return "";
  if (cleaned.length <= max) return cleaned;
  const clipped = cleaned.slice(0, max - 3);
  const boundary = clipped.lastIndexOf(" ");
  const safe = boundary > max * 0.55 ? clipped.slice(0, boundary) : clipped;
  return `${safe.trimEnd()}...`;
}

function splitSummaryFragments(text = "") {
  const cleaned = stripMarkdownToText(text);
  if (!cleaned) return [];
  return cleaned
    .split(/(?<=[.!?])\s+|(?:\s*[;|]\s*)/g)
    .map((part) => part.trim())
    .filter((part) => part.length >= 18);
}

function summaryDedupKey(text = "") {
  return stripMarkdownToText(text)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function buildStagePreviewText(agent) {
  if (!agent?.text) return "";
  const raw = String(agent.text).trim();
  const source = agent.status === "running" ? raw.slice(-520) : raw;
  return truncateCardText(source, agent.status === "running" ? 320 : 260);
}

function buildStageTakeaways(agent, previewText = "") {
  const candidates = [
    ...(agent?.summaryLines || []),
    agent?.summary || "",
    ...splitSummaryFragments(previewText),
    ...splitSummaryFragments(agent?.text || "")
  ];
  const seen = new Set();
  const takeaways = [];
  candidates.forEach((candidate) => {
    const compact = truncateCardText(candidate, 118);
    const key = summaryDedupKey(compact);
    if (!compact || compact.length < 16 || !key || seen.has(key)) return;
    seen.add(key);
    takeaways.push(compact);
  });
  if (!takeaways.length && agent?.task) {
    takeaways.push(truncateCardText(agent.task, 118));
  }
  return takeaways.slice(0, 3);
}

const ARCHITECT_CARD_META = [
  { label: "Scenario A", shortLabel: "A", title: "Scenario in Progress" },
  { label: "Scenario B", shortLabel: "B", title: "Scenario in Progress" },
  { label: "Scenario C", shortLabel: "C", title: "Scenario in Progress" }
];

function getArchitectScenarioMeta(option = {}, index = 0) {
  const fallback = ARCHITECT_CARD_META[index] || { label: `Scenario ${index + 1}`, shortLabel: String(index + 1), title: option?.title || "Scenario" };
  const title = String(option?.title || fallback.title || "").replace(/^Scenario\s+[A-Z]\s*-\s*/i, "").trim();
  return {
    ...fallback,
    title: title || fallback.title
  };
}

function hasFallbackNarrative(text = "") {
  return /llm fallback activated/i.test(String(text || ""));
}

function detectStageRole(agent = {}) {
  const text = `${agent?.nodeId || ""} ${agent?.name || ""}`.toLowerCase();
  if (text.includes("planning") || text.includes("identity") || text.includes("audience")) return "planning";
  if (text.includes("inventory") || text.includes("yield")) return "inventory";
  if (text.includes("booking") || text.includes("proposal")) return "booking";
  if (text.includes("trafficking") || text.includes("signal")) return "trafficking";
  if (text.includes("inflight") || text.includes("operation")) return "inflight";
  if (text.includes("measurement") || text.includes("attribution")) return "measurement";
  if (text.includes("compliance")) return "compliance";
  return "generic";
}

function buildAgentSummaryMetrics(agent = {}, state = {}) {
  const role = detectStageRole(agent);
  const audience = agent?.stateUpdate?.audience_summary || {};
  const inventory = agent?.stateUpdate?.inventory_summary || {};
  const booking = agent?.stateUpdate?.booking_summary || {};
  const trafficking = agent?.stateUpdate?.trafficking_summary || {};
  const inflight = agent?.stateUpdate?.inflight_summary || {};
  const measurement = agent?.stateUpdate?.measurement_summary || {};
  const reach = state?.dashboard?.reach || {};
  const lineItems = booking.line_items || [];

  if (role === "planning") {
    return [
      { label: "Users Identified", value: audience.matched_profiles ? Number(audience.matched_profiles).toLocaleString() : "Pending" },
      { label: "Unique Users", value: audience.unique_households ? Number(audience.unique_households).toLocaleString() : "Pending" },
      { label: "Lead Cohort", value: audience.top_segments?.[0]?.label || "Pending" },
      { label: "Overlap", value: audience.overlap_pct != null ? `${audience.overlap_pct}%` : "Pending" }
    ];
  }

  if (role === "inventory") {
    return [
      { label: "Inventory Available", value: inventory.capacity_impressions ? `${Number(inventory.capacity_impressions).toLocaleString()} imp` : "Pending" },
      { label: "Capacity Value", value: inventory.capacity_spend_usd ? `$${Number(inventory.capacity_spend_usd).toLocaleString()}` : "Pending" },
      { label: "Streaming CPM", value: inventory.premium_streaming_cpm ? `$${inventory.premium_streaming_cpm}` : "Pending" },
      { label: "Linear CPM", value: inventory.blended_linear_cpm ? `$${inventory.blended_linear_cpm}` : "Pending" }
    ];
  }

  if (role === "booking") {
    const streamingCount = lineItems.filter((item) => item.platform_type === "digital").length;
    const linearCount = lineItems.filter((item) => item.platform_type === "linear").length;
    return [
      { label: "Slots Chosen", value: lineItems.length ? `${lineItems.length} items` : "Pending" },
      { label: "Guaranteed", value: booking.guarantee_impressions ? `${Number(booking.guarantee_impressions).toLocaleString()} imp` : "Pending" },
      { label: "Channel Mix", value: lineItems.length ? `${streamingCount} streaming / ${linearCount} linear` : "Pending" },
      { label: "Compliance", value: booking.compliance_status || "Pending" }
    ];
  }

  if (role === "trafficking") {
    return [
      { label: "Readiness", value: trafficking.readiness_score != null ? `${trafficking.readiness_score}/100` : "Pending" },
      { label: "Signal Risks", value: trafficking.signal_risks?.length != null ? `${trafficking.signal_risks.length}` : "Pending" },
      { label: "Assets Checked", value: trafficking.asset_checks?.length ? `${trafficking.asset_checks.length} checks` : "Pending" },
      { label: "Delivery Setup", value: trafficking.signal_risks?.[0]?.network || "Ready" }
    ];
  }

  if (role === "inflight") {
    const allocation = inflight.final_allocation || {};
    return [
      { label: "Streaming Delivery", value: inflight.digital_delivery_rate_pct != null ? `${inflight.digital_delivery_rate_pct}%` : "Pending" },
      { label: "Linear Delivery", value: inflight.linear_delivery_rate_pct != null ? `${inflight.linear_delivery_rate_pct}%` : "Pending" },
      { label: "Make-Good", value: inflight.shift_budget_usd ? `$${Number(inflight.shift_budget_usd).toLocaleString()} shifted` : "Not needed" },
      { label: "Final Mix", value: allocation.streamingPct != null ? `${allocation.streamingPct}/${allocation.linearPct}/${allocation.reservePct}` : "Pending" }
    ];
  }

  if (role === "measurement") {
    return [
      { label: "Households Reached", value: measurement.matched_households ? Number(measurement.matched_households).toLocaleString() : "Pending" },
      { label: "Hit Rate", value: measurement.clean_room_match_rate_pct != null ? `${measurement.clean_room_match_rate_pct}%` : "Pending" },
      { label: "Cross-Platform Reach", value: reach.overlapPct != null ? `${reach.overlapPct}% overlap` : "Pending" }
    ];
  }

  if (role === "compliance") {
    return [
      { label: "Status", value: state?.complianceDetails?.status || agent?.status || "Pending" },
      { label: "Findings", value: state?.complianceDetails?.findings?.length != null ? `${state.complianceDetails.findings.length}` : "Pending" }
    ];
  }

  return [];
}

function joinReadableList(items = []) {
  const values = items.filter(Boolean);
  if (!values.length) return "";
  if (values.length === 1) return values[0];
  if (values.length === 2) return `${values[0]} and ${values[1]}`;
  return `${values.slice(0, -1).join(", ")}, and ${values[values.length - 1]}`;
}

function buildStageDataSourceSummary(agent = {}) {
  const keys = Object.keys(agent?.inputData || {}).slice(0, 4);
  if (!keys.length) return "the shared campaign state";
  return joinReadableList(keys.map((key) => formatPrettyLabel(key).toLowerCase()));
}

function buildStageProcessSummary(agent = {}) {
  const role = detectStageRole(agent);
  const dataSources = buildStageDataSourceSummary(agent);

  if (role === "planning") {
    return `This stage worked through ${dataSources} to score viewer profiles, suppress duplicate households, and lock the audience definition for the rest of the run.`;
  }
  if (role === "inventory") {
    return `This stage worked through ${dataSources} to test capacity, price the inventory path, and decide whether the selected scenario could be fulfilled with sensible timing and cost.`;
  }
  if (role === "booking") {
    return `This stage worked through ${dataSources} to turn the scenario into bookable line items, guarantees, and a compliant proposal structure.`;
  }
  if (role === "trafficking") {
    return `This stage worked through ${dataSources} to validate launch readiness, asset quality, and routing risk before activation.`;
  }
  if (role === "inflight") {
    return `This stage worked through ${dataSources} to simulate pacing, evaluate delivery risk, and decide whether an in-flight intervention was needed.`;
  }
  if (role === "measurement") {
    return `This stage worked through ${dataSources} to match reached households, quantify outcome signals, and write the learning package back into the campaign state.`;
  }
  if (role === "compliance") {
    return `This stage worked through ${dataSources} to check policy fit, capture evidence gaps, and keep downstream execution inside compliant guardrails.`;
  }
  return `This stage worked through ${dataSources} to update the shared campaign state and keep the pipeline moving.`;
}

function buildStageMetricNarrative(agent = {}, state = {}) {
  const role = detectStageRole(agent);
  const audience = agent?.stateUpdate?.audience_summary || {};
  const inventory = agent?.stateUpdate?.inventory_summary || {};
  const booking = agent?.stateUpdate?.booking_summary || {};
  const trafficking = agent?.stateUpdate?.trafficking_summary || {};
  const inflight = agent?.stateUpdate?.inflight_summary || {};
  const measurement = agent?.stateUpdate?.measurement_summary || {};
  const reach = state?.dashboard?.reach || {};

  if (role === "planning" && (audience.matched_profiles || audience.unique_households)) {
    return `It narrowed ${Number(audience.matched_profiles || 0).toLocaleString()} matched profiles into ${Number(audience.unique_households || 0).toLocaleString()} unique households and elevated ${audience.top_segments?.[0]?.label || "the top cohort"} as the operating audience.`;
  }
  if (role === "inventory" && (inventory.capacity_impressions || inventory.capacity_spend_usd)) {
    return `It found ${Number(inventory.capacity_impressions || 0).toLocaleString()} fulfillable impressions with premium streaming CPM around $${inventory.premium_streaming_cpm || "0"} and linear CPM around $${inventory.blended_linear_cpm || "0"}.`;
  }
  if (role === "booking" && booking.line_items?.length) {
    return `It assembled ${booking.line_items.length} line items, guaranteed ${Number(booking.guarantee_impressions || 0).toLocaleString()} impressions, and left proposal compliance at ${booking.compliance_status || "pending"}.`;
  }
  if (role === "trafficking" && (trafficking.readiness_score != null || trafficking.asset_checks?.length)) {
    return `It closed the stage at ${trafficking.readiness_score || 0}/100 readiness with ${trafficking.asset_checks?.length || 0} asset checks and ${trafficking.signal_risks?.length || 0} flagged signal risks.`;
  }
  if (role === "inflight" && (inflight.digital_delivery_rate_pct != null || inflight.linear_delivery_rate_pct != null)) {
    return inflight.make_good_triggered
      ? `It projected ${inflight.digital_delivery_rate_pct || 0}% streaming delivery versus ${inflight.linear_delivery_rate_pct || 0}% linear delivery, then triggered a $${Number(inflight.shift_budget_usd || 0).toLocaleString()} make-good to protect pacing.`
      : `It projected ${inflight.digital_delivery_rate_pct || 0}% streaming delivery versus ${inflight.linear_delivery_rate_pct || 0}% linear delivery and kept the original allocation intact because no make-good was required.`;
  }
  if (role === "measurement" && (measurement.matched_households || measurement.clean_room_match_rate_pct != null)) {
    return `It matched ${Number(measurement.matched_households || 0).toLocaleString()} households at a ${measurement.clean_room_match_rate_pct || 0}% hit rate, with ${reach.overlapPct != null ? `${reach.overlapPct}%` : "measured"} cross-platform overlap.`;
  }
  if (role === "compliance" && state?.complianceDetails) {
    return `It completed the compliance pass with status ${state.complianceDetails.status || "pending"} and ${state.complianceDetails.findings?.length || 0} recorded findings.`;
  }
  return "";
}

function buildStageFindingLines(agent = {}, state = {}, simple = false) {
  const previewText = buildStagePreviewText(agent);
  const candidates = [
    buildStageMetricNarrative(agent, state),
    ...(agent?.summaryLines || []),
    ...splitSummaryFragments(agent?.summary || ""),
    ...splitSummaryFragments(previewText)
  ];
  const seen = new Set();
  const lines = [];
  candidates.forEach((candidate) => {
    const compact = truncateCardText(candidate, simple ? 150 : 220);
    const key = summaryDedupKey(compact);
    if (!compact || compact.length < 20 || seen.has(key)) return;
    seen.add(key);
    lines.push(compact);
  });
  return lines.slice(0, simple ? 3 : 4);
}

function buildStageSubAgentSummaryLines(agent = {}, simple = false) {
  const subAgents = agent?.subAgentResults || [];
  if (!subAgents.length) return [];
  const visibleCount = simple ? 2 : 3;
  const lines = subAgents.slice(0, visibleCount).map((item) => {
    const details = (item?.details || [])
      .map((detail) => stripMarkdownToText(detail))
      .filter(Boolean);
    const detailText = truncateCardText(details.slice(0, simple ? 1 : 2).join(" "), simple ? 120 : 180);
    return `${item.name}: ${detailText || "Completed its scoped task and updated the shared stage output."}`;
  });
  if (subAgents.length > visibleCount) {
    lines.push(`Additional sub-agent output is available in the detailed breakdown (${subAgents.length - visibleCount} more recorded).`);
  }
  return lines;
}

function buildStageImpactLines(agent = {}, simple = false) {
  const candidates = [
    agent?.whyMatters || "",
    agent?.handoff ? `Next handoff: ${agent.handoff}` : ""
  ];
  const seen = new Set();
  const lines = [];
  candidates.forEach((candidate) => {
    const compact = truncateCardText(candidate, simple ? 160 : 240);
    const key = summaryDedupKey(compact);
    if (!compact || compact.length < 20 || seen.has(key)) return;
    seen.add(key);
    lines.push(compact);
  });
  return lines;
}

function renderStageSummaryBlock(agent, state, options = {}) {
  const {
    simple = false
  } = options;
  const processSummary = buildStageProcessSummary(agent);
  const findingLines = buildStageFindingLines(agent, state, simple);
  const subAgentLines = buildStageSubAgentSummaryLines(agent, simple);
  const impactLines = buildStageImpactLines(agent, simple);
  const hasContent = processSummary || findingLines.length || subAgentLines.length || impactLines.length;

  if (!hasContent) return null;

  return html`
    <section class="stage-summary-panel stage-summary-output-panel">
      <div class="stage-panel-label">Stage Summary</div>
      <div class="stage-summary-flow">
        ${processSummary ? html`
          <div class="stage-summary-section">
            <div class="stage-summary-section-title">How The Stage Worked</div>
            <p class="stage-summary-copy mb-0">${processSummary}</p>
          </div>
        ` : null}
        ${findingLines.length ? html`
          <div class="stage-summary-section">
            <div class="stage-summary-section-title">What It Found</div>
            <ul class="stage-summary-list mb-0">
              ${findingLines.map((line) => html`<li>${line}</li>`)}
            </ul>
          </div>
        ` : null}
        ${subAgentLines.length ? html`
          <div class="stage-summary-section">
            <details class="stage-output-disclosure stage-summary-disclosure">
              <summary>What The Sub-Agents Did</summary>
              <ul class="stage-summary-list mb-0">
                ${subAgentLines.map((line) => html`<li>${line}</li>`)}
              </ul>
            </details>
          </div>
        ` : null}
        ${impactLines.length ? html`
          <div class="stage-summary-section stage-summary-impact">
            <div class="stage-summary-section-title">Why It Helped</div>
            <ul class="stage-summary-list mb-0">
              ${impactLines.map((line) => html`<li>${line}</li>`)}
            </ul>
          </div>
        ` : null}
      </div>
    </section>
  `;
}

function renderStageMetricBlock(summaryMetrics = [], quickPills = []) {
  if (!summaryMetrics.length && !quickPills.length) return null;
  return html`
    <section class="stage-summary-panel stage-metric-panel">
      <div class="stage-panel-label">Stage Snapshot</div>
      ${summaryMetrics.length ? html`
        <div class="stage-summary-metric-grid">
          ${summaryMetrics.map((item) => html`
            <div class="stage-summary-metric-card">
              <div class="stage-summary-metric-label">${item.label}</div>
              <div class="stage-summary-metric-value">${item.value}</div>
            </div>
          `)}
        </div>
      ` : null}
      ${quickPills.length ? html`
        <div class="detail-pill-row stage-summary-pill-row">
          ${quickPills.map((label) => html`<span class="detail-pill">${label}</span>`)}
        </div>
      ` : null}
    </section>
  `;
}

function subAgentStatusMeta(status = "") {
  const value = (status || "").toLowerCase();
  if (value === "done") return { label: "Done", tone: "success" };
  if (value === "running") return { label: "Running", tone: "primary" };
  if (value === "error") return { label: "Error", tone: "danger" };
  return { label: "Pending", tone: "secondary" };
}

function resolveSelectedSubAgent(items = [], state = {}, contextKey = "") {
  if (!items.length) return { selected: null, selectedId: null };
  const chosenId = contextKey ? state?.subAgentSelections?.[contextKey] : null;
  const selected = items.find((item) => item.id === chosenId)
    || items.find((item) => item.status === "running")
    || items.find((item) => item.status === "error")
    || items[0];
  return { selected, selectedId: selected?.id || null };
}

function renderPrettySubAgents(items = [], state = {}, actions = {}, options = {}) {
  if (!items.length) return html`<span class="detail-null">No sub-agent results recorded.</span>`;
  const {
    contextKey = "subagents",
    sectionLabel = "Select Sub-Agent Output"
  } = options;
  const { selected, selectedId } = resolveSelectedSubAgent(items, state, contextKey);
  const selectedMeta = subAgentStatusMeta(selected?.status);
  return html`
    <div class="subagent-switcher">
      <div class="detail-section-label">${sectionLabel}</div>
      <div class="subagent-tab-row">
        ${items.map((item) => {
          const meta = subAgentStatusMeta(item.status);
          return html`
            <button
              type="button"
              class="subagent-tab ${item.id === selectedId ? "is-active" : ""}"
              @click=${() => actions?.selectSubAgent?.(contextKey, item.id)}
            >
              <span class="subagent-tab-name">${item.name || "Sub-Agent"}</span>
              <span class="subagent-tab-status tone-${meta.tone}">${meta.label}</span>
            </button>
          `;
        })}
      </div>
      ${selected ? html`
        <div class="detail-card subagent-focus-card">
          <div class="d-flex justify-content-between align-items-start gap-2 mb-2">
            <div class="detail-card-title mb-0">${selected.name || "Sub-Agent"}</div>
            <span class="badge text-bg-${selectedMeta.tone}">${selectedMeta.label}</span>
          </div>
          ${(selected.details || []).length
            ? html`<ul class="detail-list mb-0">${(selected.details || []).slice(0, 4).map((detail) => html`<li>${detail}</li>`)}</ul>`
            : selected.summary
              ? html`<div class="detail-text-panel">${selected.summary}</div>`
              : html`<span class="detail-null">No notes recorded.</span>`}
          ${selected.text ? html`
            <div class="detail-section">
              <div class="detail-section-label">Live Output</div>
              <div class="detail-text-panel">${selected.text}</div>
            </div>
          ` : null}
          ${renderPrettySection("Input Data", selected.inputData)}
          ${renderPrettySection("Output Data", selected.outputData)}
          ${selected.whyMatters ? html`
            <div class="detail-section">
              <div class="detail-section-label">Why This Matters</div>
              <div class="detail-text-panel">${selected.whyMatters}</div>
            </div>
          ` : null}
        </div>
      ` : null}
    </div>
  `;
}

function renderExpandedDataEntry(entry, wrapperClass = "") {
  const parsed = tryParseStructuredContent(entry?.content || "");
  const classes = wrapperClass ? ` ${wrapperClass}` : "";
  if (parsed && typeof parsed === "object") {
    return html`
      <div class=${`detail-expanded${classes}`}>
        ${entry?.title ? html`<div class="detail-section-label mb-2">${entry.title}</div>` : null}
        ${renderPrettyValue(parsed)}
      </div>
    `;
  }

  return html`
    <div class=${`detail-expanded${classes}`}>
      ${entry?.title ? html`<div class="detail-section-label mb-2">${entry.title}</div>` : null}
      <pre class="raw-data mb-0">${entry?.content || ""}</pre>
    </div>
  `;
}

function hasStructuredAgentDetail(agent) {
  const hasInputData = !!(agent?.inputData && Object.keys(agent.inputData).length);
  const hasStateUpdate = !!(agent?.stateUpdate && Object.keys(agent.stateUpdate).length);
  const hasSubAgents = !!(agent?.subAgentResults && agent.subAgentResults.length);
  const hasHandoff = !!(agent?.handoff && String(agent.handoff).trim());
  return hasInputData || hasStateUpdate || hasSubAgents || hasHandoff;
}

function renderAgentStructuredDetailsContent(agent, state = {}, actions = {}, options = {}) {
  if (!hasStructuredAgentDetail(agent)) return null;
  const {
    includeSubAgents = true,
    includeInputData = true,
    includeStateUpdate = true,
    includeHandoff = true,
    subAgentContextKey = agent?.nodeId ? `stage-${agent.nodeId}` : "stage-subagents",
    subAgentSectionLabel = "Select Sub-Agent Output"
  } = options;
  return html`
    ${includeInputData ? renderPrettySection("Input Data", agent.inputData) : null}
    ${includeSubAgents && agent.subAgentResults?.length ? html`
      <div class="detail-section">
        ${renderPrettySubAgents(agent.subAgentResults, state, actions, {
          contextKey: subAgentContextKey,
          sectionLabel: subAgentSectionLabel
        })}
      </div>
    ` : null}
    ${includeStateUpdate ? renderPrettySection("Campaign_State_Object Update", agent.stateUpdate) : null}
    ${includeHandoff ? renderPrettySection("Handoff", agent.handoff) : null}
  `;
}

function renderAgentStructuredDetails(agent, state = {}, actions = {}, wrapperClass = "", options = {}) {
  if (!hasStructuredAgentDetail(agent)) return null;
  const classes = wrapperClass ? ` ${wrapperClass}` : "";
  return html`
    <details class=${`detail-disclosure${classes}`}>
      <summary>Detailed execution</summary>
      ${renderAgentStructuredDetailsContent(agent, state, actions, options)}
    </details>
  `;
}

export function renderDemoCards(container, demos, savedAgents, state, actions) {
  const busy = ["architect", "run"].includes(state.stage);
  const selectedIndex = state.selectedDemoIndex;
  render(html`
    <div class="col-12 mb-3 d-flex justify-content-between align-items-start flex-wrap gap-2">
      <div>
        <h4 class="mb-1 text-body-secondary">Templates</h4>
        <p class="mb-0 small text-body-secondary">Use the custom scenario box above, or start from one of these sample prompts.</p>
      </div>
    </div>
    ${(demos || []).map((demo, index) => html`
      <div class="col-sm-6 col-lg-4">
        <div class="card demo-card h-100 shadow-sm">
          <div class="card-body d-flex flex-column">
            <div class="text-center text-primary display-5 mb-3"><i class="${demo.icon}"></i></div>
            <h5 class="card-title">${demo.title}</h5>
            <p class="card-text text-body-secondary small flex-grow-1">${demo.body}</p>
            <button class="btn btn-primary mt-auto" @click=${() => actions.planDemo(index)} ?disabled=${busy}>
              ${busy && selectedIndex === index ? "Streaming..." : (demo.cta || "Plan and Build")}
            </button>
          </div>
        </div>
      </div>
    `)}
    ${renderSavedAgents(savedAgents, state, actions)}
  `, container);
}

export function renderAuth(container, state, actions) {
  render(html`
    ${!state.supabaseConfigured
      ? html`<button class="btn btn-sm btn-outline-warning text-white" @click=${actions.configureSupabase}><i class="bi bi-gear"></i> Connect Supabase</button>`
      : !state.session
        ? html`<button class="btn btn-sm btn-light" @click=${actions.login}>Sign In</button>`
        : html`<div class="dropdown"><button class="btn btn-sm btn-outline-light dropdown-toggle" type="button" data-bs-toggle="dropdown">${state.session.user.email}</button><ul class="dropdown-menu dropdown-menu-end"><li><button class="dropdown-item" @click=${actions.logout}>Sign Out</button></li></ul></div>`}
  `, container);
}

function renderSavedAgents(agents, state, actions) {
  if (!state.session) return null;
  return html`
    <div class="col-12 mt-4 mb-3"><h4 class="mb-0 text-body-secondary">My Agents</h4></div>
    ${(!agents || !agents.length)
      ? html`<div class="col-12 text-center text-body-secondary py-3">No saved agents found.</div>`
      : agents.map((agent) => html`
        <div class="col-sm-6 col-lg-4">
          <div class="card demo-card h-100 shadow-sm border-primary">
            <div class="card-header bg-primary-subtle text-primary border-primary d-flex justify-content-between align-items-center">
              <small><i class="bi bi-robot"></i> Saved Agent</small>
              <div class="d-flex gap-2">
                <button class="btn btn-sm btn-link text-primary p-0" style="text-decoration:none" @click=${() => actions.editAgent(agent)}><i class="bi bi-pencil-square"></i></button>
                <button class="btn btn-sm btn-link text-danger p-0" style="text-decoration:none" @click=${() => actions.deleteAgent(agent.id)}><i class="bi bi-trash"></i></button>
              </div>
            </div>
            <div class="card-body d-flex flex-column">
              <h5 class="card-title">${agent.title}</h5>
              <p class="card-text text-body-secondary small flex-grow-1 text-truncate" style="max-height: 4.5em; overflow: hidden;">${agent.problem}</p>
              <button class="btn btn-primary mt-auto" @click=${() => actions.loadSavedAgent(agent)} ?disabled=${["architect", "run"].includes(state.stage)}>Load Agent</button>
            </div>
          </div>
        </div>
      `)}
  `;
}

export function renderApp(container, state, config, actions) {
  if (state.selectedDemoIndex === null) {
    render(html`<div class="text-center text-body-secondary py-5"><p>Use the custom scenario box above or select a template to generate three architect plans.</p></div>`, container);
    return;
  }
  const demo = state.selectedDemoIndex === -1 ? state.customProblem : (state.selectedDemoIndex === -2 ? state.customProblem : config.demos[state.selectedDemoIndex]);

  render(html`
    ${state.error ? html`<div class="alert alert-danger">${state.error}</div>` : null}
    <section class="card mb-4"><div class="card-body"><h3 class="h4 mb-2">${demo.title}</h3><p class="mb-0 text-body-secondary small">${demo.problem || demo.body || ""}</p></div></section>
    ${renderStageBadges(state)}
    ${renderCampaignPrompt(state, demo, actions)}
    ${renderPlan(state, actions)}
    ${renderDataInputs(state, actions)}
    ${renderFlow(state, actions)}
    ${renderTimeline(state)}
    ${renderComplianceBanner(state)}
    ${renderComplianceDetails(state, actions)}
    ${renderAgentOutputs(state, demo, actions)}
    ${renderDashboard(state, actions)}
  `, container);
}

function renderStageBadges(state) {
  const hasPlan = state.plan.length > 0;
  const hasOptions = (state.architectPlans || []).length > 0;
  const steps = [
    { label: "Orchestration", active: state.stage === "architect", done: hasOptions || hasPlan || ["data", "run", "idle"].includes(state.stage) },
    { label: "Plan Selection", active: state.stage === "plan-select", done: hasPlan },
    { label: "Data Sources", active: state.stage === "data", done: ["run", "idle"].includes(state.stage) && state.agentOutputs.length > 0 },
    { label: "Multi-Agent Flow", active: state.stage === "run", done: state.stage === "idle" && state.agentOutputs.length > 0 }
  ];
  return html`<div class="d-flex gap-2 flex-wrap mb-4">${steps.map((s) => html`<span class="badge text-bg-${s.active ? "primary" : s.done ? "success" : "secondary"}">${s.label}</span>`)}</div>`;
}

function renderCampaignPrompt(state, demo, actions) {
  return html`
    <section class="card mb-4">
      <div class="card-header d-flex justify-content-between align-items-center flex-wrap gap-2">
        <span><i class="bi bi-lightning-charge me-2"></i>Campaign Prompt</span>
        <small class="text-body-secondary">Single sentence to launch the full converged pipeline.</small>
      </div>
      <div class="card-body">
        <textarea class="form-control campaign-input" rows="2" .value=${state.campaignPrompt || demo.problem || ""} @input=${(e) => actions.setCampaignPrompt(e.target.value)}></textarea>
        <div class="d-flex justify-content-between align-items-center mt-2 flex-wrap gap-2">
          <small class="text-body-secondary">Provide total budget and objective. Agents will determine channel allocation by state-level conditions.</small>
          <button class="btn btn-sm btn-outline-warning" @click=${() => actions.setCampaignPrompt(demo.problem || "")}>Reset to Default</button>
        </div>
      </div>
    </section>`;
}

function renderArchitectLoadingCards() {
  return html`
    <div class="row g-3">
      ${ARCHITECT_CARD_META.map((meta) => html`
        <div class="col-12 col-lg-4">
          <article class="architect-scenario-card is-loading">
            <div class="architect-scenario-header">
              <div class="architect-scenario-title-wrap">
                <span class="architect-scenario-label">${meta.label}</span>
                <h5 class="mb-0">${meta.title}</h5>
              </div>
            </div>
            <div class="architect-scenario-body">
              <section class="architect-scenario-summary">
                <div class="architect-section-label">Summary</div>
                <span class="architect-skeleton w-90"></span>
                <span class="architect-skeleton w-100"></span>
                <span class="architect-skeleton w-70"></span>
              </section>
              <section class="architect-scenario-params">
                <div class="architect-section-label">Plan Parameters</div>
                <div class="architect-param-grid">
                  ${Array.from({ length: 4 }, (_, idx) => html`
                    <div class="architect-param-card" data-slot=${idx}>
                      <span class="architect-skeleton w-40"></span>
                      <span class="architect-skeleton w-90"></span>
                      <span class="architect-skeleton w-70"></span>
                    </div>
                  `)}
                </div>
              </section>
              <section class="architect-agent-plan-card">
                <div class="architect-section-label">Agent Plan</div>
                <div class="architect-loading-steps">
                  ${Array.from({ length: 4 }, (_, idx) => html`
                    <div class="architect-loading-step" data-step=${idx}>
                      <span class="architect-step-dot"></span>
                      <div class="architect-loading-copy">
                        <span class="architect-skeleton w-55"></span>
                        <span class="architect-skeleton w-85"></span>
                      </div>
                    </div>
                  `)}
                </div>
              </section>
            </div>
          </article>
        </div>
      `)}
    </div>
  `;
}

function renderArchitectPlanSteps(option = {}) {
  const steps = option.plan || [];
  if (!steps.length) return html`<p class="architect-empty-copy mb-0">Agent steps will populate after scenario generation finishes.</p>`;
  return html`
    <ol class="architect-plan-list mb-0">
      ${steps.map((agent, index) => html`
        <li class="architect-plan-step">
          <div class="architect-plan-step-head">
            <span class="architect-plan-step-index">${index + 1}</span>
            <div class="architect-plan-step-copy">
              <div class="architect-plan-step-name">${agent.agentName}</div>
              <p class="architect-plan-step-summary mb-0">${truncateCardText(agent.initialTask || "", 120)}</p>
            </div>
          </div>
          <details class="architect-prompt-disclosure">
            <summary>Prompt details</summary>
            <div class="architect-prompt-detail">
              <div class="architect-prompt-detail-label">Task</div>
              <p class="mb-3">${agent.initialTask || "No task detail provided."}</p>
              <div class="architect-prompt-detail-label">System Prompt</div>
              <p class="mb-0">${agent.systemInstruction || "No system prompt detail provided."}</p>
            </div>
          </details>
        </li>
      `)}
    </ol>
  `;
}

function renderArchitectScenarioCard(option, index, state, actions) {
  const selected = state.selectedArchitectPlanId === option.id;
  const meta = getArchitectScenarioMeta(option, index);
  const statusTone = (option.complianceValidation?.status || "").toLowerCase().includes("adjust") ? "warning" : "success";
  return html`
    <div class="col-12 col-lg-4">
      <article class="architect-scenario-card ${selected ? "is-selected" : ""}">
        <div class="architect-scenario-header">
          <div class="architect-scenario-title-wrap">
            <span class="architect-scenario-label">${meta.label}</span>
            <h5 class="mb-0">${meta.title}</h5>
          </div>
          <div class="architect-scenario-statuses">
            ${option.recommended ? html`<span class="badge text-bg-warning text-dark">Recommended</span>` : null}
            <span class="badge text-bg-${selected ? "success" : "secondary"}">${selected ? "Selected" : "Available"}</span>
          </div>
        </div>
        <div class="architect-scenario-body">
          <section class="architect-scenario-summary">
            <div class="architect-section-label">Summary</div>
            <p class="architect-summary-copy">${option.why || option.promptText || "Summary not provided."}</p>
            <p class="architect-summary-support mb-0">${option.promptText || "Scenario objective not provided."}</p>
          </section>

          <section class="architect-scenario-params">
            <div class="architect-section-label">Plan Parameters</div>
            <div class="architect-param-grid">
              <div class="architect-param-card">
                <div class="architect-param-label">Strategy</div>
                <p class="architect-param-copy mb-0">${option.strategy || "Not provided."}</p>
              </div>
              <div class="architect-param-card">
                <div class="architect-param-label">Channel Allocation</div>
                <p class="architect-param-copy mb-0">${option.allocationStrategy || "Not provided."}</p>
              </div>
              <div class="architect-param-card">
                <div class="architect-param-label">Delivery Timing</div>
                <p class="architect-param-copy mb-0">${option.deliveryTiming || "Not provided."}</p>
              </div>
              <div class="architect-param-card">
                <div class="architect-param-label">ROI Logic</div>
                <p class="architect-param-copy mb-0">${option.roiReasoning || "Not provided."}</p>
              </div>
            </div>

            <details class="architect-compliance-disclosure">
              <summary>
                <span>Compliance Check</span>
                <span class="badge text-bg-${statusTone}">${option.complianceValidation?.status || "Passed"}</span>
              </summary>
              <p class="architect-compliance-copy mb-0">${option.complianceValidation?.summary || "Compliance check summary not provided."}</p>
            </details>
          </section>

          <section class="architect-agent-plan-card">
            <div class="architect-section-label">Agent Plan</div>
            ${renderArchitectPlanSteps(option)}
          </section>

          <button class="btn btn-sm btn-primary w-100 mt-auto" @click=${() => actions.chooseArchitectPlan(option.id)} ?disabled=${["architect", "run"].includes(state.stage)}>
            ${selected ? "Current Selection" : "Choose This Scenario"}
          </button>
        </div>
      </article>
    </div>
  `;
}

function renderPlan(state, actions) {
  const streaming = state.stage === "architect";
  const selecting = state.stage === "plan-select";
  const hasOptions = (state.architectPlans || []).length > 0;
  const hasPlan = state.plan.length > 0;
  const architectFallbackNote = /falling back|credentials were not provided/i.test(state.architectBuffer || "")
    ? "Live architect output was unavailable, so the scenario cards are using the deterministic fallback plan."
    : "";
  return html`
    <section class="card mb-4" data-running-key="architect-plan">
      <div class="card-header d-flex justify-content-between align-items-center flex-wrap gap-2">
        <span><i class="bi bi-diagram-3 me-2"></i>Architect Plan Selection</span>
        <div class="d-flex align-items-center gap-2">
          <span class="badge text-bg-${streaming ? "primary" : selecting ? "warning" : hasPlan ? "success" : "secondary"}">${streaming ? "Generating" : selecting ? "Selection Required" : hasPlan ? "Selected" : "Pending"}</span>
          ${(hasPlan && state.session && state.selectedDemoIndex !== -2) ? html`<button class="btn btn-sm btn-outline-success" @click=${actions.saveAgent}><i class="bi bi-save"></i> Save</button>` : null}
        </div>
      </div>
      <div class="card-body">
        ${streaming ? html`
          <div class="architect-loading-note">
            <div class="d-flex align-items-center gap-2">
              <div class="spinner-border spinner-border-sm text-warning" role="status"></div>
              <span>Architect is generating three scenarios and tightening the agent plan.</span>
            </div>
          </div>
          ${renderArchitectLoadingCards()}
        ` : null}
        ${architectFallbackNote ? html`<div class="architect-loading-note">${architectFallbackNote}</div>` : null}
        ${hasOptions ? html`
          <div class="row g-3">
            ${state.architectPlans.map((option, index) => renderArchitectScenarioCard(option, index, state, actions))}
          </div>
        ` : null}
        ${!streaming && !hasOptions && !hasPlan ? html`<div class="text-center py-3 text-body-secondary small">Three architect plans will appear here after generation.</div>` : null}
      </div>
    </section>`;
}

function renderDataInputs(state, actions) {
  const disabled = !state.plan.length || ["architect", "plan-select", "run"].includes(state.stage);
  const selectedPlan = (state.architectPlans || []).find((item) => item.id === state.selectedArchitectPlanId);
  const launchLabel = state.stage === "run"
    ? "Launching..."
    : selectedPlan?.recommended
      ? "Execute Recommended Scenario"
      : "Launch Campaign with Synthetic Data";
  const selectedCount = state.selectedInputs?.size || 0;
  return html`
    <section class="card mb-4" data-running-key=${state.stage === "data" ? "data-inputs" : null}>
      <div class="card-header d-flex justify-content-between align-items-center flex-wrap gap-2">
        <div><i class="bi bi-database me-2"></i>Synthetic Data Sources</div>
        <div class="d-flex align-items-center gap-2 flex-wrap">
          <span class="badge text-bg-secondary">${selectedCount} selected</span>
          <button class="btn btn-sm btn-primary" @click=${actions.startAgents} ?disabled=${disabled}>${launchLabel}</button>
        </div>
      </div>
      <details class="synthetic-data-disclosure">
        <summary class="synthetic-data-summary d-flex justify-content-between align-items-center flex-wrap gap-2">
          <small class="text-body-secondary">Review the synthetic inputs feeding the six-stage workflow before launch.</small>
          <small class="text-body-secondary">Click to expand</small>
        </summary>
        <div class="card-body">
          <div class="mb-3">
            <small class="text-body-secondary">The launch button stays available in the header while this section remains collapsed.</small>
          </div>
          <div class="row g-3">
            <div class="col-lg-7">
              <div class="list-group">${state.suggestedInputs.map((input) => {
                const selected = state.selectedInputs.has(input.id);
                return html`<button type="button" class="list-group-item list-group-item-action d-flex flex-column gap-1 ${selected ? "active text-white" : ""}" @click=${() => actions.toggleSuggestedInput(input.id)}><div class="d-flex justify-content-between align-items-center w-100"><span class="fw-semibold ${selected ? "text-white" : ""}">${input.title}</span><span class="badge text-uppercase bg-secondary">${input.type}</span></div><pre class="mb-0 small ${selected ? "text-white" : "text-body-secondary"}" style="white-space: pre-wrap; word-break: break-word;">${truncate(input.content, 420)}</pre></button>`;
              })}</div>
              ${!state.suggestedInputs.length ? html`<p class="text-body-secondary small">Select an architect plan to load synthetic data sources.</p>` : null}
            </div>
            <div class="col-lg-5">
              <div class="mb-3"><label class="form-label small fw-semibold">Upload CSV, JSON, or text files</label><input class="form-control" type="file" multiple accept=".txt,.csv,.json" @change=${actions.handleFileUpload} /><ul class="list-group list-group-flush mt-2 small">${state.uploads.map((u) => html`<li class="list-group-item d-flex justify-content-between align-items-center"><div><div class="fw-semibold">${u.title}</div><div class="text-body-secondary">${formatBytes(u.meta?.size)} | ${u.type.toUpperCase()}</div></div><button class="btn btn-link btn-sm text-danger" @click=${() => actions.removeUpload(u.id)}>Remove</button></li>`)}</ul>${!state.uploads.length ? html`<p class="small text-body-secondary mt-2 mb-0">Attached files stay in the browser.</p>` : null}</div>
              <div><label class="form-label small fw-semibold">Inline notes</label><textarea class="form-control" rows="4" placeholder="Paste key metrics, performance indicators, or transcript notes..." .value=${state.notes} @input=${(e) => actions.setNotes(e.target.value)}></textarea></div>
            </div>
          </div>
        </div>
      </details>
    </section>`;
}

function renderFlow(state, actions) {
  if (!state.plan.length) return null;
  return html`
    <section class="card mb-4" data-running-key="execution-flow">
      <div class="card-header d-flex justify-content-between align-items-center flex-wrap gap-2">
        <span><i class="bi bi-diagram-3 me-2"></i>Orchestrator and Sub-Agent Flow</span>
        <small class="text-body-secondary">Gold nodes are running, green nodes are finished, slate nodes are queued, and red nodes need attention.</small>
      </div>
      <div class="card-body">
        ${renderFlowStatusBoard(state)}
        <div class="d-flex flex-column gap-3">
          <div id="flowchart-canvas" class="flowchart-canvas border rounded-3 bg-body-tertiary" data-flow-orientation=${state.flowOrientation} data-flow-columns=${state.flowColumns}></div>
          ${renderFlowOutputDisclosure(state, actions)}
        </div>
      </div>
    </section>
  `;
}

function renderTimeline(state) {
  if (!state.agentOutputs.length) return null;
  const entries = state.agentOutputs.map((agent) => {
    const start = agent.startedAt ? new Date(agent.startedAt) : null;
    const end = agent.completedAt ? new Date(agent.completedAt) : null;
    const durationMs = end && start ? end - start : null;
    return { name: agent.name, startLabel: start ? start.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "--:--:--", duration: durationMs ? `${Math.max(1, Math.round(durationMs / 1000))} sec` : "--", status: agent.status };
  });
  return html`<section class="timeline-ribbon card mb-4"><div class="card-body d-flex flex-wrap gap-2 align-items-center"><span class="text-uppercase small text-body-secondary me-2">Agent Timeline</span>${entries.map((entry) => html`<span class="timeline-chip ${entry.status}"><strong>${entry.name}</strong><span>${entry.startLabel}</span><span class="timeline-duration">${entry.duration}</span></span>`)}</div></section>`;
}

function renderComplianceBanner(state) {
  const running = state.agentOutputs.find((a) => isCompliance(a) && a.status === "running");
  if (!running) return null;
  return html`<section class="compliance-banner mb-4"><div class="d-flex align-items-center gap-3"><i class="bi bi-exclamation-triangle-fill"></i><div><div class="fw-semibold">Compliance Validation In Progress</div><div class="small">Compliance checks are running against synthetic policy sources.</div></div></div></section>`;
}

function renderComplianceDetails(state, actions) {
  if (!state.complianceDetails) return null;
  const details = state.complianceDetails;
  const statusValue = (details?.status || "").toLowerCase();
  const statusTone = statusValue.includes("fail")
    ? "danger"
    : statusValue.includes("adjust")
    ? "warning"
    : statusValue.includes("running")
      ? "primary"
      : "success";
  return html`
    <section class="card mb-4">
      <details class="compliance-disclosure">
        <summary class="card-header d-flex justify-content-between align-items-center flex-wrap gap-2">
          <span><i class="bi bi-shield-check me-2"></i>Compliance Sources and Rationale</span>
          <div class="d-flex align-items-center gap-2 flex-wrap">
            <span class="badge text-bg-${statusTone}">${details?.status || "Status Unknown"}</span>
            <span class="compliance-disclosure-hint">Expand</span>
          </div>
        </summary>
        <div class="card-body">
          <p class="text-body-secondary small mb-3">${details?.summary || "Compliance sources populate after run completion."}</p>
          ${details?.findings?.length ? html`<div class="mb-3"><h6 class="mb-2">Findings</h6><ul class="mb-0">${details.findings.map((finding) => html`<li class="small text-body-secondary">${finding}</li>`)}</ul></div>` : null}
          ${details?.alternatives?.length ? html`<div class="mb-3"><h6 class="mb-2">Compliant Alternatives</h6><ul class="mb-0">${details.alternatives.map((alt) => html`<li class="small text-body-secondary">${alt}</li>`)}</ul></div>` : null}
          ${details?.sources?.length ? html`<div class="table-responsive"><table class="table table-dark table-striped align-middle mb-0"><thead><tr><th scope="col">Policy</th><th scope="col">Source</th><th scope="col">Why Applied</th></tr></thead><tbody>${details.sources.map((item) => html`<tr><td>${item.policy}</td><td>${item.source}</td><td>${item.why}</td></tr>`)}</tbody></table></div>` : null}
          <div class="d-flex justify-content-end mt-3">
            <button class="btn btn-sm btn-outline-warning" @click=${actions.toggleComplianceExplanation}>${state.complianceExplanationOpen ? "Hide Detailed Explanation" : "Show Detailed Explanation"}</button>
          </div>
          ${state.complianceExplanationOpen ? html`<div class="explanation-panel mt-3"><p class="mb-2">${details?.detailedExplanation || "The compliance agent records every rule match and why that rule changed routing."}</p><p class="mb-0">This explanation is detailed so operations and legal reviewers can audit each policy decision path.</p></div>` : null}
        </div>
      </details>
    </section>`;
}

function renderFlowNodeStatus(status) {
  if (!status) return null;
  return html`<span class="badge text-bg-${status.c}">${status.l}</span>`;
}

function getFlowNodeOrder(state, nodeId) {
  if (!nodeId) return 9999;
  if (nodeId === "campaign-brief-node") return 0;
  if (nodeId === "orchestrator-agent") return 900;
  if (nodeId === "parallel-compliance-agent") return 350;
  if (nodeId === "campaign-output-node") return 1200;

  const masterIndex = state.plan.findIndex((item) => item.nodeId === nodeId);
  if (masterIndex >= 0) return 100 + (masterIndex * 20);

  for (let i = 0; i < state.plan.length; i += 1) {
    const master = state.plan[i];
    const subIndex = (state.subAgentCatalog?.[master.nodeId] || []).findIndex((item) => item.id === nodeId);
    if (subIndex >= 0) return 100 + (i * 20) + subIndex;
  }

  return 999;
}

function getFlowNodeCategory(state, nodeId) {
  if (nodeId === "orchestrator-agent") return 4;
  if (nodeId === "parallel-compliance-agent") return 2;
  if (state.plan.some((item) => item.nodeId === nodeId)) return 1;
  for (const items of Object.values(state.subAgentCatalog || {})) {
    if ((items || []).some((item) => item.id === nodeId)) return 0;
  }
  return 3;
}

function getFlowLiveCandidates(state) {
  const runningIds = new Set([...(state.runningNodeIds || [])]);
  if (state.stage === "run") runningIds.add(state.orchestratorNodeId || "orchestrator-agent");
  let ids = [...runningIds].filter((id) => !!resolveFlowNodeDetail(state, id));
  const withoutOrchestrator = ids.filter((id) => id !== (state.orchestratorNodeId || "orchestrator-agent"));
  if (withoutOrchestrator.length) ids = withoutOrchestrator;
  return ids
    .sort((a, b) => {
      const categoryDelta = getFlowNodeCategory(state, a) - getFlowNodeCategory(state, b);
      if (categoryDelta !== 0) return categoryDelta;
      return getFlowNodeOrder(state, a) - getFlowNodeOrder(state, b);
    })
    .map((id) => {
      const detail = resolveFlowNodeDetail(state, id);
      return {
        id,
        title: detail?.title || resolveFlowNodeTitle(state, id) || id,
        status: detail?.status || { l: "Running", c: "primary" }
      };
    });
}

function renderFlowLiveSwitcher(candidates = [], selectedId = "", actions = {}) {
  if (candidates.length <= 1) return null;
  return html`
    <div class="flow-live-switcher mb-3">
      <div class="detail-section-label mb-2">Live Output Selector</div>
      <div class="flow-live-tab-row">
        ${candidates.map((item) => html`
          <button
            type="button"
            class="flow-live-tab ${item.id === selectedId ? "is-active" : ""}"
            @click=${() => actions?.selectFlowOutputNode?.(item.id)}
          >
            <span class="flow-live-tab-name">${item.title}</span>
            <span class="flow-live-tab-status tone-${item.status?.c || "secondary"}">${item.status?.l || "Running"}</span>
          </button>
        `)}
      </div>
    </div>
  `;
}

function resolveFlowNodeTitle(state, nodeId) {
  if (!nodeId) return null;
  if (nodeId === "campaign-brief-node") return "Campaign Brief";
  if (nodeId === "orchestrator-agent") return "Orchestrator Agent";
  if (nodeId === "parallel-compliance-agent") return "Parallel Compliance Agent";
  if (nodeId === "campaign-output-node") return "Output and Measurement";

  const masterAgent = state.plan.find((item) => item.nodeId === nodeId);
  if (masterAgent) return masterAgent.agentName;

  const executionDetails = state.campaignStateObject?.executionDetails || {};
  for (const detail of Object.values(executionDetails)) {
    const subAgent = (detail?.subAgentResults || []).find((item) => item.id === nodeId);
    if (subAgent) return subAgent.name;
  }

  for (const items of Object.values(state.subAgentCatalog || {})) {
    const subAgent = (items || []).find((item) => item.id === nodeId);
    if (subAgent) return subAgent.name;
  }

  return nodeId;
}

function buildFlowStatusSummary(state) {
  const knownIds = new Set([
    "campaign-brief-node",
    state.orchestratorNodeId || "orchestrator-agent",
    "parallel-compliance-agent",
    "campaign-output-node",
    ...state.plan.map((item) => item.nodeId)
  ]);

  Object.values(state.subAgentCatalog || {}).forEach((items) => {
    (items || []).forEach((item) => knownIds.add(item.id));
  });

  const errorIds = state.agentOutputs.filter((item) => item.status === "error").map((item) => item.nodeId);
  const failedIds = new Set([...(state.issueNodeIds || []), ...errorIds]);
  const activeIds = new Set([...(state.runningNodeIds || [])]);
  const completedIds = new Set(state.agentOutputs.filter((item) => item.status === "done").map((item) => item.nodeId));
  const executionDetails = state.campaignStateObject?.executionDetails || {};

  if (state.stage === "run") activeIds.add(state.orchestratorNodeId || "orchestrator-agent");
  if (state.stage === "idle" && state.agentOutputs.length) completedIds.add(state.orchestratorNodeId || "orchestrator-agent");
  if (state.plan.length) knownIds.add("campaign-brief-node");
  if (state.dashboard) completedIds.add("campaign-output-node");

  state.plan.forEach((agent) => {
    (state.subAgentCatalog?.[agent.nodeId] || []).forEach((subAgent) => {
      knownIds.add(subAgent.id);
      const subResult = executionDetails[agent.nodeId]?.subAgentResults?.find((item) => item.id === subAgent.id);
      if (subResult?.status === "running") activeIds.add(subAgent.id);
      if (subResult?.status === "done") completedIds.add(subAgent.id);
      if (subResult?.status === "error") failedIds.add(subAgent.id);
    });
  });

  const pendingIds = [...knownIds].filter((id) => !activeIds.has(id) && !completedIds.has(id) && !failedIds.has(id));
  const currentIds = [...activeIds];
  const currentTitles = currentIds.map((id) => resolveFlowNodeTitle(state, id)).filter(Boolean);

  return {
    currentIds,
    currentTitles,
    activeCount: activeIds.size,
    completedCount: completedIds.size,
    failedCount: failedIds.size,
    pendingCount: pendingIds.length
  };
}

function renderFlowStatusBoard(state) {
  const summary = buildFlowStatusSummary(state);
  return html`
    <div class="flow-status-board mb-3">
      <div class="flow-status-card flow-status-card-running">
        <div class="flow-status-label">Current Working</div>
        <div class="flow-status-value">${summary.activeCount || 0}</div>
        <div class="flow-status-list">
          ${summary.currentTitles.length
            ? summary.currentTitles.slice(0, 3).map((title) => html`<span class="flow-status-pill running">${title}</span>`)
            : html`<span class="flow-status-pill neutral">Waiting to start</span>`}
        </div>
      </div>
      <div class="flow-status-card flow-status-card-done">
        <div class="flow-status-label">Finished</div>
        <div class="flow-status-value">${summary.completedCount}</div>
        <div class="flow-status-note">Completed agents stay green in the graph.</div>
      </div>
      <div class="flow-status-card">
        <div class="flow-status-label">Pending</div>
        <div class="flow-status-value">${summary.pendingCount}</div>
        <div class="flow-status-note">Queued nodes stay muted until they start.</div>
      </div>
      <div class="flow-status-card flow-status-card-issue">
        <div class="flow-status-label">Issues</div>
        <div class="flow-status-value">${summary.failedCount}</div>
        <div class="flow-status-note">Red nodes show failures or flagged issues.</div>
      </div>
    </div>
    <div class="flow-status-legend mb-3">
      <span class="flow-legend-item"><span class="legend-swatch status-running"></span>Running</span>
      <span class="flow-legend-item"><span class="legend-swatch status-complete"></span>Finished</span>
      <span class="flow-legend-item"><span class="legend-swatch status-pending"></span>Pending</span>
      <span class="flow-legend-item"><span class="legend-swatch status-error"></span>Issue</span>
    </div>
  `;
}

function buildFlowProgressMeta(state) {
  const summary = buildFlowStatusSummary(state);
  const totalCount = summary.activeCount + summary.completedCount + summary.failedCount + summary.pendingCount;
  const finishedCount = summary.completedCount + summary.failedCount;
  const progressPct = totalCount ? Math.round((finishedCount / totalCount) * 100) : 0;
  const currentLabel = summary.currentTitles.length
    ? (summary.currentTitles.length === 1
      ? summary.currentTitles[0]
      : `${summary.currentTitles[0]} + ${summary.currentTitles.length - 1} more`)
    : finishedCount && finishedCount === totalCount
      ? "Execution complete"
      : "Waiting to start";
  const tone = summary.failedCount
    ? "danger"
    : summary.activeCount
      ? "primary"
      : finishedCount
        ? "success"
        : "secondary";
  return {
    ...summary,
    totalCount,
    finishedCount,
    progressPct,
    currentLabel,
    tone
  };
}

function renderFlowOutputDisclosure(state, actions = {}) {
  const progress = buildFlowProgressMeta(state);
  const statusLabel = progress.failedCount
    ? `${progress.failedCount} issue${progress.failedCount === 1 ? "" : "s"}`
    : progress.activeCount
      ? `${progress.activeCount} running`
      : progress.finishedCount && progress.finishedCount === progress.totalCount
        ? "Complete"
        : "Ready";
  const progressNote = progress.totalCount
    ? `${progress.finishedCount}/${progress.totalCount} resolved • ${progress.pendingCount} pending`
    : "Expand to follow the live trace.";
  return html`
    <details class="flow-live-disclosure">
      <summary class="flow-live-disclosure-summary">
        <div class="flow-live-disclosure-copy">
          <div class="flow-live-disclosure-label">Live Output</div>
          <div class="flow-live-disclosure-title">${progress.currentLabel}</div>
          <div class="flow-live-disclosure-note">${progressNote}</div>
        </div>
        <div class="flow-live-disclosure-progress">
          <div class="flow-live-disclosure-meta">
            <span class="flow-live-tab-status tone-${progress.tone}">${statusLabel}</span>
            <span class="flow-live-progress-value">${progress.progressPct}%</span>
          </div>
          <div class="flow-live-progress-track" aria-hidden="true">
            <span class="flow-live-progress-fill tone-${progress.tone}" style=${`width: ${progress.progressPct}%`}></span>
          </div>
        </div>
      </summary>
      <div class="flow-live-disclosure-body">
        ${renderFlowOutputPanel(state, actions)}
      </div>
    </details>
  `;
}

function resolveFlowNodeDetail(state, nodeId) {
  if (!nodeId) return null;
  const executionDetails = state.campaignStateObject?.executionDetails || {};
  const masterOutput = state.agentOutputs.find((item) => item.nodeId === nodeId);
  const masterAgent = state.plan.find((item) => item.nodeId === nodeId);

  if (masterOutput) {
    return { kind: "agent", output: masterOutput, title: masterAgent?.agentName || masterOutput.name };
  }

  if (nodeId === (state.orchestratorNodeId || "orchestrator-agent")) {
    const detail = executionDetails[nodeId] || {
      summaryLines: [
        "The orchestrator will own the shared Campaign_State_Object.",
        "It will dispatch the six master agents and keep context flowing across all stages."
      ],
      whyMatters: "This manager layer is what turns the experience into a hierarchical multi-agent system."
    };
    const status = state.stage === "run"
      ? { l: "Running", c: "primary" }
      : state.agentOutputs.length
        ? { l: "Done", c: "success" }
        : { l: "Pending", c: "secondary" };
    return {
      kind: "structured",
      title: "Orchestrator Agent",
      subtitle: "Manager",
      status,
      detail
    };
  }

  if (nodeId === "campaign-brief-node") {
    return {
      kind: "structured",
      title: "Campaign Brief",
      subtitle: "Input",
      status: { l: "Loaded", c: "secondary" },
      detail: {
        summaryLines: [state.campaignPrompt || "No prompt loaded yet."],
        whyMatters: "This is the plain-language input that seeds the scenario recommendation and the shared campaign state."
      }
    };
  }

  if (nodeId === "campaign-output-node") {
    const executive = state.dashboard?.executive || null;
    return {
      kind: "structured",
      title: "Output and Measurement",
      subtitle: "Final",
      status: state.dashboard ? { l: "Ready", c: "success" } : { l: "Pending", c: "secondary" },
      detail: {
        summaryLines: state.dashboard
          ? (executive?.summaryLines || [
            `Unique households: ${state.dashboard.reach?.uniqueHouseholds || 0}.`,
            `Make-good shift: ${state.dashboard.makeGood?.shiftBudget?.toLocaleString?.() || 0} dollars.`
          ])
          : ["The final dashboard will populate after campaign execution finishes."],
        stateUpdate: executive?.metrics?.reduce((acc, item) => ({ ...acc, [item.label]: item.value }), {}) || {},
        stateUpdateLabel: "Campaign Outcome Summary",
        whyMatters: "This is where the hierarchical run turns into measurable outcomes."
      }
    };
  }

  for (const [masterId, detail] of Object.entries(executionDetails)) {
    const subAgent = (detail?.subAgentResults || []).find((item) => item.id === nodeId);
    if (!subAgent) continue;
    const parent = state.plan.find((item) => item.nodeId === masterId);
    const parentOutput = state.agentOutputs.find((item) => item.nodeId === masterId);
    const status = subAgent.status === "done"
      ? { l: "Done", c: "success" }
      : subAgent.status === "error"
        ? { l: "Error", c: "danger" }
        : subAgent.status === "running"
          ? { l: "Running", c: "primary" }
          : parentOutput?.status === "running"
            ? { l: "Running", c: "primary" }
            : { l: "Pending", c: "secondary" };
    return {
      kind: "structured",
      title: subAgent.name,
      subtitle: parent?.agentName || "Sub-Agent",
      status,
      detail: {
        summaryLines: (subAgent.details || []).slice(0, 2),
        subAgentResults: [],
        inputData: subAgent.inputData || {
          parent_master_agent: parent?.agentName || masterId,
          node_id: nodeId
        },
        stateUpdate: subAgent.outputData || {
          notes: subAgent.details || []
        },
        stateUpdateLabel: "Output Data",
        liveOutput: subAgent.text || "",
        whyMatters: subAgent.whyMatters || detail?.whyMatters || "This sub-agent contributes a bounded piece of work to its parent master agent."
      }
    };
  }

  for (const [masterId, items] of Object.entries(state.subAgentCatalog || {})) {
    const subAgent = (items || []).find((item) => item.id === nodeId);
    if (!subAgent) continue;
    const parent = state.plan.find((item) => item.nodeId === masterId);
    return {
      kind: "structured",
      title: subAgent.name,
      subtitle: parent?.agentName || "Sub-Agent",
      status: { l: "Pending", c: "secondary" },
      detail: {
        summaryLines: [subAgent.summary || "This sub-agent is configured and waiting for its master agent to run."],
        inputData: {
          parent_master_agent: parent?.agentName || masterId,
          node_id: nodeId
        },
        whyMatters: "This is one of the bounded worker agents owned by a master stage."
      }
    };
  }

  if (masterAgent) {
    return {
      kind: "structured",
      title: masterAgent.agentName,
      subtitle: masterAgent.phaseLabel || "Master Agent",
      status: { l: "Pending", c: "secondary" },
      detail: {
        summaryLines: [masterAgent.initialTask || "This master agent is configured but has not run yet."],
        whyMatters: "This master agent will coordinate its sub-agents and write back to the shared campaign state."
      }
    };
  }

  return null;
}

function renderStructuredFlowDetail(detail, state = {}, actions = {}, liveSwitcher = null) {
  const data = detail?.detail || {};
  const summaryLines = data.summaryLines || [];
  const subAgents = data.subAgentResults || [];
  return html`
    <div class="flow-output-panel h-100 d-flex flex-column">
      <div class="d-flex justify-content-between align-items-start mb-2">
        <div>
          <p class="text-uppercase small text-body-secondary mb-1">${detail.subtitle || "Flow Node"}</p>
          <h6 class="mb-1">${detail.title}</h6>
        </div>
        ${renderFlowNodeStatus(detail.status)}
      </div>
      ${liveSwitcher}
      <div class="agent-stream border rounded-3 p-3 bg-body overflow-auto flex-grow-1">
        ${summaryLines.length ? html`<div class="mb-3">${summaryLines.map((line) => html`<p class="small mb-2">${line}</p>`)}</div>` : null}
        ${subAgents.length ? html`
          <div class="mb-3">
            ${renderPrettySubAgents(subAgents, state, actions, {
              contextKey: detail.contextKey || `flow-${detail.title || "subagents"}`,
              sectionLabel: "Select Sub-Agent Output"
            })}
          </div>
        ` : null}
        ${data.liveOutput ? html`
          <div class="detail-section">
            <div class="detail-section-label">Live Output</div>
            <div class="detail-text-panel">${data.liveOutput}</div>
          </div>
        ` : null}
        ${renderPrettySection(data.stateUpdateLabel || "State Update", data.stateUpdate)}
        ${data.whyMatters ? html`
          <div class="detail-section">
            <div class="detail-section-label">Why This Matters</div>
            <div class="detail-text-panel">${data.whyMatters}</div>
          </div>
        ` : null}
      </div>
    </div>
  `;
}

function renderFlowOutputPanel(state, actions = {}) {
  const liveCandidates = getFlowLiveCandidates(state);
  const candidateIds = new Set(liveCandidates.map((item) => item.id));
  const selectedCandidateId = state.flowPanelNodeId && candidateIds.has(state.flowPanelNodeId)
    ? state.flowPanelNodeId
    : liveCandidates[0]?.id || null;
  const liveId = state.focusedNodeId
    ?? selectedCandidateId
    ?? (state.latestNodeId || state.orchestratorNodeId || (state.plan[0]?.nodeId));
  const resolved = resolveFlowNodeDetail(state, liveId);
  if (!resolved) {
    return html`<div class="flow-output-panel h-100 d-flex flex-column"><div class="agent-stream border rounded-3 p-3 bg-body flex-grow-1 overflow-auto"><div class="text-body-secondary small">Build agents to stream output.</div></div></div>`;
  }
  const liveSwitcher = renderFlowLiveSwitcher(liveCandidates, liveId, actions);
  if (resolved.kind === "agent") {
    const output = resolved.output;
    const panelTitle = state.focusedNodeId ? "Pinned Step" : "Live Output";
    const status = output ? (output.status === "done" ? { l: "Done", c: "success" } : output.status === "error" ? { l: "Error", c: "danger" } : { l: "Running", c: "primary" }) : null;
    const fallbackNote = output && (hasFallbackNarrative(output.text) || output.fallbackUsed || output.subAgentResults?.some((item) => item.fallbackUsed || hasFallbackNarrative(item.text)))
      ? html`<div class="flow-output-notice">Live API fallback applied. The structured result remains valid and the full fallback trace is shown below.</div>`
      : null;
    return html`<div class="flow-output-panel h-100 d-flex flex-column"><div class="d-flex justify-content-between align-items-start mb-2"><div><p class="text-uppercase small text-body-secondary mb-1">${panelTitle}</p><h6 class="mb-1">${resolved.title}</h6></div>${status ? html`<span class="badge text-bg-${status.c}">${status.l}</span>` : null}</div>${liveSwitcher}<div class="${output ? agentStreamClasses(output) : "agent-stream border rounded-3 p-3 bg-body"} flex-grow-1 overflow-auto">${output ? html`${fallbackNote}${renderOutputBody(output)}${renderAgentStructuredDetails(output, state, actions, "mt-3", { includeInputData: false, subAgentContextKey: `flow-${output.nodeId}` })}` : html`<div class="text-body-secondary small">Build agents to stream output.</div>`}</div></div>`;
  }
  return renderStructuredFlowDetail(resolved, state, actions, liveSwitcher);
}

function renderAgentOutputs(state, demo, actions) {
  if (!state.agentOutputs.length) return null;
  const groups = new Map();
  const handoffs = demo?.handoffs || [];
  state.agentOutputs.forEach((a) => { const p = a.phase ?? "unknown"; if (!groups.has(p)) groups.set(p, []); groups.get(p).push(a); });
  const keys = [...groups.keys()].sort((a, b) => (typeof a === "number" && typeof b === "number") ? a - b : String(a).localeCompare(String(b)));
  return html`<section class="mb-5">${keys.map((key) => {
    const group = groups.get(key);
    if (group.length > 1) {
      return html`<div class="card mb-4 shadow-sm"><div class="card-header d-flex justify-content-between align-items-center"><div class="d-flex align-items-center gap-2"><span class="badge bg-secondary">Stage ${key}</span><span class="fw-semibold">Parallel Execution</span></div></div><div class="card-body bg-body-tertiary"><div class="row g-3">${group.map((a) => html`<div class="col-lg-6 col-xl-${Math.max(Math.floor(12 / group.length), 4)}">${renderAgentCard(a, state, true, actions)}</div>`)}</div></div></div>`;
    }
    const handoff = renderHandoffBadge(group[0], handoffs);
    return html`<div class="card mb-3 shadow-sm" data-running-key=${`node-${group[0].nodeId}`}><div class="card-body row g-3 align-items-stretch">${renderAgentCard(group[0], state, false, actions)}</div></div>${handoff ? html`<div class="text-center mb-3">${handoff}</div>` : null}`;
  })}</section>`;
}

function renderAgentCard(agent, state, simple, actions) {
  const hasIssue = state.issueNodeIds?.has(agent.nodeId);
  const meta = hasIssue ? { l: "Issue", c: "danger" } : agent.status === "done" ? { l: "Done", c: "success" } : agent.status === "error" ? { l: "Error", c: "danger" } : { l: "Running", c: "primary" };
  const rawEntry = state.rawDataByAgent?.[agent.nodeId] || state.rawDataByAgent?.[agent.name];
  const isOpen = state.rawDataOpen?.has(agent.nodeId);
  const quality = state.dataQuality;
  const subAgents = agent.subAgentResults || [];
  const hasInputData = !!(agent?.inputData && Object.keys(agent.inputData).length);
  const hasStateUpdate = !!(agent?.stateUpdate && Object.keys(agent.stateUpdate).length);
  const hasHandoff = !!(agent?.handoff && String(agent.handoff).trim());
  const qualityBadge = isPlanner(agent) && quality ? html`<span class="badge data-quality">Data Quality: ${quality.duplicateCount} duplicate households, ${quality.nullDmaCount} missing designated market area values</span>` : null;
  const rawToggle = rawEntry ? html`<button class="btn btn-sm btn-outline-warning" @click=${() => actions.toggleRawData(agent.nodeId)}>${isOpen ? "Hide Raw Data" : "View Raw Data"}</button>` : null;
  const subDone = subAgents.filter((item) => item.status === "done").length;
  const subRunning = subAgents.filter((item) => item.status === "running").length;
  const previewText = buildStagePreviewText(agent);
  const missionText = truncateCardText(agent.task || agent.initialTask || "No stage mission provided.", simple ? 140 : 180);
  const summaryMetrics = buildAgentSummaryMetrics(agent, state);
  const hasFallback = hasFallbackNarrative(agent?.text) || subAgents.some((item) => item?.fallbackUsed || hasFallbackNarrative(item?.text));
  const quickPills = [
    subAgents.length ? `${subDone}/${subAgents.length} sub-agents ready` : null,
    subRunning ? `${subRunning} sub-agent live` : null,
    hasStateUpdate ? "Campaign state updated" : null,
    hasHandoff ? "Handoff ready" : null,
    rawEntry ? "Evidence attached" : null,
    hasFallback ? "Fallback used" : null
  ].filter(Boolean);
  const metricBlock = renderStageMetricBlock(summaryMetrics, quickPills);
  const summaryBlock = renderStageSummaryBlock(agent, state, { simple });
  const missionBlock = html`
    <section class="stage-mission-card" title="${agent.task || agent.initialTask || ""}">
      <div class="stage-panel-label">Stage Mission</div>
      <p class="stage-mission-text mb-0">${missionText || "No stage mission provided."}</p>
    </section>
  `;
  const outputNote = agent.text
    ? agent.status === "running"
      ? "The executed summary stays readable here while the live trace continues to stream."
      : hasFallback
        ? "A graceful fallback was applied. The executed summary below remains the primary readout."
        : "The executed summary below is the primary readout. Open the detailed output only when you need the full trace."
    : "The live output panel will populate as soon as the model starts returning text.";
  const outputPreview = agent.status === "running" && previewText
      ? html`
      <div class="stage-preview-shell">
        <div class="stage-panel-label">Live Output</div>
        <pre class="stage-preview-live mb-0">${previewText}</pre>
      </div>
    `
    : html`
      <div class=${`stage-output-state ${hasFallback ? "is-warning" : agent.status === "error" ? "is-danger" : ""}`.trim()}>
        ${agent.status === "running"
          ? "Waiting for the live output stream to populate."
          : hasFallback
          ? "The live language-model call fell back to deterministic stage logic. Open the detailed output for the full fallback note."
          : agent.status === "error"
            ? "The live output was interrupted before the stage could complete."
            : "The stage summary is complete. Use the disclosure below for the full narrative, evidence, and handoff."}
      </div>
    `;
  const stageOutputHeading = agent.status === "running"
    ? html`
      <div class="d-flex align-items-center gap-2 flex-wrap">
        <h6 class="mb-0">Live Output</h6>
        <span class="stage-live-indicator">
          <span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
          <span>Streaming...</span>
        </span>
      </div>
    `
    : html`<h6 class="mb-0">Detailed Output</h6>`;
  const fullOutputBlock = agent.text ? html`
    <details class="stage-output-disclosure mt-3">
      <summary>${agent.status === "running" ? "Open live output trace" : "Open detailed output"}</summary>
      <div class="${agentStreamClasses(agent)} mt-3">${renderOutputBody(agent)}</div>
    </details>
  ` : null;
  const subAgentDetails = subAgents.length ? html`
    <details class="stage-side-disclosure">
      <summary>Sub-Agent Breakdown</summary>
      <div class="stage-side-disclosure-body">
        ${renderAgentStructuredDetailsContent(agent, state, actions, {
          includeInputData: false,
          includeStateUpdate: false,
          includeHandoff: false,
          subAgentContextKey: `stage-${agent.nodeId}`,
          subAgentSectionLabel: "Which Sub-Agent Output Do You Want To See?"
        })}
      </div>
    </details>
  ` : null;
  const stageContextBlock = (hasInputData || hasStateUpdate || hasHandoff || rawEntry) ? html`
    <details class="stage-output-disclosure mt-3">
      <summary>Context and data</summary>
      <div class="stage-side-disclosure-body">
        ${renderAgentStructuredDetailsContent(agent, state, actions, {
          includeSubAgents: false,
          includeInputData: true,
          includeStateUpdate: true,
          includeHandoff: true
        })}
        ${rawEntry ? html`
          <div class="detail-section">
            <div class="d-flex justify-content-between align-items-center flex-wrap gap-2">
              <div class="detail-section-label mb-0">Raw Stage Data</div>
              ${rawToggle}
            </div>
            ${isOpen ? renderExpandedDataEntry(rawEntry, "mt-2") : null}
          </div>
        ` : null}
      </div>
    </details>
  ` : null;
  if (simple) {
    return html`
      <div class="h-100 d-flex flex-column border rounded-3 overflow-hidden shadow-sm stage-compact-card">
        <div class="p-3 bg-body-tertiary border-bottom d-flex justify-content-between align-items-start gap-2">
          <div class="text-truncate">
            <h6 class="mb-0 text-truncate" title="${agent.name}">${agent.name}</h6>
            <small class="text-body-secondary text-truncate d-block">${agent.phase ? `Stage ${agent.phase}` : "Workflow step"}</small>
          </div>
          <span class="badge text-bg-${meta.c}">${meta.l}</span>
        </div>
        <div class="p-3 d-flex flex-column gap-3 flex-grow-1">
          ${missionBlock}
          ${metricBlock}
          <div class="stage-output-card compact">
            <div class="small text-uppercase text-body-secondary mb-1">Stage Output</div>
            <p class="stage-output-note mb-3">${outputNote}</p>
            ${summaryBlock}
            ${outputPreview}
            ${fullOutputBlock}
            ${stageContextBlock}
          </div>
          ${qualityBadge}
          ${subAgentDetails}
        </div>
      </div>
    `;
  }
  return html`
    <div class="col-md-4 d-flex flex-column">
      <div class="stage-sidebar-card h-100">
        <div class="d-flex justify-content-between align-items-start gap-2">
          <div>
            <p class="text-uppercase small text-body-secondary mb-1">${agent.phase ? `Stage ${agent.phase}` : "Step"}</p>
            <h6 class="mb-2">${agent.name}</h6>
          </div>
          <span class="badge text-bg-${meta.c} align-self-start">${meta.l}</span>
        </div>
        ${missionBlock}
        ${metricBlock}
        ${qualityBadge}
        ${subAgentDetails}
      </div>
    </div>
    <div class="col-md-8">
      <div class="stage-output-card h-100">
        <div class="d-flex justify-content-between align-items-start gap-2 mb-2">
          <div>
            <div class="small text-uppercase text-body-secondary mb-1">Stage Output</div>
            ${stageOutputHeading}
          </div>
        </div>
        <p class="stage-output-note mb-3">${outputNote}</p>
        ${summaryBlock}
        ${outputPreview}
        ${fullOutputBlock}
        ${stageContextBlock}
      </div>
    </div>
  `;
}

function renderOutputBody(agent) {
  if (!agent.text) return loading;
  if (agent.status === "done") return html`<div class="agent-markdown">${unsafeHTML(marked.parse(agent.text))}</div>`;
  const tone = agent.status === "error" ? "text-warning" : "text-white";
  return html`<pre class="mb-0 ${tone}" style="white-space: pre-wrap;">${agent.text}</pre>`;
}

function agentStreamClasses(agent) {
  const base = "agent-stream border rounded-3 p-2";
  if (agent.status === "error") return `${base} bg-dark text-warning`;
  if (agent.status === "done") return `${base} bg-body`;
  return `${base} bg-black text-white`;
}

function renderHandoffBadge(agent, handoffs) {
  const handoff = handoffs.find((h) => h.from === agent.nodeId);
  if (!handoff || agent.status !== "done") return null;
  return html`<span class="handoff-badge"><i class="bi bi-arrow-right-circle"></i>${handoff.label}</span>`;
}

function renderDashboard(state, actions) {
  const hasDashboard = !!state.dashboard;
  const showVisualizationLoader = !!state.visualizationLoading;
  if (!hasDashboard && !showVisualizationLoader) return null;
  if (!hasDashboard && showVisualizationLoader) return renderVisualizationLoadingCard();

  const { reach, actions: actionRows } = state.dashboard;
  return html`
    <section class="card mb-5">
      <div class="card-header d-flex justify-content-between align-items-center flex-wrap gap-2"><span><i class="bi bi-bar-chart-line me-2"></i>Output and Visualization</span><div class="d-flex align-items-center gap-2 flex-wrap"><small class="text-body-secondary">Charts use synthetic metrics and explanations are generated by the language model.</small><button class="btn btn-sm btn-outline-warning" @click=${actions.toggleVisualizationExplanation}>${state.visualizationExplanationOpen ? "Hide Visualization Explanation" : "Show Visualization Explanation"}</button><button class="btn btn-sm btn-outline-warning" @click=${actions.exportApprovedPlan}><i class="bi bi-download me-1"></i>Export Plan CSV</button></div></div>
      <div class="card-body">
        ${showVisualizationLoader ? html`
          <div class="visualization-loading mb-4">
            <div class="spinner-border text-warning" role="status">
              <span class="visually-hidden">Generating visualization</span>
            </div>
            <div>
              <h6 class="mb-1">Generating visualization explanation</h6>
              <p class="small text-body-secondary mb-0">Please wait while the language model finishes the detailed explanation for the charts and action traceability.</p>
            </div>
          </div>
        ` : null}
        ${renderDashboardExecutiveSummary(state.dashboard?.executive)}
        <div class="dashboard-grid"><div class="chart-card"><div class="d-flex justify-content-between align-items-center flex-wrap gap-2 mb-2"><h6 class="mb-0">Delivery Pacing (Linear versus Digital)</h6><div class="btn-group btn-group-sm" role="group"><button class="btn btn-outline-warning ${state.pacingMode === "pct" ? "active" : ""}" @click=${() => actions.setPacingMode("pct")}>Delivery Rate (%)</button><button class="btn btn-outline-warning ${state.pacingMode === "imp" ? "active" : ""}" @click=${() => actions.setPacingMode("imp")}>Delivered Impressions</button></div></div>${renderPacingChart()}<div class="d-flex gap-3 small mt-2 text-body-secondary"><span class="d-flex align-items-center gap-2"><span class="legend-swatch linear"></span>Linear</span><span class="d-flex align-items-center gap-2"><span class="legend-swatch digital"></span>Digital</span></div></div><div class="donut-card"><h6 class="mb-3">Cross-Platform Reach</h6>${renderReachDonut(reach)}<div class="reach-metrics mt-3"><div><div class="metric-label">Unique Households</div><div class="metric-value">${formatNumber(reach.uniqueHouseholds)}</div></div><div><div class="metric-label">Devices Touched</div><div class="metric-value">${formatNumber(reach.deviceCount)}</div></div><div><div class="metric-label">Cross-Platform Overlap</div><div class="metric-value">${reach.overlapPct}%</div></div></div></div></div>
        ${renderMakeGood(state)}
        ${state.visualizationExplanationOpen ? renderVisualizationExplanation(state) : null}
        <div class="mt-4"><h6 class="mb-3">Agent Actions</h6>${renderActionTable(actionRows)}</div>
      </div>
    </section>`;
}

function renderDashboardExecutiveSummary(executive = null) {
  if (!executive) return null;
  return html`
    <section class="campaign-summary-card mb-4">
      <div class="campaign-summary-head">
        <div>
          <div class="campaign-summary-label">Campaign Summary</div>
          <h5 class="mb-1">${executive.headline || "Selected scenario"}</h5>
        </div>
        <span class="badge text-bg-warning text-dark">${executive.summaryBadge || "Plain-English Summary"}</span>
      </div>
      ${executive.summaryIntro ? html`
        <div class="campaign-summary-plain">
          <div class="campaign-summary-plain-label">In Plain English</div>
          <p class="mb-0">${executive.summaryIntro}</p>
        </div>
      ` : null}
      <div class="campaign-summary-copy">
        ${(executive.sections || []).length
          ? html`
            <div class="campaign-summary-sections">
              ${(executive.sections || []).map((section) => html`
                <div class="campaign-summary-section">
                  <div class="campaign-summary-section-title">${section.title}</div>
                  <p class="campaign-summary-section-text">${section.text}</p>
                </div>
              `)}
            </div>
          `
          : (executive.summaryLines || []).map((line) => html`<p class="mb-2">${line}</p>`)}
      </div>
      <div class="campaign-summary-grid">
        ${(executive.metrics || []).map((metric) => html`
          <div class="campaign-summary-metric">
            <div class="campaign-summary-metric-label">${metric.label}</div>
            <div class="campaign-summary-metric-value">${metric.value}</div>
            ${metric.help ? html`<div class="campaign-summary-metric-help">${metric.help}</div>` : null}
          </div>
        `)}
      </div>
      ${executive.makeGoodSummary ? html`<div class="campaign-summary-note">${executive.makeGoodSummary}</div>` : null}
    </section>
  `;
}

function renderVisualizationLoadingCard() {
  return html`
    <section class="card mb-5">
      <div class="card-header"><span><i class="bi bi-bar-chart-line me-2"></i>Output and Visualization</span></div>
      <div class="card-body">
        <div class="visualization-loading">
          <div class="spinner-border text-warning" role="status">
            <span class="visually-hidden">Generating visualization</span>
          </div>
          <div>
            <h6 class="mb-1">Generating visualization and explanation</h6>
            <p class="small text-body-secondary mb-0">Campaign execution has finished and the dashboard explanation is still being produced. Please wait before reviewing the final charts.</p>
          </div>
        </div>
      </div>
    </section>
  `;
}

function renderVisualizationExplanation(state) {
  const llmNarrative = (state.visualizationNarrative || "").trim();
  return html`
    <div class="explanation-panel mt-4">
      <h6 class="mb-2">Detailed Visualization Explanation</h6>
      ${llmNarrative
        ? html`<div class="agent-markdown">${unsafeHTML(marked.parse(llmNarrative))}</div>`
        : html`<p class="mb-0 text-body-secondary small">The detailed visualization explanation will appear after the language model completes the campaign run.</p>`}
    </div>
  `;
}

function renderPacingChart() {
  return html`<div class="pacing-chart-d3"></div>`;
}

function renderReachDonut(reach) {
  return html`<div class="reach-chart-d3"></div><div class="mt-3 text-center text-body-secondary small">Linear ${reach.linearPct}% | Digital ${reach.digitalPct}%</div>`;
}

function renderMakeGood(state) {
  const ops = state.agentOutputs.find((a) => isOps(a));
  if (!ops || ops.status !== "done") return null;
  const summary = state.dashboard?.makeGood;
  if (!summary) return null;
  return html`<div class="makegood-card mt-4"><div class="d-flex justify-content-between align-items-center flex-wrap gap-2"><div><h6 class="mb-1">Delivery Make-Good Triggered</h6><div class="small text-body-secondary">Reallocated ${summary.shiftBudget.toLocaleString()} United States dollars to stronger inventory to protect delivery.</div></div></div><div class="makegood-track mt-3"><span class="budget-chip chip-linear">Linear</span><span class="budget-chip chip-digital">Streaming</span><span class="budget-chip chip-move">${summary.shiftBudget.toLocaleString()} United States dollars</span></div></div>`;
}

function renderActionTable(actions) {
  if (!actions.length) return html`<p class="text-body-secondary small">No actions recorded.</p>`;
  return html`<div class="table-responsive"><table class="table table-dark table-striped align-middle"><thead><tr><th scope="col">Time</th><th scope="col">Agent</th><th scope="col">Action</th><th scope="col">Status</th></tr></thead><tbody>${actions.map((row) => html`<tr><td>${formatTime(row.time)}</td><td>${row.agent}</td><td>${row.summary}</td><td>${renderStatusBadge(row)}</td></tr>`)}</tbody></table></div>`;
}

function renderStatusBadge(row) {
  if (row.status === "error") return html`<span class="badge status-issue">Error</span>`;
  if (row.status === "issue") return html`<span class="badge status-issue">Issue Flagged</span>`;
  if ((row.id || "").toLowerCase().includes("ops")) return html`<span class="badge status-running">Make-Good</span>`;
  return html`<span class="badge status-ok">Completed</span>`;
}

function isCompliance(agent) {
  const text = `${agent?.nodeId || ""} ${agent?.agentName || ""} ${agent?.name || ""}`.toLowerCase();
  return text.includes("compliance");
}

function isOps(agent) {
  const text = `${agent?.nodeId || ""} ${agent?.agentName || ""} ${agent?.name || ""}`.toLowerCase();
  return text.includes("ops") || text.includes("operation");
}

function isPlanner(agent) {
  const text = `${agent?.nodeId || ""} ${agent?.agentName || ""} ${agent?.name || ""}`.toLowerCase();
  return text.includes("planner") || text.includes("audience") || text.includes("planning and identity") || text.includes("planning-identity");
}

const numberFormatter = new Intl.NumberFormat("en-US");
function formatNumber(value) {
  return numberFormatter.format(value || 0);
}

function formatTime(value) {
  if (!value) return "--:--:--";
  return new Date(value).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}
