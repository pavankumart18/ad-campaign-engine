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

function formatPrettyLabel(value = "") {
  return String(value)
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b[a-z]/g, (char) => char.toUpperCase())
    .replace(/\bId\b/g, "ID")
    .replace(/\bUsd\b/g, "USD")
    .replace(/\bPct\b/g, "%")
    .replace(/\bCpm\b/g, "CPM")
    .replace(/\bRoi\b/g, "ROI")
    .replace(/\bScte35\b/gi, "SCTE-35");
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
    const text = value.trim();
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

  return html`<pre class="raw-data mb-0">${String(value)}</pre>`;
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

function renderPrettySubAgents(items = []) {
  if (!items.length) return html`<span class="detail-null">No sub-agent results recorded.</span>`;
  return html`
    <div class="detail-card-grid">
      ${items.map((item) => html`
        <div class="detail-card">
          <div class="detail-card-title">${item.name || "Sub-Agent"}</div>
          ${(item.details || []).length
            ? html`<ul class="detail-list mb-0">${(item.details || []).map((detail) => html`<li>${detail}</li>`)}</ul>`
            : html`<span class="detail-null">No notes recorded.</span>`}
        </div>
      `)}
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

function renderAgentStructuredDetails(agent, wrapperClass = "") {
  if (!hasStructuredAgentDetail(agent)) return null;
  const classes = wrapperClass ? ` ${wrapperClass}` : "";
  return html`
    <details class=${`detail-disclosure${classes}`}>
      <summary>Detailed execution</summary>
      ${renderPrettySection("Input Data", agent.inputData)}
      ${agent.subAgentResults?.length ? html`
        <div class="detail-section">
          <div class="detail-section-label">Sub-Agent Results</div>
          ${renderPrettySubAgents(agent.subAgentResults)}
        </div>
      ` : null}
      ${renderPrettySection("Campaign_State_Object Update", agent.stateUpdate)}
      ${renderPrettySection("Handoff", agent.handoff)}
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
    ${renderFlow(state)}
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

function renderPlan(state, actions) {
  const streaming = state.stage === "architect";
  const selecting = state.stage === "plan-select";
  const hasOptions = (state.architectPlans || []).length > 0;
  const hasPlan = state.plan.length > 0;
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
          <div class="bg-dark text-white rounded-3 p-3 mb-0">
            ${state.architectBuffer
              ? html`<pre class="mb-0 text-white" style="white-space: pre-wrap; overflow-wrap: break-word;">${state.architectBuffer}</pre>`
              : html`<div class="d-flex align-items-center gap-2"><div class="spinner-border spinner-border-sm" role="status"></div><span>Streaming architect response...</span></div>`}
          </div>
        ` : null}
        ${hasOptions ? html`
          <div class="row g-3">
            ${state.architectPlans.map((option) => {
              const selected = state.selectedArchitectPlanId === option.id;
              return html`
                <div class="col-12 col-lg-4">
                  <article class="architect-option-card h-100 ${selected ? "is-selected" : ""}">
                    <div class="architect-option-head">
                      <h5 class="mb-1">${option.title}</h5>
                      <span class="badge text-bg-${selected ? "success" : "secondary"}">${selected ? "Selected" : "Available"}</span>
                    </div>
                    <div class="prompt-tile-card">
                      <div class="prompt-tile-label">Prompt Tile</div>
                      <div class="prompt-tile-title">${option.promptTile || "Architect Prompt"}</div>
                      <p class="prompt-tile-text mb-0">${option.promptText || "No prompt objective supplied."}</p>
                    </div>
                    <p class="small text-body-secondary mb-2"><strong>Strategy:</strong> ${option.strategy}</p>
                    <p class="small text-body-secondary mb-3"><strong>Why this option:</strong> ${option.why}</p>
                    <div class="small mb-2"><strong>Channel Allocation:</strong> ${option.allocationStrategy || "Allocation strategy not provided."}</div>
                    <div class="small mb-2"><strong>Delivery Timing:</strong> ${option.deliveryTiming || "Delivery timing not provided."}</div>
                    <div class="small mb-3"><strong>Return on Investment Reasoning:</strong> ${option.roiReasoning || "Return on investment reasoning not provided."}</div>
                    <div class="small mb-3">
                      <strong>Compliance Validation:</strong>
                      <span class="badge ms-2 text-bg-${(option.complianceValidation?.status || "").toLowerCase().includes("adjust") ? "warning" : "success"}">${option.complianceValidation?.status || "Passed"}</span>
                      <div class="text-body-secondary mt-1">${option.complianceValidation?.summary || "Compliance check summary not provided."}</div>
                    </div>
                    <div class="small fw-semibold mb-2">Architect Plan</div>
                    <ol class="architect-plan-list mb-3">
                      ${(option.plan || []).map((agent) => html`
                        <li>
                          <span class="fw-semibold">${agent.agentName}</span>
                          <p class="text-body-secondary mt-1 mb-2">${agent.initialTask}</p>
                          <details>
                            <summary class="small text-warning">System Prompt Detail</summary>
                            <p class="text-body-secondary small mt-2 mb-0">${agent.systemInstruction}</p>
                          </details>
                        </li>
                      `)}
                    </ol>
                    <button class="btn btn-sm btn-primary w-100" @click=${() => actions.chooseArchitectPlan(option.id)} ?disabled=${["architect", "run"].includes(state.stage)}>
                      ${selected ? "Current Selection" : "Choose This Architect Plan"}
                    </button>
                  </article>
                </div>
              `;
            })}
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
  return html`
    <section class="card mb-4" data-running-key=${state.stage === "data" ? "data-inputs" : null}>
      <div class="card-header d-flex justify-content-between align-items-center flex-wrap gap-2"><div><i class="bi bi-database me-2"></i>Synthetic Data Sources</div><button class="btn btn-sm btn-primary" @click=${actions.startAgents} ?disabled=${disabled}>${launchLabel}</button></div>
      <div class="card-body">
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
    </section>`;
}

function renderFlow(state) {
  if (!state.plan.length) return null;
  return html`<section class="card mb-4" data-running-key="execution-flow"><div class="card-header d-flex justify-content-between align-items-center flex-wrap gap-2"><span><i class="bi bi-diagram-3 me-2"></i>Orchestrator and Sub-Agent Flow</span><small class="text-body-secondary">Click the manager, master agents, or sub-agents to inspect what each one is doing.</small></div><div class="card-body"><div class="row g-3 align-items-stretch"><div class="col-xl-9 col-lg-8"><div id="flowchart-canvas" class="flowchart-canvas border rounded-3 bg-body-tertiary" data-flow-orientation=${state.flowOrientation} data-flow-columns=${state.flowColumns}></div></div><div class="col-xl-3 col-lg-4">${renderFlowOutputPanel(state)}</div></div></div></section>`;
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
      <div class="card-header d-flex justify-content-between align-items-center flex-wrap gap-2"><span><i class="bi bi-shield-check me-2"></i>Compliance Sources and Rationale</span><button class="btn btn-sm btn-outline-warning" @click=${actions.toggleComplianceExplanation}>${state.complianceExplanationOpen ? "Hide Detailed Explanation" : "Show Detailed Explanation"}</button></div>
      <div class="card-body">
        <div class="mb-2"><span class="badge text-bg-${statusTone}">${details?.status || "Status Unknown"}</span></div>
        <p class="text-body-secondary small mb-3">${details?.summary || "Compliance sources populate after run completion."}</p>
        ${details?.findings?.length ? html`<div class="mb-3"><h6 class="mb-2">Findings</h6><ul class="mb-0">${details.findings.map((finding) => html`<li class="small text-body-secondary">${finding}</li>`)}</ul></div>` : null}
        ${details?.alternatives?.length ? html`<div class="mb-3"><h6 class="mb-2">Compliant Alternatives</h6><ul class="mb-0">${details.alternatives.map((alt) => html`<li class="small text-body-secondary">${alt}</li>`)}</ul></div>` : null}
        ${details?.sources?.length ? html`<div class="table-responsive"><table class="table table-dark table-striped align-middle mb-0"><thead><tr><th scope="col">Policy</th><th scope="col">Source</th><th scope="col">Why Applied</th></tr></thead><tbody>${details.sources.map((item) => html`<tr><td>${item.policy}</td><td>${item.source}</td><td>${item.why}</td></tr>`)}</tbody></table></div>` : null}
        ${state.complianceExplanationOpen ? html`<div class="explanation-panel mt-3"><p class="mb-2">${details?.detailedExplanation || "The compliance agent records every rule match and why that rule changed routing."}</p><p class="mb-0">This explanation is detailed so operations and legal reviewers can audit each policy decision path.</p></div>` : null}
      </div>
    </section>`;
}

function renderFlowNodeStatus(status) {
  if (!status) return null;
  return html`<span class="badge text-bg-${status.c}">${status.l}</span>`;
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
    return {
      kind: "structured",
      title: "Output and Measurement",
      subtitle: "Final",
      status: state.dashboard ? { l: "Ready", c: "success" } : { l: "Pending", c: "secondary" },
      detail: {
        summaryLines: state.dashboard
          ? [
            `Unique households: ${state.dashboard.reach?.uniqueHouseholds || 0}.`,
            `Make-good shift: ${state.dashboard.makeGood?.shiftBudget?.toLocaleString?.() || 0} dollars.`
          ]
          : ["The final dashboard will populate after campaign execution finishes."],
        whyMatters: "This is where the hierarchical run turns into measurable outcomes."
      }
    };
  }

  for (const [masterId, detail] of Object.entries(executionDetails)) {
    const subAgent = (detail?.subAgentResults || []).find((item) => item.id === nodeId);
    if (!subAgent) continue;
    const parent = state.plan.find((item) => item.nodeId === masterId);
    const parentOutput = state.agentOutputs.find((item) => item.nodeId === masterId);
    const status = parentOutput?.status === "done"
      ? { l: "Done", c: "success" }
      : parentOutput?.status === "error"
        ? { l: "Error", c: "danger" }
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
        inputData: {
          parent_master_agent: parent?.agentName || masterId,
          node_id: nodeId
        },
        stateUpdate: {
          notes: subAgent.details || []
        },
        whyMatters: detail?.whyMatters || "This sub-agent contributes a bounded piece of work to its parent master agent."
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

function renderStructuredFlowDetail(detail) {
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
      <div class="agent-stream border rounded-3 p-3 bg-body overflow-auto flex-grow-1">
        ${summaryLines.length ? html`<div class="mb-3">${summaryLines.map((line) => html`<p class="small mb-2">${line}</p>`)}</div>` : null}
        ${subAgents.length ? html`
          <div class="mb-3">
            <div class="detail-section-label">Owned Agents</div>
            ${renderPrettySubAgents(subAgents)}
          </div>
        ` : null}
        ${renderPrettySection("Input Data", data.inputData)}
        ${renderPrettySection("State Update", data.stateUpdate)}
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

function renderFlowOutputPanel(state) {
  const liveId = state.focusedNodeId ?? (state.latestNodeId || state.orchestratorNodeId || (state.plan[0]?.nodeId));
  const resolved = resolveFlowNodeDetail(state, liveId);
  if (!resolved) {
    return html`<div class="flow-output-panel h-100 d-flex flex-column"><div class="agent-stream border rounded-3 p-3 bg-body flex-grow-1 overflow-auto"><div class="text-body-secondary small">Build agents to stream output.</div></div></div>`;
  }
  if (resolved.kind === "agent") {
    const output = resolved.output;
    const panelTitle = state.focusedNodeId ? "Pinned Step" : "Live Output";
    const status = output ? (output.status === "done" ? { l: "Done", c: "success" } : output.status === "error" ? { l: "Error", c: "danger" } : { l: "Running", c: "primary" }) : null;
    return html`<div class="flow-output-panel h-100 d-flex flex-column"><div class="d-flex justify-content-between align-items-start mb-2"><div><p class="text-uppercase small text-body-secondary mb-1">${panelTitle}</p><h6 class="mb-1">${resolved.title}</h6></div>${status ? html`<span class="badge text-bg-${status.c}">${status.l}</span>` : null}</div><div class="${output ? agentStreamClasses(output) : "agent-stream border rounded-3 p-3 bg-body"} flex-grow-1 overflow-auto">${output ? html`${renderOutputBody(output)}${renderAgentStructuredDetails(output, "mt-3")}` : html`<div class="text-body-secondary small">Build agents to stream output.</div>`}</div></div>`;
  }
  return renderStructuredFlowDetail(resolved);
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
  const summaryLines = agent.summaryLines || [];
  const qualityBadge = isPlanner(agent) && quality ? html`<span class="badge data-quality mt-2">Data Quality: ${quality.duplicateCount} duplicate households, ${quality.nullDmaCount} missing designated market area values</span>` : null;
  const rawToggle = rawEntry ? html`<button class="btn btn-sm btn-outline-warning" @click=${() => actions.toggleRawData(agent.nodeId)}>${isOpen ? "Hide Raw Data" : "View Raw Data"}</button>` : null;
  const detailBlock = renderAgentStructuredDetails(agent, "mt-3");
  const subAgentBlock = subAgents.length ? html`
    <div class="mt-3">
      <div class="small text-uppercase text-body-secondary mb-2">Sub-Agents</div>
      <div class="d-flex flex-wrap gap-2 mb-2">
        ${subAgents.map((item) => html`<span class="badge text-bg-secondary">${item.name}</span>`)}
      </div>
      ${subAgents.slice(0, 3).map((item) => html`<p class="small text-body-secondary mb-1"><strong>${item.name}:</strong> ${(item.details || [])[0] || ""}</p>`)}
    </div>
  ` : null;
  const summaryBlock = summaryLines.length ? html`
    <div class="mt-3">
      ${summaryLines.slice(0, 2).map((line) => html`<p class="small text-body-secondary mb-1">${line}</p>`)}
    </div>
  ` : null;
  if (simple) {
    return html`<div class="h-100 d-flex flex-column border rounded-3 overflow-hidden shadow-sm"><div class="p-3 bg-body-tertiary border-bottom d-flex justify-content-between align-items-center gap-2"><div class="text-truncate"><h6 class="mb-0 text-truncate" title="${agent.name}">${agent.name}</h6><small class="text-body-secondary text-truncate d-block" title="${agent.task}">${agent.task}</small></div><span class="badge text-bg-${meta.c}">${meta.l}</span></div>${summaryBlock ? html`<div class="px-3 pt-3">${summaryBlock}</div>` : null}${subAgentBlock ? html`<div class="px-3 pb-3">${subAgentBlock}</div>` : null}<div class="${agentStreamClasses(agent)} flex-grow-1 border-0 rounded-0" style="min-height: 200px;">${renderOutputBody(agent)}</div>${detailBlock ? html`<div class="px-3 pb-3">${detailBlock}</div>` : null}${rawEntry ? html`<div class="p-3 border-top d-flex flex-column gap-2">${rawToggle}${isOpen ? renderExpandedDataEntry(rawEntry) : null}</div>` : null}</div>`;
  }
  return html`<div class="col-md-4 d-flex flex-column"><p class="text-uppercase small text-body-secondary mb-1">${agent.phase ? `Stage ${agent.phase}` : "Step"}</p><h6 class="mb-2">${agent.name}</h6><p class="text-body-secondary small flex-grow-1 mb-3">${agent.task}</p><span class="badge text-bg-${meta.c} align-self-start">${meta.l}</span>${qualityBadge}${summaryBlock}${subAgentBlock}${rawToggle ? html`<div class="mt-3">${rawToggle}</div>` : null}</div><div class="col-md-8"><div class="${agentStreamClasses(agent)}">${renderOutputBody(agent)}</div>${detailBlock ? html`<div class="mt-3">${detailBlock}</div>` : null}${isOpen && rawEntry ? html`<div class="mt-3">${renderExpandedDataEntry(rawEntry)}</div>` : null}</div>`;
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
        <div class="dashboard-grid"><div class="chart-card"><div class="d-flex justify-content-between align-items-center flex-wrap gap-2 mb-2"><h6 class="mb-0">Delivery Pacing (Linear versus Digital)</h6><div class="btn-group btn-group-sm" role="group"><button class="btn btn-outline-warning ${state.pacingMode === "pct" ? "active" : ""}" @click=${() => actions.setPacingMode("pct")}>Delivery Rate (%)</button><button class="btn btn-outline-warning ${state.pacingMode === "imp" ? "active" : ""}" @click=${() => actions.setPacingMode("imp")}>Delivered Impressions</button></div></div>${renderPacingChart()}<div class="d-flex gap-3 small mt-2 text-body-secondary"><span class="d-flex align-items-center gap-2"><span class="legend-swatch linear"></span>Linear</span><span class="d-flex align-items-center gap-2"><span class="legend-swatch digital"></span>Digital</span></div></div><div class="donut-card"><h6 class="mb-3">Cross-Platform Reach</h6>${renderReachDonut(reach)}<div class="reach-metrics mt-3"><div><div class="metric-label">Unique Households</div><div class="metric-value">${formatNumber(reach.uniqueHouseholds)}</div></div><div><div class="metric-label">Devices Touched</div><div class="metric-value">${formatNumber(reach.deviceCount)}</div></div><div><div class="metric-label">Cross-Platform Overlap</div><div class="metric-value">${reach.overlapPct}%</div></div></div></div></div>
        ${renderMakeGood(state)}
        ${state.visualizationExplanationOpen ? renderVisualizationExplanation(state) : null}
        <div class="mt-4"><h6 class="mb-3">Agent Actions</h6>${renderActionTable(actionRows)}</div>
      </div>
    </section>`;
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
  return html`<div class="makegood-card mt-4"><div class="d-flex justify-content-between align-items-center flex-wrap gap-2"><div><h6 class="mb-1">StreamX Make-Good Triggered</h6><div class="small text-body-secondary">Reallocated ${summary.shiftBudget.toLocaleString()} United States dollars to Max ad-lite inventory.</div></div><div class="makegood-metrics"><span>Return on Investment Before Intervention: <strong>${summary.beforeRoi.toFixed(2)}</strong></span><span>Return on Investment After Intervention: <strong>${summary.afterRoi.toFixed(2)}</strong></span></div></div><div class="makegood-track mt-3"><span class="budget-chip chip-linear">Linear</span><span class="budget-chip chip-digital">Max ad-lite</span><span class="budget-chip chip-move">${summary.shiftBudget.toLocaleString()} United States dollars</span></div></div>`;
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
