// VERSION: 2026-03-06 - Added three-plan architect selection, synthetic execution, and detailed compliance explanations.
import { openaiConfig } from "bootstrap-llm-provider";
import saveform from "saveform";
import { createFlowchart } from "./flowchart.js";
import * as Utils from "./utils.js";
import * as View from "./view.js";
import { Storage } from "./storage.js";
import { datasets, datasetMeta, datasetEntries, toCsv } from "./data.js";
import * as d3 from "d3";

const $ = (s) => document.querySelector(s);
const DEFAULT_BASE_URLS = ["https://api.openai.com/v1", "https://llmfoundry.straivedemo.com/openai/v1"];
const WBD_DEMO_ID = "wbd-converged-engine";

const AGENT_DATASET_MAP = {
  planner: "audienceGraph",
  "The Planner": "audienceGraph",
  "sales-rep": "inventoryMatrix",
  "The Sales Rep": "inventoryMatrix",
  "compliance-bot": "globalLawRegistry",
  "The Compliance Bot": "globalLawRegistry",
  "ops-director": "liveDeliveryLog",
  "The Ops Director": "liveDeliveryLog"
};

const AGENT_ACTION_SUMMARY = {
  planner: "Mapped Olli Auto Intender households and documented data quality risks.",
  "The Planner": "Mapped Olli Auto Intender households and documented data quality risks.",
  "sales-rep": "Built a twelve-line converged media plan and documented Society of Cable Telecommunications Engineers cue-tone standard (SCTE-35) delivery signal risks.",
  "The Sales Rep": "Built a twelve-line converged media plan and documented Society of Cable Telecommunications Engineers cue-tone standard (SCTE-35) delivery signal risks.",
  "compliance-bot": "Validated each placement against compliance restrictions and proposed approved replacements.",
  "The Compliance Bot": "Validated each placement against compliance restrictions and proposed approved replacements.",
  "ops-director": "Triggered StreamX make-good reallocation and recalculated return on investment performance.",
  "The Ops Director": "Triggered StreamX make-good reallocation and recalculated return on investment performance."
};

const RAW_DATA_BY_AGENT = {
  planner: datasetEntries.find((d) => d.key === "audienceGraph"),
  "sales-rep": datasetEntries.find((d) => d.key === "inventoryMatrix"),
  "compliance-bot": datasetEntries.find((d) => d.key === "globalLawRegistry"),
  "ops-director": datasetEntries.find((d) => d.key === "liveDeliveryLog")
};
const ARCHITECT_PLAN_FALLBACK_TITLES = [
  "Architecture Option 1",
  "Architecture Option 2",
  "Architecture Option 3"
];
const MIN_ARCHITECT_AGENTS = 5;
const MAX_ARCHITECT_AGENTS = 6;
const TARGET_ARCHITECT_AGENTS = 5;
const PLAN_VARIANTS = [
  {
    key: "growth",
    title: "Architecture Option 1",
    promptTile: "Growth Acceleration Prompt",
    promptText: "Design a growth-first architecture that expands audience reach quickly while preserving spend efficiency.",
    strategy: "Growth-first orchestration with aggressive audience and inventory expansion.",
    why: "This option prioritizes faster scale while still maintaining compliance and pacing controls.",
    library: [
      { nodeId: "growth-audience-strategist", agentName: "Growth Audience Strategist", phase: 1, focus: "high-propensity segment expansion and deterministic lookalike logic" },
      { nodeId: "rapid-segmentation-analyst", agentName: "Rapid Segmentation Analyst", phase: 1, focus: "speed-focused segmentation thresholds and match confidence review" },
      { nodeId: "reach-amplification-planner", agentName: "Reach Amplification Planner", phase: 2, focus: "high-reach linear and streaming blend for rapid coverage gains" },
      { nodeId: "cost-velocity-optimizer", agentName: "Cost and Velocity Optimizer", phase: 3, focus: "spend efficiency guardrails while scaling impression velocity" },
      { nodeId: "growth-compliance-manager", agentName: "Growth Compliance Manager", phase: 4, focus: "compliance-safe expansion with country and category restrictions" },
      { nodeId: "growth-ops-director", agentName: "Growth Operations Director", phase: 5, focus: "pacing recovery actions that preserve scale and conversion momentum" }
    ]
  },
  {
    key: "compliance",
    title: "Architecture Option 2",
    promptTile: "Compliance-First Prompt",
    promptText: "Design a compliance-first architecture that screens legal and policy risk before media spend allocation.",
    strategy: "Policy-first orchestration that minimizes legal and regulatory rework.",
    why: "This option reduces risk by validating market and category eligibility earlier in the flow.",
    library: [
      { nodeId: "policy-intake-analyst", agentName: "Policy Intake Analyst", phase: 1, focus: "campaign claim and category intake against policy scope" },
      { nodeId: "jurisdiction-screening-lead", agentName: "Jurisdiction Screening Lead", phase: 1, focus: "country-level pre-screening and restricted-market routing constraints" },
      { nodeId: "compliance-bot", agentName: "The Compliance Bot", phase: 2, focus: "line-item policy validation and replacement recommendation matrix" },
      { nodeId: "regulatory-evidence-curator", agentName: "Regulatory Evidence Curator", phase: 3, focus: "source-level audit evidence and legal traceability records" },
      { nodeId: "safe-inventory-planner", agentName: "Safe Inventory Planner", phase: 4, focus: "approved inventory-only media construction with fallback routes" },
      { nodeId: "compliance-ops-director", agentName: "Compliance Operations Director", phase: 5, focus: "delivery monitoring with strict policy-safe make-good execution" }
    ]
  },
  {
    key: "operations",
    title: "Architecture Option 3",
    promptTile: "Operational Resilience Prompt",
    promptText: "Design an operations-first architecture that maximizes execution resilience, pacing reliability, and response to delivery shocks.",
    strategy: "Operations-first orchestration focused on resilience, monitoring, and intervention readiness.",
    why: "This option is optimized for stable delivery under variable inventory and signal conditions.",
    library: [
      { nodeId: "execution-readiness-planner", agentName: "Execution Readiness Planner", phase: 1, focus: "handoff readiness, dependency checks, and activation prerequisites" },
      { nodeId: "signal-risk-analyst", agentName: "Signal Risk Analyst", phase: 2, focus: "broadcast and streaming signal-health risk diagnostics" },
      { nodeId: "resilient-media-planner", agentName: "Resilient Media Planner", phase: 3, focus: "redundant inventory paths and fallback activation design" },
      { nodeId: "contingency-compliance-lead", agentName: "Contingency Compliance Lead", phase: 4, focus: "policy-safe contingency routing during delivery disruptions" },
      { nodeId: "ops-director", agentName: "The Ops Director", phase: 5, focus: "live pacing corrections and make-good control strategy" },
      { nodeId: "diagnostics-and-explanation-analyst", agentName: "Diagnostics and Explanation Analyst", phase: 6, focus: "root-cause analysis and detailed stakeholder explanation outputs" }
    ]
  }
];
const RAW_DATA_BY_KEY = {
  audienceGraph: datasetEntries.find((d) => d.key === "audienceGraph"),
  inventoryMatrix: datasetEntries.find((d) => d.key === "inventoryMatrix"),
  globalLawRegistry: datasetEntries.find((d) => d.key === "globalLawRegistry"),
  liveDeliveryLog: datasetEntries.find((d) => d.key === "liveDeliveryLog")
};

const DATA_QUALITY = computeDataQuality(datasets.audienceGraph || []);

let config = {};
let state = {
  selectedDemoIndex: null,
  stage: "idle",
  architectPlans: [],
  selectedArchitectPlanId: null,
  plan: [],
  suggestedInputs: [],
  selectedInputs: new Set(),
  uploads: [],
  notes: "",
  agentOutputs: [],
  architectBuffer: "",
  error: "",
  customProblem: null,
  focusedNodeId: null,
  runningNodeIds: new Set(),
  latestNodeId: null,
  flowOrientation: "horizontal",
  flowColumns: 2,
  savedAgents: [],
  supabaseConfigured: false,
  session: null,
  editingAgentId: null,
  campaignPrompt: "",
  rawDataOpen: new Set(),
  dashboard: null,
  rawDataByAgent: RAW_DATA_BY_AGENT,
  issueNodeIds: new Set(),
  dataQuality: DATA_QUALITY,
  approvedPlanCsv: "",
  pacingMode: "pct",
  complianceDetails: null,
  complianceExplanationOpen: false,
  visualizationExplanationOpen: false,
  visualizationLoading: false,
  visualizationNarrative: "",
  runToken: 0
};

const llmSession = { creds: null };
const actions = {
  planDemo: (i) => {
    selectDemo(i);
    runArchitect();
  },
  chooseArchitectPlan: (id) => {
    const picked = state.architectPlans.find((plan) => plan.id === id);
    if (!picked) return;
    const selectedPlan = expandAndEnrichPlan(
      picked.plan || [],
      clampAgentCount($("#max-agents")?.value, TARGET_ARCHITECT_AGENTS),
      state.campaignPrompt || "",
      picked.variantKey || "growth"
    );
    const normalizedInputs = buildDatasetInputs().map((input) => ({ ...input, id: Utils.uniqueId("input") }));
    setState({
      selectedArchitectPlanId: picked.id,
      plan: selectedPlan,
      suggestedInputs: normalizedInputs,
      selectedInputs: new Set(normalizedInputs.map((i) => i.id)),
      rawDataByAgent: buildRawDataByPlan(selectedPlan),
      complianceDetails: null,
      complianceExplanationOpen: false,
      visualizationExplanationOpen: false,
      visualizationLoading: false,
      visualizationNarrative: "",
      runToken: 0,
      stage: "data",
      error: ""
    });
  },
  startAgents: () => startAgents(),
  toggleSuggestedInput: (id) => {
    const next = new Set(state.selectedInputs);
    next.has(id) ? next.delete(id) : next.add(id);
    setState({ selectedInputs: next });
  },
  handleFileUpload: (e) => {
    Array.from(e.target.files || []).forEach(file => {
      const reader = new FileReader();
      reader.onload = () => setState({ uploads: [...state.uploads, { id: Utils.uniqueId("upload"), title: file.name, type: Utils.inferTypeFromName(file.name), content: reader.result?.toString() || "", meta: { size: file.size } }] });
      reader.readAsText(file);
    });
    e.target.value = "";
  },
  removeUpload: (id) => setState({ uploads: state.uploads.filter(u => u.id !== id) }),
  setNotes: (val) => setState({ notes: val }),
  setCampaignPrompt: (val) => setState({ campaignPrompt: val }),
  toggleRawData: (id) => {
    const next = new Set(state.rawDataOpen);
    next.has(id) ? next.delete(id) : next.add(id);
    setState({ rawDataOpen: next });
  },
  setPacingMode: (mode) => {
    if (mode !== "pct" && mode !== "imp") return;
    setState({ pacingMode: mode });
  },
  toggleComplianceExplanation: () => setState({ complianceExplanationOpen: !state.complianceExplanationOpen }),
  toggleVisualizationExplanation: () => setState({ visualizationExplanationOpen: !state.visualizationExplanationOpen }),
  exportApprovedPlan: () => {
    const csv = state.approvedPlanCsv || buildApprovedPlanCsv();
    if (!csv) return;
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "wbd-approved-plan.csv";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  },
  handleCustomProblemSubmit: (e) => {
    e.preventDefault();
    const val = $("#custom-problem")?.value?.trim();
    if (!val) return setState({ error: "Enter a problem first.", stage: "idle" }); // Fix focus later
    selectCustomProblem(val);
    runArchitect();
  },
  // Supabase Actions
  configureSupabase: async () => {
    // If config is missing, prompt user to edit config.json for persistence, or use prompt for temp session
    const choice = confirm("To configure Supabase permanently, add your URL and Key to 'config.json'.\n\nClick OK to reload config from file (if you edited it).\nClick Cancel to enter credentials temporarily for this session.");

    if (choice) {
      // Reload config
      try {
        const newConfig = await fetch("config.json").then(r => r.json());
        const url = newConfig.supabase?.url;
        const key = newConfig.supabase?.key;
        if (url && key) {
          const ok = await Storage.init(url, key);
          setState({ supabaseConfigured: ok, session: Storage.getSession() });
          if (ok) refreshAgents();
          alert(ok ? "Supabase connected from config.json!" : "Connection failed even with config.");
        } else {
          alert("No credentials found in config.json. Please edit the file.");
        }
      } catch (e) { alert("Error reloading config: " + e.message); }
    } else {
      const url = prompt("Enter Supabase Project URL (e.g., https://xyz.supabase.co):");
      const key = prompt("Enter Supabase PUBLIC ANON Key:\n(Note: This key is safe to use in the browser as long as your database has RLS enabled. Do NOT use the Service Role key.)");
      if (url && key) {
        localStorage.setItem("supabase_url", url);
        localStorage.setItem("supabase_key", key);
        const ok = await Storage.init(url, key);
        setState({ supabaseConfigured: ok, session: Storage.getSession() });
        if (ok) refreshAgents();
      }
    }
  },
  login: async () => {
    try { await Storage.login(); } catch (e) { alert(e.message); }
  },
  logout: async () => {
    await Storage.logout();
    setState({ session: null, savedAgents: [] });
  },
  saveAgent: async () => {
    // If editing, try to find existing title
    const existingTitle = state.editingAgentId
      ? state.savedAgents.find(a => a.id === state.editingAgentId)?.title
      : "";

    const title = prompt("Name your agent:", existingTitle || "My Custom Agent");
    if (!title) return;
    try {
      const inputsToSave = state.suggestedInputs.filter(i => state.selectedInputs.has(i.id));
      const agent = {
        id: state.editingAgentId || crypto.randomUUID(),
        title,
        problem: state.selectedDemoIndex === -1 ? state.customProblem?.problem : config.demos[state.selectedDemoIndex]?.problem,
        plan: state.plan,
        inputs: inputsToSave
      };
      await Storage.saveAgent(agent);
      refreshAgents();
      // Keep edit ID so subsequent saves update the same agent, or clear? 
      // User might want to version. But typically "Save" means save this.
      // Let's keep it.
      setState({ editingAgentId: agent.id });
      alert("Agent saved!");
    } catch (e) { alert("Save failed: " + e.message); }
  },
  deleteAgent: async (id) => {
    if (!confirm("Delete this agent?")) return;
    try {
      await Storage.deleteAgent(id);
      refreshAgents();
    } catch (e) { alert("Delete failed: " + e.message); }
  },
  loadSavedAgent: (agent) => {
    // Directly go to data/run stage, skipping architect
    // User requirement: "these saved agents should not again call plan or architect"
    // We load them into the state as if they were just planned.
    const savedMax = clampAgentCount($("#max-agents")?.value, TARGET_ARCHITECT_AGENTS);
    const savedPlan = expandAndEnrichPlan(agent.plan || [], savedMax, agent.problem || "");
    const savedInputs = (agent.inputs && agent.inputs.length)
      ? agent.inputs.map((input) => ({ ...input, id: input.id || Utils.uniqueId("input") }))
      : buildDatasetInputs().map((input) => ({ ...input, id: Utils.uniqueId("input") }));
    setState({
      selectedDemoIndex: -2, // Special index for saved agent
      customProblem: { title: agent.title, problem: agent.problem },
      architectPlans: [],
      selectedArchitectPlanId: null,
      plan: savedPlan,
      suggestedInputs: savedInputs,
      selectedInputs: new Set(savedInputs.map(i => i.id)),
      stage: "data", // Ready to start inputs or run
      agentOutputs: [],
      error: "",
      editingAgentId: null, // Clear separate edit session
      campaignPrompt: agent.problem || "",
      rawDataByAgent: buildRawDataByPlan(savedPlan),
      complianceDetails: hasComplianceRole(savedPlan) ? buildComplianceDetails() : null,
      complianceExplanationOpen: false,
      visualizationExplanationOpen: false,
      visualizationLoading: false,
      visualizationNarrative: "",
      runToken: 0
    });
  },
  editAgent: (agent) => {
    // Populate custom problem and enter edit mode
    const textarea = $("#custom-problem");
    if (textarea) textarea.value = agent.problem;

    // Reset to idle so we don't auto-scroll to 'data' or 'run' via the main render loop
    setState({
      editingAgentId: agent.id,
      error: "",
      stage: "idle",
      plan: [],
      agentOutputs: [],
      selectedDemoIndex: -1 // Ensure we are in custom mode
    });

    // UI Feedback & Focus
    setTimeout(() => {
      const section = $("#custom-problem-section");
      if (section) section.scrollIntoView({ behavior: "smooth", block: "center" });
      if (textarea) textarea.focus();
    }, 50);
  }
};

async function refreshAgents() {
  const agents = await Storage.listAgents();
  setState({ savedAgents: agents });
}

// Initialization
(async () => {
  config = await fetch("config.json").then(r => r.json());
  config.demos = (config.demos || []).map(d => {
    const inputs = d.id === WBD_DEMO_ID ? buildDatasetInputs() : (d.inputs || []);
    return { ...d, inputs: inputs.map(i => ({ ...i, id: Utils.uniqueId("input") })) };
  });

  const defaults = config.defaults || {};
  if ($("#model") && !$("#model").value) $("#model").value = defaults.model || "gpt-5-mini";
  if ($("#architect-prompt")) $("#architect-prompt").value = defaults.architectPrompt || "";

  setState({
    flowOrientation: Utils.normalizeFlowOrientation(defaults.flowOrientation),
    flowColumns: Utils.clampFlowColumns(defaults.flowColumns)
  });

  // Pre-fill settings form
  if ($("#model")) $("#model").value = defaults.model || "gpt-5-mini";
  if ($("#architect-prompt")) $("#architect-prompt").value = defaults.architectPrompt || "";
  if ($("#agent-style")) $("#agent-style").value = defaults.agentStyle || "";
  if ($("#max-agents")) $("#max-agents").value = clampAgentCount(defaults.maxAgents, TARGET_ARCHITECT_AGENTS);
  if ($("#flow-orientation")) $("#flow-orientation").value = Utils.normalizeFlowOrientation(defaults.flowOrientation);
  if ($("#flow-columns")) $("#flow-columns").value = Utils.clampFlowColumns(defaults.flowColumns);

  if ($("#flow-columns")) $("#flow-columns").value = Utils.clampFlowColumns(defaults.flowColumns);

  // Init Supabase from config.json if present, else localStorage
  let sbUrl = config.supabase?.url || localStorage.getItem("supabase_url");
  let sbKey = config.supabase?.key || localStorage.getItem("supabase_key");

  // If config has placeholders, ignore them
  if (sbUrl === "") sbUrl = null;
  if (sbKey === "") sbKey = null;

  if (sbUrl && sbKey) {
    const ok = await Storage.init(sbUrl, sbKey);
    setState({ supabaseConfigured: ok, session: Storage.getSession() });
    if (ok) refreshAgents();
  }

  View.renderDemoCards($("#demo-cards"), config.demos, state.savedAgents, state, actions);
  render();

  // Event Listeners
  $("#configure-llm")?.addEventListener("click", async () => llmSession.creds = await openaiConfig({ defaultBaseUrls: DEFAULT_BASE_URLS, show: true }));
  $("#custom-problem-form")?.addEventListener("submit", actions.handleCustomProblemSubmit);

  // Supabase UI Bindings (We will add these buttons in View)
  // For global nav buttons that might not be re-rendered:
  // We'll rely on View to render auth buttons inside the app or a specific container if we add one.
  // Actually, let's just use the View.renderApp to handling the auth UI.

  const settingsForm = saveform("#settings-form");
  $("#settings-form")?.addEventListener("reset", () => setTimeout(() => {
    settingsForm.clear();
    // simpler to just call scheduleSync
    scheduleFlowchartSync();
  }, 0));

  const updateLayout = () => {
    const o = $("#flow-orientation")?.value, c = $("#flow-columns")?.value;
    if (o && c) setState({ flowOrientation: Utils.normalizeFlowOrientation(o), flowColumns: Utils.clampFlowColumns(c) });
  };
  $("#flow-orientation")?.addEventListener("change", updateLayout);
  $("#flow-columns")?.addEventListener("input", updateLayout);

  window.addEventListener('auth-changed', (e) => {
    setState({ session: e.detail });
    if (e.detail) refreshAgents();
    else setState({ savedAgents: [] });
  });
})();

function setState(updates) {
  Object.assign(state, updates);
  render();
}

function render() {
  View.renderDemoCards($("#demo-cards"), config.demos, state.savedAgents, state, actions);
  View.renderApp($("#output"), state, config, actions);
  View.renderAuth($("#auth-controls"), state, actions);
  syncCustomButton();
  scheduleFlowchartSync();
  scheduleScrollToRunningSection();
  scheduleDashboardCharts();
}

function clampAgentCount(value, fallback = TARGET_ARCHITECT_AGENTS) {
  const parsed = Number.parseInt(value, 10);
  const safe = Number.isFinite(parsed) ? parsed : fallback;
  return Math.min(MAX_ARCHITECT_AGENTS, Math.max(MIN_ARCHITECT_AGENTS, safe));
}

// Logic
function selectDemo(index) {
  const demo = config.demos[index];
  if (!demo) {
    setState({ error: "Demo not found.", stage: "idle" });
    return;
  }
  const inputs = (demo.inputs || []).map(i => ({ ...i, id: Utils.uniqueId("input") }));
  resetRunState({
    selectedDemoIndex: index,
    suggestedInputs: inputs,
    selectedInputs: new Set(inputs.map(i => i.id)),
    editingAgentId: null,
    campaignPrompt: demo.problem || "",
    rawDataOpen: new Set(),
    dashboard: null,
    approvedPlanCsv: ""
  });
  
  // Store demo reference for immediate access in runArchitect
  state.selectedDemo = demo;
}

function selectCustomProblem(problem) {
  resetRunState({ selectedDemoIndex: -1, customProblem: { title: "Custom", body: "User Brief", problem, inputs: [] }, campaignPrompt: problem || "" });
}

function resetRunState(extras) {
  setState({
    ...extras,
    stage: "architect",
    architectPlans: [],
    selectedArchitectPlanId: null,
    plan: [],
    agentOutputs: [],
    architectBuffer: "",
    error: "",
    focusedNodeId: null,
    runningNodeIds: new Set(),
    latestNodeId: null,
    dashboard: null,
    rawDataOpen: new Set(),
    issueNodeIds: new Set(),
    approvedPlanCsv: "",
    rawDataByAgent: RAW_DATA_BY_AGENT,
    complianceDetails: null,
    complianceExplanationOpen: false,
    visualizationExplanationOpen: false,
    visualizationLoading: false,
    visualizationNarrative: "",
    runToken: 0
  });
}

function buildDatasetInputs() {
  return datasetEntries.map((entry) => ({
    title: entry.title,
    type: entry.type,
    content: entry.content
  }));
}

function buildDatasetContext(dataKey) {
  if (!dataKey || !datasets[dataKey] || !datasetMeta[dataKey]) {
    return "No dataset available for this agent.";
  }
  const meta = datasetMeta[dataKey];
  const rows = datasets[dataKey];
  const csv = toCsv(rows, meta.columns);
  return `${meta.title} (${rows.length} rows)\n${csv}`;
}

function buildDashboard(agentOutputs = [], runContext = {}) {
  const profile = buildRunProfile(agentOutputs, runContext);
  return {
    pacing: buildPacingSeries(datasets.liveDeliveryLog || [], profile),
    reach: buildReachSummary(datasets.audienceGraph || [], datasets.liveDeliveryLog || [], profile),
    actions: buildActionRows(agentOutputs, state.issueNodeIds || new Set()),
    makeGood: buildMakeGoodSummary(datasets.liveDeliveryLog || [], profile)
  };
}

function buildRunProfile(agentOutputs = [], runContext = {}) {
  const outputText = (agentOutputs || [])
    .map((agent) => `${agent.nodeId || ""}|${agent.name || ""}|${stripMarkdown(agent.text || "").slice(0, 600)}`)
    .join("|");
  const seedSource = [
    runContext.campaignPrompt || "",
    runContext.selectedPlanId || "",
    runContext.runToken || 0,
    outputText
  ].join("::");
  const seed = hashString(seedSource);
  return {
    seed,
    waveShift: seededInt(seed, 1, 7, 1),
    planShift: seededRange(seed, -0.07, 0.07, 2),
    linearDeliveryShift: seededRange(seed, -0.1, 0.08, 3),
    digitalDeliveryShift: seededRange(seed, -0.06, 0.11, 4),
    overlapShift: seededRange(seed, -6, 7, 5),
    reachShift: seededRange(seed, 0.88, 1.16, 6),
    deviceShift: seededRange(seed, 0.9, 1.22, 7),
    makeGoodHour: seededInt(seed, 10, 15, 8),
    linearDipFactor: seededRange(seed, 0.48, 0.82, 9),
    digitalBoostFactor: seededRange(seed, 1.07, 1.3, 10),
    makeGoodBudget: seededInt(seed, 2400, 4300, 11),
    roiBase: seededRange(seed, -0.08, 0.22, 12),
    roiLift: seededRange(seed, 0.02, 0.22, 13)
  };
}

function hashString(value = "") {
  let hash = 2166136261;
  const str = String(value);
  for (let i = 0; i < str.length; i += 1) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function seededRange(seed, min, max, offset = 0) {
  const mixed = (seed + Math.imul(offset + 1, 2654435761)) >>> 0;
  const ratio = mixed / 4294967295;
  return min + (max - min) * ratio;
}

function seededInt(seed, min, max, offset = 0) {
  return Math.round(seededRange(seed, min, max, offset));
}

function clampNumber(value, min, max) {
  return Math.min(max, Math.max(min, Number(value)));
}

function roundToTwo(value) {
  return Math.round(Number(value) * 100) / 100;
}

function buildPacingSeries(logs, profile = {}) {
  const hours = Array.from({ length: 24 }, (_, i) => i + 1);
  const linearPct = [];
  const digitalPct = [];
  const linearImp = [];
  const digitalImp = [];
  const linearPlannedArr = [];
  const digitalPlannedArr = [];
  hours.forEach((hour) => {
    const rows = logs.filter((l) => l.hour_of_day === hour);
    const linearRows = rows.filter((r) => r.platform_type === "linear");
    const digitalRows = rows.filter((r) => r.platform_type === "digital");
    const baseLinearPlanned = linearRows.reduce((sum, r) => sum + r.planned_impressions, 0);
    const baseLinearDelivered = linearRows.reduce((sum, r) => sum + r.delivered_impressions, 0);
    const baseDigitalPlanned = digitalRows.reduce((sum, r) => sum + r.planned_impressions, 0);
    const baseDigitalDelivered = digitalRows.reduce((sum, r) => sum + r.delivered_impressions, 0);

    const wave = Math.sin((hour + (profile.waveShift || 0)) / 3) * 0.05;
    const linearPlanFactor = clampNumber(1 + (profile.planShift || 0) + wave * 0.4, 0.82, 1.2);
    const digitalPlanFactor = clampNumber(1 + (profile.planShift || 0) - wave * 0.3, 0.82, 1.2);

    let linearPlanned = Math.round(baseLinearPlanned * linearPlanFactor);
    let digitalPlanned = Math.round(baseDigitalPlanned * digitalPlanFactor);
    let linearDelivered = Math.round(baseLinearDelivered * clampNumber(1 + (profile.linearDeliveryShift || 0) + wave, 0.55, 1.35));
    let digitalDelivered = Math.round(baseDigitalDelivered * clampNumber(1 + (profile.digitalDeliveryShift || 0) - wave * 0.5, 0.6, 1.45));

    if (hour === profile.makeGoodHour) {
      linearDelivered = Math.round(linearDelivered * clampNumber(profile.linearDipFactor || 0.58, 0.45, 0.85));
    }
    if (hour === profile.makeGoodHour + 1) {
      digitalDelivered = Math.round(digitalDelivered * clampNumber(profile.digitalBoostFactor || 1.18, 1.05, 1.32));
    }

    linearPlanned = Math.max(0, linearPlanned);
    digitalPlanned = Math.max(0, digitalPlanned);
    linearDelivered = Math.max(0, linearDelivered);
    digitalDelivered = Math.max(0, digitalDelivered);

    linearPct.push(linearPlanned ? Math.round((linearDelivered / linearPlanned) * 100) : 0);
    digitalPct.push(digitalPlanned ? Math.round((digitalDelivered / digitalPlanned) * 100) : 0);
    linearImp.push(linearDelivered);
    digitalImp.push(digitalDelivered);
    linearPlannedArr.push(linearPlanned);
    digitalPlannedArr.push(digitalPlanned);
  });
  return {
    hours,
    linearPct,
    digitalPct,
    linearImp,
    digitalImp,
    linearPlanned: linearPlannedArr,
    digitalPlanned: digitalPlannedArr,
    makeGoodHour: profile.makeGoodHour || 12
  };
}

function buildReachSummary(audience, logs, profile = {}) {
  const autoIntenders = audience.filter((row) => row.segment_name === "Auto Intenders" && row.auto_intender_score > 70);
  const uniqueMap = new Map();
  autoIntenders.forEach((row) => {
    if (!uniqueMap.has(row.olli_household_id)) {
      uniqueMap.set(row.olli_household_id, row);
    }
  });
  const baseUniqueHouseholds = uniqueMap.size;
  const baseDeviceCount = [...uniqueMap.values()].reduce((sum, row) => sum + (row.device_count || 0), 0);
  const uniqueHouseholds = Math.max(1, Math.round(baseUniqueHouseholds * clampNumber(profile.reachShift || 1, 0.84, 1.18)));
  const deviceCount = Math.max(1, Math.round(baseDeviceCount * clampNumber(profile.deviceShift || 1, 0.86, 1.2)));
  const scaledReach = Math.round((uniqueHouseholds * 32500) / 100) * 100;
  const totals = logs.reduce(
    (acc, row) => {
      acc[row.platform_type] += row.delivered_impressions;
      return acc;
    },
    { linear: 0, digital: 0 }
  );
  const adjustedLinear = Math.round(totals.linear * clampNumber(1 + (profile.linearDeliveryShift || 0), 0.7, 1.35));
  const adjustedDigital = Math.round(totals.digital * clampNumber(1 + (profile.digitalDeliveryShift || 0), 0.7, 1.4));
  const totalImpressions = adjustedLinear + adjustedDigital || 1;
  const linearPct = Math.round((adjustedLinear / totalImpressions) * 100);
  const overlapPct = Math.round(
    logs.reduce((sum, row) => sum + (row.viewer_overlap_pct || 0), 0) / Math.max(logs.length, 1)
  ) + Math.round(profile.overlapShift || 0);
  return {
    uniqueHouseholds,
    deviceCount,
    scaledReach,
    linearPct,
    digitalPct: 100 - linearPct,
    overlapPct: Math.round(clampNumber(overlapPct, 4, 64))
  };
}

function buildActionRows(agentOutputs = [], issueNodeIds = new Set()) {
  if (!Array.isArray(agentOutputs) || !agentOutputs.length) return [];
  return [...agentOutputs]
    .sort((a, b) => (a.startedAt || 0) - (b.startedAt || 0))
    .map((agent) => {
      const issueReason = extractIssueReason(agent.text || "");
      const hasIssue = issueNodeIds?.has(agent.nodeId) && !!issueReason;
      const summary = buildActionSummary(agent, issueReason);
      const status = agent.status === "error" ? "error" : hasIssue ? "issue" : "done";
      return {
        id: agent.nodeId,
        agent: agent.name,
        time: agent.completedAt || agent.startedAt || Date.now(),
        status,
        summary
      };
    });
}

function buildActionSummary(agent, issueReason = "") {
  const main = extractMainSummary(agent.text || "");
  const why = extractWhyStatement(agent.text || "") || fallbackWhyForRole(agent);
  if (issueReason) {
    return `${main} Why: ${why} Issue detail: ${issueReason}`;
  }
  return `${main} Why: ${why}`;
}

function stripMarkdown(text = "") {
  return (text || "")
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/^#{1,6}\s+/gm, "")
    .replace(/\*\*(.*?)\*\*/g, "$1")
    .replace(/\*(.*?)\*/g, "$1")
    .replace(/`([^`]*)`/g, "$1")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/\s+/g, " ")
    .trim();
}

function firstSentences(text = "", max = 2) {
  const clean = stripMarkdown(text);
  if (!clean) return "";
  const sentences = clean.match(/[^.!?]+[.!?]?/g) || [];
  return sentences.slice(0, max).map((s) => s.trim()).join(" ");
}

function extractMainSummary(text = "") {
  const candidate = firstSentences(text, 2);
  if (candidate) return candidate;
  return "This step produced no narrative detail from the current response.";
}

function extractWhyStatement(text = "") {
  const clean = stripMarkdown(text);
  if (!clean) return "";
  const sentences = clean.match(/[^.!?]+[.!?]?/g) || [];
  const whySentence = sentences.find((sentence) => /\bwhy\b/i.test(sentence));
  if (whySentence) return whySentence.trim();
  return "";
}

function fallbackWhyForRole(agent) {
  const role = inferAgentRole(agent);
  if (role === "planner") return "This audience decision matters because downstream spend should be based on reliable household targeting.";
  if (role === "sales-rep") return "This media allocation matters because channel mix directly affects delivery efficiency and cost.";
  if (role === "compliance-bot") return "This compliance decision matters because non-compliant placements can block activation and create legal risk.";
  if (role === "ops-director") return "This operational action matters because pacing recovery preserves campaign outcomes.";
  return "This action matters because it influences downstream orchestration quality and execution reliability.";
}

function extractIssueReason(text = "") {
  const clean = stripMarkdown(text);
  if (!clean) return "";
  const sentences = clean.match(/[^.!?]+[.!?]?/g) || [];
  const signalSentence = sentences.find((sentence) =>
    /\b(violation|non-compliant|banned|blocked|restriction|regulation|redline)\b/i.test(sentence)
  );
  return signalSentence ? signalSentence.trim() : "";
}

function computeDataQuality(audience = []) {
  const idCounts = new Map();
  audience.forEach((row) => {
    const id = row.olli_household_id;
    if (!id) return;
    idCounts.set(id, (idCounts.get(id) || 0) + 1);
  });
  const duplicateIds = [...idCounts.entries()].filter(([, count]) => count > 1);
  const nullDma = audience.filter((row) => row.dma_region == null).length;
  return {
    duplicateCount: duplicateIds.length,
    nullDmaCount: nullDma
  };
}

function buildApprovedPlanCsv() {
  const rows = buildApprovedPlanRows(datasets.inventoryMatrix || []);
  const columns = ["line_item_id", "network", "platform_type", "country_code", "daypart", "impressions", "spend_usd", "cpm_30s", "status"];
  return toCsv(rows, columns);
}

function buildApprovedPlanRows(inventory) {
  if (!inventory.length) return [];
  const approved = inventory.filter((row) => row.country_code !== "SA");
  const rows = approved.slice(0, 12).map((row, idx) => {
    const impressions = Math.round(row.avail_impressions_30s * 0.6);
    const spend = Math.round((impressions / 1000) * row.cpm_30s);
    return {
      line_item_id: `LINE-${String(idx + 1).padStart(3, "0")}`,
      network: row.network,
      platform_type: row.platform_type,
      country_code: row.country_code,
      daypart: row.daypart,
      impressions,
      spend_usd: spend,
      cpm_30s: row.cpm_30s,
      status: "approved"
    };
  });

  if (rows.length < 12) {
    const filler = inventory[0];
    rows.push({
      line_item_id: `LINE-${String(rows.length + 1).padStart(3, "0")}`,
      network: filler.network,
      platform_type: filler.platform_type,
      country_code: "US",
      daypart: filler.daypart,
      impressions: 120000,
      spend_usd: 8400,
      cpm_30s: filler.cpm_30s,
      status: "approved"
    });
  }

  return rows;
}

function buildMakeGoodSummary(logs, profile = {}) {
  const linear = logs.filter((row) => row.platform_type === "linear");
  const linearPlanned = linear.reduce((sum, row) => sum + row.planned_impressions, 0);
  const linearDelivered = linear.reduce((sum, row) => sum + row.delivered_impressions, 0) * clampNumber(1 + (profile.linearDeliveryShift || 0), 0.65, 1.25);
  const linearPct = linearPlanned ? linearDelivered / linearPlanned : 1;
  const beforeRoi = roundToTwo(1.58 + (profile.roiBase || 0));
  const lift = linearPct < 0.9 ? (0.18 + (profile.roiLift || 0)) : (0.08 + (profile.roiLift || 0) * 0.5);
  const afterRoi = roundToTwo(beforeRoi + lift);
  return {
    shiftBudget: Math.round(profile.makeGoodBudget || 3200),
    beforeRoi,
    afterRoi
  };
}

function getSelectedDemo() {
  if (state.selectedDemoIndex === -1) return state.customProblem;
  if (state.selectedDemoIndex === -2) return state.customProblem;
  if (state.selectedDemoIndex >= 0) return state.selectedDemo || (config.demos && config.demos[state.selectedDemoIndex]);
  return null;
}

function getCampaignVersionBaseTitle(demo) {
  const raw = (demo?.title || "").toString().trim();
  const match = raw.match(/converged\s+ad\s+campaign\s+version\s+\d+/i);
  if (match) return match[0].replace(/\s+/g, " ").trim();
  if (/converged\s+ad\s+campaign/i.test(raw)) return raw;
  if (raw) return raw;
  return "Converged Ad Campaign";
}

function buildArchitectPlanTitle(demo, optionIndex = 0) {
  const base = getCampaignVersionBaseTitle(demo);
  const optionNumber = optionIndex + 1;
  return `${base} - Architecture Option ${optionNumber}`;
}

function extractArchitectPlanCandidates(parsed) {
  if (!parsed) return [];
  if (Array.isArray(parsed.architectPlans)) return parsed.architectPlans;
  if (Array.isArray(parsed.plans)) return parsed.plans;
  if (Array.isArray(parsed.plan)) return [{ title: ARCHITECT_PLAN_FALLBACK_TITLES[0], plan: parsed.plan, inputs: parsed.inputs }];
  if (Array.isArray(parsed)) return parsed;
  return [];
}

function parseArchitectJson(rawText) {
  const text = (rawText || "").toString().trim();
  if (!text) return {};
  const direct = Utils.safeParseJson(text);
  if (Object.keys(direct).length) return direct;

  const firstBrace = text.indexOf("{");
  const lastBrace = text.lastIndexOf("}");
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    const sliced = text.slice(firstBrace, lastBrace + 1);
    const parsed = Utils.safeParseJson(sliced);
    if (Object.keys(parsed).length) return parsed;
  }
  return {};
}

function buildFallbackArchitectPlanForVariant(demo, maxAgents, variant) {
  const fallbackPlans = buildFallbackArchitectPlans(demo, maxAgents);
  return fallbackPlans.find((item) => item.variantKey === variant.key) || fallbackPlans[0];
}

function normalizeArchitectPlans(rawPlans, demo, maxAgents) {
  const normalized = (Array.isArray(rawPlans) ? rawPlans : [])
    .map((entry, index) => {
      if (!entry || typeof entry !== "object") return null;
      const variant = PLAN_VARIANTS[index % PLAN_VARIANTS.length];
      const planList = entry.plan || entry.agents || entry.workflow || entry.steps || [];
      const normalizedPlan = Utils.normalizePlan(planList, maxAgents);
      if (!normalizedPlan.length) return null;
      const variantKey = entry.variantKey || variant.key;
      const variantIndex = PLAN_VARIANTS.findIndex((item) => item.key === variantKey);
      const optionIndex = variantIndex >= 0 ? variantIndex : index;
      const fallbackTitle = buildArchitectPlanTitle(demo, optionIndex);
      const enrichedPlan = expandAndEnrichPlan(normalizedPlan, maxAgents, demo?.problem || demo?.body || "", variantKey);
      return {
        id: Utils.uniqueId("architect-plan"),
        title: fallbackTitle,
        strategy: (entry.strategy || entry.summary || variant.strategy).toString().trim(),
        why: (entry.why || entry.rationale || variant.why).toString().trim(),
        promptTile: entry.promptTile || variant.promptTile,
        promptText: entry.promptText || variant.promptText,
        variantKey,
        plan: enrichedPlan,
        inputs: buildDatasetInputs()
      };
    })
    .filter(Boolean)
    .slice(0, 3);

  if (normalized.length === 3) return normalized;
  const fallback = buildFallbackArchitectPlans(demo, maxAgents);
  fallback.forEach((plan) => {
    if (normalized.length < 3) normalized.push(plan);
  });
  return normalized.slice(0, 3);
}

function buildFallbackArchitectPlans(demo, maxAgents) {
  const campaign = (demo?.problem || demo?.body || "Launch a converged campaign").toString().trim();
  return PLAN_VARIANTS.map((variant, index) => {
    const seedPlan = (variant.library || []).slice(0, Math.min(3, maxAgents)).map((node, idx) => ({
      agentName: node.agentName,
      nodeId: node.nodeId,
      initialTask: `Stage ${idx + 1} task for ${node.agentName} in campaign: ${campaign}.`,
      systemInstruction: `Execute ${node.focus} and provide one explicit Why this matters statement.`,
      stage: idx + 1
    }));
    return {
      id: Utils.uniqueId("architect-plan"),
      title: buildArchitectPlanTitle(demo, index),
      strategy: variant.strategy,
      why: variant.why,
      promptTile: variant.promptTile,
      promptText: variant.promptText,
      variantKey: variant.key,
      plan: expandAndEnrichPlan(Utils.normalizePlan(seedPlan, maxAgents), maxAgents, campaign, variant.key),
      inputs: buildDatasetInputs()
    };
  });
}

function expandAndEnrichPlan(plan = [], maxAgents = TARGET_ARCHITECT_AGENTS, campaign = "", variantKey = "growth") {
  const targetCount = Math.min(maxAgents, TARGET_ARCHITECT_AGENTS);
  const variant = PLAN_VARIANTS.find((item) => item.key === variantKey) || PLAN_VARIANTS[0];
  const agentLibrary = variant.library || [];
  const existing = Array.isArray(plan) ? [...plan] : [];
  const usedIds = new Set(existing.map((agent) => agent.nodeId));

  for (const template of agentLibrary) {
    if (existing.length >= targetCount) break;
    if (usedIds.has(template.nodeId)) continue;
    existing.push({
      nodeId: template.nodeId,
      agentName: template.agentName,
      systemInstruction: "",
      initialTask: "",
      graphTargets: [],
      graphIncoming: [],
      phase: template.phase,
      phaseLabel: `Stage ${template.phase}`,
      branchKey: null
    });
    usedIds.add(template.nodeId);
  }

  const enriched = existing
    .sort((a, b) => (Number(a.phase) || 0) - (Number(b.phase) || 0))
    .slice(0, maxAgents)
    .map((agent, index) => {
      const template = agentLibrary.find((item) => item.nodeId === agent.nodeId)
        || agentLibrary[index % Math.max(agentLibrary.length, 1)]
        || PLAN_VARIANTS[0].library[0];
      const phase = index + 1;
      const nodeId = template?.nodeId || agent.nodeId || `agent-${phase}`;
      const agentName = template?.agentName || agent.agentName || `Agent ${phase}`;
      const detailedInstruction = buildDetailedSystemInstruction(agent, template, campaign, phase);
      const detailedTask = buildDetailedInitialTask(agent, template, campaign, phase);

      return {
        ...agent,
        nodeId,
        agentName,
        phase,
        phaseLabel: `Stage ${phase}`,
        systemInstruction: ensureWordRange(agent.systemInstruction, detailedInstruction, 45, 90),
        initialTask: ensureWordRange(agent.initialTask, detailedTask, 45, 90)
      };
    });

  return enriched.map((agent, index) => ({
    ...agent,
    graphIncoming: index > 0 ? [enriched[index - 1].nodeId] : [],
    graphTargets: index < enriched.length - 1 ? [enriched[index + 1].nodeId] : []
  }));
}

function ensureWordRange(existingText, fallbackText, minWords, maxWords) {
  let text = normalizeWhitespace(existingText);
  const fallback = normalizeWhitespace(fallbackText);

  if (!text) text = fallback;
  while (wordCount(text) < minWords) {
    text = `${text} ${fallback}`.trim();
  }
  return trimToWords(text, maxWords);
}

function normalizeWhitespace(text) {
  return (text || "").toString().replace(/\s+/g, " ").trim();
}

function wordCount(text) {
  return normalizeWhitespace(text).split(" ").filter(Boolean).length;
}

function trimToWords(text, maxWords) {
  const words = normalizeWhitespace(text).split(" ").filter(Boolean);
  if (words.length <= maxWords) return words.join(" ");
  return `${words.slice(0, maxWords).join(" ")}.`;
}

function buildDetailedSystemInstruction(agent, template, campaign, phase) {
  const role = agent.agentName || template.agentName;
  const focus = template.focus;
  return `You are ${role} in stage ${phase} of a converged campaign workflow. Focus on ${focus}. Use only the provided synthetic datasets and the campaign brief "${campaign}" to make decisions. Explain your reasoning clearly, include one explicit "Why this matters" statement, and show how your output connects to the next stage. Keep language clear for business and technical reviewers. Define abbreviations when they first appear and include key risks, assumptions, and validation checks.`;
}

function buildDetailedInitialTask(agent, template, campaign, phase) {
  const role = agent.agentName || template.agentName;
  const focus = template.focus;
  return `Complete stage ${phase} for ${role} using campaign brief "${campaign}". Prioritize ${focus}. Deliver one detailed section for objective, one for evidence used from the synthetic datasets, one for decisions taken, and one for handoff. Include a clear "Why this matters" statement and name at least one risk with mitigation. Keep the explanation specific but concise so the next stage can execute without ambiguity.`;
}

async function runArchitect() {
  try {
    const demo = getSelectedDemo();

    if (!demo) {
      setState({ error: "Demo not found.", stage: "idle" });
      return;
    }

    if (state.selectedDemoIndex === -2) {
      setState({ stage: "data" });
      return;
    }

    const problemText = (demo.problem || demo.body || "Generate a converged campaign architecture.").toString().trim();
    if (!problemText) {
      setState({ error: "No problem description available for this demo.", stage: "idle" });
      return;
    }

    const maxAgents = clampAgentCount($("#max-agents")?.value, TARGET_ARCHITECT_AGENTS);
    const model = $("#model")?.value || "gpt-5-mini";
    const customArchitectPrompt = ($("#architect-prompt")?.value || "").toString().trim();

    setState({
      stage: "architect",
      architectPlans: [],
      selectedArchitectPlanId: null,
      plan: [],
      suggestedInputs: [],
      selectedInputs: new Set(),
      architectBuffer: "",
      error: "",
      visualizationLoading: false,
      visualizationNarrative: "",
      runToken: 0
    });

    let buffer = "";
    let usedFallback = false;
    let rawCandidates = [];
    let creds = null;

    try {
      creds = await ensureCreds();
    } catch (credsError) {
      usedFallback = true;
      buffer = "Architect connection unavailable. Synthetic architect plans will be loaded for all three variants.";
      setState({ architectBuffer: buffer });
      console.warn("Architect credential fallback activated:", credsError?.message || credsError);
    }

    if (creds) {
      for (const [variantIndex, variant] of PLAN_VARIANTS.entries()) {
        const variantTitle = buildArchitectPlanTitle(demo, variantIndex);
        const header = [
          `Generating ${variantTitle}`,
          `Prompt Tile: ${variant.promptTile}`,
          `Variant Objective: ${variant.promptText}`
        ].join("\n");
        buffer = `${buffer}${buffer ? "\n\n" : ""}${header}\n`;
        setState({ architectBuffer: buffer });

        const agentCatalog = (variant.library || [])
          .map((agent) => `- ${agent.agentName} (${agent.nodeId}), stage ${agent.phase}: ${agent.focus}`)
          .join("\n");

        const systemContent = `${customArchitectPrompt ? `${customArchitectPrompt}\n\n` : ""}You are the architect for one converged advertising campaign workflow variant. Return valid JSON only using this schema:
{
  "title": "${variantTitle}",
  "strategy": "Detailed strategy summary",
  "why": "Detailed rationale",
  "promptTile": "${variant.promptTile}",
  "promptText": "${variant.promptText}",
  "variantKey": "${variant.key}",
  "plan": [
    {
      "agentName": "Agent role",
      "nodeId": "agent-node-id",
      "systemInstruction": "...",
      "initialTask": "...",
      "stage": 1
    }
  ],
  "inputs": []
}
Rules:
- Build exactly one architect plan for variant key "${variant.key}".
- Include between ${MIN_ARCHITECT_AGENTS} and ${maxAgents} agents.
- Include at least one compliance-focused agent in the plan.
- Keep the workflow aligned with this strategy: "${variant.strategy}".
- Every agent systemInstruction must be 45 to 90 words.
- Every agent initialTask must be 45 to 90 words.
- Every agent text must include one sentence that starts with "Why this matters:".
- Use full terminology and define abbreviations at first use.
- Prefer these agent archetypes when relevant:
${agentCatalog}
- Return JSON only, without Markdown fences or commentary.`;

        let variantBuffer = "";

        try {
          await Utils.streamChatCompletion({
            llm: creds,
            body: {
              model,
              stream: true,
              messages: [
                { role: "system", content: systemContent },
                {
                  role: "user",
                  content: `Campaign brief:\n${problemText}\n\nVariant rationale:\n${variant.why}`
                }
              ]
            },
            onChunk: (text) => {
              variantBuffer += text;
              buffer += text;
              setState({ architectBuffer: buffer });
            }
          });

          const parsed = parseArchitectJson(variantBuffer);
          const extracted = extractArchitectPlanCandidates(parsed);
          const candidate = (Array.isArray(extracted) && extracted.length)
            ? extracted[0]
            : parsed;

          const planList = candidate?.plan || candidate?.agents || candidate?.workflow || candidate?.steps || [];
          if (!Array.isArray(planList) || !planList.length) {
            throw new Error("Architect output missing a plan list.");
          }

          rawCandidates.push({
            ...candidate,
            title: variantTitle,
            strategy: (candidate.strategy || candidate.summary || variant.strategy).toString().trim(),
            why: (candidate.why || candidate.rationale || variant.why).toString().trim(),
            promptTile: variant.promptTile,
            promptText: variant.promptText,
            variantKey: variant.key
          });
          buffer += `\n[Completed ${variantTitle}]`;
          setState({ architectBuffer: buffer });
        } catch (variantError) {
          usedFallback = true;
          rawCandidates.push(buildFallbackArchitectPlanForVariant(demo, maxAgents, variant));
          buffer += `\n[Fallback ${variantTitle}] Synthetic plan loaded for this variant due to an architect response issue.`;
          setState({ architectBuffer: buffer });
          console.warn(`Architect fallback activated for variant ${variant.key}:`, variantError?.message || variantError);
        }
      }
    }

    if (!rawCandidates.length) {
      usedFallback = true;
      rawCandidates = buildFallbackArchitectPlans(demo, maxAgents);
      if (!buffer) {
        buffer = "Architect streaming unavailable. Loaded synthetic architect plans for this demo run.";
        setState({ architectBuffer: buffer });
      }
    }

    const architectPlans = normalizeArchitectPlans(rawCandidates, demo, maxAgents);
    const byVariant = new Map();
    architectPlans.forEach((plan) => {
      if (plan?.variantKey && !byVariant.has(plan.variantKey)) {
        byVariant.set(plan.variantKey, plan);
      }
    });
    PLAN_VARIANTS.forEach((variant) => {
      if (!byVariant.has(variant.key)) {
        byVariant.set(variant.key, buildFallbackArchitectPlanForVariant(demo, maxAgents, variant));
      }
    });
    const orderedPlans = PLAN_VARIANTS.map((variant) => byVariant.get(variant.key)).filter(Boolean).slice(0, 3);

    if (!orderedPlans.length) {
      const fallbackPlans = buildFallbackArchitectPlans(demo, maxAgents);
      setState({
        architectPlans: fallbackPlans,
        stage: "plan-select",
        error: usedFallback ? "" : "Architect output was empty. Synthetic plans loaded."
      });
      return;
    }

    setState({
      stage: "plan-select",
      architectPlans: orderedPlans,
      plan: [],
      suggestedInputs: [],
      selectedInputs: new Set(),
      rawDataByAgent: RAW_DATA_BY_AGENT,
      error: "",
      visualizationLoading: false,
      visualizationNarrative: "",
      runToken: 0
    });
  } catch (e) {
    const fallbackDemo = getSelectedDemo();
    const maxAgents = clampAgentCount($("#max-agents")?.value, TARGET_ARCHITECT_AGENTS);
    const fallbackPlans = buildFallbackArchitectPlans(fallbackDemo, maxAgents);
    setState({
      stage: "plan-select",
      architectPlans: fallbackPlans,
      plan: [],
      error: "Loaded synthetic architect plans after an orchestration error.",
      visualizationLoading: false,
      visualizationNarrative: ""
    });
    console.error(e);
  }
}

async function startAgents() {
  if (!state.plan.length) {
    setState({ error: "Choose one architect plan before launching the campaign.", stage: "plan-select" });
    return;
  }

  const entries = [...state.suggestedInputs.filter(i => state.selectedInputs.has(i.id)), ...state.uploads];
  if (state.notes?.trim()) entries.push({ title: "User Notes", type: "text", content: state.notes });
  if (!entries.length) return setState({ error: "No data." });

  try {
    const creds = await ensureCreds();
    const model = $("#model")?.value || "gpt-5-mini";
    const demo = getSelectedDemo();
    const agentStyle = demo?.agentStyle || $("#agent-style")?.value || "";
    const campaignPrompt = (state.campaignPrompt || demo?.problem || "Launch a converged ad campaign.").trim();
    const inputBlob = `Campaign Brief:\n${campaignPrompt}\n\nSupplemental Inputs:\n${Utils.formatDataEntries(entries)}`;
    const runToken = Date.now();

    setState({
      stage: "run",
      agentOutputs: [],
      error: "",
      runningNodeIds: new Set(),
      latestNodeId: null,
      dashboard: null,
      issueNodeIds: new Set(),
      approvedPlanCsv: "",
      complianceDetails: hasComplianceRole(state.plan) ? buildComplianceDetails() : null,
      complianceExplanationOpen: false,
      visualizationExplanationOpen: false,
      visualizationLoading: false,
      visualizationNarrative: "",
      runToken
    });

    const phases = new Map();
    state.plan.forEach((a) => {
      const p = a.phase ?? 0;
      if (!phases.has(p)) phases.set(p, []);
      phases.get(p).push(a);
    });
    const sortedPhases = [...phases.keys()].sort((a, b) => (typeof a === "number" && typeof b === "number") ? a - b : String(a).localeCompare(String(b)));

    let context = inputBlob;
    for (const phase of sortedPhases) {
      const agents = phases.get(phase) || [];
      const outputs = agents.map((a) => ({
        id: Utils.uniqueId("out"),
        nodeId: a.nodeId,
        phase,
        name: a.agentName,
        task: a.initialTask,
        instruction: a.systemInstruction,
        dataKey: resolveDatasetKey(a),
        text: "",
        status: "running",
        startedAt: Date.now()
      }));

      setState({
        agentOutputs: [...state.agentOutputs, ...outputs],
        runningNodeIds: new Set([...state.runningNodeIds, ...outputs.map((o) => o.nodeId)]),
        latestNodeId: outputs[0]?.nodeId
      });

      const results = await Promise.all(
        outputs.map((out) =>
          runSyntheticAgent(out, {
            creds,
            model,
            agentStyle,
            campaignPrompt,
            inputBlob,
            context
          })
        )
      );
      context += `\n\n--- Stage ${phase} ---\n${results.map((r, i) => `Output ${agents[i].agentName}:\n${r}`).join("\n")}`;
    }

    const nextDashboard = buildDashboard(state.agentOutputs, {
      campaignPrompt,
      selectedPlanId: state.selectedArchitectPlanId || "",
      runToken
    });
    setState({
      visualizationLoading: true,
      visualizationNarrative: ""
    });
    const visualizationNarrative = await buildVisualizationNarrative({
      creds,
      model,
      campaignPrompt,
      dashboard: nextDashboard,
      agentOutputs: state.agentOutputs
    });

    setState({
      stage: "idle",
      dashboard: nextDashboard,
      approvedPlanCsv: buildApprovedPlanCsv(),
      rawDataByAgent: buildRawDataByPlan(state.plan),
      complianceDetails: hasComplianceRole(state.plan) ? buildComplianceDetails() : null,
      visualizationLoading: false,
      visualizationNarrative,
      visualizationExplanationOpen: true
    });
  } catch (e) {
    setState({ error: e.message, stage: "idle", visualizationLoading: false });
  }
}

async function runSyntheticAgent(out, opts) {
  const { creds, model, agentStyle, campaignPrompt, inputBlob, context } = opts;
  let narrative = "";
  try {
    const datasetBlock = buildDatasetContext(out.dataKey);
    const priorContext = Utils.truncate(context, 2400);
    await Utils.streamChatCompletion({
      llm: creds,
      body: {
        model,
        stream: true,
        messages: [
          {
            role: "system",
            content: `${out.instruction}\n${agentStyle}\nWrite Markdown between 180 and 320 words. Include one explicit heading named "Why this matters". Keep it grounded in the provided dataset.`
          },
          {
            role: "user",
            content:
              `Campaign Brief:\n${campaignPrompt}\n\nTask:\n${out.task}\n\nRelevant Dataset:\n${datasetBlock}\n\nSupplemental Inputs:\n${inputBlob}\n\nPrior Outputs:\n${priorContext}`
          }
        ]
      },
      onChunk: (chunk) => {
        narrative += chunk;
        updateAgent(out.id, narrative, "running");
      }
    });
    updateAgent(out.id, narrative, "done");
    return narrative;
  } catch (e) {
    // LLM fallback keeps the run alive, but marks the response clearly.
    if (!narrative) {
      const fallback = buildSyntheticAgentNarrative(out, campaignPrompt, context);
      narrative = `### LLM Fallback Activated\nThe language model request did not complete for this step. A deterministic fallback narrative was generated.\n\nWhy this matters: the workflow remains connected and testable even when the model endpoint is unavailable.\n\n${fallback}`;
    }
    updateAgent(out.id, narrative || e.message || "Synthetic execution failed.", narrative ? "done" : "error");
    return narrative || "";
  } finally {
    const run = new Set(state.runningNodeIds);
    run.delete(out.nodeId);
    setState({ runningNodeIds: run });
  }
}

async function buildVisualizationNarrative({ creds, model, campaignPrompt, dashboard, agentOutputs }) {
  try {
    let buffer = "";
    const summaryPayload = {
      campaignPrompt,
      pacing: dashboard?.pacing
        ? {
          linearPeakDeliveryRate: Math.max(...(dashboard.pacing.linearPct || [0])),
          digitalPeakDeliveryRate: Math.max(...(dashboard.pacing.digitalPct || [0])),
          makeGoodHour: dashboard.pacing.makeGoodHour || 12
        }
        : {},
      reach: dashboard?.reach || {},
      makeGood: dashboard?.makeGood || {},
      actions: buildActionRows(agentOutputs, state.issueNodeIds || new Set())
    };

    await Utils.streamChatCompletion({
      llm: creds,
      body: {
        model,
        stream: true,
        messages: [
          {
            role: "system",
            content: "You are an analytics narrator. Explain visualization outputs in detailed plain language. Every section must contain a sentence that starts with 'Why this matters:'. Define abbreviations when first used."
          },
          {
            role: "user",
            content: `Use this dashboard summary and write a detailed Markdown explanation with sections for Delivery Pacing, Reach Distribution, Make-Good Decision, and Action Traceability.\n\nDashboard JSON:\n${JSON.stringify(summaryPayload, null, 2)}`
          }
        ]
      },
      onChunk: (chunk) => { buffer += chunk; }
    });

    return buffer.trim();
  } catch {
    return [
      "### Delivery Pacing",
      "The pacing chart compares planned and delivered impressions by hour for linear and digital inventory.",
      "Why this matters: pacing gaps indicate where delivery risk appears before budget is exhausted.",
      "",
      "### Reach Distribution",
      "The reach view shows unique households, devices touched, and overlap across platforms.",
      "Why this matters: overlap and device spread determine whether added spend grows incremental reach or repeats exposure.",
      "",
      "### Make-Good Decision",
      "The make-good panel explains why budget moved from under-delivering linear inventory to digital inventory.",
      "Why this matters: timely reallocation protects outcome targets when a channel under-delivers.",
      "",
      "### Action Traceability",
      "The action table is derived from actual agent outputs and each row includes a reason statement.",
      "Why this matters: traceability links visible outcomes to specific decisions in the selected architect plan."
    ].join("\n");
  }
}

function buildSyntheticAgentNarrative(out, campaignPrompt) {
  const role = inferAgentRole(out);
  const fallbackProfile = buildRunProfile([], {
    campaignPrompt,
    selectedPlanId: state.selectedArchitectPlanId || "",
    runToken: state.runToken || 0
  });
  const reach = buildReachSummary(datasets.audienceGraph || [], datasets.liveDeliveryLog || [], fallbackProfile);
  const makeGood = buildMakeGoodSummary(datasets.liveDeliveryLog || [], fallbackProfile);
  const delivered = (datasets.liveDeliveryLog || []).reduce((sum, row) => sum + (row.delivered_impressions || 0), 0);
  const effectiveCostPerThousandImpressions = delivered ? ((50000 / delivered) * 1000) : 0;
  const quality = state.dataQuality || computeDataQuality(datasets.audienceGraph || []);
  const compliance = buildComplianceDetails();

  if (role === "planner") {
    return `### Audience Resolution Narrative
For the campaign prompt "${campaignPrompt}", the planning step used the Olli household graph to isolate high-propensity Auto Intender households with deterministic filtering rules. The resulting audience contains ${reach.uniqueHouseholds.toLocaleString()} unique households and ${reach.deviceCount.toLocaleString()} connected devices, which creates a measurable base for cross-platform activation.

### Data Quality Review
This audience was validated for quality before activation. The synthetic audience dataset includes ${quality.duplicateCount} duplicate household identifiers and ${quality.nullDmaCount} records with missing designated market area values. These anomalies were explicitly flagged so that downstream budgeting decisions do not overstate unique reach.

### Planning Decision Outcome
The selected audience blueprint balances household scale and confidence score quality, and it is now ready to pass into media line construction with documented limitations and expected overlap behavior.`;
  }

  if (role === "sales-rep") {
    return `### Converged Media Plan Construction
The media planning step translated the audience blueprint into a twelve-line converged schedule covering both linear and digital distribution paths. Budget weighting follows the campaign brief and keeps inventory choices tied to available impressions, daypart performance, and signal reliability.

### Inventory and Spend Logic
Linear placements were prioritized for broad household coverage and digital placements were used to improve incremental touch frequency. Each selected line item includes spend, impression volume, and effective cost per thousand impressions. The current effective cost per thousand impressions estimate is ${effectiveCostPerThousandImpressions.toFixed(2)} United States dollars based on deterministic synthetic delivery totals.

### Risk Visibility
Potential Society of Cable Telecommunications Engineers cue-tone standard (SCTE-35) signal degradation was preserved in the plan notes so that operational teams can anticipate under-delivery risk before launch and prepare make-good paths in advance.`;
  }

  if (role === "compliance-bot") {
    return `### Compliance Validation Narrative
The compliance review step evaluated planned destinations and categories against synthetic policy rows from the Global Compliance Policy Engine dataset. The review blocked non-compliant financial services placements in Saudi Arabia, enforced disclaimer requirements for pharmaceutical creative in Germany, and preserved only approved routing outcomes.

### Policy Sources and Rationale
Source one: ${compliance.sources[0]?.policy || "Saudi Arabia financial services rule"}. Source dataset: ${compliance.sources[0]?.source || "Global Compliance Policy Engine"}. Reason applied: ${compliance.sources[0]?.why || "Restricted category was detected for the target market."}
Source two: ${compliance.sources[1]?.policy || "Germany pharmaceutical disclaimer rule"}. Source dataset: ${compliance.sources[1]?.source || "Global Compliance Policy Engine"}. Reason applied: ${compliance.sources[1]?.why || "Creative category requires disclaimer handling."}

### Operational Result
The output includes approved replacements for blocked inventory and preserves an audit trail so that reviewers can validate exactly where each policy decision originated.`;
  }

  if (role === "ops-director") {
    return `### In-Flight Operations Narrative
The operations step evaluated pacing trends across linear and digital delivery logs and identified a deterministic under-delivery condition in linear inventory during the mid-campaign window. A make-good reallocation of ${makeGood.shiftBudget.toLocaleString()} United States dollars was triggered toward digital ad-lite inventory to restore delivery confidence.

### Financial Interpretation
Before intervention, modeled return on investment measured ${makeGood.beforeRoi.toFixed(2)}. After intervention, modeled return on investment rose to ${makeGood.afterRoi.toFixed(2)}. This improvement is tied to better realized impressions and lower under-delivery exposure during constrained linear windows.

### Delivery Interpretation
This step closes the loop between planning assumptions and live execution evidence, ensuring that every budget movement is justified with transparent pacing data and business impact metrics.`;
  }

  return `### Agent Narrative
This synthetic execution step completed the assigned task "${out.task}" using deterministic campaign data. The response emphasizes transparency, operational traceability, and explicit explanation of business impact for downstream review.`;
}

function inferAgentRole(agent) {
  const id = (agent?.nodeId || "").toString().toLowerCase();
  const name = (agent?.agentName || agent?.name || "").toString().toLowerCase();
  const text = `${id} ${name}`;

  if (text.includes("compliance")) return "compliance-bot";
  if (text.includes("planner") || text.includes("audience")) return "planner";
  if (text.includes("sales") || text.includes("media") || text.includes("inventory")) return "sales-rep";
  if (text.includes("ops") || text.includes("operation") || text.includes("delivery") || text.includes("optimization")) return "ops-director";

  if (AGENT_DATASET_MAP[agent?.nodeId]) return agent.nodeId;
  if (AGENT_DATASET_MAP[agent?.agentName]) return agent.agentName;
  if (AGENT_DATASET_MAP[agent?.name]) return agent.name;

  return "generic";
}

function resolveDatasetKey(agent) {
  const direct = AGENT_DATASET_MAP[agent?.nodeId] || AGENT_DATASET_MAP[agent?.agentName] || AGENT_DATASET_MAP[agent?.name];
  if (direct) return direct;
  const role = inferAgentRole(agent);
  if (AGENT_DATASET_MAP[role]) return AGENT_DATASET_MAP[role];

  const phase = Number(agent?.phase);
  if (Number.isFinite(phase)) {
    if (phase <= 1) return "audienceGraph";
    if (phase <= 3) return "inventoryMatrix";
    if (phase <= 4) return "globalLawRegistry";
    return "liveDeliveryLog";
  }
  return "liveDeliveryLog";
}

function hasComplianceRole(plan = []) {
  return (plan || []).some((agent) => inferAgentRole(agent) === "compliance-bot");
}

function buildRawDataByPlan(plan = []) {
  if (!Array.isArray(plan) || !plan.length) return RAW_DATA_BY_AGENT;
  const map = {};
  plan.forEach((agent) => {
    const dataKey = resolveDatasetKey(agent);
    const entry = RAW_DATA_BY_KEY[dataKey];
    if (!entry) return;
    map[agent.nodeId] = entry;
    map[agent.agentName] = entry;
  });
  return Object.keys(map).length ? map : RAW_DATA_BY_AGENT;
}

function buildComplianceDetails() {
  const rows = datasets.globalLawRegistry || [];
  const saFinancial = rows.find((row) => row.country_code === "SA" && row.restriction_category === "financial_services");
  const dePharma = rows.find((row) => row.country_code === "DE" && row.restriction_category === "pharma");
  const frAlcohol = rows.find((row) => row.country_code === "FR" && row.restriction_category === "alcohol");

  const sources = [
    saFinancial ? {
      policy: `Saudi Arabia financial services policy (${saFinancial.restriction_type})`,
      source: "Global Compliance Policy Engine synthetic dataset, country_code = SA",
      why: "The campaign includes financial services messaging and the rule marks this category as blocked for Saudi Arabia placements."
    } : null,
    dePharma ? {
      policy: `Germany pharmaceutical policy (${dePharma.restriction_type})`,
      source: "Global Compliance Policy Engine synthetic dataset, country_code = DE",
      why: "Any pharmaceutical creative routed to Germany must include required on-screen disclaimer language before approval."
    } : null,
    frAlcohol ? {
      policy: `France alcohol policy (${frAlcohol.restriction_type})`,
      source: "Global Compliance Policy Engine synthetic dataset, country_code = FR",
      why: "Alcohol placements are subject to local time constraints and therefore require daypart checks before final routing."
    } : null
  ].filter(Boolean);

  return {
    summary: "Compliance decisions in this demo are sourced from the synthetic Global Compliance Policy Engine dataset bundled in data.js.",
    sources,
    detailedExplanation:
      "The compliance step is not using external legal APIs at runtime. It reads deterministic policy rows from the synthetic registry, matches each media line by country and category, and then applies a rule outcome of blocked, restricted by time, or disclaimer required. This makes the behavior auditable for demos while still showing how real policy sourcing would be surfaced in production."
  };
}

function updateAgent(id, text, status) {
  let updatedAgent = null;
  const outputs = state.agentOutputs.map(o => {
    if (o.id !== id) return o;
    const next = { ...o, text, status };
    if ((status === "done" || status === "error") && !o.completedAt) {
      next.completedAt = Date.now();
    }
    updatedAgent = next;
    return next;
  });
  const issueNodeIds = new Set(state.issueNodeIds);
  if (updatedAgent && status === "done" && shouldFlagIssue(updatedAgent)) {
    issueNodeIds.add(updatedAgent.nodeId);
  }
  setState({ agentOutputs: outputs, issueNodeIds });
}

function shouldFlagIssue(agent) {
  if (!agent || inferAgentRole(agent) !== "compliance-bot") return false;
  return !!extractIssueReason(agent.text || "");
}

let chartsScheduled = false;
function scheduleDashboardCharts() {
  if (chartsScheduled) return;
  chartsScheduled = true;
  requestAnimationFrame(() => {
    chartsScheduled = false;
    renderDashboardCharts();
  });
}

function renderDashboardCharts() {
  if (!state.dashboard || state.stage !== "idle") return;
  renderPacingChartD3();
  renderReachChartD3();
}

function renderPacingChartD3() {
  const container = document.querySelector(".pacing-chart-d3");
  if (!container) return;
  const mode = state.pacingMode || "imp";
  const pacing = state.dashboard?.pacing;
  if (!pacing) return;

  const sig = JSON.stringify({
    mode,
    hours: pacing.hours.length,
    hash: pacing.linearImp?.[0],
    linearTotal: (pacing.linearImp || []).reduce((sum, value) => sum + value, 0),
    digitalTotal: (pacing.digitalImp || []).reduce((sum, value) => sum + value, 0),
    runToken: state.runToken || 0
  });
  if (container.dataset.signature === sig) return;
  container.dataset.signature = sig;

  container.innerHTML = "";
  const width = container.clientWidth || 560;
  const height = 220;
  const padding = { top: 16, right: 28, bottom: 28, left: 44 };

  const svg = d3.select(container)
    .append("svg")
    .attr("viewBox", `0 0 ${width} ${height}`)
    .attr("width", "100%")
    .attr("height", height);

  const hours = pacing.hours;
  const x = d3.scaleLinear()
    .domain([1, hours.length])
    .range([padding.left, width - padding.right]);

  let series = [];
  if (mode === "imp") {
    series = [
      { id: "linearPlanned", label: "Linear Planned", values: pacing.linearPlanned, color: "#ffb703", dash: "6 6" },
      { id: "linearDelivered", label: "Linear Delivered", values: pacing.linearImp, color: "#ff6600" },
      { id: "digitalPlanned", label: "Digital Planned", values: pacing.digitalPlanned, color: "#8bd3ff", dash: "6 6" },
      { id: "digitalDelivered", label: "Digital Delivered", values: pacing.digitalImp, color: "#6dd3fb" }
    ];
  } else {
    series = [
      { id: "linearDelivered", label: "Linear Delivery %", values: pacing.linearPct, color: "#ff6600" },
      { id: "digitalDelivered", label: "Digital Delivery %", values: pacing.digitalPct, color: "#6dd3fb" }
    ];
  }

  const maxValue = mode === "imp"
    ? Math.max(...pacing.linearPlanned, ...pacing.linearImp, ...pacing.digitalPlanned, ...pacing.digitalImp, 1)
    : 120;

  const y = d3.scaleLinear()
    .domain([0, maxValue])
    .nice()
    .range([height - padding.bottom, padding.top]);

  svg.append("g")
    .attr("transform", `translate(0,${height - padding.bottom})`)
    .call(d3.axisBottom(x).ticks(6).tickFormat(d => `Hour ${d}`))
    .call(g => g.selectAll("text").attr("fill", "#a9b0c7"))
    .call(g => g.selectAll("path,line").attr("stroke", "rgba(255,255,255,0.15)"));

  svg.append("g")
    .attr("transform", `translate(${padding.left},0)`)
    .call(d3.axisLeft(y).ticks(4))
    .call(g => g.selectAll("text").attr("fill", "#a9b0c7"))
    .call(g => g.selectAll("path,line").attr("stroke", "rgba(255,255,255,0.15)"));

  const line = d3.line()
    .x((d, i) => x(i + 1))
    .y(d => y(d))
    .curve(d3.curveMonotoneX);

  series.forEach((s) => {
    svg.append("path")
      .datum(s.values)
      .attr("fill", "none")
      .attr("stroke", s.color)
      .attr("stroke-width", 3)
      .attr("stroke-dasharray", s.dash || null)
      .attr("d", line);
  });

  const makeGoodHour = pacing.makeGoodHour || 12;
  svg.append("line")
    .attr("x1", x(makeGoodHour))
    .attr("x2", x(makeGoodHour))
    .attr("y1", padding.top)
    .attr("y2", height - padding.bottom)
    .attr("stroke", "rgba(255,102,0,0.5)")
    .attr("stroke-dasharray", "4 4");

  svg.append("text")
    .attr("x", x(makeGoodHour) + 6)
    .attr("y", padding.top + 12)
    .attr("fill", "#ffb703")
    .attr("font-size", "11px")
    .text("Make-Good Trigger");

  const tooltip = d3.select(container)
    .append("div")
    .attr("class", "chart-tooltip");

  const overlay = svg.append("rect")
    .attr("x", padding.left)
    .attr("y", padding.top)
    .attr("width", width - padding.left - padding.right)
    .attr("height", height - padding.top - padding.bottom)
    .attr("fill", "transparent")
    .style("cursor", "crosshair");

  overlay.on("mousemove", (event) => {
    const [mx] = d3.pointer(event);
    const idx = Math.min(hours.length - 1, Math.max(0, Math.round((mx - padding.left) / ((width - padding.left - padding.right) / (hours.length - 1)))));
    const hour = hours[idx];
    const fmt = new Intl.NumberFormat("en-US");
    let label = "";
    if (mode === "imp") {
      label = `Hour ${hour} | Linear delivered ${fmt.format(pacing.linearImp[idx])} of ${fmt.format(pacing.linearPlanned[idx])} planned | Digital delivered ${fmt.format(pacing.digitalImp[idx])} of ${fmt.format(pacing.digitalPlanned[idx])} planned`;
    } else {
      label = `Hour ${hour} | Linear delivery rate ${pacing.linearPct[idx]} percent | Digital delivery rate ${pacing.digitalPct[idx]} percent`;
    }
    tooltip.text(label)
      .style("opacity", 1)
      .style("left", `${mx + 12}px`)
      .style("top", `${padding.top}px`);
  });

  overlay.on("mouseleave", () => {
    tooltip.style("opacity", 0);
  });
}

function renderReachChartD3() {
  const container = document.querySelector(".reach-chart-d3");
  if (!container) return;
  const reach = state.dashboard?.reach;
  if (!reach) return;

  const sig = JSON.stringify({
    reach: reach.uniqueHouseholds,
    overlap: reach.overlapPct,
    deviceCount: reach.deviceCount,
    runToken: state.runToken || 0
  });
  if (container.dataset.signature === sig) return;
  container.dataset.signature = sig;

  container.innerHTML = "";
  const width = 180;
  const height = 180;
  const radius = 70;
  const svg = d3.select(container)
    .append("svg")
    .attr("viewBox", `0 0 ${width} ${height}`)
    .attr("width", "100%")
    .attr("height", "180");

  const g = svg.append("g").attr("transform", `translate(${width / 2},${height / 2})`);
  const arc = d3.arc().innerRadius(radius - 18).outerRadius(radius);
  const pie = d3.pie().value(d => d.value);

  const data = [
    { label: "Overlap", value: reach.overlapPct, color: "#ff6600" },
    { label: "Unique", value: 100 - reach.overlapPct, color: "#2a2f4c" }
  ];

  g.selectAll("path")
    .data(pie(data))
    .enter()
    .append("path")
    .attr("d", arc)
    .attr("fill", d => d.data.color);

  g.append("text")
    .attr("text-anchor", "middle")
    .attr("dy", "-0.2em")
    .attr("fill", "#e9edf6")
    .attr("font-size", "18px")
    .attr("font-weight", "600")
    .text(`${reach.overlapPct}%`);

  g.append("text")
    .attr("text-anchor", "middle")
    .attr("dy", "1.2em")
    .attr("fill", "#a9b0c7")
    .attr("font-size", "11px")
    .text("Cross-Platform Overlap");
}

async function ensureCreds() {
  if (!llmSession.creds) llmSession.creds = await openaiConfig({ defaultBaseUrls: DEFAULT_BASE_URLS });
  if (!llmSession.creds?.apiKey) throw new Error("No API Key.");
  return llmSession.creds;
}

function syncCustomButton() {
  const btn = $("#run-custom-problem");
  if (btn) { btn.disabled = ["architect", "run"].includes(state.stage); btn.textContent = btn.disabled ? "Streaming..." : "Plan & Build Custom"; }
}

// Queue scrolling
let scrollSched = false;
let scrollState = { key: null, height: 0 };

function scheduleScrollToRunningSection() {
  if (scrollSched) return;
  scrollSched = true;
  requestAnimationFrame(() => {
    scrollSched = false;
    const key = getRunningScrollKey();
    if (!key) {
      scrollState = { key: null, height: 0 };
      return;
    }

    const target = document.querySelector(`[data-running-key="${key}"]`);
    if (!target) return;

    // Force scroll if the key changed implies we moved to a new step
    const keyChanged = scrollState.key !== key;

    if (keyChanged) {
      // Scroll to the new active element immediately
      target.scrollIntoView({ behavior: "smooth", block: "center" });
    } else {
      // If same key, only scroll if it's pushed off screen bottom (content growing)
      const rect = target.getBoundingClientRect();
      const viewHeight = window.innerHeight || document.documentElement.clientHeight;
      if (rect.bottom > viewHeight) {
        target.scrollIntoView({ behavior: "smooth", block: "nearest" });
      }
    }
    scrollState = { key, height: target.scrollHeight };
  });
}

function getRunningScrollKey() {
  if (state.stage === "architect" || state.stage === "plan-select") return "architect-plan";
  if (state.stage === "data") return "data-inputs";
  if (state.stage === "run") return "execution-flow";
  return null;
}

// Flowchart
let fcCtrl, fcKey = "", fcSched = false;
function scheduleFlowchartSync() {
  if (fcSched) return;
  fcSched = true;
  requestAnimationFrame(() => { fcSched = false; syncFlowchart(); });
}

function buildGraphForDemo(demo, plan) {
  const planGraph = Utils.buildGraphFromPlan(plan);
  if (demo?.flowchart?.nodes?.length) {
    const flowIds = new Set((demo.flowchart.nodes || []).map((node) => node.id));
    const planCovered = (plan || []).every((agent) => flowIds.has(agent.nodeId));
    if (!planCovered) return planGraph;
    return addFlowchartPositions(demo.flowchart);
  }
  return planGraph;
}

function nodeClassForKind(kind) {
  return kind ? `node-${kind}` : "";
}

function addFlowchartPositions(flowchart) {
  if (flowchart.nodes.some(n => n.position)) return flowchart;

  const pipelineOrder = ["ui", "orchestration", "planner", "sales-rep", "compliance-bot", "ops-director", "output"];
  const xStart = 0;
  const xStep = 210;
  const yMain = 0;
  const yZig = 90;
  const yDataOffset = 150;

  const positions = {};
  pipelineOrder.forEach((id, idx) => {
    const zig = idx % 2 === 0 ? -yZig : yZig;
    positions[id] = { x: xStart + idx * xStep, y: yMain + zig };
  });

  flowchart.nodes.forEach((node, idx) => {
    if (positions[node.id]) return;
    if (node.kind === "data" && node.bindTo && positions[node.bindTo]) {
      positions[node.id] = {
        x: positions[node.bindTo].x,
        y: positions[node.bindTo].y + yDataOffset
      };
      return;
    }
    positions[node.id] = { x: xStart + (pipelineOrder.length + idx) * xStep, y: yMain + yDataOffset };
  });

  return {
    ...flowchart,
    nodes: flowchart.nodes.map(n => ({
      ...n,
      position: positions[n.id] || { x: xStart, y: 0 }
    }))
  };
}

function syncFlowchart() {
  const canvas = $("#flowchart-canvas");
  const demo = state.selectedDemoIndex === -1
    ? state.customProblem
    : (state.selectedDemoIndex === -2 ? state.customProblem : config.demos[state.selectedDemoIndex]);
  const graph = buildGraphForDemo(demo, state.plan);
  if (!canvas || !graph.nodes.length) { fcCtrl?.destroy(); fcCtrl = null; return; }

  if (!fcCtrl || fcCtrl.container !== canvas) {
    fcCtrl?.destroy();
    fcCtrl = createFlowchart(canvas, [], { orientation: state.flowOrientation, columnCount: state.flowColumns, onNodeSelected: (id) => setState({ focusedNodeId: id }) });
    fcKey = "";
  } else {
    fcCtrl.setOrientation(state.flowOrientation);
    fcCtrl.setColumns(state.flowColumns);
  }

  const elements = [
    ...graph.nodes.map(n => ({
      data: { id: n.id, label: n.label, bindTo: n.bindTo },
      classes: nodeClassForKind(n.kind),
      position: n.position
    })),
    ...graph.edges.map(e => ({
      data: { source: e.source, target: e.target },
      classes: e.kind === "data" ? "edge-data" : ""
    }))
  ];
  const sig = JSON.stringify(elements.map(e => e.data));
  if (sig !== fcKey) { fcCtrl.setElements(elements); fcKey = sig; }

  const errorIds = state.agentOutputs.filter(o => o.status === "error").map(o => o.nodeId);
  const issueIds = new Set([...(state.issueNodeIds || []), ...errorIds]);
  fcCtrl.setNodeState({
    activeIds: [...state.runningNodeIds],
    completedIds: state.agentOutputs.filter(o => o.status === "done").map(o => o.nodeId),
    failedIds: [...issueIds],
    selectedId: state.focusedNodeId
  });
  fcCtrl.resize();
}

window.addEventListener("resize", () => scheduleFlowchartSync());
