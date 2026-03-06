// VERSION: 2026-03-06 - Three-plan converged strategies with fixed six-agent workflow, yield intelligence, and parallel compliance validation.
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

const PARALLEL_COMPLIANCE_NODE_ID = "parallel-compliance-agent";
const WORKFLOW_AGENTS = [
  {
    nodeId: "planning-identity-agent",
    agentName: "Planning and Identity Agent",
    phase: 1,
    focus: "audience planning, identity resolution, and seed segment qualification across state-level markets"
  },
  {
    nodeId: "inventory-yield-agent",
    agentName: "Inventory Yield Agent",
    phase: 2,
    focus: "inventory demand and yield optimization using trend-aware timing signals"
  },
  {
    nodeId: "booking-proposals-agent",
    agentName: "Booking and Proposals Agent",
    phase: 3,
    focus: "channel allocation, proposal construction, and return on investment-centered booking logic"
  },
  {
    nodeId: "trafficking-signals-agent",
    agentName: "Trafficking and Signals Agent",
    phase: 4,
    focus: "placement trafficking, signal integrity checks, and delivery readiness assurance"
  },
  {
    nodeId: "inflight-operations-agent",
    agentName: "In-Flight Operations Agent",
    phase: 5,
    focus: "live pacing correction, make-good routing, and budget protection during execution"
  },
  {
    nodeId: "measurement-agent",
    agentName: "Measurement Agent",
    phase: 6,
    focus: "outcome attribution, return on investment interpretation, and optimization recommendations"
  }
];

const AGENT_DATASET_MAP = {
  "planning-identity-agent": "unifiedAudienceDataset",
  "Planning and Identity Agent": "unifiedAudienceDataset",
  "inventory-yield-agent": "unifiedAudienceDataset",
  "Inventory Yield Agent": "unifiedAudienceDataset",
  "booking-proposals-agent": "unifiedAudienceDataset",
  "Booking and Proposals Agent": "unifiedAudienceDataset",
  "trafficking-signals-agent": "unifiedAudienceDataset",
  "Trafficking and Signals Agent": "unifiedAudienceDataset",
  "inflight-operations-agent": "unifiedAudienceDataset",
  "In-Flight Operations Agent": "unifiedAudienceDataset",
  "measurement-agent": "unifiedAudienceDataset",
  "Measurement Agent": "unifiedAudienceDataset",
  "planning-identity": "unifiedAudienceDataset",
  "inventory-yield": "unifiedAudienceDataset",
  "booking-proposals": "unifiedAudienceDataset",
  "trafficking-signals": "unifiedAudienceDataset",
  "inflight-operations": "unifiedAudienceDataset",
  "measurement": "unifiedAudienceDataset",
  "compliance-agent": "complianceRulebook",
  [PARALLEL_COMPLIANCE_NODE_ID]: "complianceRulebook",
  "Compliance Agent (Parallel)": "complianceRulebook",
  // Backward compatibility for older saved plans
  planner: "unifiedAudienceDataset",
  "The Planner": "unifiedAudienceDataset",
  "sales-rep": "unifiedAudienceDataset",
  "The Sales Rep": "unifiedAudienceDataset",
  "ops-director": "unifiedAudienceDataset",
  "The Ops Director": "unifiedAudienceDataset",
  "compliance-bot": "complianceRulebook",
  "The Compliance Bot": "complianceRulebook"
};

const AGENT_ACTION_SUMMARY = {
  "planning-identity-agent": "Resolved identity graph, validated state-level audience readiness, and documented targetable reach confidence.",
  "inventory-yield-agent": "Applied Yield Intelligence signals from Twitter trends, news signals, and external feeds to set high-value delivery windows.",
  "booking-proposals-agent": "Produced channel booking proposals with return on investment-focused allocation and constraint-aware alternatives.",
  "trafficking-signals-agent": "Validated trafficking package, verified signal health, and prepared compliant activation instructions.",
  "inflight-operations-agent": "Monitored delivery pacing, triggered in-flight optimizations, and protected campaign return on investment.",
  "measurement-agent": "Measured outcomes across channels, quantified return on investment impact, and documented next-iteration recommendations.",
  [PARALLEL_COMPLIANCE_NODE_ID]: "Validated campaign parameters against compliance rules and provided compliant alternatives when restrictions were detected."
};

const RAW_DATA_BY_AGENT = {
  "planning-identity-agent": datasetEntries.find((d) => d.key === "unifiedAudienceDataset"),
  "inventory-yield-agent": datasetEntries.find((d) => d.key === "yieldIntelligenceFeed"),
  "booking-proposals-agent": datasetEntries.find((d) => d.key === "unifiedAudienceDataset"),
  "trafficking-signals-agent": datasetEntries.find((d) => d.key === "unifiedAudienceDataset"),
  "inflight-operations-agent": datasetEntries.find((d) => d.key === "unifiedAudienceDataset"),
  "measurement-agent": datasetEntries.find((d) => d.key === "unifiedAudienceDataset"),
  [PARALLEL_COMPLIANCE_NODE_ID]: datasetEntries.find((d) => d.key === "complianceRulebook")
};

const ARCHITECT_PLAN_FALLBACK_TITLES = [
  "Plan A - Streaming Focused",
  "Plan B - Linear Focused",
  "Plan C - Balanced Strategy"
];
const MIN_ARCHITECT_AGENTS = 6;
const MAX_ARCHITECT_AGENTS = 6;
const TARGET_ARCHITECT_AGENTS = 6;
const PLAN_VARIANTS = [
  {
    key: "plan-a-streaming",
    planLabel: "Plan A - Streaming Focused",
    title: "Plan A - Streaming Focused",
    promptTile: "Plan A - Streaming Focused",
    promptText: "Prioritize streaming channels while preserving quality reach in core states and maximizing return on investment through dynamic optimization.",
    strategy: "Streaming-first strategy with high digital video allocation and rapid optimization loops.",
    why: "Streaming-heavy allocation can improve return on investment when high-intent audiences show stronger digital consumption patterns.",
    allocationStrategy: "Channel allocation recommendation: 62 percent streaming video, 23 percent linear television, and 15 percent social extensions.",
    deliveryTiming: "Prioritize evening and late-prime windows, then dynamically increase delivery during trend spikes detected in state-level demand signals.",
    roiReasoning: "This plan improves return on investment by concentrating spend on higher engagement inventory and rapidly reallocating when signal-aware yield data indicates better conversion potential.",
    priorities: {
      "planning-identity-agent": "Prioritize streaming-leaning households, high-device clusters, and state-level demand pockets where digital completion rates historically outperform baseline.",
      "inventory-yield-agent": "Use Yield Intelligence from Twitter trends, news signals, and external feeds to identify streaming demand spikes and recommend intra-day timing shifts.",
      "booking-proposals-agent": "Structure proposals with streaming as the primary spend lane while retaining selective linear anchors for baseline reach continuity.",
      "trafficking-signals-agent": "Optimize trafficking for digital ad serving reliability, rapid creative refresh, and low-latency signal handling in high-demand windows.",
      "inflight-operations-agent": "Monitor streaming pacing variance hourly and reallocate underperforming segments toward higher-return digital inventory.",
      "measurement-agent": "Emphasize engagement depth, incremental conversion lift, and return on investment attribution from streaming-heavy touchpoints."
    }
  },
  {
    key: "plan-b-linear",
    planLabel: "Plan B - Linear Focused",
    title: "Plan B - Linear Focused",
    promptTile: "Plan B - Linear Focused",
    promptText: "Prioritize linear television coverage to maximize broad reach and sustained brand exposure while protecting return on investment.",
    strategy: "Linear-first strategy with strong gross rating point coverage and selective digital reinforcement.",
    why: "Linear-heavy allocation supports consistent broad-reach delivery and can raise top-of-funnel impact for brand visibility objectives.",
    allocationStrategy: "Channel allocation recommendation: 58 percent linear television, 27 percent streaming video, and 15 percent social extensions.",
    deliveryTiming: "Concentrate delivery in high-index dayparts for broad household reach, with measured streaming reinforcement around event-driven windows.",
    roiReasoning: "This plan improves return on investment by stabilizing reach efficiency through linear scale while applying selective digital follow-through where incremental lift is strongest.",
    priorities: {
      "planning-identity-agent": "Prioritize broad state-level household coverage cohorts and stable identity segments that align with high-reach linear programming.",
      "inventory-yield-agent": "Use Yield Intelligence from Twitter trends, news signals, and external feeds to decide when linear tentpole inventory should receive higher pacing priority.",
      "booking-proposals-agent": "Structure proposals around linear gross rating point strength first, with digital support reserved for incremental frequency management.",
      "trafficking-signals-agent": "Focus trafficking quality controls on linear signal integrity, cue reliability, and placement continuity in large-reach schedules.",
      "inflight-operations-agent": "Protect linear pacing targets and only shift budget when delivery risk threatens return on investment thresholds.",
      "measurement-agent": "Emphasize de-duplicated reach, brand exposure continuity, and return on investment from broad-reach linear execution."
    }
  },
  {
    key: "plan-c-balanced",
    planLabel: "Plan C - Balanced Strategy",
    title: "Plan C - Balanced Strategy",
    promptTile: "Plan C - Balanced Strategy",
    promptText: "Balance streaming, linear television, and social extensions to diversify risk and maintain durable return on investment across channels.",
    strategy: "Balanced strategy with diversified channel mix, controlled risk, and adaptive pacing.",
    why: "A balanced allocation can reduce over-exposure risk and preserve return on investment stability across uncertain market conditions.",
    allocationStrategy: "Channel allocation recommendation: 40 percent streaming video, 40 percent linear television, and 20 percent social extensions.",
    deliveryTiming: "Distribute delivery across core dayparts with adaptive boosts during verified demand spikes and under-delivery recovery windows.",
    roiReasoning: "This plan improves return on investment by combining broad reach and targeted engagement while reducing concentration risk through diversified channel exposure.",
    priorities: {
      "planning-identity-agent": "Balance broad and high-intent identity segments so the campaign can scale reach without sacrificing precision in high-value states.",
      "inventory-yield-agent": "Use Yield Intelligence from Twitter trends, news signals, and external feeds to rebalance timing between channels during demand inflections.",
      "booking-proposals-agent": "Build mixed-channel proposals with balanced allocation logic and clear fallback paths for volatility in any one inventory lane.",
      "trafficking-signals-agent": "Ensure trafficking and signal checks are symmetric across linear and digital paths to maintain continuity under mixed allocation.",
      "inflight-operations-agent": "Use measured reallocation to sustain balanced pacing and protect return on investment when one channel underperforms.",
      "measurement-agent": "Evaluate return on investment through blended attribution, de-duplicated reach, and channel interaction effects."
    }
  }
];

const RAW_DATA_BY_KEY = {
  unifiedAudienceDataset: datasetEntries.find((d) => d.key === "unifiedAudienceDataset"),
  yieldIntelligenceFeed: datasetEntries.find((d) => d.key === "yieldIntelligenceFeed"),
  complianceRulebook: datasetEntries.find((d) => d.key === "complianceRulebook"),
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
  selectedPlanCompliance: null,
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
      picked.variantKey || PLAN_VARIANTS[0].key
    );
    const normalizedInputs = buildDatasetInputs().map((input) => ({ ...input, id: Utils.uniqueId("input") }));
    const complianceValidation = picked.complianceValidation || evaluatePlanCompliance({
      campaignPrompt: state.campaignPrompt || "",
      allocationStrategy: picked.allocationStrategy || "",
      variantKey: picked.variantKey || ""
    });
    setState({
      selectedArchitectPlanId: picked.id,
      plan: selectedPlan,
      suggestedInputs: normalizedInputs,
      selectedInputs: new Set(normalizedInputs.map((i) => i.id)),
      rawDataByAgent: buildRawDataByPlan(selectedPlan),
      selectedPlanCompliance: complianceValidation,
      complianceDetails: buildComplianceDetails(complianceValidation),
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
    const savedCompliance = evaluatePlanCompliance({
      campaignPrompt: agent.problem || "",
      allocationStrategy: "Saved plan allocation derived from prior run.",
      variantKey: ""
    });
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
      selectedPlanCompliance: savedCompliance,
      complianceDetails: buildComplianceDetails(savedCompliance),
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
    const inputs = buildDatasetInputs();
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
    selectedPlanCompliance: null,
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
  if (role === "planning-identity") return "This planning and identity decision matters because downstream targeting quality depends on accurate audience resolution.";
  if (role === "inventory-yield") return "This inventory yield decision matters because timing and demand signals directly influence conversion efficiency.";
  if (role === "booking-proposals") return "This booking decision matters because allocation structure drives reach, cost efficiency, and outcome potential.";
  if (role === "trafficking-signals") return "This trafficking decision matters because signal integrity and routing quality determine whether delivery can execute as planned.";
  if (role === "inflight-operations") return "This in-flight operations decision matters because pacing correction protects return on investment during live execution.";
  if (role === "measurement") return "This measurement decision matters because reliable attribution is required to improve return on investment in the next cycle.";
  if (role === "compliance-agent") return "This compliance decision matters because non-compliant placements create legal risk and force avoidable rework.";
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
  const variant = PLAN_VARIANTS[optionIndex];
  const label = variant?.planLabel || ARCHITECT_PLAN_FALLBACK_TITLES[optionIndex] || `Plan ${String.fromCharCode(65 + optionIndex)}`;
  return `${base} - ${label}`;
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

function getVariantByKey(variantKey = "") {
  return PLAN_VARIANTS.find((item) => item.key === variantKey) || PLAN_VARIANTS[0];
}

function getVariantAgentLibrary(variantKey = "") {
  const variant = getVariantByKey(variantKey);
  return WORKFLOW_AGENTS.map((stageAgent) => ({
    ...stageAgent,
    focus: variant.priorities?.[stageAgent.nodeId] || stageAgent.focus
  }));
}

function extractBudgetUsd(prompt = "") {
  const text = (prompt || "").toString();
  const compact = text.toLowerCase().replace(/,/g, "");
  const kMatch = compact.match(/\$?\s*(\d+(?:\.\d+)?)\s*k\b/);
  if (kMatch) return Math.round(Number(kMatch[1]) * 1000);
  const usdMatch = compact.match(/\$?\s*(\d{4,9}(?:\.\d+)?)/);
  if (usdMatch) return Math.round(Number(usdMatch[1]));
  return 50000;
}

function formatPlanOutputDetails(variant, campaignPrompt, existing = {}) {
  const budget = extractBudgetUsd(campaignPrompt);
  const allocationStrategy = (existing.allocationStrategy || variant.allocationStrategy || "")
    .replace(/\$BUDGET_USD/g, budget.toLocaleString());
  const deliveryTiming = (existing.deliveryTiming || variant.deliveryTiming || "").trim();
  const roiReasoning = (existing.roiReasoning || variant.roiReasoning || "").trim();
  const complianceValidation = evaluatePlanCompliance({
    campaignPrompt,
    allocationStrategy,
    variantKey: variant.key
  });
  return {
    allocationStrategy,
    deliveryTiming,
    roiReasoning,
    complianceValidation
  };
}

function normalizeArchitectPlans(rawPlans, demo, maxAgents) {
  const campaignPrompt = demo?.problem || demo?.body || "";
  const normalized = (Array.isArray(rawPlans) ? rawPlans : [])
    .map((entry, index) => {
      if (!entry || typeof entry !== "object") return null;
      const fallbackVariant = PLAN_VARIANTS[index % PLAN_VARIANTS.length];
      const planList = entry.plan || entry.agents || entry.workflow || entry.steps || [];
      const normalizedPlan = Utils.normalizePlan(planList, maxAgents);
      if (!normalizedPlan.length) return null;
      const variantKey = entry.variantKey || fallbackVariant.key;
      const variant = getVariantByKey(variantKey);
      const variantIndex = PLAN_VARIANTS.findIndex((item) => item.key === variantKey);
      const optionIndex = variantIndex >= 0 ? variantIndex : index;
      const fallbackTitle = buildArchitectPlanTitle(demo, optionIndex);
      const enrichedPlan = expandAndEnrichPlan(normalizedPlan, maxAgents, campaignPrompt, variantKey);
      const details = formatPlanOutputDetails(variant, campaignPrompt, entry);
      return {
        id: Utils.uniqueId("architect-plan"),
        title: fallbackTitle,
        strategy: (entry.strategy || entry.summary || variant.strategy).toString().trim(),
        why: (entry.why || entry.rationale || variant.why).toString().trim(),
        promptTile: entry.promptTile || variant.promptTile,
        promptText: entry.promptText || variant.promptText,
        variantKey,
        plan: enrichedPlan,
        allocationStrategy: details.allocationStrategy,
        deliveryTiming: details.deliveryTiming,
        roiReasoning: details.roiReasoning,
        complianceValidation: details.complianceValidation,
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
    const variantLibrary = getVariantAgentLibrary(variant.key);
    const seedPlan = variantLibrary.slice(0, TARGET_ARCHITECT_AGENTS).map((node, idx) => ({
      agentName: node.agentName,
      nodeId: node.nodeId,
      initialTask: `Stage ${idx + 1} task for ${node.agentName} in campaign: ${campaign}.`,
      systemInstruction: `Execute ${node.focus} and provide one explicit Why this matters statement.`,
      stage: idx + 1
    }));
    const details = formatPlanOutputDetails(variant, campaign, {});
    return {
      id: Utils.uniqueId("architect-plan"),
      title: buildArchitectPlanTitle(demo, index),
      strategy: variant.strategy,
      why: variant.why,
      promptTile: variant.promptTile,
      promptText: variant.promptText,
      variantKey: variant.key,
      plan: expandAndEnrichPlan(Utils.normalizePlan(seedPlan, maxAgents), maxAgents, campaign, variant.key),
      allocationStrategy: details.allocationStrategy,
      deliveryTiming: details.deliveryTiming,
      roiReasoning: details.roiReasoning,
      complianceValidation: details.complianceValidation,
      inputs: buildDatasetInputs()
    };
  });
}

function expandAndEnrichPlan(plan = [], maxAgents = TARGET_ARCHITECT_AGENTS, campaign = "", variantKey = PLAN_VARIANTS[0].key) {
  const targetCount = TARGET_ARCHITECT_AGENTS;
  const agentLibrary = getVariantAgentLibrary(variantKey);
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
    .slice(0, targetCount)
    .map((agent, index) => {
      const template = agentLibrary.find((item) => item.nodeId === agent.nodeId)
        || agentLibrary[index % Math.max(agentLibrary.length, 1)]
        || WORKFLOW_AGENTS[0];
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
  return `You are ${role} in stage ${phase} of a fixed six-stage converged campaign workflow. Focus on ${focus}. Use the unified audience base dataset as the core input and reference additional datasets only when needed for this stage. Treat the campaign brief "${campaign}" as a total budget objective and derive channel allocation based on state-level demand and compliance context rather than fixed pre-specified splits. Explain your reasoning clearly, include one explicit sentence that starts with "Why this matters:", and show how your output supports return on investment optimization for the next stage. Keep language clear for business and technical reviewers. Define abbreviations when they first appear and include key risks, assumptions, and validation checks.`;
}

function buildDetailedInitialTask(agent, template, campaign, phase) {
  const role = agent.agentName || template.agentName;
  const focus = template.focus;
  return `Complete stage ${phase} for ${role} using campaign brief "${campaign}". Prioritize ${focus}. Use the full campaign budget objective and determine recommendations according to state-level conditions, demand signals, and compliance constraints instead of assuming fixed channel splits. Deliver one section for objective, one for evidence from the unified audience base dataset, one for decisions taken, and one for handoff. Include a clear sentence that starts with "Why this matters:" and name at least one risk with mitigation. Keep the explanation specific but concise so the next stage can execute without ambiguity.`;
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
      complianceDetails: null,
      selectedPlanCompliance: null,
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
        const variantLibrary = getVariantAgentLibrary(variant.key);
        const header = [
          `Generating ${variantTitle}`,
          `Prompt Tile: ${variant.promptTile}`,
          `Variant Objective: ${variant.promptText}`
        ].join("\n");
        buffer = `${buffer}${buffer ? "\n\n" : ""}${header}\n`;
        setState({ architectBuffer: buffer });

        const agentCatalog = variantLibrary
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
  "allocationStrategy": "Detailed channel allocation recommendation",
  "deliveryTiming": "Detailed timing recommendation",
  "roiReasoning": "Detailed explanation of how this strategy improves return on investment",
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
- Use exactly ${TARGET_ARCHITECT_AGENTS} agents in this strict fixed order:
${variantLibrary.map((agent) => `  ${agent.phase}. ${agent.agentName} (${agent.nodeId})`).join("\n")}
- Do not add, remove, rename, or reorder these six workflow stages.
- Keep the workflow aligned with this strategy: "${variant.strategy}".
- Every agent systemInstruction must be 45 to 90 words.
- Every agent initialTask must be 45 to 90 words.
- Every agent text must include one sentence that starts with "Why this matters:".
- Use full terminology and define abbreviations at first use.
- Include Yield Intelligence guidance in stage 2 using Twitter trends, news signals, and external feeds.
- Ensure plan-level compliance can pass via a parallel compliance check against the rules dataset.
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
            allocationStrategy: (candidate.allocationStrategy || candidate.channelAllocation || variant.allocationStrategy || "").toString().trim(),
            deliveryTiming: (candidate.deliveryTiming || candidate.timingRecommendation || variant.deliveryTiming || "").toString().trim(),
            roiReasoning: (candidate.roiReasoning || candidate.roiRationale || variant.roiReasoning || "").toString().trim(),
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
      complianceDetails: null,
      selectedPlanCompliance: null,
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
      complianceDetails: null,
      selectedPlanCompliance: null,
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
    const selectedPlanRecord = state.architectPlans.find((plan) => plan.id === state.selectedArchitectPlanId);
    const baselineCompliance = state.selectedPlanCompliance || selectedPlanRecord?.complianceValidation || evaluatePlanCompliance({
      campaignPrompt,
      allocationStrategy: selectedPlanRecord?.allocationStrategy || "",
      variantKey: selectedPlanRecord?.variantKey || ""
    });
    const complianceOutput = {
      id: Utils.uniqueId("out"),
      nodeId: PARALLEL_COMPLIANCE_NODE_ID,
      phase: "Parallel",
      name: "Compliance Agent (Parallel)",
      task: "Validate campaign parameters against compliance rules while the six-stage workflow executes.",
      instruction: "Run compliance checks in parallel, flag restricted combinations, and recommend compliant alternatives. Include one sentence that starts with Why this matters:.",
      dataKey: "complianceRulebook",
      text: "",
      status: "running",
      startedAt: Date.now()
    };
    const runningCompliance = {
      ...baselineCompliance,
      status: "Running",
      summary: "Parallel compliance agent is validating campaign parameters against the compliance dataset."
    };

    setState({
      stage: "run",
      agentOutputs: [complianceOutput],
      error: "",
      runningNodeIds: new Set([PARALLEL_COMPLIANCE_NODE_ID]),
      latestNodeId: PARALLEL_COMPLIANCE_NODE_ID,
      dashboard: null,
      issueNodeIds: new Set(),
      approvedPlanCsv: "",
      selectedPlanCompliance: baselineCompliance,
      complianceDetails: buildComplianceDetails(runningCompliance),
      complianceExplanationOpen: false,
      visualizationExplanationOpen: false,
      visualizationLoading: false,
      visualizationNarrative: "",
      runToken
    });

    const compliancePromise = runParallelComplianceAgent(complianceOutput, {
      creds,
      model,
      campaignPrompt,
      selectedPlan: selectedPlanRecord
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

    const complianceResult = await compliancePromise;

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
      selectedPlanCompliance: complianceResult || baselineCompliance,
      complianceDetails: buildComplianceDetails(complianceResult || baselineCompliance),
      visualizationLoading: false,
      visualizationNarrative,
      visualizationExplanationOpen: true
    });
  } catch (e) {
    setState({ error: e.message, stage: "idle", visualizationLoading: false });
  }
}

function buildComplianceNarrative(evaluation, selectedPlan) {
  const findings = evaluation?.findings || [];
  const alternatives = evaluation?.alternatives || [];
  const sources = evaluation?.sources || [];
  const lines = [
    "### Compliance Validation Result",
    `Status: ${evaluation?.status || "Passed"}. ${evaluation?.summary || ""}`,
    "",
    "Why this matters: parallel compliance validation prevents non-compliant spend from entering activation while preserving return on investment objectives."
  ];

  if (selectedPlan?.allocationStrategy) {
    lines.push("");
    lines.push("### Allocation Context");
    lines.push(selectedPlan.allocationStrategy);
  }

  if (findings.length) {
    lines.push("");
    lines.push("### Findings");
    findings.forEach((finding) => lines.push(`- ${finding}`));
  }

  if (alternatives.length) {
    lines.push("");
    lines.push("### Compliant Alternatives");
    alternatives.forEach((item) => lines.push(`- ${item}`));
  }

  if (sources.length) {
    lines.push("");
    lines.push("### Source Traceability");
    sources.forEach((source) => lines.push(`- ${source.policy}: ${source.source}. Reason: ${source.why}`));
  }

  return lines.join("\n");
}

async function runParallelComplianceAgent(out, opts) {
  const { creds, model, campaignPrompt, selectedPlan } = opts;
  const baseline = evaluatePlanCompliance({
    campaignPrompt,
    allocationStrategy: selectedPlan?.allocationStrategy || "",
    variantKey: selectedPlan?.variantKey || ""
  });
  const context = `Parallel compliance context\nPlan strategy: ${selectedPlan?.strategy || "N/A"}\nAllocation strategy: ${selectedPlan?.allocationStrategy || "N/A"}\nDelivery timing: ${selectedPlan?.deliveryTiming || "N/A"}`;
  const inputBlob = `Compliance dataset focus:\n${buildDatasetContext("complianceRulebook")}`;
  const llmNarrative = await runSyntheticAgent(out, {
    creds,
    model,
    agentStyle: "",
    campaignPrompt,
    inputBlob,
    context
  });
  const deterministicNarrative = buildComplianceNarrative(baseline, selectedPlan);
  const merged = [llmNarrative?.trim(), deterministicNarrative].filter(Boolean).join("\n\n");
  updateAgent(out.id, merged, "done");
  return baseline;
}

function buildSupplementalDatasetContext(agent) {
  const role = inferAgentRole(agent);
  if (role === "inventory-yield") {
    return buildDatasetContext("yieldIntelligenceFeed");
  }
  return "";
}

async function runSyntheticAgent(out, opts) {
  const { creds, model, agentStyle, campaignPrompt, inputBlob, context } = opts;
  let narrative = "";
  try {
    const datasetBlock = buildDatasetContext(out.dataKey);
    const supplementalContext = buildSupplementalDatasetContext(out);
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
              `Campaign Brief:\n${campaignPrompt}\n\nTask:\n${out.task}\n\nRelevant Dataset:\n${datasetBlock}${supplementalContext ? `\n\nAdditional Yield Intelligence:\n${supplementalContext}` : ""}\n\nSupplemental Inputs:\n${inputBlob}\n\nPrior Outputs:\n${priorContext}`
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

  if (role === "planning-identity") {
    return `### Planning and Identity Narrative
For the campaign prompt "${campaignPrompt}", the planning and identity stage used the unified audience base dataset to resolve high-value audience cohorts by state. The deterministic output identifies ${reach.uniqueHouseholds.toLocaleString()} unique households and ${reach.deviceCount.toLocaleString()} connected devices that can be activated across channels.

### Data Quality Review
This stage explicitly documented data quality signals before allocation. The synthetic audience base includes ${quality.duplicateCount} duplicate household identifiers and ${quality.nullDmaCount} records with missing designated market area values, which were flagged to avoid overstating addressable reach.

### Why this matters
Accurate identity resolution determines whether downstream allocation decisions improve return on investment or simply increase unproductive delivery volume.`;
  }

  if (role === "inventory-yield") {
    const intelligence = (datasets.yieldIntelligenceFeed || []).slice(0, 3);
    return `### Inventory Yield Narrative
This stage merged unified audience context with Yield Intelligence signals to identify timing windows with higher demand probability. The signal review included sources such as Twitter trends, news signals, and external feed indicators so that delivery can be timed around audience spikes.

### Yield Intelligence Highlights
${intelligence.map((item) => `- ${item.source_type} signal "${item.topic}" in ${item.region} indicates ${item.expected_roi_lift_pct} percent expected return on investment lift with ${item.recommended_channel} emphasis during ${item.spike_window}.`).join("\n")}

### Why this matters
Yield-aware timing decisions improve return on investment by shifting spend toward periods where audience attention and conversion likelihood are measurably stronger.`;
  }

  if (role === "booking-proposals") {
    return `### Booking and Proposals Narrative
The booking stage converted yield and audience recommendations into channel proposals with deterministic allocation logic. The proposal uses return on investment as the decision objective and keeps fallback paths available when inventory quality changes by state.

### Allocation and Cost Logic
Each booking recommendation ties spend to expected impression quality, demand timing, and inventory reliability. The current effective cost per thousand impressions estimate is ${effectiveCostPerThousandImpressions.toFixed(2)} United States dollars based on synthetic delivery totals.

### Why this matters
Proposal quality directly controls whether budget is deployed into inventory that can generate measurable business outcomes.`;
  }

  if (role === "trafficking-signals") {
    return `### Trafficking and Signals Narrative
The trafficking stage prepared activation instructions and validated signal pathways before launch. Signal checks emphasized routing consistency, placement metadata quality, and execution readiness under variable daypart demand.

### Signal Reliability Interpretation
Potential signal risk points were documented with explicit mitigation instructions so operations can respond before pacing degradation affects return on investment.

### Why this matters
Even strong allocation strategy fails without reliable trafficking and signal execution, so this step protects delivery quality before budget goes live.`;
  }

  if (role === "inflight-operations") {
    return `### In-Flight Operations Narrative
The operations stage evaluated pacing trends and identified deterministic under-delivery pressure in the linear lane during the mid-campaign window. A make-good reallocation of ${makeGood.shiftBudget.toLocaleString()} United States dollars was triggered to protect delivery against target outcomes.

### Financial Interpretation
Before intervention, modeled return on investment measured ${makeGood.beforeRoi.toFixed(2)}. After intervention, modeled return on investment rose to ${makeGood.afterRoi.toFixed(2)} as pacing variance was reduced.

### Why this matters
In-flight correction protects campaign value by preventing prolonged under-delivery from eroding final return on investment.`;
  }

  if (role === "measurement") {
    return `### Measurement Narrative
The measurement stage consolidated cross-channel outcomes and quantified performance with deterministic attribution indicators. Reported reach includes ${reach.uniqueHouseholds.toLocaleString()} unique households, ${reach.deviceCount.toLocaleString()} devices, and ${reach.overlapPct} percent cross-platform overlap.

### Return on Investment Interpretation
Measurement output links delivery behavior, channel mix, and corrective actions to final outcome quality so optimization decisions can be replicated in future cycles.

### Why this matters
Without clear measurement traceability, return on investment improvements cannot be validated or scaled across campaigns.`;
  }

  if (role === "compliance-agent") {
    return `### Parallel Compliance Narrative
The compliance agent executed in parallel with the six-stage workflow and validated campaign parameters against synthetic policy rules. Rule evidence was logged with source references and compliant alternatives where restrictions were detected.

### Policy Sources and Rationale
Source one: ${compliance.sources[0]?.policy || "Financial services restriction policy"}. Source dataset: ${compliance.sources[0]?.source || "Global Compliance Policy Engine"}. Reason applied: ${compliance.sources[0]?.why || "Restricted category was detected for the target market."}
Source two: ${compliance.sources[1]?.policy || "Pharmaceutical disclaimer policy"}. Source dataset: ${compliance.sources[1]?.source || "Global Compliance Policy Engine"}. Reason applied: ${compliance.sources[1]?.why || "Creative category requires disclaimer handling."}

### Why this matters
Parallel compliance validation prevents non-compliant activation while preserving campaign pacing and return on investment goals.`;
  }

  return `### Agent Narrative
This synthetic execution step completed the assigned task "${out.task}" using deterministic campaign data. The response emphasizes transparency, operational traceability, and explicit explanation of business impact for downstream review.`;
}

function inferAgentRole(agent) {
  const id = (agent?.nodeId || "").toString().toLowerCase();
  const name = (agent?.agentName || agent?.name || "").toString().toLowerCase();
  const text = `${id} ${name}`;

  if (text.includes("parallel-compliance") || text.includes("compliance agent")) return "compliance-agent";
  if (text.includes("compliance")) return "compliance-agent";
  if (text.includes("planning-identity") || text.includes("planning and identity")) return "planning-identity";
  if (text.includes("inventory-yield") || text.includes("inventory yield")) return "inventory-yield";
  if (text.includes("booking-proposals") || text.includes("booking and proposals")) return "booking-proposals";
  if (text.includes("trafficking-signals") || text.includes("trafficking and signals")) return "trafficking-signals";
  if (text.includes("inflight-operations") || text.includes("in-flight operations")) return "inflight-operations";
  if (text.includes("measurement")) return "measurement";
  if (text.includes("planner") || text.includes("audience")) return "planning-identity";
  if (text.includes("sales") || text.includes("booking") || text.includes("proposal")) return "booking-proposals";
  if (text.includes("ops") || text.includes("operation") || text.includes("delivery") || text.includes("optimization")) return "inflight-operations";

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
    if (phase >= 1 && phase <= TARGET_ARCHITECT_AGENTS) return "unifiedAudienceDataset";
    return "complianceRulebook";
  }
  return "unifiedAudienceDataset";
}

function hasComplianceRole(plan = []) {
  return Array.isArray(plan) && plan.length > 0;
}

function buildRawDataByPlan(plan = []) {
  if (!Array.isArray(plan) || !plan.length) return RAW_DATA_BY_AGENT;
  const map = {};
  plan.forEach((agent) => {
    const role = inferAgentRole(agent);
    const dataKey = role === "inventory-yield" ? "yieldIntelligenceFeed" : resolveDatasetKey(agent);
    const entry = RAW_DATA_BY_KEY[dataKey];
    if (!entry) return;
    map[agent.nodeId] = entry;
    map[agent.agentName] = entry;
  });
  return Object.keys(map).length ? map : RAW_DATA_BY_AGENT;
}

function detectCampaignCountries(prompt = "") {
  const text = (prompt || "").toString().toLowerCase();
  const map = [
    { code: "US", terms: ["united states", "usa", " us "] },
    { code: "IN", terms: ["india", " indian "] },
    { code: "DE", terms: ["germany", " german "] },
    { code: "FR", terms: ["france", " french "] },
    { code: "SA", terms: ["saudi arabia", " saudi "] },
    { code: "UK", terms: ["united kingdom", " britain ", " uk "] },
    { code: "AE", terms: ["uae", "united arab emirates"] },
    { code: "CA", terms: ["canada"] },
    { code: "MX", terms: ["mexico"] }
  ];
  const padded = ` ${text} `;
  const hits = map
    .filter((item) => item.terms.some((term) => padded.includes(term)))
    .map((item) => item.code);
  return hits.length ? hits : ["US"];
}

function detectCampaignCategories(prompt = "") {
  const text = (prompt || "").toString().toLowerCase();
  const categories = new Set();
  if (/\b(alcohol|beer|wine|whiskey|vodka|liquor)\b/i.test(text)) categories.add("alcohol");
  if (/\b(finance|financial|bank|loan|credit|investment)\b/i.test(text)) categories.add("financial_services");
  if (/\b(credit)\b/i.test(text)) categories.add("credit");
  if (/\b(pharma|pharmaceutical|medicine|drug|healthcare)\b/i.test(text)) categories.add("pharma");
  if (/\b(prescription)\b/i.test(text)) categories.add("prescription_drugs");
  if (/\b(gambling|casino)\b/i.test(text)) categories.add("gambling");
  if (/\b(betting)\b/i.test(text)) categories.add("betting");
  if (/\b(tobacco|cigarette|nicotine)\b/i.test(text)) categories.add("tobacco");
  if (/\b(cannabis|marijuana)\b/i.test(text)) categories.add("cannabis");
  if (/\b(crypto|cryptocurrency)\b/i.test(text)) categories.add("cryptocurrency");
  if (/\b(political|election|party)\b/i.test(text)) categories.add("political");
  if (/\b(adult)\b/i.test(text)) categories.add("adult_content");
  if (/\b(children|kids|child)\b/i.test(text)) categories.add("children_products");
  if (/\b(weight loss|slimming)\b/i.test(text)) categories.add("weight_loss");
  if (/\b(job|hiring|recruitment)\b/i.test(text)) categories.add("job");
  if (/\b(housing|real estate|rental)\b/i.test(text)) categories.add("housing");
  return categories.size ? [...categories] : ["general_brand"];
}

function detectCampaignPlatforms(text = "") {
  const value = (text || "").toString().toLowerCase();
  const platforms = new Set();
  if (/\b(linear|television|tv|broadcast)\b/i.test(value)) {
    platforms.add("linear");
    platforms.add("tv");
    platforms.add("broadcast");
    platforms.add("video");
  }
  if (/\b(streaming|digital|connected tv|ott|max ad-lite|discovery\+)\b/i.test(value)) {
    platforms.add("streaming");
    platforms.add("digital");
    platforms.add("video");
  }
  if (/\b(social)\b/i.test(value)) {
    platforms.add("social");
    platforms.add("digital");
  }
  if (/\b(sports|live sports)\b/i.test(value)) {
    platforms.add("sports");
    platforms.add("live_sports");
  }
  if (!platforms.size) {
    platforms.add("streaming");
    platforms.add("linear");
  }
  return [...platforms];
}

function detectAudienceSignals(text = "") {
  const value = (text || "").toString().toLowerCase();
  const signals = new Set();
  if (/\b(minor|minors)\b/i.test(value)) signals.add("minors");
  if (/\b(under 18|under-18|u18)\b/i.test(value)) signals.add("under_18");
  if (/\b(under 21|under-21|u21)\b/i.test(value)) signals.add("under_21");
  if (/\b(teen|teenager|teens)\b/i.test(value)) signals.add("teenagers");
  if (/\b(children|kids|child)\b/i.test(value)) signals.add("children");
  if (/\b(vulnerable|economically vulnerable|financially vulnerable)\b/i.test(value)) signals.add("vulnerable_financial");
  if (/\b(religion|ethnicity|race|gender)\b/i.test(value)) {
    signals.add("religion");
    signals.add("ethnicity");
    signals.add("race");
    signals.add("gender");
  }
  return [...signals];
}

function detectScheduleSignals(text = "") {
  const value = (text || "").toString().toLowerCase();
  const signals = new Set();
  if (/\b(daytime|morning|afternoon)\b/i.test(value)) signals.add("daytime");
  if (/\b(live sports)\b/i.test(value)) signals.add("live_sports_minor");
  return [...signals];
}

function splitRuleValues(value = "") {
  return (value || "")
    .toString()
    .split("|")
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
}

function includesAnyValue(haystack = [], needles = []) {
  const set = new Set((haystack || []).map((item) => item.toString().toLowerCase()));
  return (needles || []).some((needle) => set.has(needle.toString().toLowerCase()));
}

function isAnyRuleValue(value = "") {
  const values = splitRuleValues(value || "any");
  return values.length === 0 || values.includes("any");
}

function hasSpecificRuleConstraint(rule = {}) {
  return ["applies_product", "applies_platform", "applies_audience", "applies_schedule"].some((field) => !isAnyRuleValue(rule[field]));
}

function buildComplianceContext(campaignPrompt = "", allocationStrategy = "") {
  const combined = `${campaignPrompt || ""}\n${allocationStrategy || ""}`.toLowerCase();
  const countries = detectCampaignCountries(combined);
  const geos = new Set(countries);
  geos.add("GLOBAL");
  if (countries.some((country) => ["DE", "FR"].includes(country))) geos.add("EU");
  if (countries.some((country) => ["SA", "AE"].includes(country))) geos.add("MIDDLE_EAST");
  if (countries.length > 1) geos.add("CROSS_BORDER");
  return {
    text: combined,
    countries,
    geos: [...geos],
    categories: detectCampaignCategories(combined),
    platforms: detectCampaignPlatforms(combined),
    audienceSignals: detectAudienceSignals(combined),
    scheduleSignals: detectScheduleSignals(combined)
  };
}

function ruleAppliesToContext(rule, context) {
  const geo = splitRuleValues(rule.applies_geo || "GLOBAL");
  if (!geo.includes("any") && !includesAnyValue(context.geos, geo)) return false;

  const products = splitRuleValues(rule.applies_product || "any");
  if (!products.includes("any") && !includesAnyValue(context.categories, products)) return false;

  const platforms = splitRuleValues(rule.applies_platform || "any");
  if (!platforms.includes("any") && !includesAnyValue(context.platforms, platforms)) return false;

  const audience = splitRuleValues(rule.applies_audience || "any");
  if (!audience.includes("any") && !includesAnyValue(context.audienceSignals, audience)) return false;

  const schedule = splitRuleValues(rule.applies_schedule || "any");
  if (!schedule.includes("any") && !includesAnyValue(context.scheduleSignals, schedule)) return false;

  const triggers = splitRuleValues(rule.trigger_terms || "");
  if (triggers.length && !triggers.some((term) => context.text.includes(term))) return false;

  return true;
}

function ruleEvidencePresent(rule, context) {
  const evidenceTerms = splitRuleValues(rule.required_evidence || "");
  if (!evidenceTerms.length) return false;
  return evidenceTerms.some((term) => context.text.includes(term));
}

function evaluatePlanCompliance({ campaignPrompt = "", allocationStrategy = "", variantKey = "" } = {}) {
  const rows = datasets.complianceRulebook || [];
  const context = buildComplianceContext(campaignPrompt, allocationStrategy);
  const findings = [];
  const alternatives = [];
  const sources = [];
  const matchedRules = [];
  const violatedRuleIds = new Set();

  rows.forEach((rule) => {
    if (!ruleAppliesToContext(rule, context)) return;
    matchedRules.push(rule);

    const action = (rule.required_action || "enforce_policy").toLowerCase();
    const severity = (rule.severity || "medium").toLowerCase();
    const hasEvidence = ruleEvidencePresent(rule, context);
    const hasTriggerCondition = splitRuleValues(rule.trigger_terms || "").length > 0;
    const enforceEvidence = ["require_disclaimer", "require_disclosure", "require_consent", "enforce_frequency", "enforce_safety", "enforce_metadata"].includes(action);
    const isBlockingAction = action === "block";
    const evidenceMissing = enforceEvidence && !hasEvidence;
    const policyNeedsManualReview = action === "enforce_policy" && (hasSpecificRuleConstraint(rule) || hasTriggerCondition);
    const shouldFlag = isBlockingAction || evidenceMissing || policyNeedsManualReview;
    if (!shouldFlag) return;
    violatedRuleIds.add(rule.rule_id);

    findings.push(`Rule ${rule.rule_id} (${severity.toUpperCase()}, ${rule.rule_category}): ${rule.rule_description}`);
    alternatives.push(rule.suggested_fix || "Apply compliant alternative before activation.");
    sources.push({
      policy: `Rule ${rule.rule_id} (${rule.rule_category})`,
      source: `${rule.source_reference || "Advertising Compliance Rulebook synthetic dataset"} (rule_id=${rule.rule_id})`,
      why: isBlockingAction
        ? "Campaign context matches a blocked combination under this rule."
        : evidenceMissing
          ? `Required evidence is missing (${rule.required_evidence || "policy evidence"}).`
          : "Policy review is required for this campaign context."
    });
  });

  if (!sources.length) {
    const sourceRows = matchedRules.length ? matchedRules.slice(0, 3) : rows.slice(0, 3);
    sourceRows.forEach((row) => {
      sources.push({
        policy: `Rule ${row.rule_id} (${row.rule_category})`,
        source: `${row.source_reference || "Advertising Compliance Rulebook synthetic dataset"} (rule_id=${row.rule_id})`,
        why: "This rule was checked against the current campaign context."
      });
    });
  }

  const hasBlocking = rows.some((row) =>
    violatedRuleIds.has(row.rule_id)
    && (row.required_action || "").toLowerCase() === "block"
    && (row.severity || "").toLowerCase() === "high"
  );
  const status = hasBlocking ? "Failed" : findings.length ? "Passed with Adjustments" : "Passed";
  const summary = findings.length
    ? `Parallel compliance check evaluated ${matchedRules.length} matched rules and found ${findings.length} issues in the 50-rule dataset. Apply fixes before activation.`
    : `Parallel compliance check evaluated ${matchedRules.length} matched rules in the 50-rule dataset with no actionable issues.`;

  return {
    status,
    summary,
    findings,
    alternatives,
    sources,
    allocationStrategy,
    variantKey
  };
}

function buildComplianceDetails(evaluation = null) {
  const result = evaluation || evaluatePlanCompliance({ campaignPrompt: state.campaignPrompt || "" });
  return {
    status: result.status,
    summary: result.summary,
    findings: result.findings || [],
    alternatives: result.alternatives || [],
    sources: result.sources || [],
    detailedExplanation:
      "The parallel compliance agent evaluates campaign parameters against a synthetic 50-rule Advertising Compliance Rulebook inspired by India, European Union, United Kingdom, United States, and major ad-platform policy baselines. It checks product restrictions, audience targeting constraints, geographic requirements, privacy obligations, and platform safety controls. When a rule condition is matched, it records the violated rule, explains the reason, links the underlying source reference, and suggests a compliant alternative so the six-agent workflow can continue with auditable policy coverage."
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
  if (!agent || inferAgentRole(agent) !== "compliance-agent") return false;
  const text = (agent.text || "").toLowerCase();
  if (/\b(unresolved|failed compliance|cannot proceed|activation blocked)\b/.test(text)) return true;
  return false;
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

  const pipelineOrder = [
    "ui",
    "orchestration",
    "planning-identity-agent",
    "inventory-yield-agent",
    "booking-proposals-agent",
    "trafficking-signals-agent",
    "inflight-operations-agent",
    "measurement-agent",
    PARALLEL_COMPLIANCE_NODE_ID,
    "output"
  ];
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
