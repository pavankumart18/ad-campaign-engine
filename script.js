// VERSION: 2024-01-XX - Added hardcoded plan support for grant-to-growth-predictor
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
  planner: "Matched Olli Auto Intender households and flagged data quality issues.",
  "The Planner": "Matched Olli Auto Intender households and flagged data quality issues.",
  "sales-rep": "Built 12-line converged media plan with StreamX SCTE-35 risk notes.",
  "The Sales Rep": "Built 12-line converged media plan with StreamX SCTE-35 risk notes.",
  "compliance-bot": "Flagged compliance violations and re-trafficked placements.",
  "The Compliance Bot": "Flagged compliance violations and re-trafficked placements.",
  "ops-director": "Triggered StreamX make-good and recalculated converged ROI.",
  "The Ops Director": "Triggered StreamX make-good and recalculated converged ROI."
};

const RAW_DATA_BY_AGENT = {
  planner: datasetEntries.find((d) => d.key === "audienceGraph"),
  "sales-rep": datasetEntries.find((d) => d.key === "inventoryMatrix"),
  "compliance-bot": datasetEntries.find((d) => d.key === "globalLawRegistry"),
  "ops-director": datasetEntries.find((d) => d.key === "liveDeliveryLog")
};

const DATA_QUALITY = computeDataQuality(datasets.audienceGraph || []);

let config = {};
let state = {
  selectedDemoIndex: null,
  stage: "idle",
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
  pacingMode: "pct"
};

const llmSession = { creds: null };
const actions = {
  planDemo: (i) => { 
    console.log("planDemo called with index:", i);
    // Check for hardcoded plan BEFORE selecting demo - access config directly
    if (!config || !config.demos || !config.demos[i]) {
      console.error("Config or demo not available at index:", i);
      selectDemo(i); 
      runArchitect();
      return;
    }
    
    const demo = config.demos[i];
    console.log("Demo found:", demo.title, "ID:", demo.id, "Plan structure:", demo.plan);
    
    // Explicit check for hardcoded plan structure: demo.plan.plan (array)
    // Also check by ID as fallback
    const isGrantToGrowth = demo.id === 'grant-to-growth-predictor';
    const hasPlanStructure = demo.plan && 
                              typeof demo.plan === 'object' && 
                              demo.plan.hasOwnProperty('plan') &&
                              Array.isArray(demo.plan.plan) && 
                              demo.plan.plan.length > 0;
    const hasHardcodedPlan = hasPlanStructure || (isGrantToGrowth && demo.plan);
    
    console.log("Is Grant-to-Growth?", isGrantToGrowth);
    console.log("Has plan structure?", hasPlanStructure);
    console.log("Has hardcoded plan?", hasHardcodedPlan);
    
    if (hasHardcodedPlan) {
      // Has hardcoded plan - use it directly, skip API call
      console.log("OK Using hardcoded plan - SKIPPING API CALL");
      selectDemo(i);
      useHardcodedPlan(demo);
      return; // CRITICAL: Exit here, do NOT call runArchitect
    }
    
    // No hardcoded plan - proceed normally
    console.log("✗ No hardcoded plan - will call architect API");
    selectDemo(i); 
    runArchitect(); 
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
    setState({
      selectedDemoIndex: -2, // Special index for saved agent
      customProblem: { title: agent.title, problem: agent.problem },
      plan: agent.plan,
      suggestedInputs: agent.inputs || [],
      selectedInputs: new Set((agent.inputs || []).map(i => i.id)),
      stage: "data", // Ready to start inputs or run
      agentOutputs: [],
      error: "",
      editingAgentId: null, // Clear separate edit session
      campaignPrompt: agent.problem || ""
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
  if ($("#max-agents")) $("#max-agents").value = defaults.maxAgents || 4;
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
    approvedPlanCsv: ""
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

function buildDashboard(agentOutputs = []) {
  return {
    pacing: buildPacingSeries(datasets.liveDeliveryLog || []),
    reach: buildReachSummary(datasets.audienceGraph || [], datasets.liveDeliveryLog || []),
    actions: buildActionRows(agentOutputs),
    makeGood: buildMakeGoodSummary(datasets.liveDeliveryLog || [])
  };
}

function buildPacingSeries(logs) {
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
    const linearPlanned = linearRows.reduce((sum, r) => sum + r.planned_impressions, 0);
    const linearDelivered = linearRows.reduce((sum, r) => sum + r.delivered_impressions, 0);
    const digitalPlanned = digitalRows.reduce((sum, r) => sum + r.planned_impressions, 0);
    const digitalDelivered = digitalRows.reduce((sum, r) => sum + r.delivered_impressions, 0);
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
    digitalPlanned: digitalPlannedArr
  };
}

function buildReachSummary(audience, logs) {
  const autoIntenders = audience.filter((row) => row.segment_name === "Auto Intenders" && row.auto_intender_score > 70);
  const uniqueMap = new Map();
  autoIntenders.forEach((row) => {
    if (!uniqueMap.has(row.olli_household_id)) {
      uniqueMap.set(row.olli_household_id, row);
    }
  });
  const uniqueHouseholds = uniqueMap.size;
  const deviceCount = [...uniqueMap.values()].reduce((sum, row) => sum + (row.device_count || 0), 0);
  const scaledReach = Math.round((uniqueHouseholds * 32500) / 100) * 100;
  const totals = logs.reduce(
    (acc, row) => {
      acc[row.platform_type] += row.delivered_impressions;
      return acc;
    },
    { linear: 0, digital: 0 }
  );
  const totalImpressions = totals.linear + totals.digital || 1;
  const linearPct = Math.round((totals.linear / totalImpressions) * 100);
  const overlapPct = Math.round(
    logs.reduce((sum, row) => sum + (row.viewer_overlap_pct || 0), 0) / Math.max(logs.length, 1)
  );
  return {
    uniqueHouseholds,
    deviceCount,
    scaledReach,
    linearPct,
    digitalPct: 100 - linearPct,
    overlapPct
  };
}

function buildActionRows(agentOutputs) {
  const base = agentOutputs.reduce((min, a) => Math.min(min, a.startedAt || Date.now()), Date.now());
  const autoIntenders = datasets.audienceGraph.filter((row) => row.segment_name === "Auto Intenders" && row.auto_intender_score > 70);
  const uniqueHouseholds = new Set(autoIntenders.map((row) => row.olli_household_id)).size;
  const linearBudget = 30000;
  const digitalBudget = 20000;
  const delivered = datasets.liveDeliveryLog.reduce((sum, row) => sum + row.delivered_impressions, 0);
  const effectiveCpm = delivered ? ((50000 / delivered) * 1000) : 0;

  const rows = [
    {
      id: "planner",
      agent: "The Planner",
      time: base + 0 * 1000,
      status: "done",
      summary: `Planner identified ${uniqueHouseholds.toLocaleString()} Auto Intender households from Olli graph.`
    },
    {
      id: "sales-rep",
      agent: "The Sales Rep",
      time: base + 8 * 1000,
      status: "done",
      summary: `Sales Rep allocated $${linearBudget.toLocaleString()} to TNT/CNN linear and $${digitalBudget.toLocaleString()} to Max/Discovery+ digital.`
    },
    {
      id: "compliance-bot",
      agent: "Compliance Bot",
      time: base + 15 * 1000,
      status: "done",
      summary: "Compliance Bot flagged: Auto loan ad BLOCKED for Saudi Arabia feed (Financial Services restriction). Reallocated to UK feed."
    },
    {
      id: "compliance-bot",
      agent: "Compliance Bot",
      time: base + 22 * 1000,
      status: "done",
      summary: "Compliance Bot flagged: Pharma disclaimer required for Germany — auto-appended."
    },
    {
      id: "ops-director",
      agent: "Ops Director",
      time: base + 30 * 1000,
      status: "done",
      summary: "Ops Director detected: TNT linear pacing at 42% (target: 85%). Triggering Make-Good."
    },
    {
      id: "ops-director",
      agent: "Ops Director",
      time: base + 35 * 1000,
      status: "done",
      summary: "Ops Director shifted $3,200 from TNT linear -> Max ad-lite streaming."
    },
    {
      id: "ops-director",
      agent: "Ops Director",
      time: base + 42 * 1000,
      status: "done",
      summary: `Final deterministic ROI: 4.2x ROAS, ${Math.round(uniqueHouseholds * 1.6).toLocaleString()} de-duplicated households, effective CPM $${effectiveCpm.toFixed(2)}.`
    }
  ];

  return rows;
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

function buildMakeGoodSummary(logs) {
  const linear = logs.filter((row) => row.platform_type === "linear");
  const linearPlanned = linear.reduce((sum, row) => sum + row.planned_impressions, 0);
  const linearDelivered = linear.reduce((sum, row) => sum + row.delivered_impressions, 0);
  const linearPct = linearPlanned ? linearDelivered / linearPlanned : 1;
  const beforeRoi = 1.72;
  const afterRoi = linearPct < 0.85 ? 2.04 : 1.88;
  return {
    shiftBudget: 3200,
    beforeRoi,
    afterRoi
  };
}

function useHardcodedPlan(demo) {
  // Use hardcoded plan directly - no API call needed
  console.log("OK Hardcoded plan detected, using it directly. Skipping architect API call.");
  const maxAgents = Math.min(Math.max(parseInt($("#max-agents")?.value || 5), 2), 6);
  const normalizedPlan = Utils.normalizePlan(demo.plan.plan, maxAgents);
  const normalizedInputs = (demo.inputs || []).map(i => ({ ...i, id: Utils.uniqueId("input") }));
  
  setState({
    plan: normalizedPlan,
    suggestedInputs: normalizedInputs,
    stage: "data"
  });
  // Auto-select inputs
  setState({ selectedInputs: new Set(state.suggestedInputs.map(i => i.id)) });
}

async function runArchitect() {
  try {
    // Get the demo object - try multiple sources
    let demo = null;
    if (state.selectedDemoIndex === -1) {
      demo = state.customProblem;
    } else if (state.selectedDemoIndex === -2) {
      // saved agent re-run - just go to data
      setState({ stage: "data" });
      return;
    } else if (state.selectedDemoIndex >= 0) {
      // Try stored demo first, then config
      demo = state.selectedDemo || (config.demos && config.demos[state.selectedDemoIndex]);
    }

    if (!demo) {
      setState({ error: "Demo not found. Index: " + state.selectedDemoIndex, stage: "idle" });
      return;
    }

    // DOUBLE CHECK: If demo has a hardcoded plan, use it (backup safety check)
    // The structure is: demo.plan.plan (nested plan object with plan array)
    if (demo.plan && 
        typeof demo.plan === 'object' && 
        demo.plan.plan && 
        Array.isArray(demo.plan.plan) && 
        demo.plan.plan.length > 0) {
      // Use hardcoded plan directly - no API call needed
      console.log("OK Hardcoded plan detected in runArchitect (backup check), using it directly.");
      useHardcodedPlan(demo);
      return; // CRITICAL: Early return - do not proceed to API call
    }
    
    console.log("✗ No hardcoded plan found. Demo plan structure:", demo.plan);
    console.log("Will call architect API with problem:", demo.problem || demo.body || "N/A");

    // Otherwise, generate plan using architect
    // Ensure we have a valid problem string - never null or undefined
    const problemText = (demo.problem || demo.body || "Generate a plan for this workflow.").toString().trim();
    if (!problemText || problemText.length === 0) {
      setState({ error: "No problem description available for this demo.", stage: "idle" });
      return;
    }

    const creds = await ensureCreds();
    const model = $("#model")?.value || "gpt-5-mini";
    const maxAgents = Math.min(Math.max(parseInt($("#max-agents")?.value || 5), 2), 6);

    // Ensure system prompt is also never null
    const systemPrompt = ($("#architect-prompt")?.value || "").toString().trim();
    const systemContent = systemPrompt ? `${systemPrompt}\nLimit to <= ${maxAgents} agents.` : `Limit to <= ${maxAgents} agents.`;

    setState({ stage: "architect", plan: [], architectBuffer: "" });
    let buffer = "";

    await Utils.streamChatCompletion({
      llm: creds,
      body: { 
        model, 
        stream: true, 
        messages: [
          { role: "system", content: systemContent }, 
          { role: "user", content: problemText }
        ] 
      },
      onChunk: (text) => { buffer += text; setState({ architectBuffer: buffer }); }
    });

    const parsed = Utils.safeParseJson(buffer);
    setState({
      plan: Utils.normalizePlan(parsed.plan, maxAgents),
      suggestedInputs: Utils.normalizeInputs(parsed.inputs, demo),
      stage: "data"
    });
    // Auto-select inputs
    setState({ selectedInputs: new Set(state.suggestedInputs.map(i => i.id)) });

  } catch (e) { setState({ error: e.message, stage: "idle" }); }
}

async function startAgents() {
  const entries = [...state.suggestedInputs.filter(i => state.selectedInputs.has(i.id)), ...state.uploads];
  if (state.notes?.trim()) entries.push({ title: "User Notes", type: "text", content: state.notes });
  if (!entries.length) return setState({ error: "No data." });

  try {
    const creds = await ensureCreds();
    const model = $("#model")?.value || "gpt-5-mini";
    const demo = state.selectedDemoIndex === -1 ? state.customProblem : (state.selectedDemoIndex === -2 ? state.customProblem : config.demos[state.selectedDemoIndex]);
    // Use demo-specific agentStyle if available, otherwise use form value
    const agentStyle = demo?.agentStyle || $("#agent-style")?.value || "";
    const campaignPrompt = (state.campaignPrompt || demo?.problem || "Launch a converged ad campaign.").trim();
    const inputBlob = `Campaign Brief:\n${campaignPrompt}\n\nSupplemental Inputs:\n${Utils.formatDataEntries(entries)}`;

    setState({ stage: "run", agentOutputs: [], error: "", runningNodeIds: new Set(), latestNodeId: null, dashboard: null, issueNodeIds: new Set(), approvedPlanCsv: "" });

    // Group by phase
    const phases = new Map();
    state.plan.forEach(a => { const p = a.phase ?? 0; if (!phases.has(p)) phases.set(p, []); phases.get(p).push(a); });
    const sortedPhases = [...phases.keys()].sort((a, b) => (typeof a === 'number' && typeof b === 'number') ? a - b : String(a).localeCompare(String(b)));

    let context = inputBlob;
    for (const phase of sortedPhases) {
      const agents = phases.get(phase);
      const outputs = agents.map(a => {
        const dataKey = AGENT_DATASET_MAP[a.nodeId] || AGENT_DATASET_MAP[a.agentName];
        return {
          id: Utils.uniqueId("out"),
          nodeId: a.nodeId,
          phase,
          name: a.agentName,
          task: a.initialTask,
          instruction: a.systemInstruction,
          dataKey,
          text: "",
          status: "running",
          startedAt: Date.now()
        };
      });

      setState({ agentOutputs: [...state.agentOutputs, ...outputs], runningNodeIds: new Set([...state.runningNodeIds, ...outputs.map(o => o.nodeId)]), latestNodeId: outputs[0]?.nodeId });

      const results = await Promise.all(outputs.map(out => (async () => {
        let buffer = "";
        try {
          const datasetBlock = buildDatasetContext(out.dataKey);
          const priorContext = Utils.truncate(context, 1200);
          await Utils.streamChatCompletion({
            llm: creds,
            body: {
              model, stream: true, messages: [
                { role: "system", content: `${out.instruction}\n${agentStyle}\nOutput Markdown. Keep response under 180 words.` },
                {
                  role: "user",
                  content:
                    `Campaign Brief:\n${campaignPrompt}\n\nTask:\n${out.task}\n\nRelevant Dataset:\n${datasetBlock}\n\nSupplemental Inputs:\n${inputBlob}\n\nPrior Outputs:\n${priorContext}`
                }
              ]
            },
            onChunk: (t) => { buffer += t; updateAgent(out.id, buffer, "running"); }
          });
          updateAgent(out.id, buffer, "done");
          return buffer;
        } catch (e) { updateAgent(out.id, buffer || e.message, "error"); return ""; }
        finally {
          const run = new Set(state.runningNodeIds); run.delete(out.nodeId); setState({ runningNodeIds: run });
        }
      })()));
      context += `\n\n--- Stage ${phase} ---\n${results.map((r, i) => `Output ${agents[i].agentName}:\n${r}`).join("\n")}`;
    }
    setState({ stage: "idle", dashboard: buildDashboard(state.agentOutputs), approvedPlanCsv: buildApprovedPlanCsv() });

  } catch (e) { setState({ error: e.message, stage: "idle" }); }
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
  if (!agent || agent.nodeId !== "compliance-bot") return false;
  const text = (agent.text || "").toLowerCase();
  return text.includes("violation") || text.includes("non-compliant") || text.includes("banned") || text.includes("red");
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

  const sig = JSON.stringify({ mode, hours: pacing.hours.length, hash: pacing.linearImp?.[0] });
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
    .call(d3.axisBottom(x).ticks(6).tickFormat(d => `H${d}`))
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

  const makeGoodHour = 12;
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
    .text("Make-Good");

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
      label = `H${hour} · Linear ${fmt.format(pacing.linearImp[idx])}/${fmt.format(pacing.linearPlanned[idx])} · Digital ${fmt.format(pacing.digitalImp[idx])}/${fmt.format(pacing.digitalPlanned[idx])}`;
    } else {
      label = `H${hour} · Linear ${pacing.linearPct[idx]}% · Digital ${pacing.digitalPct[idx]}%`;
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

  const sig = JSON.stringify({ reach: reach.uniqueHouseholds, overlap: reach.overlapPct });
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
  if (state.stage === "architect") return "architect-plan";
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
  if (demo?.flowchart?.nodes?.length) {
    return addFlowchartPositions(demo.flowchart);
  }
  return Utils.buildGraphFromPlan(plan);
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
      position: positions[n.id] || { x: xMain, y: 0 }
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
