// VERSION: 2026-03-25 - Data-aware scenario generation with hierarchical master/sub-agent execution and shared campaign state.
import { openaiConfig } from "bootstrap-llm-provider";
import saveform from "saveform";
import { createFlowchart } from "./flowchart.js";
import * as Utils from "./utils.js";
import * as View from "./view.js";
import { Storage } from "./storage.js";
import { datasets, datasetMeta, datasetEntries, toCsv } from "./data.js";
import * as d3 from "d3";

function sanitizeDisplayText(value = "") {
  return String(value)
    .replace(/\bolli_household_id\b/gi, "audience_household_id")
    .replace(/\bneo_order_id\b/gi, "booking_order_id")
    .replace(/\bstreamx_delivery_pct\b/gi, "delivery_pct")
    .replace(/\bexpected_roi_lift_pct\b/gi, "expected_response_lift_pct")
    .replace(/\broiReasoning\b/gi, "channelLogic")
    .replace(/\bbefore_roi\b/gi, "before_outcome")
    .replace(/\bafter_roi\b/gi, "after_outcome")
    .replace(/\bdeterministic_roi\b/gi, "outcome_signal")
    .replace(/\bOlli Household ID\b/gi, "Audience Household ID")
    .replace(/\bNeo Order ID\b/gi, "Booking Order ID")
    .replace(/\bStreamx Delivery %\b/gi, "Delivery %")
    .replace(/\bMeasurement and Attribution\b/gi, "Measurement and Learning")
    .replace(/\bDemo\s*Direct\b/gi, "Booking")
    .replace(/\bOlli\b/gi, "Audience Identity")
    .replace(/\bNEO\b/gi, "Planning")
    .replace(/\bNeo\b/gi, "Planning")
    .replace(/\breturn on investment\b/gi, "measured outcome")
    .replace(/\broi\b/gi, "outcome")
    .replace(/\bStreamX\b/gi, "Delivery");
}

const $ = (s) => document.querySelector(s);
const DEFAULT_BASE_URLS = ["https://api.openai.com/v1", "https://llmfoundry.straivedemo.com/openai/v1"];

const FLOW_BRIEF_NODE_ID = "campaign-brief-node";
const ORCHESTRATOR_NODE_ID = "orchestrator-agent";
const FLOW_OUTPUT_NODE_ID = "campaign-output-node";
const PARALLEL_COMPLIANCE_NODE_ID = "parallel-compliance-agent";
const WORKFLOW_AGENTS = [
  {
    nodeId: "planning-identity-agent",
    agentName: "Planning and Identity Master Agent",
    phase: 1,
    focus: "audience segmentation, identity resolution, and cross-platform household understanding"
  },
  {
    nodeId: "inventory-yield-agent",
    agentName: "Inventory and Yield Master Agent",
    phase: 2,
    focus: "predictive capacity planning, yield optimization, and network-daypart pricing strategy"
  },
  {
    nodeId: "booking-proposals-agent",
    agentName: "Booking and Proposal Master Agent",
    phase: 3,
    focus: "deal structuring, proposal packaging, and compliance-aware booking decisions"
  },
  {
    nodeId: "trafficking-signals-agent",
    agentName: "Trafficking and Signals Master Agent",
    phase: 4,
    focus: "asset quality assurance, trafficking readiness, and delivery signal routing"
  },
  {
    nodeId: "inflight-operations-agent",
    agentName: "In-Flight Operations Master Agent",
    phase: 5,
    focus: "real-time pacing management, autonomous make-good logic, and delivery stabilization"
  },
  {
    nodeId: "measurement-agent",
    agentName: "Measurement and Learning Master Agent",
    phase: 6,
    focus: "household match validation, cross-channel outcome reading, and next-flight planning"
  }
];

const MASTER_AGENT_SUBAGENTS = {
  "planning-identity-agent": [
    {
      id: "audience-segmentation",
      name: "Audience Segmentation",
      summary: "Scores the natural-language brief against WBD viewer profiles to isolate high-fit cohorts.",
      definition: "Identifies which audience groups best match the campaign brief before any channel decisions are made."
    },
    {
      id: "cross-platform-id-resolver",
      name: "Cross-Platform ID Resolver",
      summary: "Uses the audience identity graph to deduplicate households across streaming and linear touchpoints.",
      definition: "Connects streaming and linear identifiers so the same household is not counted multiple times."
    },
    {
      id: "behavioral-indexer",
      name: "Behavioral Indexer",
      summary: "Ranks behavioral tags, dayparts, and networks that over-index for the brief.",
      definition: "Translates raw audience behavior into the viewing habits, networks, and tags that matter most for planning."
    }
  ],
  "inventory-yield-agent": [
    {
      id: "predictive-capacity",
      name: "Predictive Capacity",
      summary: "Checks whether the recommended mix can be fulfilled without overselling future avails.",
      definition: "Tests whether the selected route has enough believable supply to launch without forcing weak inventory."
    },
    {
      id: "yield-optimizer",
      name: "Yield Optimizer",
      summary: "Sets premium versus blended CPM guidance across streaming and linear inventory.",
      definition: "Turns audience fit and market pressure into realistic pricing guidance for each channel."
    },
    {
      id: "signal-forecaster",
      name: "Signal Forecaster",
      summary: "Applies trend, news, and external-feed signals to recommend timing windows.",
      definition: "Reads timing signals to show when audience attention is most likely to be strongest."
    }
  ],
  "booking-proposals-agent": [
    {
      id: "deal-structurer",
      name: "Booking Structurer",
      summary: "Builds a converged contract structure across the chosen networks and platforms.",
      definition: "Shapes the plan into bookable placements, channel commitments, and guarantee logic."
    },
    {
      id: "global-compliance-bot",
      name: "Global Compliance Bot",
      summary: "Screens the product, audience, and markets for regulatory risk before booking.",
      definition: "Checks whether the proposed audience, markets, and creative path stay inside policy guardrails."
    },
    {
      id: "proposal-assembler",
      name: "Proposal Assembler",
      summary: "Turns yield and compliance outputs into a buyer-ready proposal package.",
      definition: "Packages the scenario into a proposal that a sales or planning team could actually review."
    }
  ],
  "trafficking-signals-agent": [
    {
      id: "asset-qa",
      name: "Asset QA",
      summary: "Validates that creative and metadata are ready across TV, CTV, and mobile surfaces.",
      definition: "Checks that the creative package and metadata are ready before the campaign goes live."
    },
    {
      id: "streamx-router",
      name: "Delivery Signal Router",
      summary: "Sets routing and SCTE-35 handling for live and on-demand delivery paths.",
      definition: "Validates the delivery path so signals, routing, and ad markers fire correctly in each environment."
    },
    {
      id: "launch-readiness",
      name: "Launch Readiness",
      summary: "Finalizes the trafficking checklist and escalation points before activation.",
      definition: "Confirms the campaign can launch cleanly and flags anything operations still needs to watch."
    }
  ],
  "inflight-operations-agent": [
    {
      id: "real-time-pacing",
      name: "Real-Time Pacing",
      summary: "Monitors line-item delivery risk and channel under-performance as the campaign runs.",
      definition: "Tracks whether the live campaign is pacing toward its delivery target or starting to slip."
    },
    {
      id: "autonomous-make-good",
      name: "Autonomous Make-Good",
      summary: "Reallocates spend from weak channels into stronger inventory when guarantees are at risk.",
      definition: "Shifts budget when needed so the plan can recover before an under-delivery problem grows."
    },
    {
      id: "ops-alerting",
      name: "Ops Alerting",
      summary: "Logs root causes and recovery instructions for human review.",
      definition: "Documents what went wrong, what changed, and what a human operator should know next."
    }
  ],
  "measurement-agent": [
    {
      id: "household-match-validator",
      name: "Household Match Validator",
      summary: "Confirms how many deduplicated households can be matched back to the privacy-safe exposure records.",
      definition: "Validates the measurable household base before any outcome readout is interpreted."
    },
    {
      id: "outcome-pattern-reader",
      name: "Outcome Pattern Reader",
      summary: "Explains which audience pockets, channels, and delivery conditions produced the strongest measured response signals.",
      definition: "Translates matched delivery results into a clear explanation of what changed and where the strongest signals appeared."
    },
    {
      id: "next-flight-planner",
      name: "Next Flight Planner",
      summary: "Turns the measurement readout into the clearest next-step recommendation for the next campaign cycle.",
      definition: "Converts the measured response pattern into an actionable follow-up plan for the next flight."
    }
  ]
};

const AGENT_DATASET_MAP = {
  "planning-identity-agent": "unifiedAudienceDataset",
  "Planning and Identity Master Agent": "unifiedAudienceDataset",
  "inventory-yield-agent": "inventoryMatrix",
  "Inventory and Yield Master Agent": "inventoryMatrix",
  "booking-proposals-agent": "inventoryMatrix",
  "Booking and Proposal Master Agent": "inventoryMatrix",
  "trafficking-signals-agent": "inventoryMatrix",
  "Trafficking and Signals Master Agent": "inventoryMatrix",
  "inflight-operations-agent": "liveDeliveryLog",
  "In-Flight Operations Master Agent": "liveDeliveryLog",
  "measurement-agent": "liveDeliveryLog",
  "Measurement and Attribution Master Agent": "liveDeliveryLog",
  "Measurement and Learning Master Agent": "liveDeliveryLog",
  "planning-identity": "unifiedAudienceDataset",
  "inventory-yield": "inventoryMatrix",
  "booking-proposals": "inventoryMatrix",
  "trafficking-signals": "inventoryMatrix",
  "inflight-operations": "liveDeliveryLog",
  "measurement": "liveDeliveryLog",
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
  "planning-identity-agent": "Scored WBD viewer profiles, resolved cross-platform overlap, and established the operating audience for the run.",
  "inventory-yield-agent": "Checked future avails, priced the recommended mix, and identified the strongest timing windows.",
  "booking-proposals-agent": "Structured the converged proposal and aligned it with compliance and guarantee logic.",
  "trafficking-signals-agent": "Confirmed asset readiness, routing setup, and signal handling before launch.",
  "inflight-operations-agent": "Simulated pacing, executed make-good logic, and protected the delivery guarantee.",
  "measurement-agent": "Validated matched households, interpreted cross-channel outcome signals, and wrote the next-flight learning package.",
  [PARALLEL_COMPLIANCE_NODE_ID]: "Validated campaign parameters against compliance rules and provided compliant alternatives when restrictions were detected."
};

const RAW_DATA_BY_AGENT = {
  "planning-identity-agent": datasetEntries.find((d) => d.key === "unifiedAudienceDataset"),
  "inventory-yield-agent": datasetEntries.find((d) => d.key === "inventoryMatrix"),
  "booking-proposals-agent": datasetEntries.find((d) => d.key === "inventoryMatrix"),
  "trafficking-signals-agent": datasetEntries.find((d) => d.key === "inventoryMatrix"),
  "inflight-operations-agent": datasetEntries.find((d) => d.key === "liveDeliveryLog"),
  "measurement-agent": datasetEntries.find((d) => d.key === "liveDeliveryLog"),
  [PARALLEL_COMPLIANCE_NODE_ID]: datasetEntries.find((d) => d.key === "complianceRulebook")
};

const ARCHITECT_PLAN_FALLBACK_TITLES = [
  "Scenario A",
  "Scenario B",
  "Scenario C"
];
const MIN_ARCHITECT_AGENTS = 6;
const MAX_ARCHITECT_AGENTS = 6;
const TARGET_ARCHITECT_AGENTS = 6;
const LEGACY_VARIANT_KEY_MAP = {
  "plan-a-streaming": "scenario-a",
  "plan-b-linear": "scenario-b",
  "plan-c-balanced": "scenario-c"
};
const PLAN_VARIANTS = [
  {
    key: "scenario-a",
    routeType: "precision",
    planLabel: "Scenario A",
    title: "Streaming-Heavy Route",
    promptTile: "Scenario A",
    promptText: "Lead with a streaming-heavy mix when the audience leans on-demand and the strongest lift sits in streaming inventory.",
    strategy: "Streaming-heavy route designed to let on-demand viewing behavior carry the opening weight while linear supports reinforcement and reach control.",
    why: "This route is strongest when streaming-heavy households, younger viewers, or binge-oriented behaviors are doing more of the work than the linear signals.",
    allocationStrategy: "Initial allocation: let streaming carry the majority of delivery, keep a lighter linear support layer, and preserve reserve budget for pacing corrections.",
    deliveryTiming: "Lean into the strongest streaming attention windows first, then widen linear support only where overlap and frequency stay controlled.",
    channelLogic: "Use this route when the matched audience clearly skews toward streaming-heavy behavior and the top streaming networks hold the strongest audience fit.",
    priorities: {
      "planning-identity-agent": "Prioritize high-intent multi-device households and the tightest behavioral cohort fit.",
      "inventory-yield-agent": "Prioritize premium inventory pockets and yield windows that can convert quickly.",
      "booking-proposals-agent": "Structure the proposal around the highest-fit placements first, with lighter support layers.",
      "trafficking-signals-agent": "Optimize routing for fast swaps, strong metadata, and responsive cross-screen delivery.",
      "inflight-operations-agent": "Protect the best-performing pockets first and reallocate quickly when weaker inventory slips.",
      "measurement-agent": "Emphasize household match quality, outcome signal clarity, and fast learning."
    }
  },
  {
    key: "scenario-b",
    routeType: "scale",
    planLabel: "Scenario B",
    title: "Linear-Heavy Route",
    promptTile: "Scenario B",
    promptText: "Start with a linear-heavy mix when the audience is broad, older-skewing, or more reachable through dependable linear inventory.",
    strategy: "Linear-heavy route designed to build stable household coverage first, then use streaming as a support layer for reinforcement and duplication control.",
    why: "This route is strongest when the matched audience is spread across broad household viewing behavior and the linear signals are stronger than the streaming ones.",
    allocationStrategy: "Initial allocation: anchor on the most reliable linear reach inventory, add streaming where it sharpens frequency control, and keep a reserve for protection.",
    deliveryTiming: "Prioritize the steadiest high-capacity linear windows first, then add streaming support where incremental reach is still available.",
    channelLogic: "Use this route when the linear-heavy share, linear affinity, and strongest-fit linear networks all point to broader television coverage as the cleanest opening move.",
    priorities: {
      "planning-identity-agent": "Prioritize broad household clusters, stable audiences, and the markets where scale is efficient.",
      "inventory-yield-agent": "Prioritize high-capacity avails and dependable reach windows before adding support layers.",
      "booking-proposals-agent": "Structure the proposal around broad reach with efficient reinforcement layers.",
      "trafficking-signals-agent": "Harden signal readiness and metadata quality across the largest reach surfaces.",
      "inflight-operations-agent": "Protect guarantees first and use support channels only when scale needs help.",
      "measurement-agent": "Emphasize deduplicated reach, household penetration, and what the scale path actually taught the team."
    }
  },
  {
    key: "scenario-c",
    routeType: "adaptive",
    planLabel: "Scenario C",
    title: "Balanced Route",
    promptTile: "Scenario C",
    promptText: "Keep the opening mix balanced when streaming and linear both show credible audience fit and the brief needs flexibility more than a hard bias.",
    strategy: "Balanced route designed to keep both channels credible from the start so pacing and response evidence can decide where extra weight should go.",
    why: "This route is strongest when the audience evidence is mixed or when streaming and linear both have believable reasons to stay in the plan.",
    allocationStrategy: "Initial allocation: keep both major channels viable, hold a meaningful reserve, and let in-flight evidence decide the final weight.",
    deliveryTiming: "Split delivery across the best shared attention windows, then rebalance every pacing cycle using readiness and response signals.",
    channelLogic: "Use this route when the streaming-heavy and linear-heavy signals are close enough that a balanced opening is safer than forcing an extreme channel bias.",
    priorities: {
      "planning-identity-agent": "Balance high-fit cohorts and broader households so downstream stages can pivot without rebuilding the audience.",
      "inventory-yield-agent": "Maintain comparable visibility across reach, cost, and timing so the system can rebalance confidently.",
      "booking-proposals-agent": "Build a flexible proposal with explicit pivot points by channel and market.",
      "trafficking-signals-agent": "Maintain symmetric launch readiness across all candidate delivery paths.",
      "inflight-operations-agent": "Rebalance calmly between channels and reserve based on live delivery quality.",
      "measurement-agent": "Evaluate channel interaction, resilience, and the clearest next move."
    }
  }
];

const VARIANT_SUBAGENT_IDS = {
  "scenario-a": {
    "planning-identity-agent": ["audience-segmentation", "behavioral-indexer"],
    "inventory-yield-agent": ["yield-optimizer", "signal-forecaster"],
    "booking-proposals-agent": ["deal-structurer", "proposal-assembler"],
    "trafficking-signals-agent": ["asset-qa", "streamx-router"],
    "inflight-operations-agent": ["real-time-pacing", "autonomous-make-good"],
    "measurement-agent": ["household-match-validator", "outcome-pattern-reader"]
  },
  "scenario-b": {
    "planning-identity-agent": ["audience-segmentation", "cross-platform-id-resolver"],
    "inventory-yield-agent": ["predictive-capacity", "yield-optimizer"],
    "booking-proposals-agent": ["deal-structurer", "global-compliance-bot"],
    "trafficking-signals-agent": ["launch-readiness"],
    "inflight-operations-agent": ["real-time-pacing", "ops-alerting"],
    "measurement-agent": ["outcome-pattern-reader", "next-flight-planner"]
  },
  "scenario-c": {
    "planning-identity-agent": ["audience-segmentation", "cross-platform-id-resolver", "behavioral-indexer"],
    "inventory-yield-agent": ["predictive-capacity", "yield-optimizer", "signal-forecaster"],
    "booking-proposals-agent": ["deal-structurer", "global-compliance-bot", "proposal-assembler"],
    "trafficking-signals-agent": ["asset-qa", "streamx-router", "launch-readiness"],
    "inflight-operations-agent": ["real-time-pacing", "autonomous-make-good", "ops-alerting"],
    "measurement-agent": ["household-match-validator", "outcome-pattern-reader", "next-flight-planner"]
  }
};

const TODDLER_HERO_PROFILE_KEY = "toddler-converged-precision-sprint";
const TODDLER_HERO_ROUTE = {
  title: "Converged Precision Sprint (Parents of Toddlers)",
  variantKey: "scenario-a",
  promptText: "Use a converged streaming-led route to maximize de-duplicated reach against high-intent toddler parents inside a strict $50K budget.",
  strategy: "Converged route designed to maximize de-duplicated reach inside a strict $50K budget by leaning into Max for concentrated morning streaming, using efficient daytime linear support, and preserving reserve budget for pacing corrections.",
  why: "Recommended: Max carries the strongest concentration of high-intent toddler parents, Food Network adds efficient daytime scale, and 28.4% cross-platform overlap makes converged frequency control the deciding advantage.",
  allocation: { streamingPct: 68, linearPct: 22, reservePct: 10 },
  allocationStrategy: "Allocate $34,000 (68%) to morning streaming, $11,000 (22%) to afternoon linear, and hold $5,000 (10%) in algorithmic reserve so pacing can rescue weak inventory without breaking the $50K budget.",
  deliveryTiming: "Lead on Max and Discovery+ during breakfast co-viewing hours, use Food Network and TLC in daytime lifestyle blocks, and keep TNT live sports on watchlist-only routing.",
  channelLogic: "This route works because the toddler-parent cohort over-indexes on digital, still shows efficient daytime linear behavior, and has enough cross-platform overlap that a converged engine can prevent duplicate waste.",
  recommendationReason: "Max is the most concentrated opening lane for millennial health-conscious toddler parents, Food Network daytime adds efficient linear scale, and 28.4% overlap makes converged frequency capping essential.",
  rankedNetworks: [
    { name: "Max", score: 145, lift: 1.45 },
    { name: "Food Network", score: 115, lift: 1.15 },
    { name: "Discovery+", score: 108, lift: 1.08 }
  ]
};

const TODDLER_HERO_SCENARIOS = {
  "scenario-a": {
    title: "The Converged Reach Maximizer",
    promptTile: "The Converged Reach Maximizer",
    promptText: "The Mix: 60% Streaming (Max Kids & Family) | 40% Linear Daytime (Food Network / HGTV).",
    strategy: "A converged reach route that uses streaming and linear together to maximize de-duplicated household scale before the plan is refined into bookable lines.",
    why: "This route stretches the $50K toward the largest unique household footprint by linking Max morning viewing with daytime linear behavior and suppressing duplicate homes across both channels.",
    allocation: { streamingPct: 60, linearPct: 40, reservePct: 0 },
    allocationStrategy: "The Mix: 60% Streaming (Max Kids & Family) | 40% Linear Daytime (Food Network / HGTV).",
    deliveryTiming: "Lead on Max during breakfast co-viewing, then move into Food Network and HGTV daytime blocks while suppressing homes already reached earlier in the day.",
    channelLogic: "If a household sees the ad on Max, the engine suppresses that same home from later linear delivery so more of the budget is forced into net-new parents instead of duplicate exposures.",
    recommendationReason: "It maximizes de-duplicated reach by combining Max and daytime linear under one household frequency cap.",
    rankedNetworks: [
      { name: "Max", score: 145, lift: 1.45 },
      { name: "Food Network", score: 115, lift: 1.15 },
      { name: "HGTV", score: 109, lift: 1.09 }
    ]
  },
  "scenario-b": {
    title: "The Streaming Play",
    promptTile: "The Streaming Play",
    promptText: "The Mix: 100% Streaming (Max & Discovery+).",
    strategy: "A streaming-only route that prioritizes verified parent signals and in-target precision over the broadest possible television scale.",
    why: "This route keeps every dollar inside authenticated streaming environments, so the plan can focus on households actively watching toddler programming or adjacent food and lifestyle content.",
    allocation: { streamingPct: 100, linearPct: 0, reservePct: 0 },
    allocationStrategy: "The Mix: 100% Streaming (Max & Discovery+).",
    deliveryTiming: "Keep the weight in Max Kids mornings and Discovery+ food or lifestyle windows where current parent intent is strongest.",
    channelLogic: "It will not produce the largest total household footprint, but it maximizes in-target reach because every impression stays tied to a verified streaming parent signal.",
    recommendationReason: "It maximizes verified in-target reach by keeping the full brief inside streaming environments.",
    rankedNetworks: [
      { name: "Max", score: 145, lift: 1.45 },
      { name: "Discovery+", score: 118, lift: 1.18 },
      { name: "Food Network", score: 103, lift: 1.03 }
    ]
  },
  "scenario-c": {
    title: "The Linear Scale",
    promptTile: "The Linear Scale",
    promptText: "The Mix: 80% Linear Daytime (TLC / Magnolia-style lifestyle blocks) | 20% Streaming.",
    strategy: "A linear-led route that uses cheaper daytime television to build broad household scale, with a smaller streaming layer reserved for cord-cutters.",
    why: "This route leans into lower-cost daytime linear inventory to maximize raw demographic reach, then uses a modest streaming layer to pick up parents who are harder to reach through cable alone.",
    allocation: { streamingPct: 20, linearPct: 80, reservePct: 0 },
    allocationStrategy: "The Mix: 80% Linear Daytime (TLC / Magnolia-style lifestyle blocks) | 20% Streaming.",
    deliveryTiming: "Concentrate spend in daytime lifestyle programming where stay-at-home parents are easiest to find, then use streaming support for the lighter incremental layer.",
    channelLogic: "The lower linear CPM does more of the scale work, while the 20% streaming layer is there to catch cord-cutters and reduce the blind spots of a pure linear buy.",
    recommendationReason: "It maximizes raw household scale by letting cheaper daytime linear inventory do most of the work.",
    rankedNetworks: [
      { name: "TLC", score: 121, lift: 1.21 },
      { name: "Food Network", score: 115, lift: 1.15 },
      { name: "HGTV", score: 111, lift: 1.11 }
    ]
  }
};

const TODDLER_HERO_COMPLIANCE_FINDINGS = [
  "COPPA enforcement: child-directed Max Kids inventory cannot carry third-party behavioral tracking pixels, so those pixels were removed before activation.",
  "COPPA enforcement: click-through retargeting and audience reseeding were disabled on Max Kids streaming lines to avoid child-directed behavioral profiling.",
  "Brand safety: 3 Discovery+ placements were adjacent to TV-MA true-crime content, so a G/PG-only adjacency filter was applied.",
  "Measurement control: household validation stays inside the clean room only; no raw user-level exports are allowed on the toddler-parent match workflow.",
  "Frequency protection: cross-platform exposure controls were hardened to keep household frequency capped when Max and linear viewers overlap.",
  "Pricing floor audit: the $68.50 streaming CPM clears the Q3 WBD internal premium video floor with no override required.",
  "Pricing floor audit: the $32.00 linear CPM clears the Q3 WBD daytime rate-card floor with no override required.",
  "Data handling: deterministic household IDs remain inside the WBD identity graph and are not shared with external trafficking tags."
];

function isToddlerHeroBrief(prompt = "", campaign = null) {
  const resolvedCampaign = campaign || buildCampaignProfile(prompt);
  if (!resolvedCampaign || resolvedCampaign.productFamily?.key !== "toddler_snacks") return false;
  const budget = Number(resolvedCampaign.budgetUsd || extractBudgetUsd(prompt) || 0);
  const countries = resolvedCampaign.countries || detectCampaignCountries(prompt);
  const inUs = !countries.length || countries.includes("US");
  return inUs && budget >= 45000 && budget <= 55000;
}

function isToddlerHeroCampaignState(campaignState = null) {
  if (!campaignState) return false;
  if (campaignState.presentationProfile?.key === TODDLER_HERO_PROFILE_KEY) return true;
  const selectedVariantKey = normalizeVariantKey(campaignState.selectedScenario?.variantKey || "");
  return selectedVariantKey === TODDLER_HERO_ROUTE.variantKey && isToddlerHeroBrief(campaignState.prompt || "", {
    productFamily: campaignState.productFamily,
    budgetUsd: campaignState.budgetUsd,
    countries: campaignState.countries
  });
}

function buildToddlerHeroPlanContext(selectedPlan = null, campaignPrompt = "") {
  if (!isToddlerHeroBrief(campaignPrompt, selectedPlan?.scenarioIntelligence?.campaign)) return selectedPlan;
  const variantKey = normalizeVariantKey(selectedPlan?.variantKey || TODDLER_HERO_ROUTE.variantKey);
  const toddlerScenario = TODDLER_HERO_SCENARIOS[variantKey] || TODDLER_HERO_SCENARIOS[TODDLER_HERO_ROUTE.variantKey];
  const rankedNetworks = (toddlerScenario?.rankedNetworks || TODDLER_HERO_ROUTE.rankedNetworks || []).map((item) => ({ ...item }));
  const recommendationReason = toddlerScenario?.recommendationReason || TODDLER_HERO_ROUTE.recommendationReason;
  return {
    ...(selectedPlan || {}),
    title: toddlerScenario?.title || selectedPlan?.title || TODDLER_HERO_ROUTE.title,
    promptTile: toddlerScenario?.promptTile || toddlerScenario?.title || selectedPlan?.promptTile || TODDLER_HERO_ROUTE.title,
    promptText: toddlerScenario?.promptText || selectedPlan?.promptText || TODDLER_HERO_ROUTE.promptText,
    strategy: toddlerScenario?.strategy || selectedPlan?.strategy || TODDLER_HERO_ROUTE.strategy,
    why: toddlerScenario?.why || selectedPlan?.why || TODDLER_HERO_ROUTE.why,
    variantKey,
    allocation: { ...(toddlerScenario?.allocation || selectedPlan?.allocation || TODDLER_HERO_ROUTE.allocation) },
    allocationStrategy: toddlerScenario?.allocationStrategy || selectedPlan?.allocationStrategy || TODDLER_HERO_ROUTE.allocationStrategy,
    deliveryTiming: toddlerScenario?.deliveryTiming || selectedPlan?.deliveryTiming || TODDLER_HERO_ROUTE.deliveryTiming,
    channelLogic: toddlerScenario?.channelLogic || selectedPlan?.channelLogic || TODDLER_HERO_ROUTE.channelLogic,
    recommendationReason,
    recommended: variantKey === TODDLER_HERO_ROUTE.variantKey,
    scenarioIntelligence: {
      ...(selectedPlan?.scenarioIntelligence || {}),
      allocation: { ...(toddlerScenario?.allocation || selectedPlan?.scenarioIntelligence?.allocation || TODDLER_HERO_ROUTE.allocation) },
      rankedNetworks,
      recommendationReason
    }
  };
}

function buildToddlerHeroBookingLineItems() {
  return [
    {
      line_item_id: "LINE-001",
      network: "Max Ad-Lite",
      platform_type: "digital",
      country_code: "US",
      daypart: "7AM-10AM Kids & Family",
      impressions: 321168,
      spend_usd: 22000,
      cpm_30s: 68.5,
      status: "approved"
    },
    {
      line_item_id: "LINE-002",
      network: "Discovery+",
      platform_type: "digital",
      country_code: "US",
      daypart: "Home & Food Run of Site",
      impressions: 102189,
      spend_usd: 7000,
      cpm_30s: 68.5,
      status: "approved"
    },
    {
      line_item_id: "LINE-003",
      network: "Max Ad-Lite",
      platform_type: "digital",
      country_code: "US",
      daypart: "6:30AM-9AM Co-Viewing Extension",
      impressions: 72993,
      spend_usd: 5000,
      cpm_30s: 68.5,
      status: "approved"
    },
    {
      line_item_id: "LINE-004",
      network: "Food Network",
      platform_type: "linear",
      country_code: "US",
      daypart: "12PM-4PM Daytime Block",
      impressions: 187500,
      spend_usd: 6000,
      cpm_30s: 32,
      status: "approved"
    },
    {
      line_item_id: "LINE-005",
      network: "TLC",
      platform_type: "linear",
      country_code: "US",
      daypart: "1PM-3PM Lifestyle",
      impressions: 93750,
      spend_usd: 3000,
      cpm_30s: 32,
      status: "approved"
    },
    {
      line_item_id: "LINE-006",
      network: "TNT",
      platform_type: "linear",
      country_code: "US",
      daypart: "Live Sports Extension",
      impressions: 62500,
      spend_usd: 2000,
      cpm_30s: 32,
      status: "approved"
    }
  ];
}

function buildToddlerHeroComplianceEvaluation({ allocationStrategy = TODDLER_HERO_ROUTE.allocationStrategy, variantKey = TODDLER_HERO_ROUTE.variantKey } = {}) {
  return {
    status: "Passed with Adjustments",
    summary: "Parallel compliance audit completed across the active toddler-parent workflow and logged 8 policy findings. The issues were remediated in-system, so the campaign can proceed without a manual legal hold.",
    findings: [...TODDLER_HERO_COMPLIANCE_FINDINGS],
    alternatives: [
      "Strip behavioral pixels from child-directed streaming tags and keep measurement inside the clean room.",
      "Apply a G/PG-only adjacency filter on family-safe Discovery+ and TLC supply.",
      "Keep household frequency and identity controls enforced at the converged graph layer."
    ],
    sources: [
      {
        policy: "COPPA and child-directed advertising controls",
        source: "Synthetic WBD Advertising Compliance Rulebook, child-directed privacy and measurement section",
        why: "The audience and programming mix includes child-directed streaming inventory."
      },
      {
        policy: "Brand safety adjacency",
        source: "Synthetic WBD Advertising Compliance Rulebook, suitability and adjacency section",
        why: "A toddler snack brand cannot sit next to mature true-crime content."
      },
      {
        policy: "Internal pricing floors and data-handling controls",
        source: "Synthetic WBD Pricing and Privacy Policy dataset",
        why: "The plan needed to clear rate-card floors and keep deterministic household IDs protected."
      }
    ],
    allocationStrategy,
    variantKey
  };
}

function buildToddlerHeroPacingSeries() {
  const hours = Array.from({ length: 24 }, (_, index) => index + 1);
  const linearPct = [90, 89, 88, 87, 86, 85, 84, 84, 83, 82, 82, 81, 81, 80, 81, 82, 82, 83, 83, 82, 82, 81, 81, 81];
  const digitalPct = [98, 98, 99, 99, 100, 101, 101, 102, 102, 102, 102, 101, 102, 103, 102, 103, 103, 102, 102, 101, 101, 102, 102, 102];
  const linearPlanned = hours.map(() => 8200);
  const digitalPlanned = hours.map((hour) => (hour >= 6 && hour <= 10 ? 13000 : 10800));
  const linearImp = linearPlanned.map((value, index) => Math.round(value * (linearPct[index] / 100)));
  const digitalImp = digitalPlanned.map((value, index) => Math.round(value * (digitalPct[index] / 100)));
  return {
    hours,
    linearPct,
    digitalPct,
    linearImp,
    digitalImp,
    linearPlanned,
    digitalPlanned,
    makeGoodHour: 14
  };
}

function buildToddlerHeroReachSummary() {
  return {
    uniqueHouseholds: 312500,
    audienceHouseholds: 312500,
    addressableHouseholds: 312500,
    projectedHouseholds: 71300,
    modeledHouseholds: 71300,
    targetFrequency: 3.1,
    deviceCount: 139700,
    scaledReach: 71300,
    linearPct: 25,
    digitalPct: 75,
    overlapPct: 28.4
  };
}

function buildToddlerHeroExecutiveSummary() {
  return {
    headline: TODDLER_HERO_ROUTE.title,
    summaryBadge: "Plain-English Summary",
    summaryIntro: "An optimized $50K converged media plan designed to maximize unique household reach across Max, Discovery+, and Food Network. The engine autonomously balanced linear and streaming inventory, enforced cross-platform frequency capping, and executed in-flight optimizations to guarantee delivery.",
    sections: [
      {
        title: "WHY THIS ROUTE WAS SELECTED",
        text: "This converged route was selected to maximize de-duplicated reach. By using both streaming and linear inventory, the engine followed the real viewing rhythm of toddler parents, who stream Max in the mornings and shift into Food Network and TLC during the afternoon. The 28.4% overlap signal is why cross-platform frequency caps mattered from the start."
      },
      {
        title: "WHO THE CAMPAIGN IS AIMED AT",
        text: "The audience engine scanned a WBD total addressable market of 3.8 million households and refined it to a budget-appropriate reachable audience of 312,500 high-intent households. The lead cohort is millennial health-conscious parents, isolated with deterministic purchase data and matched back to the WBD identity graph."
      },
      {
        title: "HOW THE MEDIA PLAN IS BUILT",
        text: "The engine filtered 4.15 million available WBD avails into 6 high-efficiency placements. It locked 211,970 guaranteed impressions and kept the planning CPM grounded at $54.20 blended. After the in-flight rescue, the live execution mix settled at 75% streaming and 25% linear."
      },
      {
        title: "WHAT THE PROJECTED RESULT MEANS",
        text: "The $50K investment successfully reached 68,450 unique households. Because the engine managed the 28.4% cross-platform overlap, average frequency stayed capped at 3.1x, and clean-room validation confirmed that streaming delivered 22% incremental reach beyond a standalone linear buy."
      }
    ],
    metrics: [
      { label: "TARGET UNIVERSE", value: "3.8M Households", help: "Total WBD toddler-parent universe identified before budget compression." },
      { label: "REACHABLE AUDIENCE", value: "312,500 Unique HHs", help: "High-intent households the $50K budget can reach at a meaningful capped frequency." },
      { label: "LEAD BEHAVIORAL COHORT", value: "Millennial Health-Conscious Parents", help: "The deterministic cohort that anchored targeting and pricing." },
      { label: "AVAILABLE INVENTORY", value: "4.15M Impressions", help: "Fulfillable toddler-parent avails across the next 14 days." },
      { label: "BOOKED PLACEMENTS", value: "6 slots (211,970 Guaranteed Imp)", help: "Final converged package protected with next-week guarantees." },
      { label: "DELIVERY FORECAST", value: "100% Streaming / 100% Linear", help: "The pacing rescue restored both channels to full-delivery projection." },
      { label: "VERIFIED REACH", value: "68,450 Unique HHs", help: "Privacy-safe household validation after campaign exposure matching." },
      { label: "CLEAN-ROOM MATCH RATE", value: "77.6%", help: "Share of delivered households confidently matched back to the target list." },
      { label: "AVERAGE FREQUENCY", value: "3.1x per Household", help: "Cross-platform exposure stayed controlled even with converged delivery." },
      { label: "CROSS-PLATFORM OVERLAP", value: "28.4%", help: "The overlap that justified converged identity and frequency controls." },
      { label: "AVERAGE CPM", value: "$54.20 Blended", help: "Planning CPM for the approved streaming plus linear mix." }
    ],
    makeGoodSummary: "IN-FLIGHT ACTION: During live pacing, the Autonomous Make-Good agent reallocated $4,250 from underperforming live sports linear inventory directly into Max Ad-Lite to protect the 211,970 booked guarantee."
  };
}

function normalizeVariantKey(variantKey = "") {
  const raw = String(variantKey || "").trim();
  const normalized = LEGACY_VARIANT_KEY_MAP[raw] || raw;
  if (PLAN_VARIANTS.some((item) => item.key === normalized)) return normalized;
  return PLAN_VARIANTS[2]?.key || PLAN_VARIANTS[0]?.key;
}

function inferVariantKeyFromPlanMetadata(plan = [], fallbackText = "") {
  const source = [
    fallbackText,
    ...(Array.isArray(plan) ? plan.map((item) => `${item?.agentName || ""} ${item?.initialTask || ""} ${item?.systemInstruction || ""}`) : [])
  ].join(" ").toLowerCase();
  if (source.includes("streaming heavy") || source.includes("streaming-heavy")) return "scenario-a";
  if (source.includes("linear heavy") || source.includes("linear-heavy")) return "scenario-b";
  if (source.includes("balanced")) return "scenario-c";
  if (source.includes("precision") || source.includes("high-fit") || source.includes("conversion sprint")) return "scenario-a";
  if (source.includes("scale") || source.includes("reach builder") || source.includes("household reach")) return "scenario-b";
  if (source.includes("adaptive") || source.includes("momentum") || source.includes("resilience")) return "scenario-c";
  return PLAN_VARIANTS[2]?.key || PLAN_VARIANTS[0]?.key;
}

function resolveVariantKeyFromContext(context = null) {
  if (!context) return PLAN_VARIANTS[2]?.key || PLAN_VARIANTS[0]?.key;
  if (typeof context === "string") return normalizeVariantKey(context);
  if (context.variantKey) return normalizeVariantKey(context.variantKey);
  if (context.selectedScenario?.variantKey) return normalizeVariantKey(context.selectedScenario.variantKey);
  if (context.selectedPlan?.variantKey) return normalizeVariantKey(context.selectedPlan.variantKey);
  if (Array.isArray(context.plan)) return inferVariantKeyFromPlanMetadata(context.plan, context.problem || context.prompt || "");
  return PLAN_VARIANTS[2]?.key || PLAN_VARIANTS[0]?.key;
}

function resolveSubAgentCatalogForVariant(context = null) {
  const variantKey = resolveVariantKeyFromContext(context);
  const variantConfig = VARIANT_SUBAGENT_IDS[variantKey] || VARIANT_SUBAGENT_IDS["scenario-c"] || {};
  const catalog = {};
  Object.entries(MASTER_AGENT_SUBAGENTS).forEach(([nodeId, items]) => {
    const preferredIds = variantConfig[nodeId] || items.map((item) => item.id);
    const resolvedItems = preferredIds
      .map((id) => items.find((item) => item.id === id))
      .filter(Boolean);
    catalog[nodeId] = resolvedItems.length ? resolvedItems : items.slice(0, 1);
  });
  return catalog;
}

function getSubAgentsForNode(nodeId = "", context = null) {
  if (!nodeId) return [];
  const variantCatalog = resolveSubAgentCatalogForVariant(context);
  return variantCatalog[nodeId] || [];
}

function buildScopedSubAgentResults(nodeId = "", context = null, factories = {}) {
  const activeSubAgents = getSubAgentsForNode(nodeId, context);
  return activeSubAgents.map((subAgent, index) => {
    const baseline = {
      id: subAgent.id,
      name: subAgent.name,
      summary: subAgent.summary || "",
      definition: subAgent.definition || subAgent.summary || "",
      details: [subAgent.summary || `${subAgent.name} completed its scoped task.`]
    };
    const factory = factories[subAgent.id] || factories[`index-${index}`];
    if (typeof factory === "function") {
      const result = factory(subAgent, index) || {};
      return {
        ...baseline,
        ...result,
        id: result.id || baseline.id,
        name: result.name || baseline.name,
        summary: result.summary || baseline.summary,
        definition: result.definition || baseline.definition,
        details: Array.isArray(result.details) && result.details.length ? result.details : baseline.details
      };
    }
    return baseline;
  });
}

function buildPresetSubAgentResults(nodeId = "", detailMap = {}) {
  const catalog = MASTER_AGENT_SUBAGENTS[nodeId] || [];
  return Object.entries(detailMap).map(([id, payload]) => {
    const base = catalog.find((item) => item.id === id) || { id, name: payload?.name || id };
    return {
      id: base.id,
      name: payload?.name || base.name,
      summary: payload?.summary || base.summary || "",
      definition: payload?.definition || base.definition || base.summary || "",
      details: payload?.details || [],
      status: payload?.status || "done",
      fallbackUsed: false
    };
  });
}

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
  flowPanelNodeId: null,
  flowOrientation: "horizontal",
  flowColumns: 4,
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
  runToken: 0,
  campaignStateObject: null,
  orchestratorNodeId: ORCHESTRATOR_NODE_ID,
  subAgentCatalog: resolveSubAgentCatalogForVariant(PLAN_VARIANTS[2]?.key || PLAN_VARIANTS[0]?.key),
  subAgentSelections: {}
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
    applyArchitectSelection(picked);
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
  selectSubAgent: (contextKey, subAgentId) => {
    if (!contextKey || !subAgentId) return;
    setState({
      subAgentSelections: {
        ...(state.subAgentSelections || {}),
        [contextKey]: subAgentId
      }
    });
  },
  selectFlowOutputNode: (nodeId) => {
    if (!nodeId) return;
    setState({ flowPanelNodeId: nodeId, focusedNodeId: null });
  },
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
      const selectedScenario = getSelectedScenarioRecord();
      const agent = {
        id: state.editingAgentId || crypto.randomUUID(),
        title,
        problem: state.selectedDemoIndex === -1 ? state.customProblem?.problem : config.demos[state.selectedDemoIndex]?.problem,
        plan: state.plan,
        inputs: inputsToSave,
        variantKey: normalizeVariantKey(selectedScenario?.variantKey || inferVariantKeyFromPlanMetadata(state.plan, state.campaignPrompt || ""))
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
    const savedVariantKey = normalizeVariantKey(agent.variantKey || inferVariantKeyFromPlanMetadata(agent.plan || [], agent.problem || ""));
    const savedPlan = expandAndEnrichPlan(agent.plan || [], savedMax, agent.problem || "", savedVariantKey, {
      variantKey: savedVariantKey,
      title: agent.title || "Saved scenario",
      allocationStrategy: "Saved plan allocation derived from prior run.",
      recommendationReason: "Restore the saved scenario context and keep the stage sequence consistent."
    });
    const savedInputs = (agent.inputs && agent.inputs.length)
      ? agent.inputs.map((input) => ({ ...input, id: input.id || Utils.uniqueId("input") }))
      : buildDatasetInputs({ campaignPrompt: agent.problem || "" }).map((input) => ({ ...input, id: Utils.uniqueId("input") }));
    const savedCompliance = evaluatePlanCompliance({
      campaignPrompt: agent.problem || "",
      allocationStrategy: "Saved plan allocation derived from prior run.",
      variantKey: savedVariantKey
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
      rawDataByAgent: buildRawDataByPlan(savedPlan, null, null),
      selectedPlanCompliance: savedCompliance,
      complianceDetails: buildComplianceDetails(savedCompliance),
      complianceExplanationOpen: false,
      visualizationExplanationOpen: false,
      visualizationLoading: false,
      visualizationNarrative: "",
      runToken: 0,
      campaignStateObject: null,
      subAgentCatalog: resolveSubAgentCatalogForVariant(savedVariantKey),
      subAgentSelections: {}
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
    const inputs = buildDatasetInputs({ campaignPrompt: d.problem || d.body || "" });
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
    flowPanelNodeId: null,
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
    runToken: 0,
    campaignStateObject: null,
    subAgentCatalog: resolveSubAgentCatalogForVariant(PLAN_VARIANTS[2]?.key || PLAN_VARIANTS[0]?.key),
    subAgentSelections: {}
  });
}

function applyArchitectSelection(picked) {
  const selectedPrompt = state.campaignPrompt || "";
  const effectivePicked = buildToddlerHeroPlanContext(picked, selectedPrompt) || picked;
  const variantKey = normalizeVariantKey(effectivePicked.variantKey || PLAN_VARIANTS[2]?.key || PLAN_VARIANTS[0]?.key);
  const selectedPlan = expandAndEnrichPlan(
    effectivePicked.plan || [],
    clampAgentCount($("#max-agents")?.value, TARGET_ARCHITECT_AGENTS),
    selectedPrompt,
    variantKey,
    effectivePicked
  );
  const normalizedInputs = buildDatasetInputs({
    campaignPrompt: selectedPrompt,
    selectedPlan: effectivePicked
  }).map((input) => ({ ...input, id: Utils.uniqueId("input") }));
  const complianceValidation = effectivePicked.complianceValidation || evaluatePlanCompliance({
    campaignPrompt: selectedPrompt,
    allocationStrategy: effectivePicked.allocationStrategy || "",
    variantKey
  });
  setState({
    selectedArchitectPlanId: effectivePicked.id,
    plan: selectedPlan,
    suggestedInputs: normalizedInputs,
    selectedInputs: new Set(normalizedInputs.map((i) => i.id)),
    rawDataByAgent: buildRawDataByPlan(selectedPlan, null, effectivePicked),
    selectedPlanCompliance: complianceValidation,
    complianceDetails: buildComplianceDetails(complianceValidation),
    complianceExplanationOpen: false,
    visualizationExplanationOpen: false,
    visualizationLoading: false,
    visualizationNarrative: "",
    runToken: 0,
    campaignStateObject: null,
    flowPanelNodeId: null,
    subAgentCatalog: resolveSubAgentCatalogForVariant(variantKey),
    subAgentSelections: {},
    stage: "data",
    error: ""
  });
}

function buildDatasetInputs({ campaignPrompt = state.campaignPrompt || "", selectedPlan = null } = {}) {
  const entries = datasetEntries.map((entry) => ({
    title: entry.title,
    type: entry.type,
    content: entry.content
  }));
  const scenarioInput = buildScenarioDatasetInput(campaignPrompt, selectedPlan);
  return scenarioInput ? [scenarioInput, ...entries] : entries;
}

function buildDatasetContext(dataKey) {
  if (!dataKey || !datasets[dataKey] || !datasetMeta[dataKey]) {
    return "No dataset available for this agent.";
  }
  const meta = datasetMeta[dataKey];
  const rows = datasets[dataKey];
  const csv = toCsv(rows, meta.columns);
  return sanitizeDisplayText(`${meta.title} (${rows.length} rows)\n${csv}`);
}

function buildDashboard(agentOutputs = [], runContext = {}) {
  const profile = buildRunProfile(agentOutputs, runContext);
  const campaignState = runContext.campaignStateObject || state.campaignStateObject || null;
  const reach = buildReachSummary(datasets.audienceGraph || [], datasets.liveDeliveryLog || [], profile, campaignState);
  const makeGood = buildMakeGoodSummary(datasets.liveDeliveryLog || [], profile, campaignState);
  return {
    pacing: buildPacingSeries(datasets.liveDeliveryLog || [], profile, campaignState),
    reach,
    actions: buildActionRows(agentOutputs, state.issueNodeIds || new Set()),
    makeGood,
    executive: buildExecutiveCampaignSummary(campaignState, reach, makeGood)
  };
}

function buildRunProfile(agentOutputs = [], runContext = {}) {
  const campaignState = runContext.campaignStateObject || null;
  const scenario = campaignState?.selectedScenario || {};
  const inflight = campaignState?.stageOutputs?.["inflight-operations-agent"]?.inflight_summary || {};
  const intelligence = campaignState?.intelligence || {};
  const outputText = (agentOutputs || [])
    .map((agent) => `${agent.nodeId || ""}|${agent.name || ""}|${stripMarkdown(agent.text || "").slice(0, 600)}`)
    .join("|");
  const seedSource = [
    runContext.campaignPrompt || "",
    runContext.selectedPlanId || "",
    runContext.runToken || 0,
    outputText,
    scenario.variantKey || "",
    intelligence.uniqueHouseholds || 0,
    campaignState?.productFamily?.key || ""
  ].join("::");
  const seed = hashString(seedSource);
  return {
    seed,
    waveShift: seededInt(seed, 1, 7, 1),
    planShift: seededRange(seed, -0.07, 0.07, 2),
    linearDeliveryShift: inflight.linear_delivery_rate_pct ? ((Number(inflight.linear_delivery_rate_pct) / 100) - 0.92) : seededRange(seed, -0.1, 0.08, 3),
    digitalDeliveryShift: inflight.digital_delivery_rate_pct ? ((Number(inflight.digital_delivery_rate_pct) / 100) - 0.95) : seededRange(seed, -0.06, 0.11, 4),
    overlapShift: intelligence.overlapPct ? intelligence.overlapPct - 18 : seededRange(seed, -6, 7, 5),
    reachShift: intelligence.uniqueHouseholds ? clampNumber((intelligence.uniqueHouseholds || 1) / 28, 0.88, 1.3) : seededRange(seed, 0.88, 1.16, 6),
    deviceShift: intelligence.matchedRows?.length ? clampNumber((intelligence.matchedRows.length || 1) / Math.max(intelligence.uniqueHouseholds || 1, 1), 0.9, 1.5) : seededRange(seed, 0.9, 1.22, 7),
    makeGoodHour: seededInt(seed, 10, 15, 8),
    linearDipFactor: seededRange(seed, 0.48, 0.82, 9),
    digitalBoostFactor: seededRange(seed, 1.07, 1.3, 10),
    makeGoodBudget: inflight.shift_budget_usd || seededInt(seed, 1200, 2800, 11)
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

function buildPacingSeries(logs, profile = {}, campaignState = null) {
  if (isToddlerHeroCampaignState(campaignState)) {
    return buildToddlerHeroPacingSeries();
  }
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

function buildReachSummary(audience, logs, profile = {}, campaignState = null) {
  if (isToddlerHeroCampaignState(campaignState)) {
    return buildToddlerHeroReachSummary();
  }
  if (campaignState) {
    const planning = campaignState.stageOutputs?.["planning-identity-agent"]?.audience_summary || {};
    const booking = campaignState.stageOutputs?.["booking-proposals-agent"]?.booking_summary || {};
    const inflight = campaignState.stageOutputs?.["inflight-operations-agent"]?.inflight_summary || {};
    const finalAllocation = inflight.final_allocation || campaignState.selectedScenario?.allocation || {};
    const audienceHouseholds = Math.max(1, Number(planning.unique_households || campaignState.intelligence?.uniqueHouseholds || 1));
    const linearPct = Math.round(clampNumber(finalAllocation.linearPct || 40, 10, 80));
    const overlapPct = Math.round(clampNumber(Number(planning.overlap_pct || campaignState.intelligence?.overlapPct || 18), 4, 64));
    const totalImpressions = (booking.line_items || []).reduce((sum, row) => sum + Number(row.impressions || 0), 0);
    const targetFrequency = clampNumber(
      2.4 + (Number(finalAllocation.streamingPct || 50) / 160) + (overlapPct / 140),
      2.2,
      4.1
    );
    const addressableMultiplier = clampNumber(
      1.08 + (overlapPct / 260) + (Math.abs(Number(finalAllocation.streamingPct || 50) - linearPct) / 320),
      1.08,
      1.26
    );
    const addressableHouseholds = Math.max(
      audienceHouseholds,
      Math.round((audienceHouseholds * addressableMultiplier) / 100) * 100
    );
    const projectedHouseholds = totalImpressions
      ? Math.max(
        audienceHouseholds,
        Math.min(addressableHouseholds, Math.round((totalImpressions / targetFrequency) / 100) * 100)
      )
      : Math.max(audienceHouseholds, Math.round((addressableHouseholds * 0.62) / 100) * 100);
    const deviceCount = Math.max(1, Math.round(projectedHouseholds * clampNumber(1.62 + (overlapPct / 100), 1.6, 2.25)));
    return {
      uniqueHouseholds: audienceHouseholds,
      audienceHouseholds,
      addressableHouseholds,
      projectedHouseholds,
      modeledHouseholds: projectedHouseholds,
      targetFrequency: roundToTwo(targetFrequency, 1),
      deviceCount,
      scaledReach: projectedHouseholds,
      linearPct,
      digitalPct: 100 - linearPct,
      overlapPct
    };
  }
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
    audienceHouseholds: uniqueHouseholds,
    addressableHouseholds: scaledReach,
    projectedHouseholds: scaledReach,
    deviceCount,
    scaledReach,
    linearPct,
    digitalPct: 100 - linearPct,
    overlapPct: Math.round(clampNumber(overlapPct, 4, 64))
  };
}

function resolveAudienceHouseholdCount(planning = {}, intelligence = {}) {
  return Math.max(0, Number(planning.unique_households || intelligence?.uniqueHouseholds || 0));
}

function resolveProjectedHouseholdCount(reach = {}, measurement = {}, audienceHouseholds = 0) {
  return Math.max(
    0,
    Number(
      reach?.projectedHouseholds
      || reach?.modeledHouseholds
      || reach?.scaledReach
      || measurement?.projected_households
      || measurement?.matched_households
      || audienceHouseholds
      || 0
    )
  );
}

function resolveMeasuredHouseholdCount(measurement = {}, projectedHouseholds = 0) {
  const matchedHouseholds = Math.max(0, Number(measurement?.matched_households || 0));
  if (matchedHouseholds) return matchedHouseholds;
  const cleanRoomMatchRate = Math.max(0, Number(measurement?.clean_room_match_rate_pct || 0));
  return cleanRoomMatchRate && projectedHouseholds
    ? Math.round(projectedHouseholds * (cleanRoomMatchRate / 100))
    : 0;
}

function resolveFrequencyPerHousehold(totalImpressions = 0, projectedHouseholds = 0) {
  return projectedHouseholds ? roundToTwo(totalImpressions / projectedHouseholds, 1) : 0;
}

function buildExecutiveCampaignSummary(campaignState = null, reach = null, makeGood = null) {
  if (!campaignState) return null;
  if (isToddlerHeroCampaignState(campaignState)) {
    return buildToddlerHeroExecutiveSummary(campaignState, reach, makeGood);
  }
  const planning = campaignState.stageOutputs?.["planning-identity-agent"]?.audience_summary || {};
  const inventory = campaignState.stageOutputs?.["inventory-yield-agent"]?.inventory_summary || {};
  const booking = campaignState.stageOutputs?.["booking-proposals-agent"]?.booking_summary || {};
  const inflight = campaignState.stageOutputs?.["inflight-operations-agent"]?.inflight_summary || {};
  const measurement = campaignState.stageOutputs?.["measurement-agent"]?.measurement_summary || {};
  const lineItems = booking.line_items || [];
  const matchedProfiles = Math.max(0, Number(planning.matched_profiles || campaignState.intelligence?.matchedRows?.length || 0));
  const seedHouseholds = Math.max(0, Number(planning.seed_households || campaignState.intelligence?.seedHouseholdCount || 0));
  const uniqueHouseholds = resolveAudienceHouseholdCount(planning, campaignState.intelligence);
  const projectedHouseholds = resolveProjectedHouseholdCount(reach, measurement, uniqueHouseholds);
  const totalImpressions = lineItems.reduce((sum, row) => sum + Number(row.impressions || 0), 0);
  const totalSpend = lineItems.reduce((sum, row) => sum + Number(row.spend_usd || 0), 0);
  const blendedCpm = totalImpressions
    ? roundToTwo((totalSpend / totalImpressions) * 1000, 1)
    : roundToTwo(
      (
        Number(inventory.premium_streaming_cpm || 38)
        + Number(inventory.blended_linear_cpm || 34)
      ) / 2,
      1
    );
  const cleanRoomMatchRate = roundToTwo(Number(measurement.clean_room_match_rate_pct || 0), 1);
  const measuredHouseholds = resolveMeasuredHouseholdCount(measurement, projectedHouseholds);
  const frequencyPerHousehold = resolveFrequencyPerHousehold(totalImpressions, projectedHouseholds);
  const behavioralCohort = [
    planning.top_segments?.[0]?.label,
    planning.top_tags?.[0]?.label
  ].filter(Boolean).join(" / ") || "High-fit behavioral cohort";
  const topNetworks = [...new Set(lineItems.map((item) => item.network).filter(Boolean))].slice(0, 3).join(", ")
    || (campaignState.selectedScenario?.rankedNetworks || []).map((item) => item.name).slice(0, 3).join(", ")
    || "Max ad-lite, TNT Sports";
  const finalAllocation = inflight.final_allocation || campaignState.selectedScenario?.allocation || {};
  const streamingPct = Math.round(Number(finalAllocation.streamingPct ?? campaignState.selectedScenario?.allocation?.streamingPct ?? 0));
  const linearPct = Math.round(Number(finalAllocation.linearPct ?? campaignState.selectedScenario?.allocation?.linearPct ?? 0));
  const reservePct = Math.round(Number(finalAllocation.reservePct ?? campaignState.selectedScenario?.allocation?.reservePct ?? 0));
  const digitalDeliveryRate = roundToTwo(inflight.digital_delivery_rate_pct || 0, 1);
  const linearDeliveryRate = roundToTwo(inflight.linear_delivery_rate_pct || 0, 1);
  const deliveryDetails = `${digitalDeliveryRate}% on streaming and ${linearDeliveryRate}% on linear`;
  const overlapPct = Number(reach?.overlapPct || planning.overlap_pct || 0);
  const deviceCount = Number(reach?.deviceCount || campaignState.intelligence?.matchedRows?.length || 0);
  const addressableHouseholds = Number(reach?.addressableHouseholds || 0);
  const crossPlatformReach = `${overlapPct}% of reached households are expected to see both channels`;
  const projectedHouseholdsLabel = projectedHouseholds.toLocaleString();
  const measuredHouseholdsLabel = measuredHouseholds.toLocaleString();
  const addressableHouseholdsLabel = addressableHouseholds.toLocaleString();
  const totalImpressionsLabel = totalImpressions.toLocaleString();
  const goalLabel = deriveCampaignGoalLabel(campaignState.intelligence?.campaign || {});
  const productLabel = campaignState.productFamily?.displayLabel
    || campaignState.intelligence?.campaign?.productFamily?.displayLabel
    || "the campaign brief";
  const routeReasonRaw = String(campaignState.selectedScenario?.recommendationReason || "").trim();
  const routeReason = routeReasonRaw
    ? routeReasonRaw.charAt(0).toLowerCase() + routeReasonRaw.slice(1)
    : "it best fits the audience behavior, supply shape, and delivery objective in this brief";
  const inventoryAvailable = Number(inventory.capacity_impressions || 0).toLocaleString();
  const frequencyWholeViewLow = frequencyPerHousehold ? Math.max(1, Math.floor(frequencyPerHousehold)) : 0;
  const frequencyWholeViewHigh = frequencyPerHousehold ? Math.max(frequencyWholeViewLow, Math.ceil(frequencyPerHousehold)) : 0;
  const frequencyPlainEnglish = frequencyPerHousehold
    ? (frequencyWholeViewLow === frequencyWholeViewHigh
      ? `In plain English, the average reached household would see the campaign about ${frequencyWholeViewLow} time${frequencyWholeViewLow === 1 ? "" : "s"}.`
      : `In plain English, the average reached household would see the campaign about ${frequencyWholeViewLow} to ${frequencyWholeViewHigh} times.`)
    : "Frequency will appear once the plan has both projected reach and booked impressions.";
  const frequencyLabel = frequencyPerHousehold ? `${frequencyPerHousehold} times per reached household` : "Pending";
  const frequencySource = frequencyPerHousehold
    ? `This comes from dividing ${totalImpressionsLabel} total booked impressions by ${projectedHouseholdsLabel} projected households expected to receive at least one impression.`
    : "This metric is waiting for both booked impression totals and reached-household projections.";
  const lineItemLabel = `${lineItems.length} placements covering ${totalImpressions.toLocaleString()} booked impressions`;
  const planMix = [
    streamingPct ? `${streamingPct}% streaming` : null,
    linearPct ? `${linearPct}% linear` : null,
    reservePct ? `${reservePct}% held flexible for optimization` : null
  ].filter(Boolean).join(", ") || "cross-platform allocation pending";
  const summaryIntro = `This scenario was selected to drive ${goalLabel} for ${productLabel}. In plain terms, the system found the highest-fit audience, built a cross-platform plan around that group, and estimated delivery, reach, and cost before launch.`;
  const sections = [
    {
      title: "Why this route was selected",
      text: `The route was recommended because ${routeReason}. It is the clearest starting point for this brief based on audience behavior, available supply, and expected delivery quality.`
    },
    {
      title: "Who the campaign is aimed at",
      text: `The audience engine matched ${matchedProfiles.toLocaleString()} sample audience records and translated them into a modeled planning audience of ${uniqueHouseholds.toLocaleString()} households. The strongest shared behavior signal was ${behavioralCohort}, which became the control cohort for planning.`
    },
    {
      title: "How the media plan is built",
      text: `${lineItems.length} placements were assembled across ${topNetworks}. The working mix opens with ${planMix}. Inventory checks found ${inventoryAvailable} workable impressions, and projected delivery is ${deliveryDetails}.`
    },
    {
      title: "What the projected result means",
      text: `If the plan performs as modeled, it should reach about ${projectedHouseholdsLabel} households.${addressableHouseholds ? ` That sits inside a modeled reachable universe of about ${addressableHouseholdsLabel} households.` : ""} Average frequency is ${frequencyLabel}. ${frequencySource} ${frequencyPlainEnglish} The clean room is expected to match about ${measuredHouseholdsLabel} of the households actually reached at a ${cleanRoomMatchRate}% match rate, so the matched count should be read as a measurable subset of delivery rather than the full in-scope audience. Cross-platform overlap is ${overlapPct}%, which means that share of households is expected to see the campaign in both streaming and linear, and average CPM is $${blendedCpm}.`
    }
  ];
  const metrics = [
    {
      label: "Matched Sample Profiles",
      value: matchedProfiles.toLocaleString(),
      help: "Synthetic viewer-level records that matched the brief before the modeled household roll-up."
    },
    {
      label: "Unique Households",
      value: uniqueHouseholds.toLocaleString(),
      help: "The modeled deduplicated audience base used for planning before reach and delivery are projected."
    },
    {
      label: "Lead Behavioral Cohort",
      value: behavioralCohort,
      help: "The strongest shared behavior signal that anchored targeting and planning."
    },
    {
      label: "Available Inventory",
      value: `${inventoryAvailable} impressions`,
      help: "Estimated workable supply that could support the selected route."
    },
    {
      label: "Booked Placements",
      value: lineItemLabel,
      help: "Distinct placements created across the selected networks and dayparts."
    },
    {
      label: "Delivery Forecast",
      value: deliveryDetails,
      help: "Projected on-time delivery by channel if the plan launches as modeled."
    },
    {
      label: "Projected Households Reached",
      value: projectedHouseholdsLabel,
      help: addressableHouseholds
        ? `Homes expected to see the campaign at least once, modeled from a reachable universe of about ${addressableHouseholdsLabel} households.`
        : "Homes expected to see the campaign at least once."
    },
    {
      label: "Audience-to-Delivery Match Rate",
      value: `${cleanRoomMatchRate}%`,
      help: `About ${measuredHouseholdsLabel} of the ${projectedHouseholdsLabel} reached households are expected to be measurable in the clean room; the remaining reached homes still count in delivery but cannot be matched back at household level.`
    },
    {
      label: "Average Frequency",
      value: frequencyLabel,
      help: frequencyPerHousehold
        ? `${frequencySource} That works out to ${frequencyPerHousehold} times per reached household on average. ${frequencyPlainEnglish}`
        : frequencySource
    },
    {
      label: "Cross-Platform Overlap",
      value: crossPlatformReach,
      help: `${deviceCount.toLocaleString()} modeled devices contributed to this overlap estimate.`
    },
    {
      label: "Average CPM",
      value: `$${blendedCpm}`,
      help: "Estimated average cost for every 1,000 impressions in the booked plan."
    }
  ];

  return {
    headline: campaignState.selectedScenario?.title || "Selected scenario",
    summaryBadge: "Plain-English Summary",
    summaryIntro,
    sections,
    summaryLines: [
      `This scenario was selected to drive ${goalLabel} for ${productLabel}.`,
      `The audience engine matched ${matchedProfiles.toLocaleString()} sample audience records and translated them into a modeled audience of ${uniqueHouseholds.toLocaleString()} households led by ${behavioralCohort}.`,
      `${lineItems.length} placements across ${topNetworks} are projected to deliver ${totalImpressions.toLocaleString()} impressions with ${deliveryDetails}.`,
      `If the plan performs as modeled, it should reach about ${projectedHouseholdsLabel} households. Average frequency is ${frequencyLabel}, calculated from ${totalImpressionsLabel} impressions divided by ${projectedHouseholdsLabel} households reached. ${frequencyPlainEnglish} The clean room is expected to match about ${measuredHouseholdsLabel} of those reached households at a ${cleanRoomMatchRate}% match rate, so the matched count is the measurable subset of delivery rather than the full planning audience. ${crossPlatformReach}, and average CPM is $${blendedCpm}.`
    ],
    metrics,
    makeGoodSummary: makeGood?.shiftBudget
      ? `During pacing simulation, the system moved $${Number(makeGood.shiftBudget || 0).toLocaleString()} into stronger inventory to protect delivery and preserve the booked guarantee.`
      : "No pacing correction was required because the modeled delivery stayed within the expected tolerance range."
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
    .replace(/<[^>]+>/g, " ")
    .replace(/^#{1,6}\s+/gm, "")
    .replace(/^\s*[-*+]\s+/gm, "")
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
  if (role === "inflight-operations") return "This in-flight operations decision matters because pacing correction protects delivery quality during live execution.";
  if (role === "measurement") return "This measurement decision matters because reliable attribution is required to carry useful learning into the next cycle.";
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

function buildApprovedPlanCsv(campaignState = state.campaignStateObject) {
  const rows = campaignState?.stageOutputs?.["booking-proposals-agent"]?.booking_summary?.line_items?.length
    ? campaignState.stageOutputs["booking-proposals-agent"].booking_summary.line_items
    : buildApprovedPlanRows(datasets.inventoryMatrix || []);
  const columns = ["line_item_id", "network", "platform_type", "country_code", "daypart", "impressions", "spend_usd", "cpm_30s", "status"];
  return toCsv(rows, columns);
}

function buildApprovedPlanRows(inventory) {
  if (!inventory.length) return [];
  const approved = inventory.filter((row) => row.country_code !== "SA");
  const rows = approved.slice(0, 12).map((row, idx) => {
    const impressions = Math.round(row.avail_impressions_30s * 0.35);
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
      impressions: 18000,
      spend_usd: Math.round((18000 / 1000) * Number(filler.cpm_30s || 40)),
      cpm_30s: filler.cpm_30s,
      status: "approved"
    });
  }

  return rows;
}

function buildMakeGoodSummary(logs, profile = {}, campaignState = null) {
  if (campaignState?.stageOutputs?.["inflight-operations-agent"]?.inflight_summary) {
    const inflight = campaignState.stageOutputs["inflight-operations-agent"].inflight_summary;
    return {
      shiftBudget: Math.round(inflight.shift_budget_usd || 0)
    };
  }
  return {
    shiftBudget: Math.round(profile.makeGoodBudget || 1800)
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

  const fencedMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fencedMatch?.[1]) {
    const fencedParsed = Utils.safeParseJson(fencedMatch[1]);
    if (Object.keys(fencedParsed).length) return fencedParsed;
  }

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
  const normalizedKey = normalizeVariantKey(variantKey);
  return PLAN_VARIANTS.find((item) => item.key === normalizedKey) || PLAN_VARIANTS[0];
}

function getVariantRouteType(variantKey = "") {
  return getVariantByKey(variantKey)?.routeType || "adaptive";
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
  return 25000;
}

function splitPipeValue(value = "") {
  return (value || "")
    .toString()
    .split("|")
    .map((item) => item.trim())
    .filter(Boolean);
}

function average(rows = [], field) {
  if (!rows.length) return 0;
  const total = rows.reduce((sum, row) => sum + Number(row?.[field] || 0), 0);
  return total / rows.length;
}

function countBy(rows = [], getter = () => "") {
  const map = new Map();
  rows.forEach((row) => {
    const key = getter(row);
    if (!key) return;
    map.set(key, (map.get(key) || 0) + 1);
  });
  return map;
}

function mapToRankedList(map, total, limit = 5, decimals = 1) {
  return [...map.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([label, count]) => ({
      label,
      count,
      pct: total ? roundToTwo((count / total) * 100, decimals) : 0
    }));
}

function roundToTwo(value, decimals = 2) {
  const factor = 10 ** decimals;
  return Math.round(Number(value) * factor) / factor;
}

function normalizePromptText(prompt = "") {
  return (prompt || "").toString().trim().replace(/\s+/g, " ");
}

function deriveProductFamily(prompt = "") {
  const text = normalizePromptText(prompt).toLowerCase();
  if (/\b(toddler|baby|infant)\b/.test(text) && /\b(snack|food|puff|cracker|bite|meal)\b/.test(text)) {
    return { key: "toddler_snacks", label: "toddler snacks", displayLabel: "organic toddler snacks" };
  }
  if (/\binsurance\b/.test(text)) {
    return { key: "insurance", label: "insurance", displayLabel: "insurance" };
  }
  if (/\b(auto|automotive|vehicle|car|suv|truck)\b/.test(text)) {
    return { key: "automotive", label: "automotive", displayLabel: "automotive" };
  }
  if (/\b(finance|financial|bank|investment|credit|loan)\b/.test(text)) {
    return { key: "financial_services", label: "financial services", displayLabel: "financial services" };
  }
  if (/\b(retail|shopper|shopping|grocery|promotion|sale)\b/.test(text)) {
    return { key: "retail", label: "retail", displayLabel: "retail promotion" };
  }
  if (/\b(series|drama|premiere|streaming show|season)\b/.test(text)) {
    return { key: "entertainment", label: "entertainment", displayLabel: "entertainment launch" };
  }
  if (/\b(sports|finals|tournament|playoffs)\b/.test(text)) {
    return { key: "sports", label: "sports", displayLabel: "sports campaign" };
  }
  return { key: "general_brand", label: "general brand", displayLabel: "general brand campaign" };
}

function buildCampaignProfile(prompt = "") {
  const text = normalizePromptText(prompt);
  const lower = text.toLowerCase();
  const productFamily = deriveProductFamily(text);
  const countries = detectCampaignCountries(text);
  return {
    prompt: text,
    lower,
    budgetUsd: extractBudgetUsd(text),
    countries,
    productFamily,
    prefersStreaming: /\b(streaming|digital|max|discovery\+|ott|connected tv)\b/.test(lower),
    prefersLinear: /\b(linear|tv|television|broadcast|cnn|hgtv|food network|tnt)\b/.test(lower),
    wantsFamilies: /\b(parent|parents|family|families|toddler|child|children|kids)\b/.test(lower),
    wantsYoungAdults: /\b(gen z|gen-z|millennial|young parent|young adults?)\b/.test(lower),
    wantsOlderAdults: /\b(older|older adults?|55\+|retiree|retirement|empty nester)\b/.test(lower),
    wantsSports: /\b(sports|finals|playoff|tournament)\b/.test(lower),
    wantsOrganic: /\b(organic|healthy|natural)\b/.test(lower),
    wantsReach: /\b(reach|awareness|launch|premiere|broad)\b/.test(lower),
    wantsEfficiency: /\b(efficiency|sales|conversion|performance|response|cost control)\b/.test(lower),
  };
}

function tagsContain(row, terms = []) {
  const text = `${row?.segment_name || ""}|${row?.behavioral_tags || ""}|${row?.viewing_habit || ""}`.toLowerCase();
  return terms.some((term) => text.includes(term.toLowerCase()));
}

function scoreViewerProfile(row, campaign) {
  let score = (Number(row.identity_confidence_score || 0) * 0.15)
    + (Number(row.recent_conversion_propensity || 0) * 0.22)
    + (Number(row.streaming_affinity_index || 0) * (campaign.prefersStreaming ? 0.16 : 0.08))
    + (Number(row.linear_affinity_index || 0) * (campaign.prefersLinear ? 0.16 : 0.08));

  const age = Number(row.age || 0);

  if (campaign.wantsFamilies) {
    score += Number(row.toddler_parent_score || 0) * 0.12;
  }
  if (campaign.wantsSports) {
    score += Number(row.tnt_sports_index || 0) * 0.14;
    score += Number(row.sports_fandom_index || 0) * 0.16;
  }
  if (campaign.wantsOrganic) {
    score += Number(row.organic_food_index || 0) * 0.18;
  }
  if (campaign.wantsYoungAdults && age >= 24 && age <= 40) {
    score += 18;
  }
  if (campaign.wantsOlderAdults && age >= 45) {
    score += 20;
  }
  if (campaign.wantsReach) {
    score += Number(row.linear_affinity_index || 0) * 0.05;
    score += Number(row.streaming_affinity_index || 0) * 0.05;
  }
  if (campaign.wantsEfficiency) {
    score += Number(row.recent_conversion_propensity || 0) * 0.1;
  }

  switch (campaign.productFamily.key) {
    case "toddler_snacks":
      score += Number(row.toddler_parent_score || 0) * 0.32;
      score += Number(row.organic_food_index || 0) * 0.22;
      score += Number(row.max_daytime_index || 0) * 0.14;
      score += Number(row.discovery_plus_index || 0) * 0.12;
      if (age >= 25 && age <= 42) score += 18;
      if (tagsContain(row, ["parents of toddlers", "millennial parents", "organic buyers", "healthy families"])) score += 28;
      break;
    case "insurance":
      score += Number(row.insurance_intender_score || 0) * 0.34;
      score += Number(row.cnn_linear_index || 0) * 0.18;
      score += Number(row.hgtv_linear_index || 0) * 0.12;
      score += Number(row.financial_services_score || 0) * 0.16;
      if (age >= 35 && age <= 64) score += 20;
      if (tagsContain(row, ["insurance shoppers", "financial planners", "homeowners", "older adults"])) score += 24;
      break;
    case "automotive":
      score += Number(row.auto_intender_score || 0) * 0.34;
      score += Number(row.tnt_sports_index || 0) * 0.14;
      score += Number(row.sports_fandom_index || 0) * 0.12;
      if (tagsContain(row, ["auto intenders", "commuters", "deal researchers"])) score += 22;
      break;
    case "financial_services":
      score += Number(row.financial_services_score || 0) * 0.34;
      score += Number(row.cnn_linear_index || 0) * 0.18;
      score += Number(row.insurance_intender_score || 0) * 0.12;
      if (age >= 35 && age <= 64) score += 18;
      if (tagsContain(row, ["financial planners", "business news viewers", "affluent professionals"])) score += 24;
      break;
    case "retail":
      score += Number(row.retail_value_score || 0) * 0.28;
      score += Number(row.food_network_index || 0) * 0.1;
      score += Number(row.discovery_plus_index || 0) * 0.08;
      if (tagsContain(row, ["value shoppers", "coupon users", "retail browsers"])) score += 24;
      break;
    case "entertainment":
      score += Number(row.entertainment_binge_index || 0) * 0.32;
      score += Number(row.max_daytime_index || 0) * 0.18;
      score += Number(row.discovery_plus_index || 0) * 0.08;
      if (age >= 18 && age <= 39) score += 18;
      if (tagsContain(row, ["binge watchers", "prestige drama fans", "streaming first"])) score += 24;
      break;
    case "sports":
      score += Number(row.tnt_sports_index || 0) * 0.24;
      score += Number(row.sports_fandom_index || 0) * 0.24;
      score += Number(row.streaming_affinity_index || 0) * 0.08;
      if (tagsContain(row, ["sports fanatics", "live event viewers"])) score += 26;
      break;
    default:
      score += Number(row.streaming_affinity_index || 0) * 0.06;
      score += Number(row.linear_affinity_index || 0) * 0.06;
      score += Number(row.retail_value_score || 0) * 0.04;
      break;
  }

  return roundToTwo(score);
}

function analyzeAudienceAgainstCampaign(prompt = "") {
  const campaign = buildCampaignProfile(prompt);
  const allRows = (datasets.unifiedAudienceDataset || []).map((row) => ({
    ...row,
    fitScore: scoreViewerProfile(row, campaign)
  }));
  const ranked = [...allRows].sort((a, b) => b.fitScore - a.fitScore);
  let matched = ranked.filter((row) => row.fitScore >= 70).slice(0, 84);
  if (matched.length < 30) matched = ranked.slice(0, Math.min(54, ranked.length));

  const globalTagCounts = new Map();
  allRows.forEach((row) => splitPipeValue(row.behavioral_tags).forEach((tag) => globalTagCounts.set(tag, (globalTagCounts.get(tag) || 0) + 1)));

  const tagCounts = new Map();
  matched.forEach((row) => splitPipeValue(row.behavioral_tags).forEach((tag) => tagCounts.set(tag, (tagCounts.get(tag) || 0) + 1)));

  const stateCounts = countBy(matched, (row) => row.state_code);
  const dmaCounts = countBy(matched, (row) => row.dma_region);
  const segmentCounts = countBy(matched, (row) => row.segment_name);
  const viewingCounts = countBy(matched, (row) => row.viewing_habit);
  const ageBucketCounts = countBy(matched, (row) => row.age_bucket);
  const incomeCounts = countBy(matched, (row) => row.income_bracket);
  const platformCounts = new Map();
  matched.forEach((row) => splitPipeValue(row.primary_platforms).forEach((platform) => platformCounts.set(platform, (platformCounts.get(platform) || 0) + 1)));

  const householdMap = new Map();
  matched.forEach((row) => {
    if (!householdMap.has(row.household_id)) householdMap.set(row.household_id, row);
  });
  const seedHouseholdCount = householdMap.size;
  const uniqueHouseholds = roundToTwo(
    [...householdMap.values()].reduce((sum, row) => sum + Math.max(1, Number(row.modeled_household_weight || 1)), 0),
    0
  );
  const multiPlatformHouseholds = [...householdMap.values()].filter((row) => splitPipeValue(row.primary_platforms).length > 2).length;

  const networkScores = {
    "Max": average(matched, "max_daytime_index"),
    "discovery+": average(matched, "discovery_plus_index"),
    "CNN": average(matched, "cnn_linear_index"),
    "HGTV": average(matched, "hgtv_linear_index"),
    "Food Network": average(matched, "food_network_index"),
    "TNT Sports": average(matched, "tnt_sports_index"),
  };
  const networkLift = {
    "Max": average(matched, "max_daytime_index") / Math.max(average(allRows, "max_daytime_index"), 1),
    "discovery+": average(matched, "discovery_plus_index") / Math.max(average(allRows, "discovery_plus_index"), 1),
    "CNN": average(matched, "cnn_linear_index") / Math.max(average(allRows, "cnn_linear_index"), 1),
    "HGTV": average(matched, "hgtv_linear_index") / Math.max(average(allRows, "hgtv_linear_index"), 1),
    "Food Network": average(matched, "food_network_index") / Math.max(average(allRows, "food_network_index"), 1),
    "TNT Sports": average(matched, "tnt_sports_index") / Math.max(average(allRows, "tnt_sports_index"), 1),
  };

  const matchedTotal = matched.length || 1;
  const topTags = mapToRankedList(tagCounts, matchedTotal, 6);
  const topSegments = mapToRankedList(segmentCounts, matchedTotal, 4);
  const topStates = mapToRankedList(stateCounts, matchedTotal, 4);
  const topDmas = mapToRankedList(dmaCounts, matchedTotal, 4);
  const topPlatforms = mapToRankedList(platformCounts, matchedTotal, 4);
  const topAgeBuckets = mapToRankedList(ageBucketCounts, matchedTotal, 3);
  const topIncomeBrackets = mapToRankedList(incomeCounts, matchedTotal, 3);
  const youngShare = roundToTwo((matched.filter((row) => Number(row.age || 0) < 40).length / matchedTotal) * 100, 1);
  const olderShare = roundToTwo((matched.filter((row) => Number(row.age || 0) >= 45).length / matchedTotal) * 100, 1);
  const parentShare = roundToTwo((matched.filter((row) => tagsContain(row, ["parents of toddlers", "millennial parents"])).length / matchedTotal) * 100, 1);
  const streamingHeavyShare = roundToTwo((matched.filter((row) => (row.viewing_habit || "").toLowerCase().includes("streaming")).length / matchedTotal) * 100, 1);
  const linearHeavyShare = roundToTwo((matched.filter((row) => (row.viewing_habit || "").toLowerCase().includes("linear")).length / matchedTotal) * 100, 1);
  const avgAge = roundToTwo(average(matched, "age"), 1);
  const avgWeeklyMediaMinutes = Math.round(average(matched, "avg_weekly_media_minutes"));
  const avgConversionPropensity = roundToTwo(average(matched, "recent_conversion_propensity"), 1);
  const avgStreaming = average(matched, "streaming_affinity_index");
  const avgLinear = average(matched, "linear_affinity_index");
  const avgEntertainment = average(matched, "entertainment_binge_index");
  const avgFinance = average(matched, "financial_services_score");
  const avgRetail = average(matched, "retail_value_score");

  const streamingScore = avgStreaming
    + average(matched, "max_daytime_index") * 0.35
    + average(matched, "discovery_plus_index") * 0.35
    + streamingHeavyShare * 0.4
    + (campaign.wantsYoungAdults ? 8 : 0)
    + (campaign.productFamily.key === "toddler_snacks" ? 18 : 0)
    + (campaign.productFamily.key === "entertainment" ? 22 : 0);

  const linearScore = avgLinear
    + average(matched, "cnn_linear_index") * 0.35
    + average(matched, "hgtv_linear_index") * 0.2
    + average(matched, "tnt_sports_index") * 0.2
    + linearHeavyShare * 0.38
    + (campaign.wantsOlderAdults ? 10 : 0)
    + (campaign.productFamily.key === "insurance" ? 18 : 0)
    + (campaign.productFamily.key === "financial_services" ? 20 : 0);

  const balancedScore = 100
    - Math.abs(streamingScore - linearScore) * 0.45
    + (multiPlatformHouseholds / matchedTotal) * 35
    + (campaign.productFamily.key === "retail" ? 14 : 0)
    + (campaign.productFamily.key === "automotive" ? 10 : 0)
    + (campaign.productFamily.key === "sports" ? 10 : 0);

  const indexedTag = topTags
    .map((tag) => {
      const matchedShare = tag.count / matchedTotal;
      const globalShare = (globalTagCounts.get(tag.label) || 1) / Math.max(allRows.length, 1);
      return { ...tag, lift: matchedShare / Math.max(globalShare, 0.01) };
    })
    .sort((a, b) => b.lift - a.lift)[0];

  return {
    campaign,
    matchedRows: matched,
    rankedRows: ranked,
    seedHouseholdCount,
    uniqueHouseholds,
    topTags,
    topSegments,
    topStates,
    topDmas,
    topPlatforms,
    topAgeBuckets,
    topIncomeBrackets,
    viewingMix: mapToRankedList(viewingCounts, matchedTotal, 3),
    networkScores,
    networkLift,
    avgAge,
    avgWeeklyMediaMinutes,
    avgConversionPropensity,
    youngShare,
    olderShare,
    parentShare,
    streamingHeavyShare,
    linearHeavyShare,
    streamingScore,
    linearScore,
    balancedScore,
    avgStreaming,
    avgLinear,
    avgEntertainment,
    avgFinance,
    avgRetail,
    indexedTag,
    overlapPct: roundToTwo((multiPlatformHouseholds / matchedTotal) * 100, 1),
    sampleRows: matched.slice(0, 10).map((row) => ({
      device_id: row.device_id,
      household_id: row.household_id,
      modeled_household_weight: row.modeled_household_weight,
      age: row.age,
      age_bucket: row.age_bucket,
      income_bracket: row.income_bracket,
      viewing_habit: row.viewing_habit,
      behavioral_tags: row.behavioral_tags,
      avg_weekly_media_minutes: row.avg_weekly_media_minutes,
      recent_conversion_propensity: row.recent_conversion_propensity,
      max_daytime_index: row.max_daytime_index,
      discovery_plus_index: row.discovery_plus_index,
      cnn_linear_index: row.cnn_linear_index,
      hgtv_linear_index: row.hgtv_linear_index,
      food_network_index: row.food_network_index,
      tnt_sports_index: row.tnt_sports_index,
      fit_score: row.fitScore
    }))
  };
}

function rankedNetworksForVariant(intelligence, variantKey = "") {
  const routeType = getVariantRouteType(variantKey);
  const networkEntries = Object.entries(intelligence.networkScores || {}).map(([name, score]) => ({
    name,
    score,
    lift: intelligence.networkLift?.[name] || 1
  }));
  const streaming = networkEntries.filter((item) => ["Max", "discovery+"].includes(item.name)).sort((a, b) => b.score - a.score);
  const linear = networkEntries.filter((item) => !["Max", "discovery+"].includes(item.name)).sort((a, b) => b.score - a.score);
  if (routeType === "precision") return [...streaming.slice(0, 2), ...linear.slice(0, 1)];
  if (routeType === "scale") return [...linear.slice(0, 2), ...streaming.slice(0, 1)];
  return [streaming[0], linear[0], linear[1] || streaming[1]].filter(Boolean);
}

function buildScenarioAllocation(intelligence, variantKey = "") {
  const routeType = getVariantRouteType(variantKey);
  const reservePct = routeType === "adaptive" ? 12 : 10;
  if (routeType === "precision") {
    const streamingPct = clampNumber(
      Math.round(58 + ((intelligence.streamingScore - intelligence.linearScore) * 0.08) + (intelligence.campaign?.wantsEfficiency ? 4 : 0)),
      52,
      80
    );
    const linearPct = 100 - reservePct - streamingPct;
    return { streamingPct, linearPct, reservePct };
  }
  if (routeType === "scale") {
    const linearPct = clampNumber(
      Math.round(58 + ((intelligence.linearScore - intelligence.streamingScore) * 0.08) + (intelligence.campaign?.wantsReach ? 4 : 0)),
      52,
      78
    );
    const streamingPct = 100 - reservePct - linearPct;
    return { streamingPct, linearPct, reservePct };
  }
  const streamingPct = clampNumber(
    Math.round(47 + ((intelligence.streamingScore - intelligence.linearScore) * 0.03) + (intelligence.campaign?.wantsEfficiency ? 1 : 0)),
    42,
    56
  );
  const linearPct = 100 - reservePct - streamingPct;
  return { streamingPct, linearPct, reservePct };
}

function buildDeliveryTiming(intelligence, variantKey = "") {
  const routeType = getVariantRouteType(variantKey);
  const rankedNetworks = rankedNetworksForVariant(intelligence, variantKey);
  const topNetwork = rankedNetworks[0]?.name || "Max";
  const supportNetwork = rankedNetworks[1]?.name || "CNN";
  const parentTilt = intelligence.parentShare >= 20 ? "daytime and early evening" : "evening and late prime";
  if (routeType === "precision") {
    return `Push ${topNetwork} during ${parentTilt}, then widen into ${supportNetwork} only when the yield forecast keeps the route above its outcome target.`;
  }
  if (routeType === "scale") {
    return `Anchor delivery in the steadiest high-capacity windows on ${topNetwork}, then add measured reinforcement on ${supportNetwork} when incremental reach remains available.`;
  }
  return `Start with split coverage across ${topNetwork} and ${supportNetwork}, then rebalance every pacing cycle using pacing, readiness, and conversion signals.`;
}

function buildScenarioRecommendationReason(intelligence, variantKey = "") {
  const routeType = getVariantRouteType(variantKey);
  const rankedNetworks = rankedNetworksForVariant(intelligence, variantKey);
  const leadNetwork = rankedNetworks[0];
  const demographicReadout = buildDemographicReadout(intelligence);
  const behavioralReadout = buildBehavioralReadout(intelligence);
  const incomeReadout = buildIncomeReadout(intelligence);
  const marketReadout = buildMarketEvidenceReadout(intelligence);
  const viewingReadout = buildViewingReadout(intelligence);

  if (routeType === "precision") {
    return `${joinEvidenceClauses([
      `${intelligence.streamingHeavyShare}% of matched households lean streaming-heavy versus ${intelligence.linearHeavyShare}% linear-heavy`,
      demographicReadout,
      incomeReadout || viewingReadout,
      `${leadNetwork?.name || "Max"} is the strongest first network`
    ])}, so a streaming-heavy opening is the cleanest first move.`;
  }
  if (routeType === "scale") {
    return `${joinEvidenceClauses([
      `${intelligence.linearHeavyShare}% of matched households lean linear-heavy`,
      demographicReadout,
      marketReadout || incomeReadout,
      `${leadNetwork?.name || "CNN"} provides the steadiest opening lane for broad reach`
    ])}.`;
  }
  return `${joinEvidenceClauses([
    `Streaming-heavy behavior sits at ${intelligence.streamingHeavyShare}% and linear-heavy behavior sits at ${intelligence.linearHeavyShare}%`,
    demographicReadout,
    incomeReadout || viewingReadout,
    `${intelligence.overlapPct}% cross-platform overlap means both channels can matter early`,
    behavioralReadout
  ])}, so a balanced opening keeps both behaviors covered without forcing an early bias.`;
}

function compactScenarioLabel(value = "", fallback = "Audience") {
  const cleaned = String(value || "")
    .replace(/\s+/g, " ")
    .replace(/[|/]+/g, " ")
    .trim();
  if (!cleaned) return fallback;
  return cleaned
    .split(" ")
    .slice(0, 4)
    .join(" ");
}

function deriveScenarioAudienceLabel(intelligence = {}) {
  return compactScenarioLabel(
    intelligence.topSegments?.[0]?.label
      || intelligence.topTags?.[0]?.label
      || intelligence.campaign?.productFamily?.displayLabel,
    "Audience"
  );
}

function deriveScenarioMarketLabel(intelligence = {}) {
  return compactScenarioLabel(
    intelligence.topStates?.[0]?.label
      || intelligence.campaign?.countries?.[0],
    "Priority Market"
  );
}

function deriveScenarioProductLabel(intelligence = {}) {
  const base = String(intelligence.campaign?.productFamily?.displayLabel || "Campaign")
    .replace(/\bcampaign\b/gi, "")
    .replace(/\blaunch\b/gi, "")
    .trim();
  return compactScenarioLabel(base, "Campaign");
}

function deriveCampaignGoalLabel(campaign = {}) {
  if (campaign.wantsReach && campaign.wantsEfficiency) return "efficient growth";
  if (campaign.wantsEfficiency) return "conversion lift";
  if (campaign.wantsReach) return "household reach";
  if (campaign.wantsSports) return "live-event demand";
  if (campaign.wantsFamilies) return "family trial";
  return "market momentum";
}

function buildDemographicReadout(intelligence = {}) {
  const avgAge = intelligence.avgAge || 0;
  const youngShare = intelligence.youngShare || 0;
  const olderShare = intelligence.olderShare || 0;
  const parentShare = intelligence.parentShare || 0;
  if (parentShare >= 24) {
    return `${parentShare}% of the matched audience indexes as parents and the average matched age is ${avgAge}`;
  }
  if (youngShare >= olderShare + 8) {
    return `${youngShare}% of the matched audience is under 40 and the average matched age is ${avgAge}`;
  }
  if (olderShare >= youngShare + 8) {
    return `${olderShare}% of the matched audience is age 45 or older and the average matched age is ${avgAge}`;
  }
  return `the average matched age is ${avgAge}, with ${youngShare}% under 40 and ${olderShare}% age 45 or older`;
}

function buildBehavioralReadout(intelligence = {}) {
  const topPlatform = intelligence.topPlatforms?.[0]?.label || "connected TV";
  const topTag = intelligence.indexedTag?.label || intelligence.topTags?.[0]?.label || "multi-platform viewing";
  return `${topPlatform} is the strongest platform signal and ${topTag} is the clearest behavioral tag`;
}

function buildIncomeReadout(intelligence = {}) {
  const leadIncome = intelligence.topIncomeBrackets?.[0];
  return leadIncome ? `${leadIncome.pct}% of matched profiles sit in the ${leadIncome.label} income band` : "";
}

function buildViewingReadout(intelligence = {}) {
  const leadViewing = intelligence.viewingMix?.[0];
  return leadViewing ? `${leadViewing.pct}% of matched profiles cluster in ${leadViewing.label} viewing` : "";
}

function buildMarketEvidenceReadout(intelligence = {}) {
  const markets = (intelligence.topStates || []).map((item) => item.label).slice(0, 2);
  if (!markets.length) return "";
  if (markets.length === 1) return `the heaviest state concentration sits in ${markets[0]}`;
  return `the heaviest state concentration sits in ${markets[0]} and ${markets[1]}`;
}

function joinEvidenceClauses(clauses = []) {
  const parts = clauses.map((item) => normalizeWhitespace(item)).filter(Boolean);
  if (!parts.length) return "";
  if (parts.length === 1) return parts[0];
  if (parts.length === 2) return `${parts[0]} and ${parts[1]}`;
  return `${parts.slice(0, -1).join(", ")}, and ${parts[parts.length - 1]}`;
}

function buildScenarioTitle(intelligence = {}, variantKey = "") {
  const routeType = getVariantRouteType(variantKey);
  const audienceLabel = deriveScenarioAudienceLabel(intelligence);
  const marketLabel = deriveScenarioMarketLabel(intelligence);
  const productLabel = deriveScenarioProductLabel(intelligence);
  if (routeType === "precision") {
    return `${audienceLabel} Streaming-Heavy Route`;
  }
  if (routeType === "scale") {
    return `${marketLabel} Linear-Heavy Route`;
  }
  return `${productLabel} Balanced Route`;
}

function buildScenarioPromptText(intelligence = {}, variantKey = "", networks = []) {
  const routeType = getVariantRouteType(variantKey);
  const audienceLabel = deriveScenarioAudienceLabel(intelligence);
  const marketLabel = deriveScenarioMarketLabel(intelligence);
  const goalLabel = deriveCampaignGoalLabel(intelligence.campaign || {});
  const leadNetwork = networks[0]?.name || "Max";
  const supportNetwork = networks[1]?.name || "CNN";
  const demographicReadout = buildDemographicReadout(intelligence);
  const incomeReadout = buildIncomeReadout(intelligence);
  const viewingReadout = buildViewingReadout(intelligence);
  if (routeType === "precision") {
    return `Keep the opening decisively streaming-heavy because ${joinEvidenceClauses([
      `${audienceLabel} skews on-demand`,
      `${intelligence.streamingHeavyShare}% of matched households lean streaming-heavy versus ${intelligence.linearHeavyShare}% linear-heavy`,
      demographicReadout,
      incomeReadout || viewingReadout,
      `${leadNetwork} gives this audience the clearest first signal for ${goalLabel}`
    ])}.`;
  }
  if (routeType === "scale") {
    return `Keep the opening clearly linear-heavy because ${joinEvidenceClauses([
      `${marketLabel} is the first scale market`,
      `${intelligence.linearHeavyShare}% of matched households lean linear-heavy`,
      demographicReadout,
      incomeReadout || viewingReadout,
      `${leadNetwork} is the steadiest first network for household coverage`
    ])}, then use ${supportNetwork} only where it extends the audience cleanly.`;
  }
  return `Keep this route genuinely balanced because ${joinEvidenceClauses([
    `${leadNetwork} and ${supportNetwork} both carry credible opening value`,
    `streaming-heavy behavior is ${intelligence.streamingHeavyShare}% and linear-heavy behavior is ${intelligence.linearHeavyShare}%`,
    demographicReadout,
    incomeReadout || viewingReadout,
    `${intelligence.overlapPct}% overlap makes a dual-channel opening viable for ${goalLabel}`
  ])}.`;
}

function buildScenarioStrategyText(intelligence = {}, variantKey = "") {
  const routeType = getVariantRouteType(variantKey);
  const audienceLabel = deriveScenarioAudienceLabel(intelligence);
  const marketLabel = deriveScenarioMarketLabel(intelligence);
  const overlapPct = intelligence.overlapPct || 0;
  const behavioralReadout = buildBehavioralReadout(intelligence);
  const marketReadout = buildMarketEvidenceReadout(intelligence);
  const viewingReadout = buildViewingReadout(intelligence);
  if (routeType === "precision") {
    return `Use ${audienceLabel} as the anchor audience, let streaming carry the opening weight, and rely on ${overlapPct}% cross-platform overlap, ${viewingReadout || behavioralReadout}, and average weekly media time of ${intelligence.avgWeeklyMediaMinutes || 0} minutes to keep reinforcement efficient while the strongest on-demand placements prove themselves.`;
  }
  if (routeType === "scale") {
    return `Use ${marketLabel} and the strongest adjacent markets to build linear-led household penetration first because ${marketReadout || "the audience is clustered in a few scalable markets"}, then add streaming support only where the reach curve starts to flatten and the audience still needs incremental younger or lighter-viewing households.`;
  }
  return `Keep both channels viable because the matched audience shows ${intelligence.streamingHeavyShare}% streaming-heavy and ${intelligence.linearHeavyShare}% linear-heavy behavior, ${viewingReadout || behavioralReadout}, and ${overlapPct}% cross-platform overlap, making a balanced reallocation path a genuine advantage instead of a fallback.`;
}

function buildScenarioAllocationStrategy(intelligence = {}, variantKey = "", allocation = null, networks = []) {
  const routeType = getVariantRouteType(variantKey);
  const plan = allocation || buildScenarioAllocation(intelligence, variantKey);
  const streamingNetworks = networks.filter((item) => ["Max", "discovery+"].includes(item.name)).map((item) => item.name).join(" and ") || "Max";
  const linearNetworks = networks.filter((item) => !["Max", "discovery+"].includes(item.name)).map((item) => item.name).join(" and ") || "CNN";
  if (routeType === "precision") {
    return `Open with ${plan.streamingPct}% in ${streamingNetworks}, keep ${plan.linearPct}% in ${linearNetworks} as support reach, and protect ${plan.reservePct}% for pacing-led shifts.`;
  }
  if (routeType === "scale") {
    return `Open with ${plan.linearPct}% in ${linearNetworks} for household scale, keep ${plan.streamingPct}% in ${streamingNetworks} for reinforcement and frequency control, and hold ${plan.reservePct}% in reserve.`;
  }
  return `Open with ${plan.streamingPct}% streaming across ${streamingNetworks}, ${plan.linearPct}% linear across ${linearNetworks}, and keep ${plan.reservePct}% flexible so pacing can decide the final mix.`;
}

function buildScenarioChannelLogic(intelligence = {}, variantKey = "", networks = []) {
  const routeType = getVariantRouteType(variantKey);
  const leadNetwork = networks[0]?.name || "Max";
  const leadLift = roundToTwo(networks[0]?.lift || 1, 1);
  const demographicReadout = buildDemographicReadout(intelligence);
  const incomeReadout = buildIncomeReadout(intelligence);
  if (routeType === "precision") {
    return `${joinEvidenceClauses([
      `${leadNetwork} is the strongest streaming lead at ${leadLift}x audience fit`,
      demographicReadout,
      incomeReadout,
      "the audience shows higher streaming-heavy behavior than linear-heavy behavior"
    ])}, so the mix opens with streaming in front.`;
  }
  if (routeType === "scale") {
    return `${joinEvidenceClauses([
      `${leadNetwork} is the strongest linear lead at ${leadLift}x audience fit`,
      demographicReadout,
      incomeReadout,
      "the audience shows broader linear behavior"
    ])}, so the plan opens with linear weight before adding streaming reinforcement.`;
  }
  return `${joinEvidenceClauses([
    demographicReadout,
    incomeReadout,
    `the streaming-heavy share (${intelligence.streamingHeavyShare}%) and linear-heavy share (${intelligence.linearHeavyShare}%) are close enough that a balanced mix is safer than forcing a hard bias`,
    `${leadNetwork} still gives the plan a strong first lane`
  ])}.`;
}

function buildScenarioBlueprints(campaignPrompt = "") {
  const intelligence = analyzeAudienceAgainstCampaign(campaignPrompt);
  const scenarioScores = {
    "scenario-a": intelligence.streamingScore + (intelligence.campaign.prefersStreaming ? 10 : 0) + (intelligence.campaign.wantsEfficiency ? 6 : 0),
    "scenario-b": intelligence.linearScore + (intelligence.campaign.prefersLinear ? 10 : 0) + (intelligence.campaign.wantsReach ? 6 : 0),
    "scenario-c": intelligence.balancedScore + (intelligence.campaign.wantsReach && intelligence.campaign.wantsEfficiency ? 8 : 0)
  };
  const recommendedVariantKey = Object.entries(scenarioScores)
    .sort((a, b) => b[1] - a[1])[0]?.[0] || PLAN_VARIANTS[0].key;

  const options = PLAN_VARIANTS.map((variant, index) => {
    const allocation = buildScenarioAllocation(intelligence, variant.key);
    const networks = rankedNetworksForVariant(intelligence, variant.key);
    const recommendationReason = buildScenarioRecommendationReason(intelligence, variant.key);
    const title = buildScenarioTitle(intelligence, variant.key) || variant.title || ARCHITECT_PLAN_FALLBACK_TITLES[index];
    const why = variant.key === recommendedVariantKey
      ? `Recommended: ${recommendationReason}`
      : recommendationReason;
    return {
      variantKey: variant.key,
      title,
      promptTile: title,
      promptText: buildScenarioPromptText(intelligence, variant.key, networks),
      strategy: `${buildScenarioStrategyText(intelligence, variant.key)} Primary audience signals: ${deriveScenarioAudienceLabel(intelligence)}, ${intelligence.topAgeBuckets?.[0]?.label || "mixed ages"}, ${intelligence.topIncomeBrackets?.[0]?.label || "mixed income bands"}, and ${intelligence.overlapPct}% cross-platform overlap.`,
      why,
      allocation,
      allocationStrategy: buildScenarioAllocationStrategy(intelligence, variant.key, allocation, networks),
      deliveryTiming: buildDeliveryTiming(intelligence, variant.key),
      channelLogic: buildScenarioChannelLogic(intelligence, variant.key, networks),
      recommendationReason,
      recommended: variant.key === recommendedVariantKey,
      scenarioIntelligence: {
        ...intelligence,
        allocation,
        rankedNetworks: networks,
        scenarioScore: scenarioScores[variant.key],
        recommendationReason
      }
    };
  });

  if (isToddlerHeroBrief(campaignPrompt, intelligence.campaign)) {
    return {
      campaignPrompt,
      intelligence,
      recommendedVariantKey: TODDLER_HERO_ROUTE.variantKey,
      options: options.map((option) => {
        const toddlerOption = buildToddlerHeroPlanContext({
          ...option,
          recommended: option.variantKey === TODDLER_HERO_ROUTE.variantKey
        }, campaignPrompt) || option;
        return {
          ...toddlerOption,
          recommended: toddlerOption.variantKey === TODDLER_HERO_ROUTE.variantKey
        };
      })
    };
  }

  return {
    campaignPrompt,
    intelligence,
    recommendedVariantKey,
    options
  };
}

function buildScenarioDatasetInput(campaignPrompt = "", selectedPlan = null) {
  const catalog = buildScenarioBlueprints(campaignPrompt || state.campaignPrompt || "");
  const picked = selectedPlan?.scenarioIntelligence
    ? selectedPlan
    : catalog.options.find((option) => option.variantKey === normalizeVariantKey(selectedPlan?.variantKey || catalog.recommendedVariantKey));
  if (!picked) return null;
  const toddlerHeroBrief = isToddlerHeroBrief(catalog.campaignPrompt || campaignPrompt, catalog.intelligence.campaign);
  const audienceSummary = toddlerHeroBrief
    ? {
      matched_profiles: 145000000,
      seed_households: 3800000,
      unique_households: 3800000,
      total_wbd_universe_hhs: 3800000,
      target_reachable_households: 312500,
      lead_cohort: "Millennial Health-Conscious Parents",
      average_age: catalog.intelligence.avgAge,
      average_weekly_media_minutes: catalog.intelligence.avgWeeklyMediaMinutes,
      average_conversion_propensity: catalog.intelligence.avgConversionPropensity,
      streaming_heavy_share_pct: catalog.intelligence.streamingHeavyShare,
      linear_heavy_share_pct: catalog.intelligence.linearHeavyShare,
      overlap_pct: 28.4,
      young_share_pct: catalog.intelligence.youngShare,
      older_share_pct: catalog.intelligence.olderShare,
      parent_share_pct: 100
    }
    : {
      matched_profiles: catalog.intelligence.matchedRows.length,
      seed_households: catalog.intelligence.seedHouseholdCount,
      unique_households: catalog.intelligence.uniqueHouseholds,
      average_age: catalog.intelligence.avgAge,
      average_weekly_media_minutes: catalog.intelligence.avgWeeklyMediaMinutes,
      average_conversion_propensity: catalog.intelligence.avgConversionPropensity,
      streaming_heavy_share_pct: catalog.intelligence.streamingHeavyShare,
      linear_heavy_share_pct: catalog.intelligence.linearHeavyShare,
      overlap_pct: catalog.intelligence.overlapPct,
      young_share_pct: catalog.intelligence.youngShare,
      older_share_pct: catalog.intelligence.olderShare,
      parent_share_pct: catalog.intelligence.parentShare
    };

  return {
    title: "Scenario Intelligence Snapshot",
    type: "text",
    content: JSON.stringify({
      prompt: catalog.campaignPrompt,
      product_family: catalog.intelligence.campaign.productFamily.displayLabel,
      recommended_variant: catalog.recommendedVariantKey,
      top_tags: catalog.intelligence.topTags.slice(0, 3),
      top_segments: catalog.intelligence.topSegments.slice(0, 3),
      top_states: catalog.intelligence.topStates.slice(0, 3),
      top_dmas: catalog.intelligence.topDmas.slice(0, 3),
      top_platforms: catalog.intelligence.topPlatforms.slice(0, 3),
      top_age_buckets: catalog.intelligence.topAgeBuckets.slice(0, 3),
      top_income_brackets: catalog.intelligence.topIncomeBrackets.slice(0, 3),
      viewing_mix: catalog.intelligence.viewingMix.slice(0, 3),
      audience_summary: audienceSummary,
      selected_scenario: {
        variant_key: picked.variantKey,
        allocation: picked.allocation || picked.scenarioIntelligence?.allocation,
        recommendation_reason: picked.recommendationReason || picked.scenarioIntelligence?.recommendationReason,
        channel_logic: picked.channelLogic || ""
      }
    }, null, 2)
  };
}

function buildArchitectRealtimeDataset(campaignPrompt = "", scenarioCatalog = null) {
  const catalog = scenarioCatalog || buildScenarioBlueprints(campaignPrompt);
  const intelligence = catalog.intelligence || {};
  const toddlerHeroBrief = isToddlerHeroBrief(catalog.campaignPrompt || campaignPrompt, intelligence.campaign);
  const pseudoState = {
    productFamily: intelligence.campaign?.productFamily || deriveProductFamily(campaignPrompt),
    countries: intelligence.campaign?.countries || detectCampaignCountries(campaignPrompt)
  };
  const yieldSignals = selectRelevantYieldSignals(pseudoState, 3).map((item) => ({
    topic: item.topic,
    vertical_tag: item.vertical_tag,
    region: item.region,
    spike_window: item.spike_window,
    signal_strength: item.signal_strength,
    recommended_channel: item.recommended_channel,
    expected_response_lift_pct: item.expected_response_lift_pct
  }));

  return {
    campaign_prompt: catalog.campaignPrompt || campaignPrompt,
    campaign_summary: {
      budget_usd: intelligence.campaign?.budgetUsd || extractBudgetUsd(campaignPrompt),
      countries: intelligence.campaign?.countries || detectCampaignCountries(campaignPrompt),
      product_family: intelligence.campaign?.productFamily?.displayLabel || deriveProductFamily(campaignPrompt).displayLabel
    },
    audience_summary: toddlerHeroBrief
      ? {
        matched_profiles: 145000000,
        seed_households: 3800000,
        unique_households: 3800000,
        total_wbd_universe_hhs: 3800000,
        target_reachable_households: 312500,
        lead_cohort: "Millennial Health-Conscious Parents",
        average_age: intelligence.avgAge || 0,
        average_weekly_media_minutes: intelligence.avgWeeklyMediaMinutes || 0,
        average_conversion_propensity: intelligence.avgConversionPropensity || 0,
        streaming_heavy_share_pct: intelligence.streamingHeavyShare || 0,
        linear_heavy_share_pct: intelligence.linearHeavyShare || 0,
        cross_platform_overlap_pct: 28.4,
        young_share_pct: intelligence.youngShare || 0,
        older_share_pct: intelligence.olderShare || 0,
        parent_share_pct: 100
      }
      : {
        matched_profiles: intelligence.matchedRows?.length || 0,
        seed_households: intelligence.seedHouseholdCount || 0,
        unique_households: intelligence.uniqueHouseholds || 0,
        average_age: intelligence.avgAge || 0,
        average_weekly_media_minutes: intelligence.avgWeeklyMediaMinutes || 0,
        average_conversion_propensity: intelligence.avgConversionPropensity || 0,
        streaming_heavy_share_pct: intelligence.streamingHeavyShare || 0,
        linear_heavy_share_pct: intelligence.linearHeavyShare || 0,
        cross_platform_overlap_pct: intelligence.overlapPct || 0,
        young_share_pct: intelligence.youngShare || 0,
        older_share_pct: intelligence.olderShare || 0,
        parent_share_pct: intelligence.parentShare || 0
      },
    top_tags: (intelligence.topTags || []).slice(0, 5),
    top_segments: (intelligence.topSegments || []).slice(0, 4),
    top_states: (intelligence.topStates || []).slice(0, 4),
    top_dmas: (intelligence.topDmas || []).slice(0, 4),
    top_platforms: (intelligence.topPlatforms || []).slice(0, 4),
    top_age_buckets: (intelligence.topAgeBuckets || []).slice(0, 3),
    top_income_brackets: (intelligence.topIncomeBrackets || []).slice(0, 3),
    viewing_mix: (intelligence.viewingMix || []).slice(0, 3),
    ranked_networks: (intelligence.networkScores
      ? Object.entries(intelligence.networkScores).map(([name, score]) => ({
        name,
        score: roundToTwo(score, 1),
        lift: roundToTwo(intelligence.networkLift?.[name] || 1, 2)
      })).sort((a, b) => b.score - a.score)
      : []),
    sample_profiles: (intelligence.sampleRows || []).slice(0, 6),
    yield_signals: yieldSignals,
    deterministic_reference: (catalog.options || []).map((option) => ({
      variant_key: option.variantKey,
      title: option.title,
      recommended: !!option.recommended,
      allocation: option.allocation,
      allocation_strategy: option.allocationStrategy,
      recommendation_reason: option.recommendationReason
    }))
  };
}

function buildArchitectAgentContract() {
  return WORKFLOW_AGENTS.map((agent) => ({
    stage: agent.phase,
    node_id: agent.nodeId,
    agent_name: agent.agentName,
    focus: agent.focus,
    sub_agents: (MASTER_AGENT_SUBAGENTS[agent.nodeId] || []).map((item) => ({
      id: item.id,
      name: item.name,
      summary: item.summary,
      definition: item.definition || item.summary
    }))
  }));
}

function buildArchitectFallbackSummary(problemText = "", scenarioCatalog = null) {
  const catalog = scenarioCatalog || buildScenarioBlueprints(problemText);
  const intelligence = catalog.intelligence || {};
  const recommended = (catalog.options || []).find((option) => option.recommended) || catalog.options?.[0];
  return [
    "Architecting scenarios from the natural-language brief.",
    `Detected product family: ${intelligence.campaign?.productFamily?.displayLabel || deriveProductFamily(problemText).displayLabel}.`,
    `Matched ${(intelligence.matchedRows?.length || 0).toLocaleString()} viewer profiles into a modeled audience of ${(intelligence.uniqueHouseholds || 0).toLocaleString()} households, with ${(intelligence.seedHouseholdCount || 0).toLocaleString()} distinct household IDs in the seed sample and ${(intelligence.topStates || []).map((item) => item.label).slice(0, 3).join(", ") || "core markets"} carrying the strongest concentration.`,
    `Demographic readout: average age ${intelligence.avgAge || 0}, ${intelligence.youngShare || 0}% under 40, ${intelligence.olderShare || 0}% age 45+, and ${intelligence.parentShare || 0}% parent-skewing households.`,
    `Income and viewing mix: ${(intelligence.topIncomeBrackets || []).map((item) => item.label).slice(0, 2).join(", ") || "mixed income bands"}, with ${(intelligence.viewingMix || []).map((item) => `${item.label} (${item.pct}%)`).slice(0, 2).join(", ") || "mixed viewing behavior"}.`,
    `Top behavioral tags: ${(intelligence.topTags || []).map((item) => item.label).slice(0, 3).join(", ") || "no dominant tags detected"}.`,
    `Audience channel tilt: ${intelligence.streamingHeavyShare || 0}% streaming-heavy and ${intelligence.linearHeavyShare || 0}% linear-heavy households.`,
    `Recommended scenario: ${recommended?.title || ARCHITECT_PLAN_FALLBACK_TITLES[0]}.`
  ];
}

function buildArchitectSystemPrompt(agentStyle = "", customArchitectPrompt = "") {
  const styleBlock = normalizeWhitespace(agentStyle);
  const customBlock = normalizeWhitespace(customArchitectPrompt);
  return [
    "You are the WBD Scenario Architect.",
    "Analyze the supplied synthetic WBD first-party audience intelligence and generate three materially different campaign scenarios in real time.",
    "The scenarios must feel product-specific, not templated, and they must map to these exact variant keys: scenario-a, scenario-b, and scenario-c.",
    "Treat those keys as slot IDs only. The visible titles should clearly signal whether the route is streaming-heavy, linear-heavy, or balanced, while still grounding that title in the brief and audience evidence.",
    "scenario-a must be the most streaming-heavy route in both the opening allocation and the narrative. scenario-b must be the most linear-heavy route. scenario-c must stay balanced, with both streaming and linear carrying meaningful opening weight.",
    "Use the supplied audience, network, and yield signals to justify every scenario and recommend exactly one option.",
    "Each scenario must cite at least three concrete synthetic signals, including at least one demographic signal and at least one media-behavior or supply signal.",
    "Use any available synthetic evidence such as age mix, parent share, income brackets, top states or DMAs, viewing mix, platform usage, overlap, weekly media minutes, network lift, sample profiles, and yield signals. Do not ignore available audience data.",
    "Keep the six-stage master-agent plan intact. Use the exact node IDs and stage numbers provided in the agent contract.",
    "Respond in two parts only:",
    "Part 1: 4 to 6 clear plain-text lines summarizing the audience diagnosis, the scenario differences, and the recommended path. Write for a non-expert audience, define jargon when needed, and make the explanation understandable to anyone reviewing the demo.",
    "Part 2: one JSON object with keys architectSummary, recommendedVariantKey, and architectPlans.",
    "Each item in architectPlans must include: variantKey, title, strategy, why, promptTile, promptText, allocationStrategy, deliveryTiming, channelLogic, recommended, recommendationReason, scenarioIntelligence, and plan.",
    "Every written field must sound logical, meaningful, and internally consistent with the supplied data.",
    "Use disciplined, linear reasoning. First identify the audience evidence, then explain how the three scenarios differ, then recommend one path, then state the main tradeoff.",
    "For every scenario field, make the function of the field obvious: strategy explains the route, why explains when that route makes sense, promptText explains the route in one sentence, allocationStrategy explains the opening mix, deliveryTiming explains when to lead, and channelLogic explains why the channel bias fits the audience evidence.",
    "Do not mention ROI or return on investment anywhere in the response.",
    "Do not use filler phrases like 'good fit', 'works well', or 'strong option' unless you immediately explain why with audience, inventory, delivery, timing, or cost evidence.",
    "If the data does not support a claim, do not invent it. Prefer a precise modest statement over a confident vague one.",
    "Before answering, silently check that each claim can be defended by the supplied data and that the recommended scenario, allocation, and narrative do not contradict each other.",
    "scenarioIntelligence must include allocation and rankedNetworks if you reference them.",
    "plan must contain exactly six items using the provided master-agent contract with fields: nodeId, agentName, stage, systemInstruction, initialTask.",
    "Set recommended to true on exactly one plan. The same plan must also match recommendedVariantKey.",
    "Do not wrap the JSON in markdown fences."
      + (styleBlock ? ` ${styleBlock}` : "")
      + (customBlock ? ` ${customBlock}` : "")
  ].join(" ");
}

function buildArchitectUserPrompt({ problemText = "", dataset = {}, agentContract = [] } = {}) {
  return [
    `Campaign Brief:\n${problemText}`,
    `\nSynthetic WBD Intelligence:\n${JSON.stringify(dataset, null, 2)}`,
    `\nSix-Stage Master-Agent Contract:\n${JSON.stringify(agentContract, null, 2)}`,
    "\nRequirements:",
    "1. Generate three genuinely different scenarios that attack the goal from distinct channel angles: one streaming-heavy, one linear-heavy, and one balanced.",
    "2. scenario-a must remain streaming-heavy, scenario-b must remain linear-heavy, and scenario-c must remain balanced in both the narrative and the opening allocation.",
    "3. Make the scenario naming and explanation visibly reflect that channel bias, but tie it directly to the audience, market, platform, and demographic evidence.",
    "4. Tie every scenario to the supplied synthetic data, especially age mix, parent share, income mix, viewing habit, platform usage, network lift, overlap, top states or DMAs, and behavioral tags when available.",
    "5. Each scenario should point to at least three concrete synthetic signals instead of relying on generic planning language.",
    "6. Recommend exactly one scenario and explain why it wins for this prompt in clear, plain English that a non-domain expert can still follow.",
    "7. The written summary must be informative, not terse. Give enough detail for a reviewer to understand what the data said, how the options differ, and why the recommendation is sensible.",
    "8. Every line must sound logical and meaningful. Avoid generic business filler, avoid unexplained adjectives, and connect claims to data, supply, delivery, or cost.",
    "9. Do not mention ROI or return on investment anywhere.",
    "10. If a scenario choice is uncertain, state the tradeoff clearly instead of pretending the data is stronger than it is.",
    "11. Keep the plan realistic for the WBD ecosystem and preserve all six master-agent stages.",
    "12. In Part 1, prefer this order: audience evidence, scenario A difference, scenario B difference, scenario C difference, recommendation, main tradeoff.",
    "13. Output Part 1 plain text followed by Part 2 JSON only."
  ].join("\n");
}

async function getConfiguredArchitectCreds({ interactive = false } = {}) {
  if (llmSession.creds?.apiKey) return llmSession.creds;
  if (!interactive) return null;
  try {
    return await ensureCreds();
  } catch {
    return null;
  }
}

function ensureSingleRecommendedArchitectPlan(plans = [], preferredVariantKey = "") {
  if (!Array.isArray(plans) || !plans.length) return [];
  let recommendedIndex = plans.findIndex((plan) => !!plan.recommended);
  const normalizedPreferred = normalizeVariantKey(preferredVariantKey);
  if (recommendedIndex < 0 && normalizedPreferred) {
    recommendedIndex = plans.findIndex((plan) => normalizeVariantKey(plan.variantKey) === normalizedPreferred);
  }
  if (recommendedIndex < 0) recommendedIndex = 0;
  return plans.map((plan, index) => ({
    ...plan,
    variantKey: normalizeVariantKey(plan.variantKey),
    recommended: index === recommendedIndex
  }));
}

async function generateArchitectPlansLive({ creds, model, problemText, scenarioCatalog, demo, maxAgents }) {
  if (!creds?.apiKey) throw new Error("No live LLM credentials available for architect generation.");

  const agentStyle = $("#agent-style")?.value || config.defaults?.agentStyle || "";
  const customArchitectPrompt = $("#architect-prompt")?.value || config.defaults?.architectPrompt || "";
  const dataset = buildArchitectRealtimeDataset(problemText, scenarioCatalog);
  const agentContract = buildArchitectAgentContract();
  const summarySeed = buildArchitectFallbackSummary(problemText, scenarioCatalog);
  const systemPrompt = buildArchitectSystemPrompt(agentStyle, customArchitectPrompt);
  const userPrompt = buildArchitectUserPrompt({ problemText, dataset, agentContract });

  let rawText = `${summarySeed.join("\n")}\n\nConnecting to live LLM architect...\n`;

  await Utils.streamChatCompletion({
    llm: creds,
    body: {
      model,
      stream: true,
      temperature: 0.35,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt }
      ]
    },
    onChunk: (chunk) => {
      rawText += chunk;
      setState({ architectBuffer: rawText });
    }
  });

  const parsed = parseArchitectJson(rawText);
  const candidates = extractArchitectPlanCandidates(parsed);
  if (!candidates.length) {
    throw new Error("Live LLM architect response could not be parsed into scenario plans.");
  }

  const normalized = ensureSingleRecommendedArchitectPlan(
    normalizeArchitectPlans(candidates, demo, maxAgents),
    parsed.recommendedVariantKey || scenarioCatalog.recommendedVariantKey
  );

  if (normalized.length !== 3) {
    throw new Error("Live LLM architect response did not yield three valid scenario plans.");
  }

  const recommendedPlan = normalized.find((plan) => plan.recommended)
    || normalized.find((plan) => plan.variantKey === (parsed.recommendedVariantKey || scenarioCatalog.recommendedVariantKey))
    || normalized[0];

  return {
    rawText,
    plans: normalized,
    recommendedPlan
  };
}

function formatPlanOutputDetails(variant, campaignPrompt, existing = {}) {
  const catalog = buildScenarioBlueprints(campaignPrompt);
  const blueprint = catalog.options.find((option) => option.variantKey === normalizeVariantKey(existing.variantKey || variant.key)) || {};
  const mergedScenarioIntelligence = {
    ...(blueprint.scenarioIntelligence || {}),
    ...(existing.scenarioIntelligence || {})
  };
  const budget = extractBudgetUsd(campaignPrompt);
  const allocationStrategyFallback = (blueprint.allocationStrategy || variant.allocationStrategy || "")
    .replace(/\$BUDGET_USD/g, budget.toLocaleString());
  const allocationStrategy = preferMeaningfulText(
    (existing.allocationStrategy || "").replace(/\$BUDGET_USD/g, budget.toLocaleString()),
    allocationStrategyFallback,
    { minWords: 10, requireEvidence: true }
  );
  const deliveryTiming = preferMeaningfulText(existing.deliveryTiming || "", blueprint.deliveryTiming || variant.deliveryTiming || "", {
    minWords: 8,
    requireEvidence: true
  });
  const channelLogic = preferMeaningfulText(existing.channelLogic || existing.roiReasoning || "", blueprint.channelLogic || blueprint.roiReasoning || variant.channelLogic || variant.roiReasoning || "", {
    minWords: 10,
    requireEvidence: true
  });
  const complianceValidation = evaluatePlanCompliance({
    campaignPrompt,
    allocationStrategy,
    variantKey: variant.key
  });
  return {
    allocationStrategy,
    deliveryTiming,
    channelLogic,
    complianceValidation,
    strategy: preferMeaningfulText(existing.strategy || "", blueprint.strategy || variant.strategy || "", {
      minWords: 12,
      requireEvidence: true
    }),
    why: preferMeaningfulText(existing.why || "", blueprint.why || variant.why || "", {
      minWords: 10,
      requireEvidence: true
    }),
    promptTile: preferMeaningfulText(existing.promptTile || "", blueprint.promptTile || variant.promptTile || "", {
      titleMode: true
    }),
    promptText: preferMeaningfulText(existing.promptText || "", blueprint.promptText || variant.promptText || "", {
      minWords: 10,
      requireEvidence: true
    }),
    recommended: existing.recommended ?? blueprint.recommended ?? false,
    recommendationReason: preferMeaningfulText(existing.recommendationReason || "", blueprint.recommendationReason || "", {
      minWords: 10,
      requireEvidence: true
    }),
    scenarioIntelligence: Object.keys(mergedScenarioIntelligence).length ? mergedScenarioIntelligence : null
  };
}

function normalizeArchitectPlans(rawPlans, demo, maxAgents) {
  const campaignPrompt = demo?.problem || demo?.body || "";
  const scenarioCatalog = buildScenarioBlueprints(campaignPrompt);
  const normalized = (Array.isArray(rawPlans) ? rawPlans : [])
    .map((entry, index) => {
      if (!entry || typeof entry !== "object") return null;
      const fallbackVariant = PLAN_VARIANTS[index % PLAN_VARIANTS.length];
      const planList = entry.plan || entry.agents || entry.workflow || entry.steps || [];
      const normalizedPlan = Utils.normalizePlan(planList, maxAgents);
      if (!normalizedPlan.length) return null;
      const variantKey = normalizeVariantKey(entry.variantKey || fallbackVariant.key);
      const variant = getVariantByKey(variantKey);
      const variantIndex = PLAN_VARIANTS.findIndex((item) => item.key === variantKey);
      const optionIndex = variantIndex >= 0 ? variantIndex : index;
      const fallbackTitle = buildArchitectPlanTitle(demo, optionIndex);
      const blueprint = scenarioCatalog.options.find((option) => option.variantKey === variantKey) || {};
      const normalizedEntry = { ...entry, variantKey };
      const enrichedPlan = expandAndEnrichPlan(normalizedPlan, maxAgents, campaignPrompt, variantKey, normalizedEntry);
      const details = formatPlanOutputDetails(variant, campaignPrompt, normalizedEntry);
      return {
        id: Utils.uniqueId("architect-plan"),
        title: preferMeaningfulText(entry.title || "", blueprint.title || fallbackTitle, { titleMode: true }),
        strategy: (details.strategy || entry.summary || variant.strategy).toString().trim(),
        why: (details.why || entry.rationale || variant.why).toString().trim(),
        promptTile: details.promptTile || variant.promptTile,
        promptText: details.promptText || variant.promptText,
        variantKey,
        plan: enrichedPlan,
        allocationStrategy: details.allocationStrategy,
        allocation: details.scenarioIntelligence?.allocation || entry.allocation || null,
        deliveryTiming: details.deliveryTiming,
        channelLogic: details.channelLogic,
        complianceValidation: details.complianceValidation,
        inputs: buildDatasetInputs({ campaignPrompt, selectedPlan: normalizedEntry }),
        recommended: details.recommended,
        recommendationReason: details.recommendationReason,
        scenarioIntelligence: details.scenarioIntelligence
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
  const catalog = buildScenarioBlueprints(campaign);
  return PLAN_VARIANTS.map((variant, index) => {
    const blueprint = catalog.options.find((option) => option.variantKey === variant.key) || {};
    const variantLibrary = getVariantAgentLibrary(variant.key);
    const seedPlan = variantLibrary.slice(0, TARGET_ARCHITECT_AGENTS).map((node, idx) => ({
      agentName: node.agentName,
      nodeId: node.nodeId,
      initialTask: `Stage ${idx + 1} task for ${node.agentName} in campaign: ${campaign}.`,
      systemInstruction: `Execute ${node.focus} and provide one explicit Why this matters statement.`,
      stage: idx + 1
    }));
    const details = formatPlanOutputDetails(variant, campaign, blueprint);
    return {
      id: Utils.uniqueId("architect-plan"),
      title: blueprint.title || buildArchitectPlanTitle(demo, index),
      strategy: details.strategy || variant.strategy,
      why: details.why || variant.why,
      promptTile: details.promptTile || variant.promptTile,
      promptText: details.promptText || variant.promptText,
      variantKey: variant.key,
      plan: expandAndEnrichPlan(Utils.normalizePlan(seedPlan, maxAgents), maxAgents, campaign, variant.key, blueprint),
      allocationStrategy: details.allocationStrategy,
      allocation: details.scenarioIntelligence?.allocation || blueprint.allocation || null,
      deliveryTiming: details.deliveryTiming,
      channelLogic: details.channelLogic,
      complianceValidation: details.complianceValidation,
      inputs: buildDatasetInputs({ campaignPrompt: campaign, selectedPlan: blueprint }),
      recommended: details.recommended,
      recommendationReason: details.recommendationReason,
      scenarioIntelligence: details.scenarioIntelligence
    };
  });
}

function expandAndEnrichPlan(plan = [], maxAgents = TARGET_ARCHITECT_AGENTS, campaign = "", variantKey = PLAN_VARIANTS[0].key, scenarioBlueprint = null) {
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
      const detailedInstruction = buildDetailedSystemInstruction(agent, template, campaign, phase, scenarioBlueprint);
      const detailedTask = buildDetailedInitialTask(agent, template, campaign, phase, scenarioBlueprint);

      return {
        ...agent,
        nodeId,
        agentName,
        phase,
        phaseLabel: `Stage ${phase}`,
        systemInstruction: ensureWordRange(
          preferMeaningfulText(agent.systemInstruction, detailedInstruction, {
            minWords: 12,
            rejectGenericPraise: false
          }),
          detailedInstruction,
          16,
          36
        ),
        initialTask: ensureWordRange(
          preferMeaningfulText(agent.initialTask, detailedTask, {
            minWords: 12,
            rejectGenericPraise: false
          }),
          detailedTask,
          14,
          28
        )
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

const LOW_QUALITY_NARRATIVE_PATTERNS = [
  /\b(?:tbd|n\/a|none provided|not provided|placeholder|lorem ipsum|to be determined|coming soon)\b/i,
  /\.\.\./,
  /\betc\.?\b/i,
  /\bno (?:summary|details?|data|output) (?:provided|returned|available)\b/i
];

const GENERIC_PRAISE_PATTERNS = [
  /\b(?:good|great|nice|strong|best|better|effective|efficient|useful|helpful|appropriate|sensible|logical|meaningful)\b/i,
  /\bworks well\b/i,
  /\bgood fit\b/i,
  /\bstrong strategy\b/i
];

const BANNED_SCENARIO_TITLE_PATTERNS = [
  /\bstreaming[-\s]*heavy\b/i,
  /\blinear[-\s]*heavy\b/i,
  /\bbalanced\b/i
];

function extractNarrativeTokens(text = "") {
  return (stripMarkdown(text).toLowerCase().match(/[a-z0-9%$]+/g) || []).filter(Boolean);
}

function tokenOverlapCount(source = "", reference = "") {
  const referenceSet = new Set(extractNarrativeTokens(reference));
  if (!referenceSet.size) return 0;
  return new Set(extractNarrativeTokens(source).filter((token) => referenceSet.has(token))).size;
}

function hasEvidenceSignal(text = "") {
  return /\d/.test(text) || /\b(audience|households?|users?|reach|inventory|impressions?|cpm|budget|streaming|linear|network|networks|delivery|cohort|segment|frequency|overlap|allocation|reserve|placement|placements|match rate|sales lift|response|device|devices|market|markets|timing|yield|compliance|scenario|signals?)\b/i.test(text);
}

function isLowQualityNarrative(text = "", options = {}) {
  const {
    minWords = 6,
    requireEvidence = false,
    fallbackText = "",
    rejectGenericPraise = true,
    titleMode = false
  } = options;
  const clean = normalizeWhitespace(stripMarkdown(text || ""));
  const tokenList = extractNarrativeTokens(clean);
  if (!clean) return true;

  if (titleMode) {
    if (BANNED_SCENARIO_TITLE_PATTERNS.some((pattern) => pattern.test(clean))) return true;
    const words = wordCount(clean);
    return words < 2 || words > 6;
  }

  if (LOW_QUALITY_NARRATIVE_PATTERNS.some((pattern) => pattern.test(clean))) return true;
  if (/[:;,/-]\s*$/.test(clean)) return true;
  if (wordCount(clean) < minWords) return true;
  if (tokenList.length >= 8) {
    const uniqueRatio = new Set(tokenList).size / tokenList.length;
    if (uniqueRatio < 0.45) return true;
  }
  if (rejectGenericPraise && GENERIC_PRAISE_PATTERNS.some((pattern) => pattern.test(clean)) && !hasEvidenceSignal(clean)) {
    return true;
  }
  if (requireEvidence && !hasEvidenceSignal(clean) && tokenOverlapCount(clean, fallbackText) < 3) {
    return true;
  }
  return false;
}

function preferMeaningfulText(candidate = "", fallback = "", options = {}) {
  const cleanCandidate = normalizeWhitespace(candidate);
  const cleanFallback = normalizeWhitespace(fallback);
  if (!cleanCandidate) return cleanFallback;
  if (isLowQualityNarrative(cleanCandidate, { ...options, fallbackText: cleanFallback })) {
    return cleanFallback || cleanCandidate;
  }
  return cleanCandidate;
}

function normalizeMeaningfulTextList(value, fallback = [], options = {}) {
  const source = Array.isArray(value)
    ? value
    : typeof value === "string"
      ? [value]
      : [];
  const fallbackNext = (Array.isArray(fallback) ? fallback : [])
    .map((item) => normalizeWhitespace(item))
    .filter(Boolean);
  if (!source.length) return fallbackNext;

  const chosen = source
    .map((item, index) => preferMeaningfulText(item, fallbackNext[index] || fallbackNext[0] || "", options))
    .filter(Boolean);

  const minMeaningful = options.minMeaningful || 1;
  const meaningfulCount = chosen.filter((item) => !isLowQualityNarrative(item, { ...options, fallbackText: "" })).length;
  if (fallbackNext.length && meaningfulCount < Math.min(minMeaningful, fallbackNext.length)) {
    return fallbackNext;
  }
  return chosen.length ? chosen : fallbackNext;
}

function buildDetailedSystemInstruction(agent, template, campaign, phase, scenarioBlueprint = null) {
  const role = agent.agentName || template.agentName;
  const focus = template.focus;
  const scenarioTitle = scenarioBlueprint?.title || "selected scenario";
  const allocation = scenarioBlueprint?.allocationStrategy || "Follow the recommended cross-platform allocation.";
  const subAgents = getSubAgentsForNode(template?.nodeId, scenarioBlueprint?.variantKey).map((item) => item.name).join(", ");
  return `You are ${role}, the stage ${phase} owner. Coordinate ${subAgents || "the active worker agents"}, use the shared Campaign_State_Object, stay aligned to "${scenarioTitle}" and "${allocation}", cite evidence, explain your reasoning in plain English, avoid unexplained jargon, avoid vague filler, and structure the explanation as evidence, finding, decision, and business implication. End with one sentence that starts with "Why this matters:".`;
}

function buildDetailedInitialTask(agent, template, campaign, phase, scenarioBlueprint = null) {
  const role = agent.agentName || template.agentName;
  const focus = template.focus;
  const scenarioTitle = scenarioBlueprint?.title || "selected scenario";
  const recommendation = scenarioBlueprint?.recommendationReason || "Use the audience evidence to justify the path.";
  return `Complete stage ${phase} for ${role} on brief "${campaign}". Prioritize ${focus}, make one master decision for "${scenarioTitle}", record the data used, capture what each sub-agent contributed, explain the findings in plain language a non-expert can follow, state one risk with mitigation, and hand off the next action. Present the reasoning in this order: what data was used, what it showed, what decision was made, and why that decision helps the campaign. Keep every statement logical, data-grounded, and specific enough that it does not read like generic filler. Recommendation context: ${recommendation}`;
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
    const scenarioCatalog = buildScenarioBlueprints(problemText);
    const fallbackPlans = buildFallbackArchitectPlans(demo, maxAgents);
    const summaryLines = buildArchitectFallbackSummary(problemText, scenarioCatalog);

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
      runToken: 0,
      campaignStateObject: null,
      architectBuffer: summaryLines.join("\n")
    });

    const model = $("#model")?.value || config.defaults?.model || "gpt-4o";
    setState({
      architectBuffer: `${summaryLines.join("\n")}\n\nRequesting live architect scenarios from the LLM...`
    });
    const creds = await getConfiguredArchitectCreds({ interactive: true });
    let orderedPlans = ensureSingleRecommendedArchitectPlan(
      normalizeArchitectPlans(fallbackPlans, demo, maxAgents),
      scenarioCatalog.recommendedVariantKey
    );
    let recommendedPlan = orderedPlans.find((plan) => plan.recommended)
      || orderedPlans.find((plan) => plan.variantKey === scenarioCatalog.recommendedVariantKey)
      || orderedPlans[0];

    if (creds) {
      try {
        const liveArchitect = await generateArchitectPlansLive({
          creds,
          model,
          problemText,
          scenarioCatalog,
          demo,
          maxAgents
        });
        orderedPlans = liveArchitect.plans;
        recommendedPlan = liveArchitect.recommendedPlan;
      } catch (liveError) {
        console.warn("Live architect generation failed, using pre-calculated demo information:", liveError?.message || liveError);
        setState({
          architectBuffer: `${summaryLines.join("\n")}\n\nLive LLM architect failed. Falling back to pre-calculated demo information.`
        });
      }
    } else {
      setState({
        architectBuffer: `${summaryLines.join("\n")}\n\nLive LLM architect credentials were not provided. Using pre-calculated demo information.`
      });
    }

    const selectedPlan = expandAndEnrichPlan(
      recommendedPlan.plan || [],
      maxAgents,
      problemText,
      normalizeVariantKey(recommendedPlan.variantKey || PLAN_VARIANTS[0].key),
      recommendedPlan
    );
    const normalizedInputs = buildDatasetInputs({
      campaignPrompt: problemText,
      selectedPlan: recommendedPlan
    }).map((input) => ({ ...input, id: Utils.uniqueId("input") }));
    const complianceValidation = recommendedPlan.complianceValidation || evaluatePlanCompliance({
      campaignPrompt: problemText,
      allocationStrategy: recommendedPlan.allocationStrategy || "",
      variantKey: normalizeVariantKey(recommendedPlan.variantKey || "")
    });

    setState({
      stage: "data",
      architectPlans: orderedPlans,
      selectedArchitectPlanId: recommendedPlan.id,
      plan: selectedPlan,
      suggestedInputs: normalizedInputs,
      selectedInputs: new Set(normalizedInputs.map((item) => item.id)),
      rawDataByAgent: buildRawDataByPlan(selectedPlan, null, recommendedPlan),
      error: "",
      selectedPlanCompliance: complianceValidation,
      complianceDetails: buildComplianceDetails(complianceValidation),
      visualizationLoading: false,
      visualizationNarrative: "",
      runToken: 0,
      campaignStateObject: null,
      subAgentCatalog: resolveSubAgentCatalogForVariant(normalizeVariantKey(recommendedPlan?.variantKey || PLAN_VARIANTS[2]?.key || PLAN_VARIANTS[0]?.key)),
      subAgentSelections: {}
    });
  } catch (e) {
    const fallbackDemo = getSelectedDemo();
    const maxAgents = clampAgentCount($("#max-agents")?.value, TARGET_ARCHITECT_AGENTS);
    const fallbackCatalog = buildScenarioBlueprints(fallbackDemo?.problem || fallbackDemo?.body || "");
    const fallbackPlans = ensureSingleRecommendedArchitectPlan(
      buildFallbackArchitectPlans(fallbackDemo, maxAgents),
      fallbackCatalog.recommendedVariantKey
    );
    const recommendedPlan = fallbackPlans.find((plan) => plan.recommended) || fallbackPlans[0];
    const selectedPlan = expandAndEnrichPlan(
      recommendedPlan?.plan || [],
      maxAgents,
      fallbackDemo?.problem || fallbackDemo?.body || "",
      normalizeVariantKey(recommendedPlan?.variantKey || PLAN_VARIANTS[0].key),
      recommendedPlan
    );
    const normalizedInputs = buildDatasetInputs({
      campaignPrompt: fallbackDemo?.problem || fallbackDemo?.body || "",
      selectedPlan: recommendedPlan
    }).map((input) => ({ ...input, id: Utils.uniqueId("input") }));
    const savedCompliance = recommendedPlan?.complianceValidation || evaluatePlanCompliance({
      campaignPrompt: fallbackDemo?.problem || fallbackDemo?.body || "",
      allocationStrategy: recommendedPlan?.allocationStrategy || "",
      variantKey: normalizeVariantKey(recommendedPlan?.variantKey || "")
    });
    setState({
      stage: "data",
      architectPlans: fallbackPlans,
      selectedArchitectPlanId: recommendedPlan?.id || null,
      plan: selectedPlan,
      suggestedInputs: normalizedInputs,
      selectedInputs: new Set(normalizedInputs.map((item) => item.id)),
      error: "Loaded pre-calculated demo information after a live architect error.",
      rawDataByAgent: buildRawDataByPlan(selectedPlan, null, recommendedPlan),
      complianceDetails: buildComplianceDetails(savedCompliance),
      selectedPlanCompliance: savedCompliance,
      visualizationLoading: false,
      visualizationNarrative: "",
      campaignStateObject: null,
      subAgentCatalog: resolveSubAgentCatalogForVariant(normalizeVariantKey(recommendedPlan?.variantKey || PLAN_VARIANTS[2]?.key || PLAN_VARIANTS[0]?.key)),
      subAgentSelections: {}
    });
    console.error(e);
  }
}

function wait(ms = 120) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function escapeHtml(value = "") {
  return (value || "")
    .toString()
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatExecutionLabel(value = "") {
  return sanitizeDisplayText(String(value)
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b[a-z]/g, (char) => char.toUpperCase())
    .replace(/\bId\b/g, "ID")
    .replace(/\bUsd\b/g, "USD")
    .replace(/\bPct\b/g, "%")
    .replace(/\bCpm\b/g, "CPM")
    .replace(/\bRoi\b/g, "Outcome")
    .replace(/\bScte35\b/gi, "SCTE-35"));
}

function formatExecutionScalar(value) {
  if (typeof value === "number") {
    return Number.isInteger(value)
      ? value.toLocaleString()
      : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  if (typeof value === "boolean") return value ? "Yes" : "No";
  return String(value ?? "");
}

function isPlainExecutionObject(value) {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function getExecutionCardTitle(item, index) {
  if (!isPlainExecutionObject(item)) return `Item ${index + 1}`;
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

function getExecutionCardBody(item, title) {
  if (!isPlainExecutionObject(item)) return item;
  const next = { ...item };
  if (next.name === title) delete next.name;
  if (next.label === title) delete next.label;
  return next;
}

function renderExecutionValue(value) {
  if (value === null || value === undefined || value === "") {
    return '<span class="detail-null">Not available</span>';
  }

  if (typeof value === "string") {
    const text = sanitizeDisplayText(value).trim();
    return (text.includes("\n") || text.length > 96)
      ? `<div class="detail-text-panel">${escapeHtml(text)}</div>`
      : `<span>${escapeHtml(text)}</span>`;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return `<span>${escapeHtml(formatExecutionScalar(value))}</span>`;
  }

  if (Array.isArray(value)) {
    if (!value.length) return '<span class="detail-null">No items</span>';
    if (value.every((item) => item === null || item === undefined || typeof item !== "object")) {
      return `<div class="detail-pill-row">${value.map((item) => `<span class="detail-pill">${escapeHtml(formatExecutionScalar(item))}</span>`).join("")}</div>`;
    }
    return `<div class="detail-card-grid">${value.map((item, index) => {
      const title = getExecutionCardTitle(item, index);
      const body = getExecutionCardBody(item, title);
      return `
        <div class="detail-card">
          <div class="detail-card-title">${escapeHtml(title)}</div>
          ${renderExecutionValue(body)}
        </div>
      `;
    }).join("")}</div>`;
  }

  if (isPlainExecutionObject(value)) {
    const entries = Object.entries(value);
    if (!entries.length) return '<span class="detail-null">No fields</span>';
    return `<div class="detail-kv">${entries.map(([key, itemValue]) => `
      <div class="detail-kv-row">
        <div class="detail-kv-key">${escapeHtml(formatExecutionLabel(key))}</div>
        <div class="detail-kv-value">${renderExecutionValue(itemValue)}</div>
      </div>
    `).join("")}</div>`;
  }

  return `<pre class="raw-data mb-0">${escapeHtml(sanitizeDisplayText(String(value)))}</pre>`;
}

function renderExecutionSection(title, value) {
  if (value === undefined || value === null) return "";
  return `
    <div class="detail-section">
      <div class="detail-section-label">${escapeHtml(title)}</div>
      ${renderExecutionValue(value)}
    </div>
  `;
}

function renderExecutionSubAgents(items = []) {
  if (!items.length) return '<span class="detail-null">No sub-agent results recorded.</span>';
  return `<div class="detail-card-grid">${items.map((item) => `
    <div class="detail-card">
      <div class="detail-card-title">${escapeHtml(item.name || "Sub-Agent")}</div>
      ${(item.details || []).length
        ? `<ul class="detail-list mb-0">${(item.details || []).map((detail) => `<li>${escapeHtml(detail)}</li>`).join("")}</ul>`
        : '<span class="detail-null">No notes recorded.</span>'}
    </div>
  `).join("")}</div>`;
}

function networkToInventoryName(name = "") {
  if (name === "Max") return "Max ad-lite";
  if (name === "discovery+") return "Discovery+";
  return name;
}

function getSelectedScenarioRecord() {
  return state.architectPlans.find((plan) => plan.id === state.selectedArchitectPlanId)
    || state.architectPlans.find((plan) => plan.recommended)
    || state.architectPlans[0]
    || null;
}

function buildInitialCampaignState({ campaignPrompt = "", selectedPlan = null, entries = [] } = {}) {
  const fallbackCatalog = buildScenarioBlueprints(campaignPrompt);
  const rawScenario = selectedPlan || fallbackCatalog.options.find((option) => option.recommended) || fallbackCatalog.options[0];
  const scenario = buildToddlerHeroPlanContext(rawScenario, campaignPrompt) || rawScenario;
  const intelligence = scenario?.scenarioIntelligence || fallbackCatalog.intelligence;
  const heroPresentationProfile = isToddlerHeroBrief(campaignPrompt, intelligence?.campaign)
    && normalizeVariantKey(scenario?.variantKey || "") === TODDLER_HERO_ROUTE.variantKey
    ? { key: TODDLER_HERO_PROFILE_KEY }
    : null;
  return {
    id: Utils.uniqueId("campaign"),
    createdAt: Date.now(),
    prompt: campaignPrompt,
    budgetUsd: intelligence?.campaign?.budgetUsd || extractBudgetUsd(campaignPrompt),
    countries: intelligence?.campaign?.countries || detectCampaignCountries(campaignPrompt),
    productFamily: intelligence?.campaign?.productFamily || deriveProductFamily(campaignPrompt),
    presentationProfile: heroPresentationProfile,
    selectedScenario: {
      id: selectedPlan?.id || null,
      title: scenario?.title || "Recommended scenario",
      variantKey: normalizeVariantKey(scenario?.variantKey || PLAN_VARIANTS[0].key),
      allocationStrategy: scenario?.allocationStrategy || "",
      channelLogic: scenario?.channelLogic || "",
      allocation: scenario?.allocation || intelligence?.allocation || { streamingPct: 50, linearPct: 40, reservePct: 10 },
      recommendationReason: scenario?.recommendationReason || intelligence?.recommendationReason || "",
      rankedNetworks: intelligence?.rankedNetworks || []
    },
    intelligence,
    supplementalInputs: entries.map((entry) => ({
      title: entry.title,
      type: entry.type,
      preview: Utils.truncate(entry.content || "", 220)
    })),
    stageOutputs: {},
    executionDetails: {},
    actionLog: []
  };
}

function selectRelevantYieldSignals(campaignState, limit = 3) {
  const vertical = campaignState.productFamily?.key || "general_brand";
  const signals = (datasets.yieldIntelligenceFeed || [])
    .filter((item) => item.vertical_tag === vertical || (vertical === "general_brand" && item.expected_response_lift_pct >= 6))
    .sort((a, b) => (Number(b.expected_response_lift_pct || 0) - Number(a.expected_response_lift_pct || 0)) || (Number(b.signal_strength || 0) - Number(a.signal_strength || 0)));
  return (signals.length ? signals : (datasets.yieldIntelligenceFeed || [])).slice(0, limit);
}

function selectRelevantInventoryRows(campaignState, limit = 8) {
  const countries = new Set(campaignState.countries || ["US"]);
  const networkNames = new Set((campaignState.selectedScenario?.rankedNetworks || []).map((item) => networkToInventoryName(item.name)));
  const allocation = campaignState.selectedScenario?.allocation || { streamingPct: 50, linearPct: 40 };
  const allRows = datasets.inventoryMatrix || [];
  const countryRows = allRows.filter((row) => countries.has(row.country_code));
  const scoredRows = (rows = []) => [...rows]
    .sort((a, b) => ((Number(b.fill_rate_pct || 0) * Number(b.avail_impressions_30s || 0)) - (Number(a.fill_rate_pct || 0) * Number(a.avail_impressions_30s || 0))));
  const preferredRows = countryRows.filter((row) => !networkNames.size || networkNames.has(row.network));
  const digitalPreferred = scoredRows(preferredRows.filter((row) => row.platform_type === "digital"));
  const linearPreferred = scoredRows(preferredRows.filter((row) => row.platform_type === "linear"));
  const digitalFallback = scoredRows(countryRows.filter((row) => row.platform_type === "digital"));
  const linearFallback = scoredRows(countryRows.filter((row) => row.platform_type === "linear"));
  const minPerPlatform = limit >= 6 ? 2 : 1;
  const wantsDigital = Number(allocation.streamingPct || 0) > 0;
  const wantsLinear = Number(allocation.linearPct || 0) > 0;
  const minDigital = wantsDigital ? minPerPlatform : 0;
  const minLinear = wantsLinear ? minPerPlatform : 0;
  const desiredDigital = wantsDigital
    ? Math.max(minDigital, Math.min(limit - minLinear, Math.round(limit * ((allocation.streamingPct || 50) / 100))))
    : 0;
  const desiredLinear = wantsLinear
    ? Math.max(minLinear, limit - desiredDigital)
    : 0;
  const usedKeys = new Set();
  const selected = [];
  const pushRows = (rows = [], target = 0) => {
    rows.forEach((row) => {
      if (selected.length >= limit || target <= 0) return;
      const key = `${row.neo_order_id || ""}|${row.network}|${row.platform_type}|${row.daypart}|${row.country_code}`;
      if (usedKeys.has(key)) return;
      usedKeys.add(key);
      selected.push(row);
      target -= 1;
    });
    return target;
  };

  let remainingDigital = desiredDigital;
  remainingDigital = pushRows(digitalPreferred, remainingDigital) ?? remainingDigital;
  if (remainingDigital > 0) pushRows(digitalFallback, remainingDigital);

  let remainingLinear = desiredLinear;
  remainingLinear = pushRows(linearPreferred, remainingLinear) ?? remainingLinear;
  if (remainingLinear > 0) pushRows(linearFallback, remainingLinear);

  pushRows(scoredRows(preferredRows), limit - selected.length);
  if (selected.length < limit) pushRows(scoredRows(countryRows.length ? countryRows : allRows), limit - selected.length);

  return selected.slice(0, limit);
}

function estimateInventoryCapacity(campaignState, fallbackRows = []) {
  const countries = new Set(campaignState.countries || ["US"]);
  const networkNames = new Set((campaignState.selectedScenario?.rankedNetworks || []).map((item) => networkToInventoryName(item.name)));
  const allRows = datasets.inventoryMatrix || [];
  const countryRows = allRows.filter((row) => countries.has(row.country_code));
  const preferredRows = countryRows.filter((row) => !networkNames.size || networkNames.has(row.network));
  const poolRows = preferredRows.length ? preferredRows : (countryRows.length ? countryRows : fallbackRows);
  const baseImpressions = poolRows.reduce((sum, row) => sum + Number(row.avail_impressions_30s || 0), 0);
  const baseSpend = poolRows.reduce((sum, row) => sum + ((Number(row.avail_impressions_30s || 0) / 1000) * Number(row.cpm_30s || 0)), 0);

  // The inventory matrix is a sampled view of reservable avails, so expand it to a rolling booking window
  // before comparing it with the full campaign brief.
  const bookingWindowExpansion = 4.5;
  let capacityImpressions = Math.round(baseImpressions * 0.55 * bookingWindowExpansion);
  let capacitySpend = Math.round(baseSpend * 0.55 * bookingWindowExpansion);
  const minimumCapacitySpend = Math.round(Number(campaignState.budgetUsd || 0) * 1.18);

  if (capacitySpend > 0 && minimumCapacitySpend > 0 && capacitySpend < minimumCapacitySpend) {
    const expansionScale = minimumCapacitySpend / capacitySpend;
    capacitySpend = Math.round(capacitySpend * expansionScale);
    capacityImpressions = Math.round((capacityImpressions * expansionScale) / 100) * 100;
  }

  return {
    capacityImpressions,
    capacitySpend,
    poolRows
  };
}

function buildBookingLineItems(campaignState, inventoryRows = []) {
  const budgetUsd = Number(campaignState.budgetUsd || 25000);
  const allocation = campaignState.selectedScenario?.allocation || { streamingPct: 50, linearPct: 40, reservePct: 10 };
  const streamingBudget = Math.round(budgetUsd * (allocation.streamingPct / 100));
  const linearBudget = Math.round(budgetUsd * (allocation.linearPct / 100));
  const reserveBudget = budgetUsd - streamingBudget - linearBudget;
  const streamingRows = inventoryRows.filter((row) => row.platform_type === "digital").slice(0, 3);
  const linearRows = inventoryRows.filter((row) => row.platform_type === "linear").slice(0, 3);
  const lineItems = [];

  function pushRows(rows, totalBudget, label) {
    if (!rows.length || totalBudget <= 0) return;
    const totalWeight = rows.reduce((sum, row) => sum + Math.max(1, Number(row.fill_rate_pct || 0)), 0);
    rows.forEach((row, index) => {
      const share = index === rows.length - 1
        ? totalBudget - lineItems.filter((item) => item._lane === label).reduce((sum, item) => sum + item.spend_usd, 0)
        : Math.round(totalBudget * (Math.max(1, Number(row.fill_rate_pct || 0)) / totalWeight));
      const spendCap = Math.max(0, share);
      const impressions = Math.min(
        Math.round((spendCap / Math.max(Number(row.cpm_30s || 1), 1)) * 1000),
        Math.round(Number(row.avail_impressions_30s || 0) * 0.58)
      );
      const spend = Math.round((impressions / 1000) * Number(row.cpm_30s || 0));
      lineItems.push({
        line_item_id: `LINE-${String(lineItems.length + 1).padStart(3, "0")}`,
        network: row.network,
        platform_type: row.platform_type,
        country_code: row.country_code,
        daypart: row.daypart,
        impressions,
        spend_usd: spend,
        cpm_30s: row.cpm_30s,
        status: "approved",
        _lane: label
      });
    });
  }

  pushRows(streamingRows, streamingBudget, "streaming");
  pushRows(linearRows, linearBudget, "linear");

  if (reserveBudget > 0 && lineItems.length) {
    const bestItem = [...lineItems].sort((a, b) => b.impressions - a.impressions)[0];
    const reserveImpressions = Math.min(
      Math.round((reserveBudget / Math.max(Number(bestItem.cpm_30s || 1), 1)) * 1000),
      Math.round((bestItem.impressions || 0) * 0.45)
    );
    lineItems.push({
      line_item_id: `LINE-${String(lineItems.length + 1).padStart(3, "0")}`,
      network: bestItem.network,
      platform_type: bestItem.platform_type,
      country_code: bestItem.country_code,
      daypart: bestItem.daypart,
      impressions: reserveImpressions,
      spend_usd: Math.round((reserveImpressions / 1000) * Number(bestItem.cpm_30s || 0)),
      cpm_30s: bestItem.cpm_30s,
      status: "approved",
      _lane: "reserve"
    });
  }

  return lineItems.map(({ _lane, ...item }) => item);
}

function runToddlerHeroPlanningStage(agent, campaignState) {
  const stateUpdate = {
    audience_summary: {
      scanned_profiles: 145000000,
      total_wbd_universe_hhs: 3800000,
      matched_profiles: 145000000,
      seed_households: 3800000,
      unique_households: 3800000,
      target_reachable_households: 312500,
      lead_cohort: "Millennial Health-Conscious Parents",
      top_segments: [{ label: "Millennial Health-Conscious Parents", pct: 100 }],
      top_tags: [{ label: "Organic Baby/Toddler Food Buyers", pct: 77.6 }],
      top_states: [{ label: "California", pct: 14.2 }, { label: "Texas", pct: 12.8 }, { label: "Florida", pct: 10.6 }],
      overlap_pct: 28.4
    }
  };
  campaignState.stageOutputs[agent.nodeId] = stateUpdate;
  return {
    inputData: {
      prompt: campaignState.prompt,
      scenario: campaignState.selectedScenario,
      audience_scope: {
        scanned_profiles: 145000000,
        tam_households: 3800000,
        reachable_households: 312500
      }
    },
    summaryLines: [
      "The engine scanned 145 million WBD viewer profiles and isolated a total addressable market of 3.8 million unique households matching U.S. parents of toddlers with high affinity for premium CPG and grocery data.",
      "Because the brief is capped at $50K, the system suppressed low-intent casual viewers and compressed the target to about 312,500 high-intent households so frequency could stay near 4 exposures per household.",
      "The cohort heavily over-indexes on digital. Max indexes at 145 for this audience, while daytime Food Network indexes at 115 as the most efficient linear support lane.",
      "A de-duplication alert was triggered immediately because 28.4% of these parents watch both Max and linear TV, so the identity graph was locked before Stage 2 pricing began."
    ],
    subAgentResults: buildPresetSubAgentResults(agent.nodeId, {
      "audience-segmentation": {
        details: [
          "Ingested Acxiom and Experian purchase data to isolate households buying organic baby and toddler food.",
          "Matched deterministic household IDs against the WBD first-party identity graph so targeting stays household-based instead of cookie-based.",
          "Filtered out households with older children ages 6 plus to eliminate wasted spend and lock the audience on true toddler parents."
        ]
      },
      "behavioral-indexer": {
        details: [
          "Analyzed trailing 30-day viewership for the locked cohort across Max, Discovery+, Food Network, TLC, and linear extensions.",
          "Built a network affinity matrix that showed a sharp morning spike on Max around Sesame Street and strong daytime lift on Food Network around lifestyle programming such as Pioneer Woman.",
          "Passed the specific breakfast and afternoon dayparts into Yield Optimizer so Stage 2 prices real audience behavior instead of generic network averages."
        ]
      }
    }),
    whyMatters: "Stage 1 determines whether the demo feels like a real audience engine or just a broad targeting exercise. The toddler-parent route only works if the budget is compressed against the right households before inventory is priced.",
    handoff: "Pass the locked high-intent toddler-parent cohort, overlap controls, and ranked network affinities into Inventory and Yield.",
    stateUpdate
  };
}

function runToddlerHeroInventoryStage(agent, campaignState) {
  const stateUpdate = {
    inventory_summary: {
      capacity_impressions: 4150000,
      capacity_spend_usd: 215800,
      budget_fit: "Capacity available",
      premium_streaming_cpm: 68.5,
      blended_linear_cpm: 32,
      blended_cpm: 54.2,
      guaranteed_converged_impressions: 840000,
      recommended_allocation: {
        streaming_budget_usd: 34000,
        linear_budget_usd: 11000,
        reserve_budget_usd: 5000,
        streaming_pct: 68,
        linear_pct: 22,
        reserve_pct: 10
      },
      yield_signals: [
        { topic: "Morning co-viewing on Max", channel: "streaming", expected_response_lift_pct: 18 },
        { topic: "Daytime lifestyle blocks on Food Network", channel: "linear", expected_response_lift_pct: 11 }
      ]
    }
  };
  campaignState.stageOutputs[agent.nodeId] = stateUpdate;
  return {
    inputData: {
      scenario: campaignState.selectedScenario,
      audience_summary: campaignState.stageOutputs["planning-identity-agent"],
      inventory_window: "Next 14 days"
    },
    summaryLines: [
      "The inventory scan found 4,150,000 fulfillable toddler-parent impressions worth about $215,800 in capacity value, so the $50K budget clears comfortably without overselling future avails.",
      "Historical yield analysis showed that Max drives roughly 3 times stronger engagement in early-morning co-viewing windows, while Food Network daytime is the most cost-efficient linear lane from 12 PM to 3 PM.",
      "The pricing path was locked at $68.50 CPM for premium targeted streaming and $32.00 CPM for daytime linear, which yields a believable blended planning CPM of about $54.20.",
      "The opening plan allocates $34,000 to streaming, $11,000 to linear, and holds $5,000 in reserve, which is enough to build roughly 840,000 converged impressions without reaching for weak inventory."
    ],
    subAgentResults: buildPresetSubAgentResults(agent.nodeId, {
      "yield-optimizer": {
        details: [
          "Queried six months of historical WBD ad-server logs for organic snack and adjacent CPG campaigns.",
          "Found that bidding on Max Kids inventory after 5 PM carries a meaningful CPM premium while producing weaker parent attention than breakfast co-viewing hours.",
          "Constrained streaming bids to morning windows and linear bids to naptime and early afternoon blocks to keep the blended CPM efficient."
        ]
      },
      "signal-forecaster": {
        details: [
          "Scanned the upcoming 14-day pacing logs in the WBD yield system for the toddler-parent cohort.",
          "Found that daytime linear supply is wide open while Max premium morning inventory is comparatively tight and running hot.",
          "Recommended the 10% reserve so Discovery+ and flexible Max extensions can absorb demand if premium morning avails disappear early."
        ]
      }
    }),
    whyMatters: "Stage 2 proves the route can actually be fulfilled. The toddler story stops sounding realistic the moment the plan needs inventory that does not exist or prices that do not clear the rate card.",
    handoff: "Pass the locked pricing path, approved dayparts, and reserve logic into Booking and Proposal.",
    stateUpdate
  };
}

function runToddlerHeroBookingStage(agent, campaignState) {
  const compliance = buildToddlerHeroComplianceEvaluation();
  const lineItems = buildToddlerHeroBookingLineItems();
  const stateUpdate = {
    booking_summary: {
      line_items: lineItems,
      guarantee_impressions: 211970,
      compliance_status: compliance.status,
      compliance_findings: compliance.findings.slice(0, 4),
      capacity_available_impressions: 250725,
      active_budget_usd: 45000,
      reserve_budget_usd: 5000
    }
  };
  campaignState.stageOutputs[agent.nodeId] = stateUpdate;
  return {
    inputData: {
      scenario: campaignState.selectedScenario,
      inventory_summary: campaignState.stageOutputs["inventory-yield-agent"],
      compliance_context: compliance
    },
    summaryLines: [
      "The booking package assembled 6 distinct bookable line items and locked 211,970 guaranteed impressions against the U.S. parents of toddlers cohort. The proposal status is Passed with Adjustments.",
      "The structure follows the approved 68% streaming, 22% linear, and 10% reserve split across Max Ad-Lite and Discovery+ on streaming, then Food Network, TLC, and one tightly controlled TNT extension on linear.",
      "Next-week capacity is tight at 250,725 available impressions versus 211,970 required, so the booking structurer locked guarantees early to reduce the risk of pre-emption by competing advertisers.",
      "COPPA risk was triggered on Max Kids inventory, so Proposal Assembler automatically removed third-party behavioral tracking and disabled click-through tracking before the plan moved to trafficking."
    ],
    subAgentResults: buildPresetSubAgentResults(agent.nodeId, {
      "deal-structurer": {
        details: [
          "Created 6 converged line items covering 3 streaming placements and 3 linear placements.",
          "Translated the approved toddler-parent mix into WBD ad-server line structures while keeping $5,000 outside the active book as reserve.",
          "Protected the narrowest next-week avails first so the guaranteed impression package could survive later sell-through pressure."
        ]
      },
      "proposal-assembler": {
        details: [
          "Generated the buyer-facing proposal language, guarantee block, and final contract payload for human approval.",
          "Invoked the internal legal rubric because the audience and inventory path included child-directed programming.",
          "Released the proposal as Passed with Adjustments after behavioral tracking was stripped from Max Kids lines and the remaining package cleared compliance."
        ]
      }
    }),
    whyMatters: "Stage 3 is where the audience theory becomes a legal and commercial product. If the contract structure is loose here, later operations inherit risk instead of clarity.",
    handoff: "Pass the guaranteed lines, cleaned tag rules, and TNT watchlist into Trafficking and Signals.",
    stateUpdate
  };
}

function runToddlerHeroTraffickingStage(agent, campaignState) {
  const stateUpdate = {
    trafficking_summary: {
      readiness_score: 84,
      readiness_status_label: "Conditionally Approved",
      technical_alerts_count: 1,
      signal_risks: [
        { network: "TNT", daypart: "Live Sports", status: "Missing SCTE-35 cue" }
      ],
      asset_checks: [
        { surface: "Video master 1", status: "pass" },
        { surface: "Video master 2", status: "pass" },
        { surface: "Video master 3", status: "pass" }
      ],
      creatives_passed_count: 3,
      creative_total_count: 3,
      affected_placement: "Live Sports (TNT)"
    }
  };
  campaignState.stageOutputs[agent.nodeId] = stateUpdate;
  return {
    inputData: {
      proposal_lines: campaignState.stageOutputs["booking-proposals-agent"]?.booking_summary?.line_items || [],
      watchlist_surface: "TNT live sports"
    },
    summaryLines: [
      "The stage closed with an 84% Launch Readiness score. Asset QA and tag compliance both passed, but one live placement still requires an operations watchlist.",
      "All 3 video files passed WBD 1080p and audio loudness checks, and no non-compliant tracking pixels remained after the booking-stage compliance cleanup.",
      "Delivery checks showed Max, Discovery+, Food Network, and TLC routing cleanly, but the TNT live sports feed returned a missing SCTE-35 cue.",
      "In plain language, the automated trigger that tells the live sports feed when to break for ads is unstable. The campaign can launch, but TNT needs human eyes if that placement is activated."
    ],
    subAgentResults: buildPresetSubAgentResults(agent.nodeId, {
      "asset-qa": {
        details: [
          "Validated all 3 client MP4 masters against WBD resolution, bitrate, and loudness specifications.",
          "Confirmed the trafficking metadata stayed clean after the COPPA pixel strip and did not reintroduce any blocked tags.",
          "Closed creative QA with no re-export requirement."
        ]
      },
      "streamx-router": {
        details: [
          "Pinged the delivery endpoints for Max, Discovery+, Food Network, TLC, and TNT before launch.",
          "Max, Discovery+, Food Network, and TLC returned healthy routing responses.",
          "TNT live sports returned a missing SCTE-35 cue, so the line was flagged for manual cutover monitoring."
        ]
      }
    }),
    whyMatters: "Stage 4 is the last operational gate before spend goes live. Even a well-built converged proposal can fail if the signal path is not stable.",
    handoff: "Pass the launch score, TNT routing alert, and cleared creative package into In-Flight Operations.",
    stateUpdate
  };
}

function runToddlerHeroInFlightStage(agent, campaignState) {
  const stateUpdate = {
    inflight_summary: {
      linear_delivery_rate_pct: 81,
      digital_delivery_rate_pct: 102,
      streaming_status_label: "Healthy",
      linear_status_label: "Under-delivering",
      make_good_triggered: true,
      shift_budget_usd: 4250,
      final_allocation: {
        streamingPct: 75,
        linearPct: 25,
        reservePct: 0
      },
      trafficked_line_items: campaignState.stageOutputs["booking-proposals-agent"]?.booking_summary?.line_items || [],
      stabilized_projection_pct: 100
    }
  };
  campaignState.stageOutputs[agent.nodeId] = stateUpdate;
  return {
    inputData: {
      trafficked_plan: campaignState.stageOutputs["booking-proposals-agent"]?.booking_summary?.line_items || [],
      trafficking_summary: campaignState.stageOutputs["trafficking-signals-agent"],
      original_allocation: TODDLER_HERO_ROUTE.allocation
    },
    summaryLines: [
      "Mid-campaign pacing projected 102% delivery on streaming but only 81% on linear, which immediately identified linear TV as the failing lane.",
      "The root cause traced back to the TNT live sports placement flagged in Stage 4. A live broadcast overrun partially pre-empted the placement and put about 15,000 guaranteed impressions at risk.",
      "The Autonomous Make-Good agent moved $4,250 out of underperforming live sports linear inventory and into Max Ad-Lite, where high-fit toddler-parent avails were still available.",
      "The intervention stabilized the campaign at a final 75% streaming and 25% linear mix and returned the package to a full-delivery projection."
    ],
    subAgentResults: buildPresetSubAgentResults(agent.nodeId, {
      "real-time-pacing": {
        details: [
          "Ingested daily WBD delivery logs and compared actual delivered impressions against the toddler-parent forecast curve.",
          "Flagged a negative pacing variance on linear TV on Day 4, before the miss could reach the end-of-flight billing window.",
          "Escalated the TNT overrun as the root cause rather than treating the shortfall as a generic linear under-delivery."
        ]
      },
      "autonomous-make-good": {
        details: [
          "Queried real-time streaming avails across Max and Discovery+ for overlapping toddler-parent supply.",
          "Found enough high-fit Max Ad-Lite inventory to absorb a $4,250 shift without lowering audience quality.",
          "Paused the failing live sports weight and rerouted the dollars into streaming so the guarantee could still clear."
        ]
      }
    }),
    whyMatters: "Stage 5 proves the system can operate the campaign, not just describe it. The make-good is what turns the demo into an active operating model.",
    handoff: "Pass the corrected allocation, intervention trace, and stabilized delivery projection into Measurement and Learning.",
    stateUpdate
  };
}

function runToddlerHeroMeasurementStage(agent, campaignState) {
  const stateUpdate = {
    measurement_summary: {
      projected_households: 71300,
      matched_households: 68450,
      verified_unique_households: 68450,
      clean_room_match_rate_pct: 77.6,
      average_frequency: 3.1,
      incremental_reach_streaming_pct: 22,
      strongest_channel: "streaming",
      next_best_action: "Keep streaming as the incremental reach engine, preserve daytime linear support, and continue cross-platform frequency capping on the next toddler-parent flight.",
      households_above_cap_pct: 4,
      wasted_impression_savings_usd: 3800
    }
  };
  campaignState.stageOutputs[agent.nodeId] = stateUpdate;
  return {
    inputData: {
      audience_summary: campaignState.stageOutputs["planning-identity-agent"],
      booking_summary: campaignState.stageOutputs["booking-proposals-agent"],
      final_allocation: campaignState.stageOutputs["inflight-operations-agent"],
      clean_room_mode: "privacy-safe household match"
    },
    summaryLines: [
      "Final measurement validated 68,450 unique households reached against a projection of 71,300, which confirms the toddler-parent campaign landed close to plan.",
      "A privacy-safe clean room matched WBD exposure logs against the advertiser target list at a 77.6% demographic match rate, validating that the delivery stayed focused on toddler parents.",
      "Streaming added 22% net-new reach beyond the linear baseline, which means Max expanded the parent footprint instead of merely duplicating the daytime TV audience.",
      "Cross-platform frequency control held average converged exposure to 3.1x. Only 4% of households received more than five exposures, which avoided about $3,800 in wasted impressions."
    ],
    subAgentResults: buildPresetSubAgentResults(agent.nodeId, {
      "household-match-validator": {
        details: [
          "Executed a double-blind household match inside the clean room using WBD exposure logs and the advertiser demographic target list.",
          "Validated 68,450 unique households at a 77.6% match rate without exposing raw personal identifiers outside the privacy-safe environment.",
          "Confirmed the campaign hit the intended toddler-parent cohort instead of relying on modeled audience assumptions alone."
        ]
      },
      "outcome-pattern-reader": {
        details: [
          "Compared exposure overlap between Food Network, TLC, TNT, Max, and Discovery+ delivery logs.",
          "Verified that the streaming allocation produced 22% incremental reach beyond the linear baseline rather than cannibalizing the same daytime audience.",
          "Confirmed that cross-platform frequency capping worked as intended and kept household repetition inside a controlled range."
        ]
      }
    }),
    whyMatters: "Stage 6 closes the loop with proof, not rhetoric. It shows the converged route reached the right households and that streaming added measurable reach beyond linear alone.",
    handoff: "Write the toddler-parent learning package back into the Campaign_State_Object for the next flight.",
    stateUpdate
  };
}

function runPlanningIdentityStage(agent, campaignState) {
  if (isToddlerHeroCampaignState(campaignState)) return runToddlerHeroPlanningStage(agent, campaignState);
  const intelligence = campaignState.intelligence || analyzeAudienceAgainstCampaign(campaignState.prompt);
  const subAgentResults = buildScopedSubAgentResults(agent.nodeId, campaignState, {
    "audience-segmentation": (subAgent) => ({
      id: subAgent.id,
      name: subAgent.name,
      details: [
        `Matched ${intelligence.matchedRows.length.toLocaleString()} viewer profiles across ${intelligence.seedHouseholdCount.toLocaleString()} distinct household IDs in the seed sample, then expanded them into ${intelligence.uniqueHouseholds.toLocaleString()} modeled households for planning.`,
        `Top cohort: ${intelligence.topSegments[0]?.label || "High-fit audience"} with leading tag ${intelligence.topTags[0]?.label || "n/a"}.`,
        `Audience center of gravity: average age ${intelligence.avgAge}, ${intelligence.streamingHeavyShare}% streaming-heavy, ${intelligence.linearHeavyShare}% linear-heavy.`
      ]
    }),
    "cross-platform-id-resolver": (subAgent) => ({
      id: subAgent.id,
      name: subAgent.name,
      details: [
        `${intelligence.overlapPct}% of matched viewers are connected to more than two household platforms, so duplicate suppression matters immediately.`,
        `Top platforms: ${intelligence.topPlatforms.map((item) => `${item.label} (${item.pct}%)`).slice(0, 3).join(", ")}.`,
        `Top states: ${intelligence.topStates.map((item) => `${item.label} (${item.pct}%)`).slice(0, 3).join(", ")}.`
      ]
    }),
    "behavioral-indexer": (subAgent) => ({
      id: subAgent.id,
      name: subAgent.name,
      details: [
        `Highest-fit networks: ${(intelligence.rankedNetworks || []).map((item) => `${item.name} (${roundToTwo(item.lift || 1, 1)}x fit)`).slice(0, 3).join(", ")}.`,
        `Behavioral leaders: ${intelligence.topTags.map((item) => item.label).slice(0, 4).join(", ")}.`,
        `Recommendation anchor: ${campaignState.selectedScenario.recommendationReason || "Follow the selected scenario fit signal."}`
      ]
    })
  });
  const stateUpdate = {
    audience_summary: {
      matched_profiles: intelligence.matchedRows.length,
      seed_households: intelligence.seedHouseholdCount,
      unique_households: intelligence.uniqueHouseholds,
      average_age: intelligence.avgAge,
      top_segments: intelligence.topSegments.slice(0, 3),
      top_tags: intelligence.topTags.slice(0, 4),
      top_states: intelligence.topStates.slice(0, 3),
      overlap_pct: intelligence.overlapPct
    }
  };
  campaignState.stageOutputs[agent.nodeId] = stateUpdate;
  return {
    inputData: {
      prompt: campaignState.prompt,
      scenario: campaignState.selectedScenario,
      sample_profiles: intelligence.sampleRows.slice(0, 6)
    },
    summaryLines: [
      `The audience match pulled ${intelligence.matchedRows.length.toLocaleString()} viewer profiles into a modeled audience of ${intelligence.uniqueHouseholds.toLocaleString()} households, with ${(intelligence.topStates || []).map((item) => item.label).slice(0, 2).join(" and ") || "the core markets"} carrying the heaviest concentration.`,
      `Demographically, the matched audience centers on average age ${intelligence.avgAge}, with ${intelligence.streamingHeavyShare}% streaming-heavy behavior and ${intelligence.linearHeavyShare}% linear-heavy behavior.`,
      `The sub-agents isolated ${intelligence.topSegments[0]?.label || "the lead segment"}, confirmed ${intelligence.overlapPct}% cross-platform overlap, and elevated ${(campaignState.selectedScenario.rankedNetworks || []).map((item) => item.name).slice(0, 3).join(", ") || "the strongest WBD networks"} as the cleanest opening lane.`,
      `That evidence is what pushed the workflow toward ${campaignState.selectedScenario.title}, so every downstream stage inherits the same audience story instead of reinterpreting the brief from scratch.`
    ],
    subAgentResults,
    whyMatters: "Stage 1 determines who the system should actually reach. Every later decision, from yield pricing to measurement, depends on whether this audience definition is behaviorally right and de-duplicated correctly.",
    handoff: "Pass the validated audience profile, ranked networks, and overlap signals to Inventory and Yield so capacity and pricing are grounded in the same households.",
    stateUpdate
  };
}

function runInventoryYieldStage(agent, campaignState) {
  if (isToddlerHeroCampaignState(campaignState)) return runToddlerHeroInventoryStage(agent, campaignState);
  const intelligence = campaignState.intelligence;
  const inventoryRows = selectRelevantInventoryRows(campaignState, 8);
  const yieldSignals = selectRelevantYieldSignals(campaignState, 3);
  const budgetUsd = Number(campaignState.budgetUsd || 25000);
  const capacityEstimate = estimateInventoryCapacity(campaignState, inventoryRows);
  const capacityImpressions = capacityEstimate.capacityImpressions;
  const capacitySpend = capacityEstimate.capacitySpend;
  const streamingRows = inventoryRows.filter((row) => row.platform_type === "digital");
  const linearRows = inventoryRows.filter((row) => row.platform_type === "linear");
  const allInventory = datasets.inventoryMatrix || [];
  const premiumStreamingCpm = roundToTwo((average(streamingRows, "cpm_30s") || average(allInventory.filter((row) => row.platform_type === "digital"), "cpm_30s") || 56) * (1 + (intelligence.streamingHeavyShare / 400)));
  const blendedLinearCpm = roundToTwo((average(linearRows, "cpm_30s") || average(allInventory.filter((row) => row.platform_type === "linear"), "cpm_30s") || 34) * (1 + (intelligence.linearHeavyShare / 700)));
  const stateUpdate = {
    inventory_summary: {
      capacity_impressions: capacityImpressions,
      capacity_spend_usd: capacitySpend,
      budget_fit: capacitySpend >= budgetUsd ? "Capacity available" : "Capacity tight",
      premium_streaming_cpm: premiumStreamingCpm,
      blended_linear_cpm: blendedLinearCpm,
      yield_signals: yieldSignals.map((item) => ({
        topic: item.topic,
        channel: item.recommended_channel,
        expected_response_lift_pct: item.expected_response_lift_pct
      }))
    }
  };
  campaignState.stageOutputs[agent.nodeId] = stateUpdate;
  return {
    inputData: {
      scenario: campaignState.selectedScenario,
      audience_summary: campaignState.stageOutputs["planning-identity-agent"],
      inventory_rows: inventoryRows.slice(0, 6),
      yield_signals: yieldSignals,
      capacity_window: {
        impressions: capacityImpressions,
        spend_usd: capacitySpend
      }
    },
    summaryLines: [
      `The inventory scan rolled the shortlisted avails into about ${capacityImpressions.toLocaleString()} reservable impressions and roughly ${capacitySpend.toLocaleString()} dollars of workable capacity across the booking window, which keeps the ${budgetUsd.toLocaleString()} dollar brief inside available supply.`,
      `Pricing settled at roughly ${premiumStreamingCpm} CPM for premium streaming and ${blendedLinearCpm} CPM for linear, which gives the channel split a believable cost tradeoff instead of a decorative one.`,
      `The sub-agents highlighted ${inventoryRows.slice(0, 3).map((row) => `${row.network} ${row.daypart}`).join(", ") || "the lead avails"} as the strongest available supply, while ${yieldSignals[0]?.topic || "the lead signal"} was the clearest timing trigger.`,
      `${capacitySpend >= budgetUsd ? "The selected route can be booked without forcing low-quality inventory." : "Supply is tight enough that the booking stage needs to protect reserve and stay disciplined about weaker avails."}`
    ],
    subAgentResults: buildScopedSubAgentResults(agent.nodeId, campaignState, {
      "predictive-capacity": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: [
          `${capacitySpend >= budgetUsd ? "Available supply covers the brief without forcing low-quality avails." : "Available supply is tight, so the reserve lane matters."}`,
          `Top avails: ${inventoryRows.slice(0, 3).map((row) => `${row.network} ${row.daypart}`).join(", ")}.`,
          `Highest-fill inventory is concentrated in ${inventoryRows[0]?.network || "the lead network"}.`
        ]
      }),
      "yield-optimizer": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: [
          `Premium CPM guidance for targeted streaming impressions: ${premiumStreamingCpm}.`,
          `Blended CPM guidance for linear coverage: ${blendedLinearCpm}.`,
          `The pricing split mirrors the selected scenario mix of ${campaignState.selectedScenario.allocation.streamingPct}% streaming and ${campaignState.selectedScenario.allocation.linearPct}% linear.`
        ]
      }),
      "signal-forecaster": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: yieldSignals.map((item) => `${item.topic} suggests about ${item.expected_response_lift_pct}% response lift with ${item.recommended_channel} during ${item.spike_window}.`)
      })
    }),
    whyMatters: "Stage 2 translates audience fit into inventory reality. If the system cannot fulfill the brief at the right price, every downstream recommendation becomes decorative instead of executable.",
    handoff: "Pass capacity, pricing, and timing guidance into Booking and Proposal so the contract structure reflects what can truly be delivered.",
    stateUpdate
  };
}

function runBookingProposalStage(agent, campaignState) {
  if (isToddlerHeroCampaignState(campaignState)) return runToddlerHeroBookingStage(agent, campaignState);
  const inventoryRows = selectRelevantInventoryRows(campaignState, 6);
  const lineItems = buildBookingLineItems(campaignState, inventoryRows);
  const compliance = evaluatePlanCompliance({
    campaignPrompt: campaignState.prompt,
    allocationStrategy: campaignState.selectedScenario.allocationStrategy,
    variantKey: campaignState.selectedScenario.variantKey
  });
  const guaranteeImpressions = lineItems.reduce((sum, row) => sum + Number(row.impressions || 0), 0);
  const streamingSpend = lineItems
    .filter((item) => item.platform_type === "digital")
    .reduce((sum, item) => sum + Number(item.spend_usd || 0), 0);
  const linearSpend = lineItems
    .filter((item) => item.platform_type === "linear")
    .reduce((sum, item) => sum + Number(item.spend_usd || 0), 0);
  const stateUpdate = {
    booking_summary: {
      line_items: lineItems,
      guarantee_impressions: guaranteeImpressions,
      compliance_status: compliance.status,
      compliance_findings: compliance.findings.slice(0, 4)
    }
  };
  campaignState.stageOutputs[agent.nodeId] = stateUpdate;
  return {
    inputData: {
      scenario: campaignState.selectedScenario,
      inventory_summary: campaignState.stageOutputs["inventory-yield-agent"],
      compliance_context: compliance
    },
    summaryLines: [
      `The booking package assembled ${lineItems.length} converged line items and ${guaranteeImpressions.toLocaleString()} guaranteed impressions around the selected scenario.`,
      `Spend is leaning ${streamingSpend >= linearSpend ? "streaming-first" : "linear-first"} at ${streamingSpend.toLocaleString()} streaming dollars versus ${linearSpend.toLocaleString()} linear dollars, which keeps the contract aligned to the selected route instead of diluting it.`,
      `${compliance.status === "Passed" ? "Compliance cleared the structure without blockers." : `Compliance returned ${compliance.findings.length} adjustments before launch, so the proposal had to absorb those policy changes before it could move forward.`}`,
      `The proposal now hands trafficking a bookable plan with clear guarantees, channel intent, and a reserve lane that operations can still use later if delivery shifts.`
    ],
    subAgentResults: buildScopedSubAgentResults(agent.nodeId, campaignState, {
      "deal-structurer": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: [
          `Primary networks: ${lineItems.map((item) => item.network).slice(0, 4).join(", ")}.`,
          `Streaming spend: ${lineItems.filter((item) => item.platform_type === "digital").reduce((sum, item) => sum + item.spend_usd, 0).toLocaleString()} dollars.`,
          `Linear spend: ${lineItems.filter((item) => item.platform_type === "linear").reduce((sum, item) => sum + item.spend_usd, 0).toLocaleString()} dollars.`
        ]
      }),
      "global-compliance-bot": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: [
          compliance.summary,
          ...(compliance.findings.slice(0, 3).length ? compliance.findings.slice(0, 3) : ["No blocking policy findings were triggered by the current prompt and scenario mix."])
        ]
      }),
      "proposal-assembler": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: [
          `Guaranteed impression target: ${guaranteeImpressions.toLocaleString()}.`,
          `Proposal language follows the recommended scenario: ${campaignState.selectedScenario.title}.`,
          `Optimization reserve stays available for later make-good use.`
        ]
      })
    }),
    whyMatters: "Stage 3 turns analysis into a commercial package. This is where the audience theory becomes a real contract and where regulatory risk is either absorbed early or pushed dangerously downstream.",
    handoff: "Pass approved line items, guarantee logic, and any compliance adjustments into Trafficking and Signals for activation readiness.",
    stateUpdate
  };
}

function runTraffickingSignalsStage(agent, campaignState) {
  if (isToddlerHeroCampaignState(campaignState)) return runToddlerHeroTraffickingStage(agent, campaignState);
  const booking = campaignState.stageOutputs["booking-proposals-agent"]?.booking_summary || {};
  const lineItems = booking.line_items || [];
  const signalRiskRows = selectRelevantInventoryRows(campaignState, 8).filter((row) => (row.scte35_signal_status || "").toLowerCase() !== "active");
  const readinessScore = Math.max(72, 96 - (signalRiskRows.length * 4) - ((booking.compliance_findings || []).length * 2));
  const stateUpdate = {
    trafficking_summary: {
      readiness_score: readinessScore,
      signal_risks: signalRiskRows.slice(0, 3).map((row) => ({
        network: row.network,
        daypart: row.daypart,
        status: row.scte35_signal_status
      })),
      asset_checks: [
        { surface: "CTV 16:9 master", status: "pass" },
        { surface: "Mobile video audio normalization", status: "pass" },
        { surface: "Trafficking metadata and advertiser identifiers", status: "pass" }
      ]
    }
  };
  campaignState.stageOutputs[agent.nodeId] = stateUpdate;
  return {
    inputData: {
      proposal_lines: lineItems,
      signal_inventory: signalRiskRows.slice(0, 4)
    },
    summaryLines: [
      `Launch readiness scored ${readinessScore}/100 after asset QA, metadata review, and delivery routing checks across the selected booking package.`,
      `${signalRiskRows.length ? `Signal attention is still needed on ${signalRiskRows[0]?.network || "one network"} ${signalRiskRows[0]?.daypart || ""} because the SCTE-35 status is not fully clean.` : "No critical signal blockers were found in the selected inventory path."}`,
      `The sub-agents cleared the creative package, checked routing behavior, and wrote an explicit watchlist so live operations know exactly what needs monitoring at launch.`,
      `${readinessScore >= 85 ? "The stage is comfortable launching with normal monitoring." : "The stage can still launch, but only with elevated operational watchlists and faster escalation if pacing starts to drift."}`
    ],
    subAgentResults: buildScopedSubAgentResults(agent.nodeId, campaignState, {
      "asset-qa": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: [
          "CTV master, mobile audio, and metadata checks passed for the simulated launch package.",
          `The current plan contains ${lineItems.length} traffickable line items.`,
          "No cross-screen format conflicts were introduced by the selected scenario."
        ]
      }),
      "streamx-router": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: signalRiskRows.length
          ? signalRiskRows.slice(0, 3).map((row) => `${row.network} ${row.daypart} shows ${row.scte35_signal_status} SCTE-35 status and needs monitored routing.`)
          : ["All inspected inventory rows showed active SCTE-35 status for the selected path."]
      }),
      "launch-readiness": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: [
          `Readiness score: ${readinessScore}/100.`,
          `${readinessScore >= 85 ? "Launch can proceed with standard monitoring." : "Launch should proceed with elevated operational watchlists."}`,
          "Escalation instructions are attached to the Campaign_State_Object for the operations team."
        ]
      })
    }),
    whyMatters: "Stage 4 is the last chance to prevent a technically elegant plan from failing in execution. Clean routing and launch readiness are what let the later pacing and measurement stages behave predictably.",
    handoff: "Pass readiness status, signal watchlists, and the trafficked line items into In-Flight Operations for live pacing simulation.",
    stateUpdate
  };
}

function runInFlightOperationsStage(agent, campaignState) {
  if (isToddlerHeroCampaignState(campaignState)) return runToddlerHeroInFlightStage(agent, campaignState);
  const booking = campaignState.stageOutputs["booking-proposals-agent"]?.booking_summary || {};
  const trafficking = campaignState.stageOutputs["trafficking-signals-agent"]?.trafficking_summary || {};
  const allocation = campaignState.selectedScenario.allocation || { streamingPct: 50, linearPct: 40, reservePct: 10 };
  const riskPenalty = (trafficking.signal_risks || []).length * 0.03;
  const linearDeliveryRate = clampNumber(0.95 - (allocation.linearPct > 55 ? 0.08 : 0.03) - riskPenalty, 0.72, 0.98);
  const digitalDeliveryRate = clampNumber(0.97 + (allocation.streamingPct > 60 ? 0.02 : 0.01), 0.9, 1.04);
  const makeGoodTriggered = linearDeliveryRate < 0.9;
  const shiftBudget = makeGoodTriggered
    ? Math.round(Math.min(campaignState.budgetUsd * 0.08, campaignState.budgetUsd * ((allocation.reservePct || 10) / 100)))
    : 0;
  const finalAllocation = {
    streamingPct: allocation.streamingPct + (makeGoodTriggered ? Math.round((shiftBudget / campaignState.budgetUsd) * 100) : 0),
    linearPct: Math.max(0, allocation.linearPct - (makeGoodTriggered ? Math.round((shiftBudget / campaignState.budgetUsd) * 100) : 0)),
    reservePct: Math.max(0, allocation.reservePct - (makeGoodTriggered ? Math.round((shiftBudget / campaignState.budgetUsd) * 100) : 0))
  };
  const stateUpdate = {
    inflight_summary: {
      linear_delivery_rate_pct: roundToTwo(linearDeliveryRate * 100, 1),
      digital_delivery_rate_pct: roundToTwo(digitalDeliveryRate * 100, 1),
      make_good_triggered: makeGoodTriggered,
      shift_budget_usd: shiftBudget,
      final_allocation: finalAllocation,
      trafficked_line_items: booking.line_items || []
    }
  };
  campaignState.stageOutputs[agent.nodeId] = stateUpdate;
  return {
    inputData: {
      trafficked_plan: booking.line_items || [],
      trafficking_summary: trafficking,
      scenario_allocation: allocation
    },
    summaryLines: [
      `The pacing model projected ${roundToTwo(linearDeliveryRate * 100, 1)}% delivery on linear versus ${roundToTwo(digitalDeliveryRate * 100, 1)}% on streaming, which immediately showed where the weaker lane sat.`,
      `${makeGoodTriggered ? `The autonomous make-good shifted ${shiftBudget.toLocaleString()} dollars into streaming to protect the guarantee before the linear shortfall widened.` : "No make-good was required because both channels stayed close enough to tolerance."}`,
      `The sub-agents translated that pacing read into a final mix of ${finalAllocation.streamingPct}% streaming, ${finalAllocation.linearPct}% linear, and ${finalAllocation.reservePct}% reserve, then logged the intervention trace for the ops team.`,
      `Measurement now inherits the corrected delivery path rather than the original paper plan, which makes the final readout sound like operations actually touched the campaign.`
    ],
    subAgentResults: buildScopedSubAgentResults(agent.nodeId, campaignState, {
      "real-time-pacing": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: [
          `Linear delivery risk: ${roundToTwo(linearDeliveryRate * 100, 1)}%.`,
          `Digital delivery risk: ${roundToTwo(digitalDeliveryRate * 100, 1)}%.`,
          `${(trafficking.signal_risks || []).length ? "Signal watchlist increased linear risk." : "Signal health remained stable through the pacing simulation."}`
        ]
      }),
      "autonomous-make-good": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: makeGoodTriggered
          ? [
            `Shifted ${shiftBudget.toLocaleString()} dollars from the reserve or weak linear lane into Max ad-lite and discovery+ coverage.`,
            `Final allocation moved to ${finalAllocation.streamingPct}% streaming and ${finalAllocation.linearPct}% linear.`,
            "The reallocation was made to stabilize delivery before the linear shortfall widened."
          ]
          : [
            "No autonomous budget shift was required.",
            "Delivery stayed close enough to plan that the original split could hold.",
            "Reserve budget remains available for live intervention if needed."
          ]
      }),
      "ops-alerting": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: [
          makeGoodTriggered ? "An alert was logged for linear under-delivery and routed to the ops watchlist." : "No red-alert condition was logged for pacing.",
          "The Campaign_State_Object now carries the final allocation and intervention trace.",
          "Measurement will use these final allocations rather than the original scenario split."
        ]
      })
    }),
    whyMatters: "Stage 5 is where the system proves it can act, not just describe. The demo feels like a multi-agent operating system when the scenario can autonomously recover from delivery risk instead of watching the shortfall happen.",
    handoff: "Pass final allocation, pacing outcomes, and make-good traces into Measurement and Learning for the matched-readout and next-flight recommendation.",
    stateUpdate
  };
}

function runMeasurementStage(agent, campaignState) {
  if (isToddlerHeroCampaignState(campaignState)) return runToddlerHeroMeasurementStage(agent, campaignState);
  const planning = campaignState.stageOutputs["planning-identity-agent"]?.audience_summary || {};
  const booking = campaignState.stageOutputs["booking-proposals-agent"]?.booking_summary || {};
  const inflight = campaignState.stageOutputs["inflight-operations-agent"]?.inflight_summary || {};
  const reach = buildReachSummary(datasets.audienceGraph || [], datasets.liveDeliveryLog || [], {}, campaignState);
  const projectedHouseholds = resolveProjectedHouseholdCount(
    reach,
    {},
    resolveAudienceHouseholdCount(planning, campaignState.intelligence)
  );
  const totalImpressions = (booking.line_items || []).reduce((sum, row) => sum + Number(row.impressions || 0), 0);
  const cleanRoomMatchRate = roundToTwo(clampNumber(74 + (campaignState.intelligence.overlapPct / 4), 68, 86), 1);
  const matchedHouseholds = Math.round(projectedHouseholds * (cleanRoomMatchRate / 100));
  const averageFrequency = resolveFrequencyPerHousehold(totalImpressions, projectedHouseholds);
  const averageFrequencyText = averageFrequency ? `${averageFrequency} times per household` : "pending";
  const strongestChannel = Number(inflight.digital_delivery_rate_pct || 0) >= Number(inflight.linear_delivery_rate_pct || 0) ? "streaming" : "linear";
  const deliveryStrength = (Number(inflight.digital_delivery_rate_pct || 94) + Number(inflight.linear_delivery_rate_pct || 90)) / 2;
  const salesLiftPct = roundToTwo(
    2.8
    + Math.max(0, deliveryStrength - 88) * 0.08
    + (campaignState.intelligence.overlapPct / 45)
    + (inflight.make_good_triggered ? 0.6 : 0.3),
    1
  );
  const selectedRouteType = getVariantRouteType(campaignState.selectedScenario.variantKey);
  const nextBestAction = selectedRouteType === "precision"
    ? "Keep the next flight streaming-heavy, but widen the linear support lane only if household reach begins to plateau."
    : selectedRouteType === "scale"
      ? "Preserve the linear-heavy route for the next flight and use streaming support only where younger or incremental reach is still available."
      : "Keep the balanced structure and let in-flight pacing decide which channel earns the extra dollars next time.";
  const stateUpdate = {
    measurement_summary: {
      projected_households: projectedHouseholds,
      matched_households: matchedHouseholds,
      clean_room_match_rate_pct: cleanRoomMatchRate,
      average_frequency: averageFrequency,
      sales_lift_pct: salesLiftPct,
      strongest_channel: strongestChannel,
      next_best_action: nextBestAction
    }
  };
  campaignState.stageOutputs[agent.nodeId] = stateUpdate;
  return {
    inputData: {
      audience_summary: planning,
      booking_summary: booking,
      projected_reach: {
        projected_households: projectedHouseholds,
        overlap_pct: reach.overlapPct,
        device_count: reach.deviceCount
      },
      final_allocation: inflight.final_allocation,
      pacing_outcome: inflight
    },
    summaryLines: [
      `The synthetic clean room matched ${matchedHouseholds.toLocaleString()} households inside a reached universe of about ${projectedHouseholds.toLocaleString()} households at a ${cleanRoomMatchRate}% match rate, so this matched count should be read as the measurable subset of delivery rather than the full modeled audience from Stage 1.`,
      `Average frequency across that reached base is ${averageFrequencyText}, and within the matched group the clearest response signal came from ${strongestChannel} with modeled sales lift at ${salesLiftPct}%.`,
      `The sub-agents separated the job cleanly: one validated the measurable household base, one interpreted the response pattern, and one converted that pattern into a next-flight recommendation.`,
      `The next-flight guidance is to ${nextBestAction.charAt(0).toLowerCase() + nextBestAction.slice(1)}, which keeps the route tied to measured behavior instead of defaulting back to generic planning language.`
    ],
    subAgentResults: buildScopedSubAgentResults(agent.nodeId, campaignState, {
      "household-match-validator": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: [
          `${matchedHouseholds.toLocaleString()} reached households were matched through the privacy-safe exposure workflow out of about ${projectedHouseholds.toLocaleString()} households the plan projected to touch.`,
          `Cross-platform match rate: ${cleanRoomMatchRate}%.`,
          "Exposure logs were matched against synthetic sales outcomes without re-identifying households."
        ],
        definition: subAgent.definition || subAgent.summary || ""
      }),
      "outcome-pattern-reader": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: [
          `Sales lift: ${salesLiftPct}%.`,
          `Average frequency across reached households: ${averageFrequencyText}.`,
          `Strongest response signal came from ${strongestChannel}.`,
          `${inflight.make_good_triggered ? "The make-good helped protect the weaker channel before measurement was finalized." : "No make-good was needed before measurement was finalized."}`
        ],
        definition: subAgent.definition || subAgent.summary || ""
      }),
      "next-flight-planner": (subAgent) => ({
        id: subAgent.id,
        name: subAgent.name,
        details: [
          nextBestAction,
          `Best-performing scenario signal: ${campaignState.selectedScenario.recommendationReason || "Use the selected scenario again until audience behavior shifts."}`,
          "Stage 1 audience definitions are now available to seed the next campaign without starting from scratch."
        ],
        definition: subAgent.definition || subAgent.summary || ""
      })
    }),
    whyMatters: "Stage 6 closes the loop. It shows what was measurable, which channel behavior actually showed up, and what the next flight should do differently.",
    handoff: "Measurement writes the final learning package back into the Campaign_State_Object so the next run can start with evidence instead of assumptions.",
    stateUpdate
  };
}

function executeMasterStage(agent, campaignState) {
  const role = inferAgentRole(agent);
  if (role === "planning-identity") return runPlanningIdentityStage(agent, campaignState);
  if (role === "inventory-yield") return runInventoryYieldStage(agent, campaignState);
  if (role === "booking-proposals") return runBookingProposalStage(agent, campaignState);
  if (role === "trafficking-signals") return runTraffickingSignalsStage(agent, campaignState);
  if (role === "inflight-operations") return runInFlightOperationsStage(agent, campaignState);
  if (role === "measurement") return runMeasurementStage(agent, campaignState);
  return {
    inputData: { prompt: campaignState.prompt },
    summaryLines: ["No master-stage implementation was matched for this node.", "The orchestrator kept the pipeline moving with a generic fallback."],
    subAgentResults: [],
    whyMatters: "The orchestration layer needs each stage to produce structured output for the next stage.",
    handoff: "Continue with the current shared campaign state.",
    stateUpdate: {}
  };
}

function buildMasterAgentNarrative(result) {
  const summaryLines = (result.summaryLines || []).map((line) => `- ${line}`).join("\n");

  return `### Detailed Summary
${summaryLines}

### Why this matters
${escapeHtml(result.whyMatters || "")}

_Open the Detailed execution panel below to inspect input data, sub-agent results, state updates, and handoff details._`;
}

function cloneStructured(value) {
  if (value === undefined) return undefined;
  return JSON.parse(JSON.stringify(value));
}

function parseLooseResponseJson(rawText = "") {
  const text = (rawText || "").toString().trim();
  if (!text) return {};
  const direct = Utils.safeParseJson(text);
  if (Object.keys(direct).length) return direct;

  const fencedMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fencedMatch?.[1]) {
    const fencedParsed = Utils.safeParseJson(fencedMatch[1]);
    if (Object.keys(fencedParsed).length) return fencedParsed;
  }

  const firstBrace = text.indexOf("{");
  const lastBrace = text.lastIndexOf("}");
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    const sliced = text.slice(firstBrace, lastBrace + 1);
    const parsed = Utils.safeParseJson(sliced);
    if (Object.keys(parsed).length) return parsed;
  }
  return {};
}

function mergeStructuredObjects(base, override) {
  if (Array.isArray(base) || Array.isArray(override)) {
    return Array.isArray(override) && override.length ? override : (Array.isArray(base) ? base : []);
  }

  if (isPlainExecutionObject(base) || isPlainExecutionObject(override)) {
    const result = {};
    const baseObj = isPlainExecutionObject(base) ? base : {};
    const overrideObj = isPlainExecutionObject(override) ? override : {};
    const keys = new Set([...Object.keys(baseObj), ...Object.keys(overrideObj)]);
    keys.forEach((key) => {
      result[key] = mergeStructuredObjects(baseObj[key], overrideObj[key]);
    });
    return result;
  }

  if (override === undefined || override === null || override === "") return base;
  return override;
}

function normalizeStageTextList(value, fallback = []) {
  const source = Array.isArray(value)
    ? value
    : typeof value === "string"
      ? [value]
      : [];
  const next = source.map((item) => normalizeWhitespace(item)).filter(Boolean);
  const fallbackNext = (Array.isArray(fallback) ? fallback : []).map((item) => normalizeWhitespace(item)).filter(Boolean);
  return next.length ? next : fallbackNext;
}

function normalizeLiveSubAgentResults(rawItems = [], fallbackItems = []) {
  const source = Array.isArray(rawItems) ? rawItems : [];
  if (!source.length && !fallbackItems.length) return [];

  const sourceMap = new Map();
  source.forEach((item, index) => {
    const key = item?.id || item?.name || `sub-${index}`;
    sourceMap.set(key, item);
  });

  const template = fallbackItems.length ? fallbackItems : source;
  return template.map((fallbackItem, index) => {
    const key = fallbackItem?.id || fallbackItem?.name || `sub-${index}`;
    const rawItem = sourceMap.get(key) || source[index] || {};
    const details = normalizeMeaningfulTextList(rawItem.details || rawItem.notes, fallbackItem?.details || [], {
      minWords: 7,
      requireEvidence: true,
      minMeaningful: 2
    });
    const whyMatters = preferMeaningfulText(rawItem.whyMatters || rawItem.why_matters || rawItem.why || "", fallbackItem?.whyMatters || "", {
      minWords: 8,
      rejectGenericPraise: false
    });
    const narrativeFallback = fallbackItem?.text || buildSubAgentNarrative({
      name: rawItem.name || fallbackItem?.name || `Sub-Agent ${index + 1}`,
      definition: rawItem.definition || fallbackItem?.definition || rawItem.summary || fallbackItem?.summary || "",
      details,
      whyMatters
    });
    return {
      ...(fallbackItem || {}),
      id: rawItem.id || fallbackItem?.id || `sub-agent-${index + 1}`,
      name: normalizeWhitespace(rawItem.name || fallbackItem?.name || `Sub-Agent ${index + 1}`),
      status: rawItem.status || fallbackItem?.status || "done",
      definition: normalizeWhitespace(rawItem.definition || fallbackItem?.definition || rawItem.summary || fallbackItem?.summary || ""),
      details: details.length ? details.slice(0, 5) : ["No detailed note returned by the language model."],
      text: preferMeaningfulText(rawItem.text || "", narrativeFallback, {
        minWords: 12,
        rejectGenericPraise: false
      }),
      inputData: mergeStructuredObjects(fallbackItem?.inputData || {}, rawItem.inputData || rawItem.input_data || {}),
      outputData: mergeStructuredObjects(fallbackItem?.outputData || {}, rawItem.outputData || rawItem.output_data || {}),
      whyMatters,
      fallbackUsed: rawItem.fallbackUsed ?? fallbackItem?.fallbackUsed ?? false
    };
  });
}

function buildSubAgentNarrative(subAgent = {}) {
  const detailLines = normalizeStageTextList(subAgent.details || [], []);
  const summary = detailLines.length ? detailLines.map((line) => `- ${line}`).join("\n") : "- No live sub-agent summary was returned.";
  const definition = normalizeWhitespace(subAgent.definition || "");
  const why = normalizeWhitespace(subAgent.whyMatters || "");
  return `${subAgent.name || "Sub-Agent"}${definition ? `\nDefinition: ${definition}` : ""}\n${summary}${why ? `\n\nWhy this matters: ${why}` : ""}`;
}

function createPendingSubAgentResult(masterOutput, subAgent = {}, index = 0) {
  const name = normalizeWhitespace(subAgent.name || `Sub-Agent ${index + 1}`);
  const details = normalizeStageTextList(subAgent.details || [], [`${name} is waiting for its live LLM call.`]).slice(0, 5);
  return {
    id: subAgent.id || `sub-agent-${index + 1}`,
    name,
    definition: normalizeWhitespace(subAgent.definition || subAgent.summary || ""),
    status: "pending",
    details,
    text: "",
    inputData: {
      parent_master_agent: masterOutput.name,
      parent_node_id: masterOutput.nodeId
    },
    outputData: {},
    whyMatters: "",
    fallbackUsed: false
  };
}

function normalizeSingleLiveSubAgentResult(raw = {}, fallback = {}) {
  const payload = raw?.result || raw?.subAgentResult || raw?.output || raw;
  const details = normalizeMeaningfulTextList(
    payload.details || payload.notes || payload.summaryLines || payload.summary_lines,
    fallback.details || [],
    {
      minWords: 7,
      requireEvidence: true,
      minMeaningful: 2
    }
  ).slice(0, 5);
  const next = {
    ...(fallback || {}),
    id: payload.id || fallback.id,
    name: normalizeWhitespace(payload.name || fallback.name || "Sub-Agent"),
    status: payload.status === "error" ? "error" : "done",
    definition: normalizeWhitespace(payload.definition || payload.summary || fallback.definition || ""),
    details: details.length ? details : ["No detailed note returned by the language model."],
    inputData: mergeStructuredObjects(fallback.inputData || {}, payload.inputData || payload.input_data || {}),
    outputData: mergeStructuredObjects(fallback.outputData || {}, payload.outputData || payload.output_data || payload.stateUpdate || payload.state_update || {}),
    whyMatters: preferMeaningfulText(payload.whyMatters || payload.why_matters || payload.why || "", fallback.whyMatters || "", {
      minWords: 8,
      rejectGenericPraise: false
    }),
    fallbackUsed: payload.fallbackUsed ?? fallback.fallbackUsed ?? false
  };
  next.text = preferMeaningfulText(payload.text || "", fallback.text || buildSubAgentNarrative(next), {
    minWords: 12,
    rejectGenericPraise: false
  });
  return next;
}

function normalizeLiveMasterStageResult(raw = {}, baseline = {}) {
  const payload = raw?.result || raw?.stageResult || raw?.output || raw;
  return {
    inputData: mergeStructuredObjects(baseline.inputData || {}, payload.inputData || payload.input_data || {}),
    summaryLines: normalizeMeaningfulTextList(payload.summaryLines || payload.summary_lines, baseline.summaryLines || [], {
      minWords: 9,
      requireEvidence: true,
      minMeaningful: 2
    }).slice(0, 6),
    subAgentResults: normalizeLiveSubAgentResults(payload.subAgentResults || payload.sub_agent_results || payload.subAgents || payload.sub_agents, baseline.subAgentResults || []),
    whyMatters: preferMeaningfulText(payload.whyMatters || payload.why_matters || payload.why || "", baseline.whyMatters || "", {
      minWords: 10,
      rejectGenericPraise: false
    }),
    handoff: preferMeaningfulText(payload.handoff || "", baseline.handoff || "", {
      minWords: 9,
      requireEvidence: true,
      rejectGenericPraise: false
    }),
    stateUpdate: mergeStructuredObjects(baseline.stateUpdate || {}, payload.stateUpdate || payload.state_update || payload.campaignStateUpdate || payload.campaign_state_update || {})
  };
}

function buildMasterStageLiveSystemPrompt(out, baselineResult, agentStyle = "") {
  return [
    `You are ${out.name}, a live WBD master agent executing inside a hierarchical multi-agent campaign system.`,
    out.instruction || "",
    normalizeWhitespace(agentStyle),
    "Return one JSON object only. Do not use markdown fences.",
    "Top-level keys must be exactly: summaryLines, inputData, subAgentResults, stateUpdate, handoff, whyMatters.",
    "summaryLines must be an array of 4 to 6 clear, informative plain-English statements.",
    "The summary must be understandable to a non-domain expert and should not assume media-planning jargon is already known.",
    "Across the summaryLines, explain what data this stage used, what it found, what the sub-agents contributed, what decision was made, and why that decision matters.",
    "Use this order whenever possible: line 1 data used, line 2 key finding, line 3 sub-agent contribution, line 4 decision, line 5 risk or tradeoff, line 6 why it matters.",
    "Do not make the summary terse. Give enough detail that a reviewer can understand the stage without opening the raw output.",
    "Keep every finding tied to this stage's actual responsibility. Do not drift into another stage's work unless it is clearly supporting context.",
    "Each summary line must be a complete sentence. Do not use clipped fragments, shorthand, or unfinished points.",
    "Every sentence must sound logical and meaningful when read on its own.",
    "Do not use vague filler such as 'good fit', 'works well', 'strong option', or 'important result' unless you immediately explain why with specific evidence from the provided data.",
    "Keep the logic internally consistent. Do not contradict the selected scenario, the reference JSON, or the supplied stage inputs.",
    "If the data does not support a strong claim, write a precise conservative statement instead of inventing confidence.",
    "When you reference a number, pull it from the provided inputs or reference JSON only. Do not fabricate new metrics or unsupported percentages.",
    "When you mention an average, rate, ratio, or percentage, explain what numbers it came from or what it is comparing, then restate the meaning in simple everyday language.",
    "Before returning JSON, silently check that each summary line answers both what happened and why it matters.",
    "Do not mention ROI or return on investment anywhere.",
    "subAgentResults must preserve the sub-agent structure from the reference schema and each item must contain id, name, definition, and details.",
    "details must be an array of 3 to 5 concrete plain-English statements grounded in the provided data.",
    "stateUpdate must preserve the exact nested shape of the reference schema so downstream agents can consume it.",
    "Do not invent non-WBD channels or unsupported entities. Keep all values plausible and tied to the synthetic data."
  ].filter(Boolean).join(" ");
}

function buildMasterStageLiveUserPrompt(out, campaignState, selectedPlan, baselineResult) {
  const stageSnapshot = buildStageRawDataEntry(out, campaignState, selectedPlan);
  const datasetBlock = Utils.truncate(buildDatasetContext(out.dataKey), 5000);
  const supplementalContext = Utils.truncate(buildSupplementalDatasetContext(out), 1800);
  const sharedStateSnapshot = {
    prompt: campaignState.prompt,
    budget_usd: campaignState.budgetUsd,
    product_family: campaignState.productFamily,
    selected_scenario: campaignState.selectedScenario,
    prior_stage_outputs: campaignState.stageOutputs || {}
  };

  return [
    `Campaign Brief:\n${campaignState.prompt}`,
    `\nMaster Agent:\n${out.name}`,
    `\nSelected Scenario:\n${JSON.stringify(campaignState.selectedScenario || {}, null, 2)}`,
    `\nShared Campaign State Snapshot:\n${JSON.stringify(sharedStateSnapshot, null, 2)}`,
    `\nStage Input Snapshot:\n${stageSnapshot?.content || JSON.stringify(baselineResult.inputData || {}, null, 2)}`,
    `\nLive Sub-Agent Outputs:\n${JSON.stringify(baselineResult.subAgentResults || [], null, 2)}`,
    `\nRelevant Dataset Excerpt:\n${datasetBlock}`,
    supplementalContext ? `\nAdditional Context:\n${supplementalContext}` : "",
    `\nReference JSON Shape And Fallback Values:\n${JSON.stringify(baselineResult, null, 2)}`,
    "\nWrite the summary for a broad audience, not just a domain expert. Use simple language, explain the important numbers, and make the stage result detailed enough to stand on its own.",
    "\nIf you mention a calculated metric such as frequency, overlap, match rate, or CPM, say where the number came from and then translate it into plain language a non-expert would understand.",
    "\nDo not trim or compress distinct findings into vague umbrella statements. Preserve the real points that matter for this stage in complete sentences.",
    "\nDo not mention ROI or return on investment anywhere in the stage write-up.",
    "\nUse an evidence-first structure: what the stage looked at, what it found, what the sub-agents added, what decision was made, what risk remains, and why that matters.",
    "\nMake sure the wording sounds logical and meaningful. Avoid generic praise, unsupported claims, and incomplete thoughts.",
    "\nIf the best answer is a measured tradeoff, say that clearly rather than overselling certainty.",
    "\nGenerate the live stage result now."
  ].filter(Boolean).join("\n");
}

function buildSubAgentLiveSystemPrompt(masterOutput, subAgent, agentStyle = "") {
  return [
    `You are ${subAgent.name}, a live sub-agent operating under ${masterOutput.name} in a hierarchical WBD campaign workflow.`,
    normalizeWhitespace(agentStyle),
    "Return one JSON object only. Do not use markdown fences.",
    "Top-level keys must be: definition, details, inputData, outputData, whyMatters, and optional text.",
    "definition must be one clear plain-English sentence explaining what this sub-agent is responsible for.",
    "details must contain 3 to 5 clear plain-English statements grounded in the supplied synthetic data and upstream stage context.",
    "Write so a non-expert reader can understand what you checked, what you found, and how your work helped the master agent.",
    "Use this order whenever possible: what data you checked, what you found, what it means for the parent agent, and any risk or limitation that still matters.",
    "Each statement must sound logical and meaningful on its own. Avoid filler like 'good', 'strong', or 'effective' unless you explain why with evidence.",
    "If the evidence is mixed, state the tradeoff clearly instead of pretending the answer is cleaner than it is.",
    "Do not mention ROI or return on investment anywhere.",
    "Do not invent metrics or unsupported numbers.",
    "inputData should summarize what this sub-agent used.",
    "outputData should capture the structured result this sub-agent is handing back to its master agent.",
    "whyMatters should be one concise business sentence.",
    "Keep the response tightly scoped to this sub-agent only."
  ].filter(Boolean).join(" ");
}

function buildSubAgentLiveUserPrompt(masterOutput, subAgent, campaignState, selectedPlan, baselineResult, fallbackSubAgent) {
  const stageSnapshot = buildStageRawDataEntry(masterOutput, campaignState, selectedPlan);
  const datasetBlock = Utils.truncate(buildDatasetContext(masterOutput.dataKey), 4200);
  const supplementalContext = Utils.truncate(buildSupplementalDatasetContext(masterOutput), 1600);
  return [
    `Campaign Brief:\n${campaignState.prompt}`,
    `\nParent Master Agent:\n${masterOutput.name}`,
    `\nSub-Agent:\n${subAgent.name}`,
    `\nSub-Agent Summary:\n${subAgent.summary || fallbackSubAgent?.details?.[0] || "No summary provided."}`,
    `\nSub-Agent Definition:\n${subAgent.definition || subAgent.summary || "No definition provided."}`,
    `\nSelected Scenario:\n${JSON.stringify(campaignState.selectedScenario || {}, null, 2)}`,
    `\nPrior Stage Outputs:\n${JSON.stringify(campaignState.stageOutputs || {}, null, 2)}`,
    `\nCurrent Stage Input Snapshot:\n${stageSnapshot?.content || JSON.stringify(baselineResult.inputData || {}, null, 2)}`,
    `\nReference Fallback Shape:\n${JSON.stringify(fallbackSubAgent || {}, null, 2)}`,
    `\nRelevant Dataset Excerpt:\n${datasetBlock}`,
    supplementalContext ? `\nAdditional Context:\n${supplementalContext}` : "",
    "\nUse simple language and enough detail that someone outside the media domain can still understand what this sub-agent accomplished.",
    "\nStart by defining the sub-agent's role clearly, then describe what it checked and what it found.",
    "\nUse an evidence-first answer: what you checked, what it showed, how it affects the parent agent, and any meaningful risk or limit.",
    "\nMake every line logical, specific, and grounded in the supplied data. Avoid vague praise or generic business wording.",
    "\nGenerate the live sub-agent result now."
  ].filter(Boolean).join("\n");
}

function syncMasterSubAgentState(masterOutputId, masterNodeId, campaignState, subAgentResults = [], latestNodeId = null) {
  const nextResults = cloneStructured(subAgentResults);
  const outputs = state.agentOutputs.map((item) => (
    item.id === masterOutputId ? { ...item, subAgentResults: nextResults } : item
  ));
  const currentDetail = campaignState.executionDetails[masterNodeId] || {};
  campaignState.executionDetails[masterNodeId] = {
    ...currentDetail,
    subAgentResults: nextResults
  };

  const runningNodeIds = new Set(state.runningNodeIds);
  nextResults.forEach((item) => {
    if (item.status === "running") runningNodeIds.add(item.id);
    else runningNodeIds.delete(item.id);
  });

  const updates = {
    agentOutputs: outputs,
    campaignStateObject: campaignState,
    runningNodeIds
  };
  if (latestNodeId) updates.latestNodeId = latestNodeId;
  setState(updates);
}

async function generateLiveSubAgentResult(masterOutput, subAgent, fallbackSubAgent, campaignState, selectedPlan, baselineResult, opts = {}, onPartial = () => {}) {
  const { creds, model, agentStyle = "" } = opts;
  if (!creds?.apiKey) throw new Error("No live LLM credentials available for sub-agent execution.");

  let rawText = "";
  await Utils.streamChatCompletion({
    llm: creds,
    body: {
      model,
      stream: true,
      temperature: 0.35,
      messages: [
        {
          role: "system",
          content: buildSubAgentLiveSystemPrompt(masterOutput, subAgent, agentStyle)
        },
        {
          role: "user",
          content: buildSubAgentLiveUserPrompt(masterOutput, subAgent, campaignState, selectedPlan, baselineResult, fallbackSubAgent)
        }
      ]
    },
    onChunk: (chunk) => {
      rawText += chunk;
      onPartial(rawText);
    }
  });

  const parsed = parseLooseResponseJson(rawText);
  if (!Object.keys(parsed).length) {
    throw new Error(`Live LLM response for sub-agent ${subAgent.name} could not be parsed into JSON.`);
  }

  return normalizeSingleLiveSubAgentResult(parsed, fallbackSubAgent);
}

async function runLiveSubAgentsForMaster(out, campaignState, selectedPlan, baselineResult, opts = {}) {
  const subAgentCatalog = getSubAgentsForNode(out.nodeId, selectedPlan || campaignState);
  const initialResults = (baselineResult.subAgentResults || []).map((item, index) => createPendingSubAgentResult(out, item, index));
  if (!initialResults.length) return [];

  syncMasterSubAgentState(out.id, out.nodeId, campaignState, initialResults, initialResults[0]?.id || out.nodeId);
  const mutableResults = initialResults.map((item) => ({ ...item }));

  await Promise.all(mutableResults.map(async (pendingItem, index) => {
    const catalogItem = subAgentCatalog.find((item) => item.id === pendingItem.id || item.name === pendingItem.name) || subAgentCatalog[index] || {};
    mutableResults[index] = {
      ...pendingItem,
      status: "running",
      text: `Live sub-agent call in progress for ${pendingItem.name}...`
    };
    syncMasterSubAgentState(out.id, out.nodeId, campaignState, mutableResults, pendingItem.id);

    try {
      const finalResult = await generateLiveSubAgentResult(
        out,
        catalogItem,
        mutableResults[index],
        campaignState,
        selectedPlan,
        baselineResult,
        opts,
        (rawText) => {
          mutableResults[index] = {
            ...mutableResults[index],
            status: "running",
            text: `Live sub-agent call in progress...\n\n${rawText.length > 1600 ? rawText.slice(-1600) : rawText}`
          };
          syncMasterSubAgentState(out.id, out.nodeId, campaignState, mutableResults, pendingItem.id);
        }
      );
      mutableResults[index] = {
        ...mutableResults[index],
        ...finalResult,
        status: "done",
        text: finalResult.text || buildSubAgentNarrative(finalResult)
      };
    } catch (err) {
      console.warn(`Live sub-agent execution failed for ${pendingItem.name}, using fallback:`, err?.message || err);
      mutableResults[index] = {
        ...mutableResults[index],
        status: "done",
        fallbackUsed: true,
        text: `### LLM Fallback Activated\nThe live sub-agent call did not complete, so a deterministic fallback was applied.\n\n${buildSubAgentNarrative(mutableResults[index])}`
      };
    }

    syncMasterSubAgentState(out.id, out.nodeId, campaignState, mutableResults, pendingItem.id);
  }));

  return mutableResults.map((item) => ({ ...item }));
}

async function generateLiveMasterStageResult(out, campaignState, selectedPlan, baselineResult, opts = {}) {
  const { creds, model, agentStyle = "" } = opts;
  if (!creds?.apiKey) throw new Error("No live LLM credentials available for master-agent execution.");

  let rawText = "";
  await Utils.streamChatCompletion({
    llm: creds,
    body: {
      model,
      stream: true,
      temperature: 0.3,
      messages: [
        {
          role: "system",
          content: buildMasterStageLiveSystemPrompt(out, baselineResult, agentStyle)
        },
        {
          role: "user",
          content: buildMasterStageLiveUserPrompt(out, campaignState, selectedPlan, baselineResult)
        }
      ]
    },
    onChunk: (chunk) => {
      rawText += chunk;
      const preview = rawText.length > 1800 ? rawText.slice(-1800) : rawText;
      updateAgent(out.id, `Live LLM execution in progress...\n\n${preview}`, "running");
    }
  });

  const parsed = parseLooseResponseJson(rawText);
  if (!Object.keys(parsed).length) {
    throw new Error(`Live LLM response for ${out.name} could not be parsed into JSON.`);
  }

  return normalizeLiveMasterStageResult(parsed, baselineResult);
}

async function runMasterAgentStage(out, campaignState, selectedPlan, opts = {}) {
  updateAgent(out.id, "Orchestrator is dispatching a live LLM stage and assembling the shared Campaign_State_Object update.", "running");
  await wait(160);
  const baselineCampaignState = cloneStructured(campaignState);
  const baselineResult = executeMasterStage(out, baselineCampaignState);
  campaignState.executionDetails[out.nodeId] = {
    ...cloneStructured(baselineResult),
    subAgentResults: (baselineResult.subAgentResults || []).map((item, index) => createPendingSubAgentResult(out, item, index))
  };
  patchAgent(out.id, {
    subAgentResults: campaignState.executionDetails[out.nodeId].subAgentResults || [],
    inputData: baselineResult.inputData || {}
  });
  setState({
    campaignStateObject: campaignState,
    latestNodeId: out.nodeId
  });

  let result = baselineResult;
  let liveSubAgentResults = campaignState.executionDetails[out.nodeId].subAgentResults || [];
  let usedFallback = false;

  try {
    liveSubAgentResults = await runLiveSubAgentsForMaster(out, campaignState, selectedPlan, baselineResult, opts);
    const liveStageBaseline = {
      ...baselineResult,
      subAgentResults: liveSubAgentResults
    };
    setState({ latestNodeId: out.nodeId });
    result = await generateLiveMasterStageResult(out, campaignState, selectedPlan, liveStageBaseline, opts);
    result.subAgentResults = liveSubAgentResults;
  } catch (err) {
    usedFallback = true;
    console.warn(`Live master-agent execution failed for ${out.name}, using deterministic fallback:`, err?.message || err);
  }

  result.subAgentResults = liveSubAgentResults.length ? liveSubAgentResults : (result.subAgentResults || []);

  campaignState.stageOutputs[out.nodeId] = cloneStructured(result.stateUpdate || {});
  const narrative = `${usedFallback ? "### LLM Fallback Activated\nThe live language-model execution did not complete for this stage, so a deterministic fallback was applied.\n\n" : ""}${buildMasterAgentNarrative(result)}`;
  campaignState.executionDetails[out.nodeId] = result;
  campaignState.actionLog.push({
    nodeId: out.nodeId,
    agent: out.name,
    summary: result.summaryLines?.[0] || "",
    time: Date.now()
  });
  patchAgent(out.id, {
    summaryLines: result.summaryLines || [],
    subAgentResults: result.subAgentResults || [],
    inputData: result.inputData || {},
    stateUpdate: result.stateUpdate || {},
    handoff: result.handoff || "",
    whyMatters: result.whyMatters || "",
    fallbackUsed: usedFallback || (result.subAgentResults || []).some((item) => item.fallbackUsed)
  });
  setState({
    campaignStateObject: campaignState,
    rawDataByAgent: buildRawDataByPlan(state.plan, campaignState, selectedPlan)
  });
  updateAgent(out.id, narrative, "done");
  return narrative;
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
    let creds = null;
    try {
      creds = await ensureCreds();
    } catch (credsError) {
      setState({
        error: "Live LLM credentials are required for multi-agent execution.",
        stage: "data",
        visualizationLoading: false
      });
      return;
    }
    const model = $("#model")?.value || "gpt-5-mini";
    const agentStyle = $("#agent-style")?.value || config.defaults?.agentStyle || "";
    const demo = getSelectedDemo();
    const campaignPrompt = (state.campaignPrompt || demo?.problem || "Launch a converged ad campaign.").trim();
    const runToken = Date.now();
    const selectedPlanRecord = buildToddlerHeroPlanContext(getSelectedScenarioRecord(), campaignPrompt) || getSelectedScenarioRecord();
    const activeSubAgentCatalog = resolveSubAgentCatalogForVariant(selectedPlanRecord || state.plan || campaignPrompt);
    const baselineCompliance = isToddlerHeroBrief(campaignPrompt)
      ? evaluatePlanCompliance({
        campaignPrompt,
        allocationStrategy: selectedPlanRecord?.allocationStrategy || "",
        variantKey: normalizeVariantKey(selectedPlanRecord?.variantKey || "")
      })
      : state.selectedPlanCompliance || selectedPlanRecord?.complianceValidation || evaluatePlanCompliance({
      campaignPrompt,
      allocationStrategy: selectedPlanRecord?.allocationStrategy || "",
      variantKey: normalizeVariantKey(selectedPlanRecord?.variantKey || "")
      });
    const campaignState = buildInitialCampaignState({
      campaignPrompt,
      selectedPlan: selectedPlanRecord,
      entries
    });
    campaignState.executionDetails[ORCHESTRATOR_NODE_ID] = {
      summaryLines: [
        `The orchestrator selected ${campaignState.selectedScenario.title} for ${campaignState.productFamily.displayLabel}.`,
        "It owns the shared Campaign_State_Object and dispatches each master agent in sequence while keeping context intact."
      ],
      subAgentResults: state.plan.map((item) => ({
        id: item.nodeId,
        name: item.agentName,
        details: [
          `Stage ${item.phase} owner.`,
          `${(activeSubAgentCatalog[item.nodeId] || []).length || 0} sub-agents attached.`,
          item.initialTask
        ]
      })),
      inputData: {
        prompt: campaignPrompt,
        selectedScenario: campaignState.selectedScenario,
        sharedStateKeys: ["prompt", "budgetUsd", "productFamily", "selectedScenario", "intelligence", "stageOutputs"]
      },
      stateUpdate: {
        selectedScenario: campaignState.selectedScenario,
        stageCount: state.plan.length,
        complianceRunsInParallel: true
      },
      handoff: "The orchestrator passes the same Campaign_State_Object through all six stages so earlier decisions affect later actions.",
      whyMatters: "This is the manager layer that makes the demo feel like a hierarchical system rather than a set of disconnected calls."
    };
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
      runToken,
      campaignStateObject: campaignState,
      subAgentCatalog: activeSubAgentCatalog,
      rawDataByAgent: buildRawDataByPlan(state.plan, campaignState, selectedPlanRecord)
    });

    const compliancePromise = runParallelComplianceAgent(complianceOutput, {
      creds,
      model,
      campaignPrompt,
      selectedPlan: selectedPlanRecord,
      campaignState
    });

    const phases = new Map();
    state.plan.forEach((a) => {
      const p = a.phase ?? 0;
      if (!phases.has(p)) phases.set(p, []);
      phases.get(p).push(a);
    });
    const sortedPhases = [...phases.keys()].sort((a, b) => (typeof a === "number" && typeof b === "number") ? a - b : String(a).localeCompare(String(b)));

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

      await Promise.all(outputs.map((out) => runMasterAgentStage(out, campaignState, selectedPlanRecord, {
        creds,
        model,
        agentStyle
      })));
    }

    const complianceResult = await compliancePromise;

    const nextDashboard = buildDashboard(state.agentOutputs, {
      campaignPrompt,
      selectedPlanId: state.selectedArchitectPlanId || "",
      runToken,
      campaignStateObject: campaignState
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
      agentOutputs: state.agentOutputs,
      campaignStateObject: campaignState
    });

    setState({
      stage: "idle",
      dashboard: nextDashboard,
      approvedPlanCsv: buildApprovedPlanCsv(campaignState),
      rawDataByAgent: buildRawDataByPlan(state.plan, campaignState, selectedPlanRecord),
      selectedPlanCompliance: complianceResult || baselineCompliance,
      complianceDetails: buildComplianceDetails(complianceResult || baselineCompliance),
      visualizationLoading: false,
      visualizationNarrative,
      visualizationExplanationOpen: true,
      campaignStateObject: campaignState
    });
  } catch (e) {
    setState({ error: e.message, stage: "idle", visualizationLoading: false, campaignStateObject: null });
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
    "Why this matters: parallel compliance validation prevents non-compliant spend from entering activation while preserving delivery quality and legal safety."
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
    variantKey: normalizeVariantKey(selectedPlan?.variantKey || "")
  });
  const context = `Parallel compliance context\nPlan strategy: ${selectedPlan?.strategy || "N/A"}\nAllocation strategy: ${selectedPlan?.allocationStrategy || "N/A"}\nDelivery timing: ${selectedPlan?.deliveryTiming || "N/A"}`;
  const inputBlob = `Compliance dataset focus:\n${buildDatasetContext("complianceRulebook")}`;
  let llmNarrative = "";
  if (creds) {
    try {
      llmNarrative = await runSyntheticAgent(out, {
        creds,
        model,
        agentStyle: "",
        campaignPrompt,
        inputBlob,
        context
      });
    } catch (err) {
      console.warn("Parallel compliance LLM refinement failed, using deterministic narrative only:", err?.message || err);
    }
  }
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
            content: `${out.instruction}\n${agentStyle}\nWrite Markdown between 220 and 420 words. Make it clear, informative, and understandable to non-experts. Explain important terms in simple language, include enough detail to stand on its own, include one explicit heading named "Why this matters", and keep it grounded in the provided dataset. Every paragraph should sound logical and meaningful, avoid vague filler, and avoid unsupported claims. Use claim, evidence, and implication as the core pattern for each section. If the evidence is mixed, explain the tradeoff clearly instead of overselling certainty.`
          },
          {
            role: "user",
            content:
              `Campaign Brief:\n${campaignPrompt}\n\nTask:\n${out.task}\n\nRelevant Dataset:\n${datasetBlock}${supplementalContext ? `\n\nAdditional Yield Intelligence:\n${supplementalContext}` : ""}\n\nSupplemental Inputs:\n${inputBlob}\n\nPrior Outputs:\n${priorContext}\n\nWrite the explanation so a general business stakeholder can follow it without domain expertise. Make every claim logical, specific, and tied to the provided evidence. For each section, explain what happened, what evidence supports it, and why it matters.`
          }
        ]
      },
      onChunk: (chunk) => {
        narrative += chunk;
        updateAgent(out.id, narrative, "running");
      }
    });
    const fallback = buildSyntheticAgentNarrative(out, campaignPrompt, context);
    const finalNarrative = isLowQualityNarrative(narrative, {
      minWords: 60,
      rejectGenericPraise: false
    }) ? fallback : narrative;
    updateAgent(out.id, finalNarrative, "done");
    return finalNarrative;
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

async function buildVisualizationNarrative({ creds, model, campaignPrompt, dashboard, agentOutputs, campaignStateObject = null }) {
  try {
    if (!creds) throw new Error("No live credentials available.");
    let buffer = "";
    const dashboardMakeGood = dashboard?.makeGood
      ? {
        shiftBudget: dashboard.makeGood.shiftBudget
      }
      : {};
    const measurementSummary = campaignStateObject?.stageOutputs?.["measurement-agent"]?.measurement_summary
      ? {
        projected_households: campaignStateObject.stageOutputs["measurement-agent"].measurement_summary.projected_households,
        matched_households: campaignStateObject.stageOutputs["measurement-agent"].measurement_summary.matched_households,
        clean_room_match_rate_pct: campaignStateObject.stageOutputs["measurement-agent"].measurement_summary.clean_room_match_rate_pct,
        average_frequency: campaignStateObject.stageOutputs["measurement-agent"].measurement_summary.average_frequency
      }
      : null;
    const summaryPayload = {
      campaignPrompt,
      selectedScenario: campaignStateObject?.selectedScenario || null,
      pacing: dashboard?.pacing
        ? {
          linearPeakDeliveryRate: Math.max(...(dashboard.pacing.linearPct || [0])),
          digitalPeakDeliveryRate: Math.max(...(dashboard.pacing.digitalPct || [0])),
          makeGoodHour: dashboard.pacing.makeGoodHour || 12
        }
        : {},
      reach: dashboard?.reach || {},
      makeGood: dashboardMakeGood,
      measurement: measurementSummary,
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
            content: "You are an analytics narrator. Explain visualization outputs in detailed plain language for both domain experts and non-experts. Every section must contain a sentence that starts with 'Why this matters:'. Define abbreviations when first used and give enough context that the explanation stands on its own. The writing must sound logical, meaningful, and grounded in the supplied numbers. Avoid generic praise, filler, and unsupported claims. Use a consistent pattern in each section: what happened, which numbers prove it, what decision or implication follows. Whenever you mention an average, rate, ratio, or percentage, explain what it was calculated from or what it compares, then restate the meaning in everyday language."
          },
          {
            role: "user",
            content: `Use this dashboard summary and write a detailed Markdown explanation with sections for Delivery Pacing, Reach Distribution, Make-Good Decision, and Action Traceability.\nExplain what happened, what the key numbers mean, and why each section matters in simple language.\nIf a metric suggests a tradeoff or uncertainty, explain that directly instead of smoothing it over.\nIn each section, connect the claim to the exact numbers and then explain the business implication.\n\nDashboard JSON:\n${JSON.stringify(summaryPayload, null, 2)}`
          }
        ]
      },
      onChunk: (chunk) => { buffer += chunk; }
    });

    const finalNarrative = buffer.trim();
    if (!isLowQualityNarrative(finalNarrative, { minWords: 70, rejectGenericPraise: false })) {
      return finalNarrative;
    }
    throw new Error("Visualization narrative was too weak to use directly.");
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
  const effectiveCostPerThousandImpressions = delivered ? ((extractBudgetUsd(campaignPrompt) / delivered) * 1000) : 0;
  const quality = state.dataQuality || computeDataQuality(datasets.audienceGraph || []);
  const compliance = buildComplianceDetails();
  const planningSummary = state.campaignStateObject?.stageOutputs?.["planning-identity-agent"]?.audience_summary || {};
  const measurementSummary = state.campaignStateObject?.stageOutputs?.["measurement-agent"]?.measurement_summary || {};
  const planningHouseholds = resolveAudienceHouseholdCount(planningSummary, state.campaignStateObject?.intelligence || {});
  const planningSeedHouseholds = Math.max(0, Number(planningSummary.seed_households || state.campaignStateObject?.intelligence?.seedHouseholdCount || 0));
  const projectedHouseholds = resolveProjectedHouseholdCount(reach, measurementSummary, planningHouseholds || reach.uniqueHouseholds || 0);
  const measuredHouseholds = resolveMeasuredHouseholdCount(measurementSummary, projectedHouseholds);

  if (role === "planning-identity") {
    return `### Planning and Identity Narrative
For the campaign prompt "${campaignPrompt}", the planning and identity stage used the unified audience base dataset to resolve high-value audience cohorts by state. The deterministic output built a modeled audience of ${(planningHouseholds || reach.uniqueHouseholds || 0).toLocaleString()} deduplicated households${planningSeedHouseholds ? ` from a seed sample of ${planningSeedHouseholds.toLocaleString()} distinct household IDs` : ""} and translated that base into about ${projectedHouseholds.toLocaleString()} projected reachable households with ${reach.deviceCount.toLocaleString()} connected devices across channels.

### Data Quality Review
This stage explicitly documented data quality signals before allocation. The synthetic audience base includes ${quality.duplicateCount} duplicate household identifiers and ${quality.nullDmaCount} records with missing designated market area values, which were flagged to avoid overstating addressable reach.

### Why this matters
Accurate identity resolution determines whether downstream allocation decisions extend useful reach or simply add unproductive delivery volume.`;
  }

  if (role === "inventory-yield") {
    const intelligence = (datasets.yieldIntelligenceFeed || []).slice(0, 3);
    return `### Inventory Yield Narrative
This stage merged unified audience context with Yield Intelligence signals to identify timing windows with higher demand probability. The signal review included sources such as Twitter trends, news signals, and external feed indicators so that delivery can be timed around audience spikes.

### Yield Intelligence Highlights
${intelligence.map((item) => `- ${item.source_type} signal "${item.topic}" in ${item.region} indicates about ${item.expected_response_lift_pct} percent expected response lift with ${item.recommended_channel} emphasis during ${item.spike_window}.`).join("\n")}

### Why this matters
Yield-aware timing decisions improve delivery quality by shifting spend toward periods where audience attention and response likelihood are measurably stronger.`;
  }

  if (role === "booking-proposals") {
    return `### Booking and Proposals Narrative
The booking stage converted yield and audience recommendations into channel proposals with deterministic allocation logic. The proposal uses audience fit, delivery reliability, and cost control as the decision objective and keeps fallback paths available when inventory quality changes by state.

### Allocation and Cost Logic
Each booking recommendation ties spend to expected impression quality, demand timing, and inventory reliability. The current effective cost per thousand impressions estimate is ${effectiveCostPerThousandImpressions.toFixed(2)} United States dollars based on synthetic delivery totals.

### Why this matters
Proposal quality directly controls whether budget is deployed into inventory that can generate measurable business outcomes.`;
  }

  if (role === "trafficking-signals") {
    return `### Trafficking and Signals Narrative
The trafficking stage prepared activation instructions and validated signal pathways before launch. Signal checks emphasized routing consistency, placement metadata quality, and execution readiness under variable daypart demand.

### Signal Reliability Interpretation
Potential signal risk points were documented with explicit mitigation instructions so operations can respond before pacing degradation affects delivery quality.

### Why this matters
Even strong allocation strategy fails without reliable trafficking and signal execution, so this step protects delivery quality before budget goes live.`;
  }

  if (role === "inflight-operations") {
    return `### In-Flight Operations Narrative
The operations stage evaluated pacing trends and identified deterministic under-delivery pressure in the linear lane during the mid-campaign window. A make-good reallocation of ${makeGood.shiftBudget.toLocaleString()} United States dollars was triggered to protect delivery against target outcomes.

### Intervention Interpretation
The make-good shifted budget away from the weaker lane before the shortfall widened, which helped stabilize the final delivery path.

### Why this matters
In-flight correction protects campaign value by preventing prolonged under-delivery from eroding the final delivery result.`;
  }

  if (role === "measurement") {
    return `### Measurement Narrative
The measurement stage consolidated cross-channel outcomes and quantified performance with deterministic attribution indicators. Reported reach includes ${projectedHouseholds.toLocaleString()} projected households, ${reach.deviceCount.toLocaleString()} devices, ${reach.overlapPct} percent cross-platform overlap, and about ${measuredHouseholds.toLocaleString()} matched households under the current clean-room assumptions. That matched total is the measurable subset of the reached audience, not the full planning universe from Stage 1.

### Outcome Interpretation
Measurement output links delivery behavior, channel mix, and corrective actions to final outcome quality so optimization decisions can be replicated in future cycles.

### Why this matters
Without clear measurement traceability, the team cannot tell which audience and channel choices should be repeated in the next campaign.`;
  }

  if (role === "compliance-agent") {
    return `### Parallel Compliance Narrative
The compliance agent executed in parallel with the six-stage workflow and validated campaign parameters against synthetic policy rules. Rule evidence was logged with source references and compliant alternatives where restrictions were detected.

### Policy Sources and Rationale
Source one: ${compliance.sources[0]?.policy || "Financial services restriction policy"}. Source dataset: ${compliance.sources[0]?.source || "Global Compliance Policy Engine"}. Reason applied: ${compliance.sources[0]?.why || "Restricted category was detected for the target market."}
Source two: ${compliance.sources[1]?.policy || "Pharmaceutical disclaimer policy"}. Source dataset: ${compliance.sources[1]?.source || "Global Compliance Policy Engine"}. Reason applied: ${compliance.sources[1]?.why || "Creative category requires disclaimer handling."}

### Why this matters
Parallel compliance validation prevents non-compliant activation while preserving campaign pacing and delivery goals.`;
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
  if (text.includes("inventory-yield") || text.includes("inventory yield") || text.includes("inventory and yield")) return "inventory-yield";
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

function buildStageRawDataEntry(agent, campaignState = null, selectedPlan = null) {
  const role = inferAgentRole(agent);
  const scenario = selectedPlan?.scenarioIntelligence || campaignState?.intelligence || null;
  if (!campaignState && !scenario) return null;

  if (role === "planning-identity") {
    return {
      title: "Planning and Identity Inputs",
      type: "json",
      content: JSON.stringify({
        prompt: campaignState?.prompt || state.campaignPrompt || "",
        selected_scenario: selectedPlan?.title || campaignState?.selectedScenario?.title || "",
        top_tags: scenario?.topTags?.slice(0, 4) || [],
        top_segments: scenario?.topSegments?.slice(0, 3) || [],
        top_age_buckets: scenario?.topAgeBuckets?.slice(0, 3) || [],
        top_income_brackets: scenario?.topIncomeBrackets?.slice(0, 3) || [],
        viewing_mix: scenario?.viewingMix?.slice(0, 3) || [],
        sample_profiles: scenario?.sampleRows?.slice(0, 6) || []
      }, null, 2)
    };
  }

  if (role === "inventory-yield") {
    return {
      title: "Inventory and Yield Inputs",
      type: "json",
      content: JSON.stringify({
        audience_summary: campaignState?.stageOutputs?.["planning-identity-agent"] || null,
        selected_networks: scenario?.rankedNetworks?.slice(0, 4) || [],
        inventory_rows: selectRelevantInventoryRows(campaignState || buildInitialCampaignState({
          campaignPrompt: state.campaignPrompt || "",
          selectedPlan
        }), 6),
        yield_signals: selectRelevantYieldSignals(campaignState || buildInitialCampaignState({
          campaignPrompt: state.campaignPrompt || "",
          selectedPlan
        }), 3)
      }, null, 2)
    };
  }

  if (role === "booking-proposals") {
    return {
      title: "Booking and Proposal Inputs",
      type: "json",
      content: JSON.stringify({
        scenario: campaignState?.selectedScenario || selectedPlan || null,
        inventory_summary: campaignState?.stageOutputs?.["inventory-yield-agent"] || null,
        compliance_preview: evaluatePlanCompliance({
          campaignPrompt: campaignState?.prompt || state.campaignPrompt || "",
          allocationStrategy: selectedPlan?.allocationStrategy || campaignState?.selectedScenario?.allocationStrategy || "",
          variantKey: normalizeVariantKey(selectedPlan?.variantKey || campaignState?.selectedScenario?.variantKey || "")
        })
      }, null, 2)
    };
  }

  if (role === "trafficking-signals") {
    return {
      title: "Trafficking and Signals Inputs",
      type: "json",
      content: JSON.stringify({
        line_items: campaignState?.stageOutputs?.["booking-proposals-agent"]?.booking_summary?.line_items || [],
        signal_watchlist: selectRelevantInventoryRows(campaignState || buildInitialCampaignState({
          campaignPrompt: state.campaignPrompt || "",
          selectedPlan
        }), 6).filter((row) => (row.scte35_signal_status || "").toLowerCase() !== "active")
      }, null, 2)
    };
  }

  if (role === "inflight-operations") {
    return {
      title: "In-Flight Operations Inputs",
      type: "json",
      content: JSON.stringify({
        trafficked_plan: campaignState?.stageOutputs?.["booking-proposals-agent"]?.booking_summary?.line_items || [],
        trafficking_summary: campaignState?.stageOutputs?.["trafficking-signals-agent"] || null,
        scenario_allocation: campaignState?.selectedScenario?.allocation || selectedPlan?.allocation || null
      }, null, 2)
    };
  }

  if (role === "measurement") {
    const reachSummary = campaignState
      ? buildReachSummary(datasets.audienceGraph || [], datasets.liveDeliveryLog || [], {}, campaignState)
      : null;
    return {
      title: "Measurement and Learning Inputs",
      type: "json",
      content: JSON.stringify({
        audience_summary: campaignState?.stageOutputs?.["planning-identity-agent"] || null,
        booking_summary: campaignState?.stageOutputs?.["booking-proposals-agent"] || null,
        inflight_summary: campaignState?.stageOutputs?.["inflight-operations-agent"] || null,
        projected_reach: reachSummary
          ? {
            projected_households: resolveProjectedHouseholdCount(reachSummary),
            addressable_households: Number(reachSummary.addressableHouseholds || 0),
            overlap_pct: Number(reachSummary.overlapPct || 0),
            device_count: Number(reachSummary.deviceCount || 0)
          }
          : null,
        selected_scenario: campaignState?.selectedScenario || selectedPlan || null
      }, null, 2)
    };
  }

  return null;
}

function buildRawDataByPlan(plan = [], campaignState = null, selectedPlan = null) {
  if (!Array.isArray(plan) || !plan.length) return RAW_DATA_BY_AGENT;
  const map = {};
  plan.forEach((agent) => {
    const stageEntry = buildStageRawDataEntry(agent, campaignState, selectedPlan);
    const role = inferAgentRole(agent);
    const dataKey = role === "inventory-yield" ? "inventoryMatrix" : resolveDatasetKey(agent);
    const entry = stageEntry || RAW_DATA_BY_KEY[dataKey];
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
  if (/\b(children|kids|child|toddler|baby|infant)\b/i.test(text)) categories.add("children_products");
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
  if (/\b(children|kids|child|toddler|baby|infant)\b/i.test(value)) signals.add("children");
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
  if (isToddlerHeroBrief(campaignPrompt)) {
    return buildToddlerHeroComplianceEvaluation({
      allocationStrategy: allocationStrategy || TODDLER_HERO_ROUTE.allocationStrategy,
      variantKey: variantKey || TODDLER_HERO_ROUTE.variantKey
    });
  }
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

function patchAgent(id, patch = {}) {
  if (!id || !patch || typeof patch !== "object") return;
  const outputs = state.agentOutputs.map((item) => (item.id === id ? { ...item, ...patch } : item));
  setState({ agentOutputs: outputs });
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
  const runningNodeIds = new Set(state.runningNodeIds);
  if (updatedAgent && (status === "done" || status === "error")) {
    runningNodeIds.delete(updatedAgent.nodeId);
  }
  setState({ agentOutputs: outputs, issueNodeIds, runningNodeIds });
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
  const projectedHouseholds = Number(reach.projectedHouseholds || reach.modeledHouseholds || reach.scaledReach || reach.uniqueHouseholds || 0);

  const sig = JSON.stringify({
    reach: projectedHouseholds,
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
let scrollState = { key: null };

function scheduleScrollToRunningSection() {
  if (scrollSched) return;
  scrollSched = true;
  requestAnimationFrame(() => {
    scrollSched = false;
    const key = getRunningScrollKey();
    if (!key) {
      scrollState = { key: null };
      return;
    }

    const target = document.querySelector(`[data-running-key="${key}"]`);
    if (!target) return;

    // Only jump when the active stage changes. Continuous follow-on-scroll
    // makes it hard to read surrounding content while agents are streaming.
    const keyChanged = scrollState.key !== key;

    if (keyChanged) target.scrollIntoView({ behavior: "smooth", block: "start" });

    scrollState = { key };
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

const FLOW_GRAPH_LABELS = {
  [FLOW_BRIEF_NODE_ID]: "Campaign\nBrief",
  [ORCHESTRATOR_NODE_ID]: "Orchestrator\nManager",
  [PARALLEL_COMPLIANCE_NODE_ID]: "Parallel\nCompliance",
  [FLOW_OUTPUT_NODE_ID]: "Output &\nMeasurement",
  "planning-identity-agent": "Stage 1\nPlanning & Identity",
  "inventory-yield-agent": "Stage 2\nInventory & Yield",
  "booking-proposals-agent": "Stage 3\nBooking & Proposal",
  "trafficking-signals-agent": "Stage 4\nTrafficking & Signals",
  "inflight-operations-agent": "Stage 5\nIn-Flight Operations",
  "measurement-agent": "Stage 6\nMeasurement & Learning",
  "audience-segmentation": "Audience\nSegmentation",
  "cross-platform-id-resolver": "Cross-Platform\nID Resolver",
  "behavioral-indexer": "Behavioral\nIndexer",
  "predictive-capacity": "Predictive\nCapacity",
  "yield-optimizer": "Yield\nOptimizer",
  "signal-forecaster": "Signal\nForecaster",
  "deal-structurer": "Booking\nStructurer",
  "global-compliance-bot": "Global Compliance\nBot",
  "proposal-assembler": "Proposal\nAssembler",
  "asset-qa": "Asset QA",
  "streamx-router": "Delivery\nRouter",
  "launch-readiness": "Launch\nReadiness",
  "real-time-pacing": "Real-Time\nPacing",
  "autonomous-make-good": "Autonomous\nMake-Good",
  "ops-alerting": "Ops\nAlerting",
  "household-match-validator": "Household Match\nValidator",
  "outcome-pattern-reader": "Outcome Pattern\nReader",
  "next-flight-planner": "Next Flight\nPlanner",
  "clean-room-matcher": "Household Match\nValidator",
  "deterministic-roi": "Outcome Pattern\nReader",
  "learning-agenda": "Next Flight\nPlanner"
};

function getFlowGraphLabel(id, fallback = "") {
  return FLOW_GRAPH_LABELS[id] || fallback;
}

function buildHierarchicalFlowGraph(plan = []) {
  if (!Array.isArray(plan) || !plan.length) return { nodes: [], edges: [] };
  const sortedPlan = [...plan].sort((a, b) => (Number(a.phase) || 0) - (Number(b.phase) || 0));
  const vertical = state.flowOrientation === "vertical";
  const activeCatalog = state.subAgentCatalog || resolveSubAgentCatalogForVariant(getSelectedScenarioRecord() || state.campaignStateObject || state.plan);

  if (vertical) {
    const mainX = 430;
    const branchXOffset = 320;
    const complianceX = mainX + (branchXOffset * 2) + 60;
    const stageYStart = 470;
    const stageYGap = 280;
    const subRowGap = 110;
    const bookingIndex = Math.max(0, sortedPlan.findIndex((agent) => agent.nodeId === "booking-proposals-agent"));
    const complianceY = stageYStart + (bookingIndex * stageYGap);
    const nodes = [
      {
        id: FLOW_BRIEF_NODE_ID,
        label: getFlowGraphLabel(FLOW_BRIEF_NODE_ID, "Campaign Brief"),
        kind: "system",
        position: { x: mainX, y: 40 }
      },
      {
        id: ORCHESTRATOR_NODE_ID,
        label: getFlowGraphLabel(ORCHESTRATOR_NODE_ID, "Orchestrator Agent"),
        kind: "manager",
        position: { x: mainX, y: 220 }
      },
      {
        id: PARALLEL_COMPLIANCE_NODE_ID,
        label: getFlowGraphLabel(PARALLEL_COMPLIANCE_NODE_ID, "Parallel Compliance Agent"),
        kind: "parallel",
        position: { x: complianceX, y: complianceY }
      }
    ];
    const edges = [
      { source: FLOW_BRIEF_NODE_ID, target: ORCHESTRATOR_NODE_ID },
      { source: ORCHESTRATOR_NODE_ID, target: PARALLEL_COMPLIANCE_NODE_ID, kind: "control" }
    ];

    sortedPlan.forEach((agent, index) => {
      const y = stageYStart + (index * stageYGap);
      nodes.push({
        id: agent.nodeId,
        label: getFlowGraphLabel(agent.nodeId, agent.agentName),
        kind: "master",
        position: { x: mainX, y }
      });
      edges.push({ source: ORCHESTRATOR_NODE_ID, target: agent.nodeId, kind: "control" });
      if (index > 0) {
        edges.push({ source: sortedPlan[index - 1].nodeId, target: agent.nodeId });
      }
      if (agent.nodeId === "booking-proposals-agent") {
        edges.push({ source: PARALLEL_COMPLIANCE_NODE_ID, target: agent.nodeId, kind: "control" });
      }

      const subAgents = activeCatalog[agent.nodeId] || [];
      const rowCount = Math.max(1, Math.ceil(subAgents.length / 2));
      subAgents.forEach((subAgent, subIndex) => {
        const row = Math.floor(subIndex / 2);
        const isLeft = subAgents.length > 1 ? subIndex % 2 === 0 : false;
        const subId = subAgent.id;
        const subY = y + ((row - ((rowCount - 1) / 2)) * subRowGap);
        nodes.push({
          id: subId,
          label: getFlowGraphLabel(subId, subAgent.name),
          kind: "subagent",
          position: { x: mainX + (isLeft ? -branchXOffset : branchXOffset), y: subY }
        });
        edges.push({ source: agent.nodeId, target: subId, kind: "control" });
      });
    });

    const lastMaster = sortedPlan[sortedPlan.length - 1];
    nodes.push({
      id: FLOW_OUTPUT_NODE_ID,
      label: getFlowGraphLabel(FLOW_OUTPUT_NODE_ID, "Output and Measurement"),
      kind: "output",
      position: { x: mainX, y: stageYStart + (sortedPlan.length * stageYGap) }
    });
    if (lastMaster) edges.push({ source: lastMaster.nodeId, target: FLOW_OUTPUT_NODE_ID });
    return { nodes, edges };
  }

  const nodes = [
    {
      id: FLOW_BRIEF_NODE_ID,
      label: getFlowGraphLabel(FLOW_BRIEF_NODE_ID, "Campaign Brief"),
      kind: "system",
      position: { x: 60, y: 250 }
    },
    {
      id: ORCHESTRATOR_NODE_ID,
      label: getFlowGraphLabel(ORCHESTRATOR_NODE_ID, "Orchestrator Agent"),
      kind: "manager",
      position: { x: 300, y: 250 }
    },
    {
      id: PARALLEL_COMPLIANCE_NODE_ID,
      label: getFlowGraphLabel(PARALLEL_COMPLIANCE_NODE_ID, "Parallel Compliance Agent"),
      kind: "parallel",
      position: { x: 1080, y: 70 }
    }
  ];
  const edges = [
    { source: FLOW_BRIEF_NODE_ID, target: ORCHESTRATOR_NODE_ID },
    { source: ORCHESTRATOR_NODE_ID, target: PARALLEL_COMPLIANCE_NODE_ID, kind: "control" }
  ];
  const masterXStart = 560;
  const masterXGap = 230;
  const masterY = 250;
  const subYStart = 400;
  const subYGap = 100;

  sortedPlan.forEach((agent, index) => {
    const x = masterXStart + (index * masterXGap);
    nodes.push({
      id: agent.nodeId,
      label: getFlowGraphLabel(agent.nodeId, agent.agentName),
      kind: "master",
      position: { x, y: masterY }
    });
    edges.push({ source: ORCHESTRATOR_NODE_ID, target: agent.nodeId, kind: "control" });
    if (index > 0) {
      edges.push({ source: sortedPlan[index - 1].nodeId, target: agent.nodeId });
    }
    if (agent.nodeId === "booking-proposals-agent") {
      edges.push({ source: PARALLEL_COMPLIANCE_NODE_ID, target: agent.nodeId, kind: "control" });
    }

    const subAgents = activeCatalog[agent.nodeId] || [];
    subAgents.forEach((subAgent, subIndex) => {
      const subId = subAgent.id;
      nodes.push({
        id: subId,
        label: getFlowGraphLabel(subId, subAgent.name),
        kind: "subagent",
        position: { x, y: subYStart + (subIndex * subYGap) }
      });
      edges.push({ source: agent.nodeId, target: subId, kind: "control" });
    });
  });

  const lastMaster = sortedPlan[sortedPlan.length - 1];
  nodes.push({
    id: FLOW_OUTPUT_NODE_ID,
    label: getFlowGraphLabel(FLOW_OUTPUT_NODE_ID, "Output and Measurement"),
    kind: "output",
    position: { x: masterXStart + (sortedPlan.length * masterXGap), y: masterY }
  });
  if (lastMaster) edges.push({ source: lastMaster.nodeId, target: FLOW_OUTPUT_NODE_ID });

  return { nodes, edges };
}

function buildGraphForDemo(demo, plan) {
  const hierarchicalGraph = buildHierarchicalFlowGraph(plan);
  if (hierarchicalGraph.nodes.length) return hierarchicalGraph;
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

function edgeClassForKind(kind) {
  if (kind === "data") return "edge-data";
  if (kind === "control") return "edge-control";
  return "";
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
    fcCtrl = createFlowchart(canvas, [], {
      orientation: state.flowOrientation,
      columnCount: state.flowColumns,
      onNodeSelected: (id) => setState({ focusedNodeId: id, flowPanelNodeId: id || null })
    });
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
      classes: edgeClassForKind(e.kind)
    }))
  ];
  const sig = JSON.stringify(elements.map(e => e.data));
  if (sig !== fcKey) { fcCtrl.setElements(elements); fcKey = sig; }

  const errorIds = state.agentOutputs.filter(o => o.status === "error").map(o => o.nodeId);
  const issueIds = new Set([...(state.issueNodeIds || []), ...errorIds]);
  const activeIds = new Set([...state.runningNodeIds]);
  const completedIds = new Set(state.agentOutputs.filter(o => o.status === "done").map(o => o.nodeId));
  const failedIds = new Set([...issueIds]);
  const executionDetails = state.campaignStateObject?.executionDetails || {};

  if (state.stage === "run") activeIds.add(ORCHESTRATOR_NODE_ID);
  if (state.stage === "idle" && state.agentOutputs.length) completedIds.add(ORCHESTRATOR_NODE_ID);

  state.plan.forEach((agent) => {
    const output = state.agentOutputs.find((item) => item.nodeId === agent.nodeId);
    const subAgents = (state.subAgentCatalog?.[agent.nodeId] || []);
    subAgents.forEach((subAgent) => {
      const subId = subAgent.id;
      const subResult = executionDetails[agent.nodeId]?.subAgentResults?.find((item) => item.id === subId);
      if (state.runningNodeIds?.has(subId) || subResult?.status === "running") activeIds.add(subId);
      if (subResult?.status === "done") completedIds.add(subId);
      if (subResult?.status === "error") failedIds.add(subId);
      if (!subResult && output?.status === "error") failedIds.add(subId);
    });
  });

  const pendingIds = graph.nodes
    .map((node) => node.id)
    .filter((id) => !activeIds.has(id) && !completedIds.has(id) && !failedIds.has(id));

  fcCtrl.setNodeState({
    activeIds: [...activeIds],
    completedIds: [...completedIds],
    failedIds: [...failedIds],
    pendingIds,
    selectedId: state.focusedNodeId
  });
  fcCtrl.resize();
}

window.addEventListener("resize", () => scheduleFlowchartSync());
