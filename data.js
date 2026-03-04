const SEED = 42;

function mulberry32(seed) {
  let t = seed >>> 0;
  return function () {
    t += 0x6d2b79f5;
    let r = Math.imul(t ^ (t >>> 15), 1 | t);
    r ^= r + Math.imul(r ^ (r >>> 7), 61 | r);
    return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
  };
}

const rand = mulberry32(SEED);
const pick = (arr) => arr[Math.floor(rand() * arr.length)];
const chance = (p) => rand() < p;
const pad = (num, size) => String(num).padStart(size, "0");
const intBetween = (min, max) => Math.floor(rand() * (max - min + 1)) + min;

export const datasetMeta = {
  audienceGraph: {
    title: "Olli Audience Graph",
    columns: [
      "olli_household_id",
      "segment_name",
      "household_size",
      "device_count",
      "platforms",
      "dma_region",
      "income_bracket",
      "auto_intender_score",
    ],
  },
  inventoryMatrix: {
    title: "NEO / StreamX Inventory Matrix",
    columns: [
      "neo_order_id",
      "network",
      "platform_type",
      "daypart",
      "program_genre",
      "avail_impressions_30s",
      "cpm_30s",
      "fill_rate_pct",
      "country_code",
      "scte35_signal_status",
    ],
  },
  globalLawRegistry: {
    title: "Global Compliance Policy Engine",
    columns: [
      "country_code",
      "restriction_category",
      "network_scope",
      "restriction_type",
      "restriction_detail",
      "effective_date",
    ],
  },
  liveDeliveryLog: {
    title: "StreamX Live Delivery Log",
    columns: [
      "delivery_id",
      "campaign_id",
      "network",
      "platform_type",
      "hour_of_day",
      "planned_impressions",
      "delivered_impressions",
      "streamx_delivery_pct",
      "frequency_cap_hit",
      "scte35_signal_status",
      "viewer_overlap_pct",
    ],
  },
};

const audienceSegments = [
  "Auto Intenders",
  "First-Time Home Buyers",
  "Sports Enthusiasts",
  "Cord-Cutters",
  "Suburban Parents",
  "Luxury Travelers",
  "Streaming-First",
  "Value Shoppers",
  "Foodies",
  "News Loyalists",
];

const audiencePlatforms = [
  "cable_box",
  "max_app",
  "discovery_plus",
  "mobile_web",
  "connected_tv",
  "tablet",
];

const dmaRegions = [
  "NYC",
  "LA",
  "CHI",
  "DAL",
  "ATL",
  "SEA",
  "MIA",
  "PHX",
  "DEN",
  "BOS",
  "SFO",
  "MSP",
];

const incomeBrackets = ["low", "mid", "upper_mid", "high"];

export const audienceGraph = [];
for (let i = 1; i <= 100; i += 1) {
  const isAutoIntender = i <= 15;
  const segment = isAutoIntender ? "Auto Intenders" : pick(audienceSegments);
  const platforms = Array.from(
    new Set(
      Array.from({ length: intBetween(1, 3) }, () => pick(audiencePlatforms))
    )
  );
  audienceGraph.push({
    olli_household_id: `HH-${pad(i, 5)}`,
    segment_name: segment,
    household_size: intBetween(1, 6),
    device_count: intBetween(1, 8),
    platforms,
    dma_region: pick(dmaRegions),
    income_bracket: pick(incomeBrackets),
    auto_intender_score: isAutoIntender ? intBetween(71, 99) : intBetween(5, 68),
  });
}

// Data quality issues: duplicate household IDs
audienceGraph[92].olli_household_id = audienceGraph[4].olli_household_id;
audienceGraph[93].olli_household_id = audienceGraph[19].olli_household_id;
audienceGraph[94].olli_household_id = audienceGraph[32].olli_household_id;

// Data quality issues: null DMA region
audienceGraph[70].dma_region = null;
audienceGraph[88].dma_region = null;

const networks = [
  "TNT Sports",
  "CNN",
  "Food Network",
  "HGTV",
  "TLC",
  "Discovery Channel",
  "Max ad-lite",
  "Discovery+",
];

const dayparts = ["Morning", "Daytime", "Primetime", "Late Night"];
const genres = [
  "Live Sports",
  "Breaking News",
  "Reality",
  "Home & Lifestyle",
  "Food",
  "Documentary",
  "Drama",
  "Comedy",
];
const countries = ["US", "CA", "MX", "BR", "UK", "FR", "DE", "SA", "AE", "IN"];

export const inventoryMatrix = [];
let inventoryCounter = 1;

function pushInventoryRow(row) {
  inventoryMatrix.push({
    neo_order_id: `NEO-${pad(inventoryCounter++, 4)}`,
    ...row,
  });
}

// Force SCTE-35 issues on TNT Sports primetime
for (let i = 0; i < 5; i += 1) {
  pushInventoryRow({
    network: "TNT Sports",
    platform_type: "linear",
    daypart: "Primetime",
    program_genre: "Live Sports",
    avail_impressions_30s: intBetween(180000, 520000),
    cpm_30s: intBetween(28, 45),
    fill_rate_pct: intBetween(72, 94),
    country_code: "US",
    scte35_signal_status: i % 2 === 0 ? "degraded" : "missing",
  });
}

// Force high CPM Max ad-lite primetime rows
for (let i = 0; i < 3; i += 1) {
  pushInventoryRow({
    network: "Max ad-lite",
    platform_type: "digital",
    daypart: "Primetime",
    program_genre: pick(genres),
    avail_impressions_30s: intBetween(140000, 360000),
    cpm_30s: intBetween(60, 65),
    fill_rate_pct: intBetween(70, 90),
    country_code: "US",
    scte35_signal_status: "active",
  });
}

// Force CNN Saudi Arabia inventory to trigger compliance
pushInventoryRow({
  network: "CNN",
  platform_type: "linear",
  daypart: "Primetime",
  program_genre: "Breaking News",
  avail_impressions_30s: intBetween(90000, 180000),
  cpm_30s: intBetween(30, 52),
  fill_rate_pct: intBetween(65, 88),
  country_code: "SA",
  scte35_signal_status: "active",
});

while (inventoryMatrix.length < 80) {
  const network = pick(networks);
  const platform_type = network === "Max ad-lite" || network === "Discovery+"
    ? "digital"
    : "linear";
  pushInventoryRow({
    network,
    platform_type,
    daypart: pick(dayparts),
    program_genre: pick(genres),
    avail_impressions_30s: intBetween(50000, 880000),
    cpm_30s: intBetween(8, 58),
    fill_rate_pct: intBetween(45, 98),
    country_code: pick(countries),
    scte35_signal_status: chance(0.12) ? pick(["degraded", "missing"]) : "active",
  });
}

// Deprecated SKU flags embedded in order IDs
inventoryMatrix[11].neo_order_id = `${inventoryMatrix[11].neo_order_id}-DEP`;
inventoryMatrix[26].neo_order_id = `${inventoryMatrix[26].neo_order_id}-DEP`;
inventoryMatrix[41].neo_order_id = `${inventoryMatrix[41].neo_order_id}-DEP`;

const restrictionCategories = ["alcohol", "gambling", "financial_services", "pharma", "political", "tobacco"];
const restrictionTypes = ["banned", "time_restricted", "disclaimer_required"];

export const globalLawRegistry = [
  {
    country_code: "SA",
    restriction_category: "financial_services",
    network_scope: "all",
    restriction_type: "banned",
    restriction_detail: "Financial services advertising prohibited in broadcast and digital placements.",
    effective_date: "2023-06-01",
  },
  {
    country_code: "FR",
    restriction_category: "alcohol",
    network_scope: "all",
    restriction_type: "time_restricted",
    restriction_detail: "Alcohol ads allowed only after 21:00 local time.",
    effective_date: "2022-01-15",
  },
  {
    country_code: "DE",
    restriction_category: "pharma",
    network_scope: "all",
    restriction_type: "disclaimer_required",
    restriction_detail: "Pharma ads must include on-screen safety disclaimer in German.",
    effective_date: "2021-09-10",
  },
];

while (globalLawRegistry.length < 60) {
  globalLawRegistry.push({
    country_code: pick(countries),
    restriction_category: pick(restrictionCategories),
    network_scope: chance(0.2) ? pick(networks) : "all",
    restriction_type: pick(restrictionTypes),
    restriction_detail: `Regulation ${intBetween(100, 999)}-${intBetween(1, 9)} compliance required.`,
    effective_date: `202${intBetween(1, 5)}-${pad(intBetween(1, 12), 2)}-${pad(intBetween(1, 28), 2)}`,
  });
}

const logNetworks = ["TNT Sports", "CNN", "Food Network", "Max ad-lite", "Discovery+"];
export const liveDeliveryLog = [];
let deliveryId = 1;

for (let hour = 1; hour <= 24; hour += 1) {
  for (const network of logNetworks) {
    const platform_type = network === "Max ad-lite" || network === "Discovery+"
      ? "digital"
      : "linear";
    const planned = intBetween(9000, 28000);
    let delivered = Math.round(planned * (0.85 + rand() * 0.3));
    let delivery_pct = Math.round((delivered / planned) * 100);
    let scte35_signal_fired = platform_type === "digital" ? true : chance(0.9);

    if (hour === 12 && network === "TNT Sports") {
      delivered = Math.round(planned * 0.42);
      delivery_pct = 42;
      scte35_signal_fired = false;
    }
    if (hour === 12 && network === "Max ad-lite") {
      delivered = Math.round(planned * 1.15);
      delivery_pct = 115;
    }

    liveDeliveryLog.push({
      delivery_id: `DLV-${pad(deliveryId++, 5)}`,
      campaign_id: "CAMP-AUTO-50K",
      network,
      platform_type,
      hour_of_day: hour,
      planned_impressions: planned,
      delivered_impressions: delivered,
      streamx_delivery_pct: delivery_pct,
      frequency_cap_hit: chance(0.12),
      scte35_signal_status: scte35_signal_fired ? "fired" : "missing",
      viewer_overlap_pct: intBetween(6, 45),
    });
  }
}

// Timezone offset issues (non-standard hour values)
if (liveDeliveryLog[9]) liveDeliveryLog[9].hour_of_day = "12-0500";
if (liveDeliveryLog[57]) liveDeliveryLog[57].hour_of_day = "19+0100";

export const datasets = {
  audienceGraph,
  inventoryMatrix,
  globalLawRegistry,
  liveDeliveryLog,
};

function csvEscape(value) {
  if (value == null) return "";
  const text = Array.isArray(value) ? JSON.stringify(value) : value.toString();
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

export function toCsv(rows, columns) {
  if (!rows || !rows.length) return columns.join(",");
  const header = columns.join(",");
  const lines = rows.map((row) =>
    columns.map((col) => csvEscape(row[col])).join(",")
  );
  return [header, ...lines].join("\n");
}

export const datasetEntries = Object.entries(datasetMeta).map(([key, meta]) => ({
  key,
  title: meta.title,
  type: "csv",
  content: toCsv(datasets[key], meta.columns),
}));
