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
  unifiedAudienceDataset: {
    title: "Unified Audience Base Dataset",
    columns: [
      "unified_household_id",
      "state_code",
      "segment_name",
      "identity_confidence_score",
      "streaming_affinity_index",
      "linear_affinity_index",
      "social_affinity_index",
      "avg_weekly_media_minutes",
      "recent_conversion_propensity",
    ],
  },
  yieldIntelligenceFeed: {
    title: "Yield Intelligence Feed (Twitter, News, External)",
    columns: [
      "signal_id",
      "source_type",
      "topic",
      "region",
      "spike_window",
      "signal_strength",
      "recommended_channel",
      "expected_roi_lift_pct",
    ],
  },
  complianceRulebook: {
    title: "Advertising Compliance Rulebook (50 Rules)",
    columns: [
      "rule_id",
      "rule_category",
      "rule_description",
      "severity",
      "applies_geo",
      "applies_platform",
      "applies_product",
      "applies_audience",
      "applies_schedule",
      "required_action",
      "required_evidence",
      "trigger_terms",
      "suggested_fix",
      "source_reference",
    ],
  },
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

export const unifiedAudienceDataset = audienceGraph.map((row, idx) => {
  const streamingAffinity = intBetween(30, 96);
  const linearAffinity = intBetween(28, 94);
  const socialAffinity = intBetween(24, 90);
  const confidenceBase = row.auto_intender_score > 70 ? intBetween(82, 98) : intBetween(60, 90);
  const conversionPropensity = Math.max(4, Math.min(99, Math.round((row.auto_intender_score * 0.75) + intBetween(2, 20))));
  return {
    unified_household_id: `UA-${pad(idx + 1, 5)}`,
    state_code: row.dma_region || pick(["CA", "NY", "TX", "FL", "IL", "PA", "WA", "GA", "NC", "OH"]),
    segment_name: row.segment_name,
    identity_confidence_score: confidenceBase,
    streaming_affinity_index: streamingAffinity,
    linear_affinity_index: linearAffinity,
    social_affinity_index: socialAffinity,
    avg_weekly_media_minutes: intBetween(320, 1680),
    recent_conversion_propensity: conversionPropensity,
  };
});

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

const intelligenceTopics = [
  "Auto awards show buzz",
  "Fuel price policy update",
  "Celebrity motorsport appearance",
  "New vehicle launch coverage",
  "Regional weather mobility alert",
  "National holiday travel planning",
  "Sports finals social surge",
  "Prime-time interview trend",
  "Breaking consumer finance story",
  "Major product review cycle"
];

const intelligenceRegions = ["US-CA", "US-TX", "US-NY", "US-FL", "DE-BE", "IN-MH", "UK-LON", "AE-DU"];
const intelligenceSources = ["twitter_trends", "news_signal", "external_feed"];
const intelligenceChannels = ["streaming_video", "linear_television", "social_extension", "balanced_mix"];

export const yieldIntelligenceFeed = [];
for (let i = 1; i <= 36; i += 1) {
  const sourceType = pick(intelligenceSources);
  const topic = pick(intelligenceTopics);
  const region = pick(intelligenceRegions);
  const startHour = intBetween(6, 22);
  const endHour = Math.min(23, startHour + intBetween(1, 3));
  yieldIntelligenceFeed.push({
    signal_id: `YLD-${pad(i, 4)}`,
    source_type: sourceType,
    topic,
    region,
    spike_window: `${pad(startHour, 2)}:00-${pad(endHour, 2)}:59`,
    signal_strength: intBetween(55, 99),
    recommended_channel: pick(intelligenceChannels),
    expected_roi_lift_pct: intBetween(4, 22),
  });
}

function makeComplianceRule(rule) {
  return {
    rule_id: rule.rule_id,
    rule_category: rule.rule_category,
    rule_description: rule.rule_description,
    severity: rule.severity || "medium",
    applies_geo: rule.applies_geo || "GLOBAL",
    applies_platform: rule.applies_platform || "any",
    applies_product: rule.applies_product || "any",
    applies_audience: rule.applies_audience || "any",
    applies_schedule: rule.applies_schedule || "any",
    required_action: rule.required_action || "enforce_policy",
    required_evidence: rule.required_evidence || "",
    trigger_terms: rule.trigger_terms || "",
    suggested_fix: rule.suggested_fix || "Review targeting, content, and market controls before activation.",
    source_reference: rule.source_reference || "Global advertising policy baseline",
  };
}

export const complianceRulebook = [
  makeComplianceRule({
    rule_id: 1,
    rule_category: "restricted_product",
    rule_description: "Alcoholic beverages cannot be advertised in India on television.",
    severity: "high",
    applies_geo: "IN",
    applies_platform: "tv|broadcast|linear",
    applies_product: "alcohol",
    required_action: "block",
    suggested_fix: "Remove alcohol creative from India television inventory or reroute to compliant markets.",
    source_reference: "India broadcast advertising restrictions baseline"
  }),
  makeComplianceRule({
    rule_id: 2,
    rule_category: "restricted_product",
    rule_description: "Tobacco products and cigarettes cannot be advertised across most broadcast media globally.",
    severity: "high",
    applies_geo: "GLOBAL",
    applies_platform: "broadcast|tv|linear",
    applies_product: "tobacco",
    required_action: "block",
    suggested_fix: "Remove tobacco category promotion from broadcast delivery.",
    source_reference: "Global tobacco advertising restriction baseline"
  }),
  makeComplianceRule({
    rule_id: 3,
    rule_category: "restricted_product",
    rule_description: "Gambling advertisements must not target minors.",
    severity: "high",
    applies_product: "gambling|betting",
    applies_audience: "minors|under_18",
    required_action: "block",
    suggested_fix: "Exclude under-18 audiences from gambling targeting segments.",
    source_reference: "Global youth protection advertising baseline"
  }),
  makeComplianceRule({
    rule_id: 4,
    rule_category: "restricted_product",
    rule_description: "Prescription drug advertisements must include medical disclaimers.",
    severity: "high",
    applies_product: "prescription_drugs|pharma",
    required_action: "require_disclaimer",
    required_evidence: "medical disclaimer|safety disclaimer",
    suggested_fix: "Add required medical and safety disclaimer text before trafficking.",
    source_reference: "Healthcare advertising disclaimer baseline"
  }),
  makeComplianceRule({
    rule_id: 5,
    rule_category: "restricted_product",
    rule_description: "Cannabis advertisements are restricted in regions where it is illegal.",
    severity: "high",
    applies_geo: "IN|SA|AE|US_STATE_RESTRICTED",
    applies_product: "cannabis",
    required_action: "block",
    suggested_fix: "Limit cannabis promotions to verified legal markets and remove restricted geos.",
    source_reference: "Regional cannabis legality baseline"
  }),
  makeComplianceRule({
    rule_id: 6,
    rule_category: "restricted_product",
    rule_description: "Financial investment advertisements must disclose risk statements.",
    severity: "high",
    applies_product: "financial_services|investment",
    required_action: "require_disclaimer",
    required_evidence: "risk statement|investment risk",
    suggested_fix: "Add clear investment risk disclosures in the ad creative and landing page.",
    source_reference: "Financial promotion disclosure baseline"
  }),
  makeComplianceRule({
    rule_id: 7,
    rule_category: "restricted_product",
    rule_description: "Cryptocurrency advertisements must include volatility warnings.",
    severity: "high",
    applies_product: "cryptocurrency|crypto",
    required_action: "require_disclaimer",
    required_evidence: "volatility warning|crypto risk",
    suggested_fix: "Include explicit volatility and loss risk language in all cryptocurrency creatives.",
    source_reference: "Crypto advertising warning baseline"
  }),
  makeComplianceRule({
    rule_id: 8,
    rule_category: "restricted_product",
    rule_description: "Political advertisements must include sponsor disclosure.",
    severity: "high",
    applies_product: "political",
    required_action: "require_disclosure",
    required_evidence: "sponsor disclosure|paid for by",
    suggested_fix: "Add sponsor disclosure metadata and visible sponsor text before launch.",
    source_reference: "Election advertising transparency baseline"
  }),
  makeComplianceRule({
    rule_id: 9,
    rule_category: "restricted_product",
    rule_description: "Adult content advertising is restricted during daytime broadcast hours.",
    severity: "high",
    applies_product: "adult_content",
    applies_platform: "tv|broadcast|linear",
    applies_schedule: "daytime",
    required_action: "block",
    suggested_fix: "Move adult content placements out of daytime windows or remove the creative.",
    source_reference: "Broadcast watershed policy baseline"
  }),
  makeComplianceRule({
    rule_id: 10,
    rule_category: "restricted_product",
    rule_description: "Betting advertisements cannot appear during live sports targeting minors.",
    severity: "high",
    applies_product: "betting|gambling",
    applies_platform: "live_sports|sports",
    applies_audience: "minors|under_18",
    applies_schedule: "live_sports_minor",
    required_action: "block",
    suggested_fix: "Exclude minors and youth proxy segments from live sports betting delivery.",
    source_reference: "Sports betting youth protection baseline"
  }),
  makeComplianceRule({
    rule_id: 11,
    rule_category: "targeting",
    rule_description: "Ads for alcohol cannot target audiences below 21 years.",
    severity: "high",
    applies_product: "alcohol",
    applies_audience: "under_21",
    required_action: "block",
    suggested_fix: "Set age floor to 21 and remove age-unknown audience pools.",
    source_reference: "Age-gated alcohol targeting baseline"
  }),
  makeComplianceRule({
    rule_id: 12,
    rule_category: "targeting",
    rule_description: "Gambling ads cannot target under-18 demographics.",
    severity: "high",
    applies_product: "gambling|betting",
    applies_audience: "under_18|minors",
    required_action: "block",
    suggested_fix: "Exclude under-18 and minor proxy segments from all gambling campaigns.",
    source_reference: "Youth gambling targeting restrictions"
  }),
  makeComplianceRule({
    rule_id: 13,
    rule_category: "targeting",
    rule_description: "Children's products must not collect personal data without parental consent.",
    severity: "high",
    applies_product: "children_products",
    applies_audience: "children",
    required_action: "require_consent",
    required_evidence: "parental consent|guardian consent",
    suggested_fix: "Enable parental consent flow and suppress personal data collection until consent is captured.",
    source_reference: "Children data consent baseline"
  }),
  makeComplianceRule({
    rule_id: 14,
    rule_category: "targeting",
    rule_description: "Weight-loss products cannot target teenagers.",
    severity: "high",
    applies_product: "weight_loss",
    applies_audience: "teenagers",
    required_action: "block",
    suggested_fix: "Exclude teenage audiences from weight-loss targeting criteria.",
    source_reference: "Health-sensitive youth targeting restrictions"
  }),
  makeComplianceRule({
    rule_id: 15,
    rule_category: "targeting",
    rule_description: "Financial investment ads must not target financially vulnerable users.",
    severity: "high",
    applies_product: "financial_services|investment",
    applies_audience: "vulnerable_financial",
    required_action: "block",
    suggested_fix: "Remove vulnerable financial cohorts and re-target to eligible adult investor segments.",
    source_reference: "Responsible finance advertising baseline"
  }),
  makeComplianceRule({
    rule_id: 16,
    rule_category: "targeting",
    rule_description: "Political ads cannot target users based on religion or ethnicity.",
    severity: "high",
    applies_product: "political",
    applies_audience: "religion|ethnicity",
    required_action: "block",
    suggested_fix: "Remove religion and ethnicity attributes from political targeting settings.",
    source_reference: "Political sensitive-attribute targeting restrictions"
  }),
  makeComplianceRule({
    rule_id: 17,
    rule_category: "targeting",
    rule_description: "Ads must not discriminate based on race, gender, or religion.",
    severity: "high",
    applies_audience: "race|gender|religion",
    required_action: "block",
    suggested_fix: "Remove discriminatory targeting and update policy filters for protected classes.",
    source_reference: "Anti-discrimination advertising baseline"
  }),
  makeComplianceRule({
    rule_id: 18,
    rule_category: "targeting",
    rule_description: "Job advertisements must follow equal opportunity policies.",
    severity: "high",
    applies_product: "job",
    required_action: "require_disclosure",
    required_evidence: "equal opportunity|inclusive hiring",
    suggested_fix: "Add equal opportunity policy language and remove discriminatory audience filters.",
    source_reference: "Employment ad policy baseline"
  }),
  makeComplianceRule({
    rule_id: 19,
    rule_category: "targeting",
    rule_description: "Housing advertisements must not exclude protected demographic groups.",
    severity: "high",
    applies_product: "housing",
    required_action: "block",
    suggested_fix: "Remove protected demographic exclusions and re-run fair housing checks.",
    source_reference: "Housing advertising fairness baseline"
  }),
  makeComplianceRule({
    rule_id: 20,
    rule_category: "targeting",
    rule_description: "Credit advertisements must not target economically vulnerable populations.",
    severity: "high",
    applies_product: "credit|financial_services",
    applies_audience: "vulnerable_financial",
    required_action: "block",
    suggested_fix: "Exclude economically vulnerable populations from credit targeting pools.",
    source_reference: "Responsible lending ad policy baseline"
  }),
  makeComplianceRule({
    rule_id: 21,
    rule_category: "content",
    rule_description: "Ads must not contain false or misleading claims.",
    severity: "high",
    trigger_terms: "guaranteed returns|risk free|no risk|100 percent success",
    required_action: "enforce_policy",
    suggested_fix: "Remove unsubstantiated claim language and provide verifiable evidence statements.",
    source_reference: "Truth in advertising baseline"
  }),
  makeComplianceRule({
    rule_id: 22,
    rule_category: "content",
    rule_description: "Ads must clearly disclose paid sponsorship.",
    severity: "medium",
    required_action: "require_disclosure",
    required_evidence: "sponsored|paid partnership|advertisement",
    suggested_fix: "Add paid sponsorship disclosure in creative and metadata.",
    source_reference: "Sponsored content disclosure baseline"
  }),
  makeComplianceRule({
    rule_id: 23,
    rule_category: "content",
    rule_description: "Ads cannot contain hate speech or offensive language.",
    severity: "high",
    trigger_terms: "hate speech|racial slur|offensive term",
    required_action: "block",
    suggested_fix: "Remove offensive language and submit revised creative for moderation.",
    source_reference: "Platform hate speech policy baseline"
  }),
  makeComplianceRule({
    rule_id: 24,
    rule_category: "content",
    rule_description: "Ads must not misrepresent product capabilities.",
    severity: "high",
    trigger_terms: "instant cure|guaranteed outcome|works for everyone",
    required_action: "enforce_policy",
    suggested_fix: "Align claims with documented product capabilities and approved substantiation.",
    source_reference: "Product claim substantiation baseline"
  }),
  makeComplianceRule({
    rule_id: 25,
    rule_category: "content",
    rule_description: "Ads cannot contain deepfake or deceptive synthetic media without disclosure.",
    severity: "high",
    trigger_terms: "deepfake|synthetic avatar|ai generated spokesperson",
    required_action: "require_disclosure",
    required_evidence: "synthetic media disclosure|ai disclosure",
    suggested_fix: "Add clear synthetic media disclosure and human review approval.",
    source_reference: "Synthetic media disclosure baseline"
  }),
  makeComplianceRule({
    rule_id: 26,
    rule_category: "content",
    rule_description: "Ads cannot promote illegal activities.",
    severity: "high",
    trigger_terms: "illegal activity|counterfeit|unlicensed",
    required_action: "block",
    suggested_fix: "Remove illegal activity references and redirect campaign to lawful offers.",
    source_reference: "Illegal activity promotion prohibition baseline"
  }),
  makeComplianceRule({
    rule_id: 27,
    rule_category: "content",
    rule_description: "Ads cannot include graphic violence.",
    severity: "high",
    trigger_terms: "graphic violence|bloodshed|gore",
    required_action: "block",
    suggested_fix: "Replace violent creative with brand-safe alternatives.",
    source_reference: "Graphic violence content policy baseline"
  }),
  makeComplianceRule({
    rule_id: 28,
    rule_category: "content",
    rule_description: "Ads cannot promote self-harm behaviors.",
    severity: "high",
    trigger_terms: "self harm|suicide encouragement",
    required_action: "block",
    suggested_fix: "Remove self-harm language and escalate for policy review.",
    source_reference: "Self-harm prevention policy baseline"
  }),
  makeComplianceRule({
    rule_id: 29,
    rule_category: "content",
    rule_description: "Ads must clearly disclose limited time offers conditions.",
    severity: "medium",
    trigger_terms: "limited time offer|today only|expires soon",
    required_action: "require_disclosure",
    required_evidence: "offer terms|conditions apply",
    suggested_fix: "Add explicit offer conditions and expiry criteria to the creative.",
    source_reference: "Promotional offer disclosure baseline"
  }),
  makeComplianceRule({
    rule_id: 30,
    rule_category: "content",
    rule_description: "Ads must not impersonate news organizations or government bodies.",
    severity: "high",
    trigger_terms: "official government notice|breaking news alert|public authority notice",
    required_action: "block",
    suggested_fix: "Remove impersonation cues and brand as advertiser content.",
    source_reference: "Impersonation policy baseline"
  }),
  makeComplianceRule({
    rule_id: 31,
    rule_category: "geographic",
    rule_description: "Ads must comply with local country regulations where they are delivered.",
    severity: "high",
    trigger_terms: "global|cross-border|international|multi-country",
    required_action: "enforce_policy",
    required_evidence: "local compliance mapping",
    suggested_fix: "Attach country-level compliance mapping before activation.",
    source_reference: "Cross-border regulatory baseline"
  }),
  makeComplianceRule({
    rule_id: 32,
    rule_category: "geographic",
    rule_description: "Alcohol advertising is banned in Saudi Arabia and several Middle Eastern countries.",
    severity: "high",
    applies_geo: "SA|AE|MIDDLE_EAST",
    applies_product: "alcohol",
    required_action: "block",
    suggested_fix: "Remove alcohol ads from Saudi Arabia and affected Middle Eastern markets.",
    source_reference: "Middle East alcohol advertising restriction baseline"
  }),
  makeComplianceRule({
    rule_id: 33,
    rule_category: "geographic",
    rule_description: "Gambling ads are restricted in India and some United States states.",
    severity: "high",
    applies_geo: "IN|US_STATE_RESTRICTED",
    applies_product: "gambling|betting",
    required_action: "block",
    suggested_fix: "Exclude India and restricted United States states from gambling delivery.",
    source_reference: "Regional gambling advertising restriction baseline"
  }),
  makeComplianceRule({
    rule_id: 34,
    rule_category: "geographic",
    rule_description: "Pharmaceutical ads must comply with FDA or CDSCO regulations.",
    severity: "high",
    applies_geo: "US|IN",
    applies_product: "pharma|prescription_drugs",
    required_action: "require_disclaimer",
    required_evidence: "fda|cdsco|medical disclaimer",
    suggested_fix: "Add regulator-aligned pharmaceutical disclaimers before publishing.",
    source_reference: "FDA and CDSCO promotional baseline"
  }),
  makeComplianceRule({
    rule_id: 35,
    rule_category: "privacy",
    rule_description: "Ads in the European Union must comply with General Data Protection Regulation consent requirements.",
    severity: "high",
    applies_geo: "EU",
    required_action: "require_consent",
    required_evidence: "gdpr consent|consent record",
    suggested_fix: "Enable General Data Protection Regulation consent capture for European Union audiences.",
    source_reference: "General Data Protection Regulation consent baseline"
  }),
  makeComplianceRule({
    rule_id: 36,
    rule_category: "privacy",
    rule_description: "Ads targeting children in the United States must comply with COPPA regulations.",
    severity: "high",
    applies_geo: "US",
    applies_audience: "children",
    required_action: "require_consent",
    required_evidence: "coppa|parental consent",
    suggested_fix: "Apply COPPA-compliant parental consent workflow for child-targeted campaigns.",
    source_reference: "COPPA compliance baseline"
  }),
  makeComplianceRule({
    rule_id: 37,
    rule_category: "geographic",
    rule_description: "Financial ads must comply with SEC and SEBI guidelines.",
    severity: "high",
    applies_geo: "US|IN",
    applies_product: "financial_services|investment",
    required_action: "require_disclaimer",
    required_evidence: "sec|sebi|risk disclosure",
    suggested_fix: "Attach Securities and Exchange Commission or SEBI-aligned risk disclosures.",
    source_reference: "SEC and SEBI financial promotion baseline"
  }),
  makeComplianceRule({
    rule_id: 38,
    rule_category: "privacy",
    rule_description: "Data collection in ads must comply with regional privacy laws.",
    severity: "high",
    trigger_terms: "data collection|tracking|pixel|cookies",
    required_action: "require_consent",
    required_evidence: "privacy consent|lawful basis",
    suggested_fix: "Collect explicit regional privacy consent before enabling data collection.",
    source_reference: "Regional privacy compliance baseline"
  }),
  makeComplianceRule({
    rule_id: 39,
    rule_category: "privacy",
    rule_description: "Ads targeting European Union audiences must include cookie consent mechanisms.",
    severity: "high",
    applies_geo: "EU",
    trigger_terms: "cookie|tracking",
    required_action: "require_consent",
    required_evidence: "cookie consent",
    suggested_fix: "Enable cookie consent banner and consent storage for European Union targeting.",
    source_reference: "European Union cookie consent baseline"
  }),
  makeComplianceRule({
    rule_id: 40,
    rule_category: "geographic",
    rule_description: "Political ads must follow election commission advertising guidelines.",
    severity: "high",
    applies_product: "political",
    required_action: "require_disclosure",
    required_evidence: "election compliance|sponsor disclosure",
    suggested_fix: "Attach election compliance declaration and sponsor identification.",
    source_reference: "Election commission political ad baseline"
  }),
  makeComplianceRule({
    rule_id: 41,
    rule_category: "platform",
    rule_description: "Ads must not exceed allowed ad duration limits.",
    severity: "medium",
    applies_platform: "tv|streaming|digital|video",
    trigger_terms: "seconds|duration",
    required_action: "enforce_policy",
    required_evidence: "duration compliant",
    suggested_fix: "Adjust creative duration to fit the platform limit policy.",
    source_reference: "Platform duration policy baseline"
  }),
  makeComplianceRule({
    rule_id: 42,
    rule_category: "platform",
    rule_description: "Ads must not autoplay audio without user consent.",
    severity: "high",
    trigger_terms: "autoplay audio",
    required_action: "require_consent",
    required_evidence: "user consent",
    suggested_fix: "Disable autoplay audio or capture explicit user consent before playback.",
    source_reference: "Autoplay audio consent baseline"
  }),
  makeComplianceRule({
    rule_id: 43,
    rule_category: "platform",
    rule_description: "Ads must follow platform content moderation guidelines.",
    severity: "medium",
    required_action: "enforce_policy",
    required_evidence: "moderation approved",
    suggested_fix: "Route creatives through platform moderation checks before publishing.",
    source_reference: "Platform moderation policy baseline"
  }),
  makeComplianceRule({
    rule_id: 44,
    rule_category: "platform",
    rule_description: "Ads must not contain malicious links or phishing content.",
    severity: "high",
    trigger_terms: "link|http|redirect",
    required_action: "block",
    required_evidence: "link safety scan",
    suggested_fix: "Replace unsafe links and complete phishing and malware scanning.",
    source_reference: "Malicious content policy baseline"
  }),
  makeComplianceRule({
    rule_id: 45,
    rule_category: "platform",
    rule_description: "Ads must not redirect users to unsafe websites.",
    severity: "high",
    trigger_terms: "redirect|link",
    required_action: "block",
    required_evidence: "safe destination validation",
    suggested_fix: "Use verified safe landing pages and block unsafe redirect chains.",
    source_reference: "Landing page safety policy baseline"
  }),
  makeComplianceRule({
    rule_id: 46,
    rule_category: "platform",
    rule_description: "Ads must respect frequency capping limits.",
    severity: "medium",
    required_action: "enforce_frequency",
    required_evidence: "frequency cap",
    suggested_fix: "Apply frequency caps before launch and monitor cap breaches in flight.",
    source_reference: "Frequency governance policy baseline"
  }),
  makeComplianceRule({
    rule_id: 47,
    rule_category: "platform",
    rule_description: "Ads must not violate brand safety standards.",
    severity: "high",
    required_action: "enforce_safety",
    required_evidence: "brand safety",
    suggested_fix: "Apply brand safety filters and remove unsafe contextual placements.",
    source_reference: "Brand safety policy baseline"
  }),
  makeComplianceRule({
    rule_id: 48,
    rule_category: "platform",
    rule_description: "Ads must not run in sensitive content environments.",
    severity: "high",
    required_action: "enforce_safety",
    required_evidence: "sensitive exclusion",
    suggested_fix: "Enable sensitive content exclusion lists for all placements.",
    source_reference: "Sensitive inventory exclusion baseline"
  }),
  makeComplianceRule({
    rule_id: 49,
    rule_category: "platform",
    rule_description: "Ads must respect user opt-out preferences.",
    severity: "high",
    required_action: "require_consent",
    required_evidence: "opt-out respected|consent preference",
    suggested_fix: "Honor user opt-out state and suppress delivery for opted-out users.",
    source_reference: "User choice and preference compliance baseline"
  }),
  makeComplianceRule({
    rule_id: 50,
    rule_category: "platform",
    rule_description: "Ads must include proper advertiser identification metadata.",
    severity: "medium",
    required_action: "enforce_metadata",
    required_evidence: "advertiser id|sponsor metadata",
    suggested_fix: "Attach advertiser identification metadata before trafficking.",
    source_reference: "Advertiser transparency metadata baseline"
  }),
];

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
    country_code: "IN",
    restriction_category: "alcohol",
    network_scope: "all",
    restriction_type: "banned",
    restriction_detail: "Alcohol advertisements cannot run in India for this campaign category and must be replaced with compliant alternatives.",
    effective_date: "2024-01-01",
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
  unifiedAudienceDataset,
  yieldIntelligenceFeed,
  complianceRulebook,
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
