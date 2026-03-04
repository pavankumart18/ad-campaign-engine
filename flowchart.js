import cytoscape from "cytoscape";

const NODE_BASE_STYLE = {
  "background-color": "#3b3f57",
  "border-width": 3,
  "border-color": "#5a607b",
  "border-style": "solid",
  shape: "round-rectangle",
  label: "data(label)",
  "text-valign": "center",
  "text-halign": "center",
  color: "#ffffff",
  "text-outline-width": 0,
  "text-outline-opacity": 0,
  padding: "18px",
  "font-family": "Space Grotesk, Segoe UI, -apple-system, sans-serif",
  "font-size": "15px",
  "font-weight": "600",
  "text-wrap": "wrap",
  "text-max-width": "260px",
  "text-margin-y": 0,
  width: "label",
  height: "label",
  "min-zoomed-font-size": 10,
  "overlay-opacity": 0,
  "transition-property": "background-color, border-color, shadow-blur",
  "transition-duration": "200ms",
};

const EDGE_BASE_STYLE = {
  width: 4,
  "line-color": "#ff6600",
  "target-arrow-color": "#ff6600",
  "target-arrow-shape": "triangle",
  "target-arrow-fill": "filled",
  "arrow-scale": 1.8,
  "curve-style": "bezier",
  opacity: 1,
  "line-style": "solid",
  "source-endpoint": "outside-to-node",
  "target-endpoint": "outside-to-node",
  "target-distance-from-node": 5,
};

const STATE_STYLES = [
  {
    selector: "node.is-active",
    style: {
      "background-color": "#ffb703",
      "border-color": "#fb8500",
      color: "#2b261f",
      "shadow-blur": 20,
      "shadow-color": "#ffb70366",
      "shadow-opacity": 1,
      "shadow-offset-x": 0,
      "shadow-offset-y": 0,
    },
  },
  {
    selector: "node.is-complete",
    style: {
      "background-color": "#2d6a4f",
      "border-color": "#1b4332",
    },
  },
  {
    selector: "node.is-selected",
    style: {
      "border-color": "#ffd166",
      "border-width": 5,
      "shadow-blur": 18,
      "shadow-color": "#ffd16666",
      "shadow-opacity": 1,
      "shadow-offset-x": 0,
      "shadow-offset-y": 0,
    },
  },
  {
    selector: "node.is-failed",
    style: {
      "background-color": "#9b2226",
      "border-color": "#ae2012",
    },
  },
  {
    selector: "node.node-system",
    style: {
      "background-color": "#2a2f4c",
      "border-color": "#3e4566",
      "font-size": "13px",
      "font-weight": "600",
    },
  },
  {
    selector: "node.node-data",
    style: {
      "background-color": "#20243a",
      "border-color": "#3a4164",
      "border-style": "dashed",
      "font-size": "12px",
      "font-weight": "500",
    },
  },
  {
    selector: "node.node-output",
    style: {
      "background-color": "#303658",
      "border-color": "#ff6600",
    },
  },
  {
    selector: "edge.edge-data",
    style: {
      "line-style": "dashed",
      "line-color": "#4a5176",
      "target-arrow-color": "#4a5176",
      "target-arrow-shape": "triangle",
      "arrow-scale": 1.2,
      opacity: 0.8,
    },
  },
  {
    selector: "edge.edge-hover",
    style: {
      width: 5,
      "line-color": "#ffd166",
      "target-arrow-color": "#ffd166",
      "arrow-scale": 2.1,
      opacity: 1,
    },
  },
  {
    selector: "node.node-hover",
    style: {
      "border-color": "#ffd166",
      "shadow-blur": 16,
      "shadow-color": "#ffd16666",
      "shadow-opacity": 1,
    },
  },
  {
    selector: "node.node-neighbor",
    style: {
      "border-color": "#6dd3fb",
    },
  },
];

const DEFAULT_OPTIONS = {
  orientation: "horizontal",
  animate: false,
  columnCount: 2,
  onNodeSelected: () => { },
};

export function createFlowchart(containerOrSelector, elements = [], options = {}) {
  const mergedOptions = { ...DEFAULT_OPTIONS, ...options };
  const container =
    typeof containerOrSelector === "string" ? document.querySelector(containerOrSelector) : containerOrSelector;
  if (!container) throw new Error("Flowchart container not found.");

  const layoutOptions = createLayoutOptions(mergedOptions.orientation, mergedOptions.columnCount);

  const cy = cytoscape({
    container,
    elements: [],
    layout: layoutOptions,
    style: [
      { selector: "node", style: NODE_BASE_STYLE },
      { selector: "edge", style: EDGE_BASE_STYLE },
      ...STATE_STYLES,
    ],
    userZoomingEnabled: true,
    userPanningEnabled: true,
  });

  let nodeState = { activeIds: [], completedIds: [], failedIds: [], selectedId: null };
  let docClickHandler = null;
  function alignDataNodes() {
    const dataNodes = cy.nodes(".node-data");
    if (!dataNodes.length) return;
    dataNodes.forEach((node) => {
      const bindTo = node.data("bindTo");
      if (!bindTo) return;
      const target = cy.getElementById(bindTo);
      if (!target || target.empty()) return;
      const targetPos = target.position();
      const offset = 240;
      node.position({ x: targetPos.x + offset, y: targetPos.y });
    });
  }

  function attachDocumentHandler() {
    if (docClickHandler) return;
    docClickHandler = (event) => {
      if (!container.contains(event.target)) {
        mergedOptions.onNodeSelected?.(null);
      }
    };
    document.addEventListener("click", docClickHandler);
  }

  function detachDocumentHandler() {
    if (!docClickHandler) return;
    document.removeEventListener("click", docClickHandler);
    docClickHandler = null;
  }

  attachDocumentHandler();

  cy.on("tap", "node", (event) => {
    mergedOptions.onNodeSelected?.(event.target.id());
  });

  cy.on("tap", (event) => {
    if (event.target === cy) {
      mergedOptions.onNodeSelected?.(null);
    }
  });

  cy.on("mouseover", "node", (event) => {
    const node = event.target;
    node.addClass("node-hover");
    node.connectedEdges().addClass("edge-hover");
    node.connectedNodes().addClass("node-neighbor");
  });

  cy.on("mouseout", "node", () => {
    cy.nodes().removeClass("node-hover node-neighbor");
    cy.edges().removeClass("edge-hover");
  });

  const controller = {
    container,
    cy,
    orientation: mergedOptions.orientation === "vertical" ? "vertical" : "horizontal",
    columnCount: sanitizeColumnCount(mergedOptions.columnCount),
    usePreset: false,
    setElements(nextElements = []) {
      cy.startBatch();
      cy.elements().remove();
      if (Array.isArray(nextElements) && nextElements.length) {
        cy.add(nextElements);
      }
      cy.endBatch();
      controller.usePreset = Array.isArray(nextElements)
        ? nextElements.some((el) => el && el.position)
        : false;
      controller.runLayout();
      controller.applyNodeState();
    },
    setOrientation(nextOrientation = "horizontal") {
      const clean = nextOrientation === "vertical" ? "vertical" : "horizontal";
      if (clean === controller.orientation) return;
      controller.orientation = clean;
      controller.runLayout();
    },
    setColumns(nextColumns = 2) {
      const clean = sanitizeColumnCount(nextColumns);
      if (clean === controller.columnCount) return;
      controller.columnCount = clean;
      controller.runLayout();
    },
    runLayout() {
      const layoutOpts = controller.usePreset
        ? { name: "preset", fit: true, padding: 40 }
        : createLayoutOptions(controller.orientation, controller.columnCount);
      layoutOpts.animate = false;

      const layout = cy.layout(layoutOpts);
      layout.one("layoutstop", () => {
        if (!controller.usePreset) {
          alignDataNodes();
        }
        const elements = cy.elements();
        if (elements.length === 0) return;

        const bb = elements.boundingBox({ includeLabels: true, includeOverlays: true });
        const PADDING_Y = 100;
        const MIN_HEIGHT = 480;
        const desiredHeight = bb.h + PADDING_Y;
        const rect = container.getBoundingClientRect();
        const available = window.innerHeight - rect.top - 24;
        const maxHeight = Math.max(MIN_HEIGHT, Math.floor(available * 0.75));
        const newHeight = Math.min(Math.max(MIN_HEIGHT, desiredHeight), maxHeight);

        if (Math.abs(container.offsetHeight - newHeight) > 5) {
          container.style.height = `${newHeight}px`;
          cy.resize();
        }
        controller.fit();
      });
      layout.run();
    },
    fit() {
      if (!cy.elements().length) return;
      cy.fit(cy.elements(), 30); // 30px padding around fit
      if (cy.zoom() > 1.2) cy.zoom(1.2); // Don't zoom in too much if few nodes
      cy.center(cy.elements());
    },
    setNodeState(nextState = {}) {
      nodeState = {
        activeIds: Array.isArray(nextState.activeIds)
          ? nextState.activeIds.filter(Boolean)
          : nextState.activeId
            ? [nextState.activeId]
            : [],
        completedIds: Array.isArray(nextState.completedIds) ? nextState.completedIds.filter(Boolean) : [],
        failedIds: Array.isArray(nextState.failedIds) ? nextState.failedIds.filter(Boolean) : [],
        selectedId: nextState.selectedId ?? null,
      };
      controller.applyNodeState();
    },
    applyNodeState() {
      cy.startBatch();
      cy.nodes().forEach((node) => node.removeClass("is-active is-complete is-selected is-failed"));
      cy.nodes().forEach((node) => {
        const id = node.id();
        if (nodeState.activeIds.includes(id)) node.addClass("is-active");
        if (nodeState.selectedId === id) node.addClass("is-selected");
        if (nodeState.completedIds.includes(id)) node.addClass("is-complete");
        if (nodeState.failedIds.includes(id)) node.addClass("is-failed");
      });
      cy.endBatch();
    },
    resize() {
      cy.resize();
      controller.fit();
    },
    destroy() {
      detachDocumentHandler();
      cy.destroy();
    },
  };

  controller.setElements(elements);
  return controller;
}

function createLayoutOptions(orientation = "horizontal", columnCount = 2) {
  const horizontal = orientation !== "vertical";
  const columns = sanitizeColumnCount(columnCount);

  const baseSpacing = horizontal
    ? (columns <= 2 ? 0.95 : 0.95 + (columns - 2) * 0.12)
    : 1 + columns * 0.2;
  const spacingFactor = Math.min(Math.max(baseSpacing, 0.85), 1.35);

  const padding = 24 + columns * 6;
  return {
    name: "breadthfirst",
    directed: true,
    padding,
    spacingFactor,
    avoidOverlap: true,
    nodeDimensionsIncludeLabels: true,
    animate: true,
    animationDuration: 550,
    transform: (_node, position) => (horizontal ? { x: position.y, y: position.x } : position),
  };
}

function sanitizeColumnCount(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return 2;
  return Math.min(8, Math.max(1, Math.round(num)));
}
