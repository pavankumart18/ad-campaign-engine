# WBD Converged Ad Campaign Engine

Browser-based multi-agent demo that simulates Warner Bros. Discovery's converged ad operations pipeline.
It orchestrates Olli audience planning, NEO + DemoDirect media planning, Global Compliance Policy Engine checks,
and StreamX in-flight optimization with SCTE-35 resilience.

## Features

- Single prompt to launch a 4-agent pipeline (Planner, Sales Rep, Compliance Bot, Ops Director).
- Live flowchart with zig-zag orchestration path and data sources.
- Streaming agent output with handoff badges.
- D3 visualizations: pacing chart (planned vs delivered), reach overlap donut, interactive tooltips.
- Compliance redline banner and make-good animation.
- Export approved plan CSV.

## Quick Start

1. Start a local server:
```bash
cd agent-builder
python -m http.server 8000
```

2. Open `http://localhost:8000`.

3. Click `Load Pipeline`, add your OpenAI API key, then `Launch Campaign`.

## Configuration

- `config.json`: Demo title, prompts, and flowchart layout.
- `data.js`: Synthetic datasets (audience graph, inventory, compliance, delivery logs).

## Project Structure

- `index.html`: Main layout and import map.
- `styles.css`: WBD dark theme and UI styling.
- `script.js`: App controller, orchestration, D3 rendering, and exports.
- `view.js`: UI rendering and dashboard markup.
- `flowchart.js`: Cytoscape visualization and node/edge interactivity.
- `data.js`: Synthetic datasets and CSV helpers.

## Notes

- Run via a local server (module imports do not work reliably via `file://`).
- The demo is browser-only; no backend required beyond the OpenAI API.
