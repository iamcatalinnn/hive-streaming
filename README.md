# Hive Streaming — Data Engineering Assignment

A scalable stream processing pipeline that ingests telemetry data from video
players, computes Quality of Service (QoS) metrics per viewer session, and
visualises the results in an interactive dashboard.

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Data Exploration & Key Findings](#data-exploration--key-findings)
- [Pipeline Design Decisions](#pipeline-design-decisions)
- [QoS Scoring](#qos-scoring)
- [Running the Pipeline](#running-the-pipeline)
- [Running the Dashboard](#running-the-dashboard)
- [Running the Tests](#running-the-tests)
- [Configuration](#configuration)
- [Secondary Assignment — Telemetry Collection](#secondary-assignment--telemetry-collection)

---

## Overview

Hive Streaming provides a P2P-assisted video delivery platform for enterprise
webcasts. This pipeline processes telemetry data emitted by viewers' video
players and produces per-session QoS metrics including buffering behaviour,
video quality consumption, and traffic distribution (CDN vs P2P).

**Input:** Delta Lake table partitioned by `eventDate`, containing one row per
viewer heartbeat (~30 second intervals).

**Output:** Three partitioned parquet tables following a Medallion architecture:
- `silver_sessions` — one row per viewer per heartbeat window
- `silver_quality` — one row per viewer per heartbeat window per quality level
- `gold` — one row per viewer session with full QoS analysis

---

## Project Structure

```
hive_streaming/
├── src/
│   └── pipeline/
│       ├── models/
│       │   └── schemas.py          # Pydantic data contracts
│       ├── ingestion/
│       │   └── reader.py           # Bronze Delta table reader
│       ├── transformations/
│       │   ├── silver_sessions.py  # Bronze → Silver sessions
│       │   ├── silver_quality.py   # Bronze → Silver quality
│       │   └── gold.py             # Silver → Gold aggregation
│       ├── quality/
│       │   └── qos.py              # QoS scoring logic
│       └── io/
│           └── writer.py           # Partitioned parquet writer
├── dashboard/
│   └── app.py                      # Streamlit dashboard
├── tests/
│   ├── conftest.py                 # Shared pytest fixtures
│   ├── test_silver_sessions.py
│   ├── test_silver_quality.py
│   ├── test_gold.py
│   └── test_qos.py
├── data/                           # Input Delta table (Bronze)
├── output/                         # Pipeline outputs
├── main.py                         # Pipeline orchestrator
├── config.yaml                     # Configurable thresholds and paths
└── requirements.txt
```

---

## Architecture

The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold),
a standard pattern for data lake pipelines.

```
Input Delta Table (Bronze)
        │
        ├──────────────────────────────────────┐
        ▼                                      ▼
silver_sessions                         silver_quality
(1 row per viewer per window)           (1 row per viewer per window per quality)
        │                                      │
        └──────────────────┬───────────────────┘
                           ▼
                          gold
                 (1 row per viewer session)
                           │
                           ▼
                   Streamlit Dashboard
```

### Why Medallion?

| Layer | Responsibility | Adds Value By |
|---|---|---|
| Bronze | Raw input — unchanged | Already exists as the Delta table |
| Silver | Flatten, clean, enrich | Normalising nested structs, computing windows |
| Gold | Aggregate, score | Session-level metrics, QoS scoring |

Each layer preserves data from the layer below — Silver never deletes rows,
Gold aggregates but Silver remains queryable independently.

### Why Two Silver Tables?

The raw data has two distinct concerns:

- **Buffering** — session-level metrics (bufferings count, buffering time)
- **Quality** — one entry per quality level per window

Mixing them would cause buffering metrics to be duplicated across quality rows,
making aggregation in Gold error-prone. Separating them keeps each table clean
and independently queryable.

```
silver_sessions  PK: customer_id + content_id + client_id + window_end
silver_quality   PK: customer_id + content_id + client_id + window_end + quality
```

They join on `customer_id + content_id + client_id + window_end`.

---

## Data Exploration & Key Findings

Before designing the pipeline, I explored the raw data and made several
important discoveries that shaped the design:

### 1. The Data Is Pre-Aggregated Heartbeats, Not Raw Events

Each row is not a raw event (e.g. `buffering_start`, `buffering_end`) but a
**~30-second summary window** sent periodically by the player plugin. One viewer
watching a 13-minute webcast generates ~28 heartbeat rows.

### 2. Duplicate `clientId` Rows Are Heartbeats, Not Errors

546 out of 566 rows share a `clientId` with another row. Investigation showed
these are the same viewer sending periodic heartbeats for the same
`contentId` — timestamps are ~30 seconds apart confirming the heartbeat pattern.

### 3. No Explicit `window_start` in the Data

I investigated all timestamp fields (`timestampInfo.server` and
`timestampInfo.agent`) and found they differ by only 20–100ms — this is network
latency, not a window boundary. The data does not contain an explicit session
start timestamp.

**Decision:** `window_start` for the first heartbeat per viewer is set to
`NULL` rather than fabricating a value. This is honest about what the data
actually tells us. In production, we would work with the plugin team to emit an
explicit `session_start` event.

### 4. Quality Distribution Is a List of Tuples

`qualityDistribution` contains one entry per quality level consumed in the
window, each with separate `sourceTraffic` and `p2pTraffic` breakdowns. This
reflects Hive Streaming's P2P delivery model where video chunks can come from
either the central CDN or other viewers.

### 5. 7 Viewers Had Empty Quality Distributions

These viewers connected but never loaded a video segment. They are flagged with
`has_quality_data = False` in Silver and preserved, they represent a valid QoS
signal (connection without playback).

### 6. Quality Switch Overcounting

My initial implementation counted every quality row transition, including
multiple quality entries within the same 30-second window. This inflated quality
switch counts to 40–68 per session. The fix was to first reduce each window to
its **dominant quality** (quality with most received bytes), then count
transitions across windows only. This brought switch counts to a realistic 6–19
per session.

### 7. Delta Log Files Must Be Excluded

The `_delta_log/` folder inside the Delta table contains parquet checkpoint
files with a different schema. Naively globbing all parquet files would include
these and corrupt the DataFrame. The reader explicitly patterns on
`eventDate=*/*.parquet` to exclude them.

---

## Pipeline Design Decisions

### Dependency Injection Throughout

No function reads config files or accesses the filesystem internally. All
dependencies (paths, config, DataFrames) are passed as parameters. This makes
every function independently testable without side effects.

```python
# Bad — hard to test
def compute_qos(df):
    config = yaml.safe_load(open('config.yaml'))  # reaches out to filesystem
    ...

# Good — testable with any config
def compute_qos(df: pd.DataFrame, config: QoSConfig) -> pd.DataFrame:
    ...
```

### Pydantic Data Contracts

Each layer has a Pydantic schema defining its expected shape and validation
rules. The pipeline validates every row after transformation and logs warnings
for any that fail, catching data quality issues at layer boundaries rather than
propagating them silently.

Validation rules include:
- `bufferings >= 0`, `buffering_time_ms >= 0`
- All traffic bytes fields `>= 0`
- `quality` must be a known value (`144p`, `270p`, `360p`, `480p`, `540p`,
  `720p`, `1080p`, `1440p`, `2160p`)
- `buffering_ratio`, `p2p_ratio`, `delivery_rate` between 0 and 1
- `qos_label` must be `green`, `yellow`, or `red`

### Configurable QoS Thresholds

All QoS scoring thresholds live in `config.yaml` and are loaded into a
`QoSConfig` Pydantic model. This means thresholds can be tuned without touching
pipeline code — important for a product where QoS definitions may evolve.

### Partitioned Output

Outputs mirror the input partitioning pattern (`eventDate=YYYY-MM-DD`). When
the pipeline runs daily, each day's data is isolated, a single day can be
reprocessed without touching other partitions.

### Orchestration

For this assessment, `main.py` acts as the orchestrator. In production, each
layer would be wrapped in an **Airflow DAG** with task dependencies
`Bronze → Silver → Gold`, retries, and alerting on failure.

---

## QoS Scoring

Each viewer session receives a composite QoS score (0–1) and a label
(green/yellow/red) based on three components:

### Buffering Score (weight: 50%)

| Buffering Ratio | Score |
|---|---|
| < 5% | 1.0 (excellent) |
| 5% – 35% | 0.75 (degraded) |
| > 35% | 0.25 (poor) |

Buffering gets the highest weight because it is the most noticeable degradation
for a viewer, the video literally stops.

### Quality Score (weight: 30%)

```
quality_score = rank(dominant_quality) / max_rank
```

Quality levels are ranked 1–9 (144p → 2160p). The dominant quality is the
quality level with the most received bytes across the session. Score is
normalised to 0–1 against the maximum rank.

### Stability Score (weight: 20%)

| Quality Switches per Minute | Score |
|---|---|
| < 1 | 1.0 (stable) |
| 1 – 2 | 0.75 (some switching) |
| 2 – 4 | 0.5 (unstable) |
| > 4 | 0.25 (very unstable) |

Quality switches are counted as transitions between **dominant quality per
window**, not raw quality row transitions, to avoid counting intra-window
quality variety as instability.

### Final Score & Label

```
qos_score = (buffering_score × 0.5) +
            (quality_score   × 0.3) +
            (stability_score × 0.2)

qos_label:
  >= 0.75  →  green  (good)
  >= 0.50  →  yellow (degraded)
  <  0.50  →  red    (poor)
```

> All weights and thresholds are configurable via `config.yaml`.

### Results on Sample Data

| Label | Viewers | % |
|---|---|---|
| 🟢 Green | 16 | 80% |
| 🟡 Yellow | 4 | 20% |
| 🔴 Red | 0 | 0% |

---

## Running the Pipeline

### Prerequisites

```bash
pip3 install -r requirements.txt
```

### Setup

Place the Delta table folder inside `data/`:

```
data/
├── eventDate=2025-11-13/
│   └── *.parquet
└── _delta_log/
```

### Run

```bash
python3 main.py
```

Output will be written to:

```
output/
├── silver_sessions/eventDate=2025-11-13/part-0.parquet
├── silver_quality/eventDate=2025-11-13/part-0.parquet
└── gold/eventDate=2025-11-13/part-0.parquet
```

---

## Running the Dashboard

```bash
python3 -m streamlit run dashboard/app.py
```

Opens at `http://localhost:8501`. The dashboard includes:

- **KPI row** — total viewers, QoS label distribution, average buffering ratio
- **QoS distribution** — donut chart of green/yellow/red viewers
- **Buffering ratio per viewer** — bar chart with threshold lines
- **Dominant quality distribution** — quality level breakdown across viewers
- **P2P vs CDN traffic** — stacked bar chart per viewer
- **QoS score histogram** — distribution of scores
- **Session duration vs buffering** — scatter plot with bubble size = quality switches
- **Quality switches per viewer** — stability analysis
- **Delivery rate per viewer** — request fulfilment rate
- **Viewer detail table** — full session metrics with colour coded QoS labels

---

## Running the Tests

```bash
python3 -m pytest tests/ -v
```

Expected output: **20 passed**.

### What Is Tested

| File | Tests | What It Covers |
|---|---|---|
| `test_silver_sessions.py` | 7 | NULL first window_start, LAG logic, field extraction, flags, snake_case rename |
| `test_silver_quality.py` | 5 | Empty quality drop, explode row count, valid quality values, traffic fields, join key consistency |
| `test_gold.py` | 8 | Row count, session start, buffering sum, quality switch counting, dominant quality |
| `test_qos.py` | 11 | Green/yellow/red thresholds, boundary conditions, quality score, stability score |

---

## Configuration

`config.yaml` controls all pipeline behaviour:

```yaml
pipeline:
  input_path: "data/"
  output_path: "output/"

qos:
  buffering_green_threshold: 0.05   # below 5%  → green
  buffering_red_threshold:   0.35   # above 35% → red
  buffering_weight:          0.5    # 50% of QoS score
  quality_weight:            0.3    # 30% of QoS score
  stability_weight:          0.2    # 20% of QoS score
```

---

## Secondary Assignment — Telemetry Collection

The secondary part asks for a TypeScript library that collects telemetry from
an `hls.js` video player and sends it downstream.

This part of the assignment is outside my primary expertise as a data engineer.
JavaScript/TypeScript frontend development and browser SDK design are not skills
I work with day to day, and I believe it's more professional to be transparent
about that than to present work I cannot fully stand behind.

That said, working through Part 1 gave me a clear understanding of **what this
plugin must produce**, because its output is exactly what I spent time
exploring and processing. The Bronze Delta table schema is the plugin's output.
I understand the data contract even if I am not the right person to implement
the collection side.

---

### What I Understand From Part 1

By exploring the parquet data before designing anything, I was able to reverse
engineer what the plugin must be doing:

| What I observed in the data | What the plugin must be doing |
|---|---|
| One row per viewer every ~30 seconds | Sending periodic heartbeats, not per-event |
| 546/566 rows had duplicate `clientId` | Same viewer sends many heartbeats per session |
| `player.bufferings` and `player.bufferingTime` | Counting and timing buffering events per window |
| `qualityDistribution` is a list per window | Tracking multiple quality levels within each window |
| `sourceTraffic` and `p2pTraffic` per quality | Distinguishing CDN vs P2P delivery per chunk |
| `timestampInfo.agent` vs `timestampInfo.server` | Client sends its own timestamp, server adds its own on receipt |

This understanding directly shaped the pipeline design, for example, knowing
that rows are heartbeat deltas (not cumulative) determined how I aggregate
buffering in the Gold layer.

---

### A Data Quality Improvement Worth Investigating

While building the pipeline I identified a structural gap in the current
telemetry schema that the plugin could address.

Each heartbeat represents approximately 30 seconds of activity, but the
schema only contains two timestamps:

- `timestampInfo.agent` — when the heartbeat was **sent**
- `timestampInfo.server` — when the heartbeat was **received**

Neither tells us when the **window started**. The difference between them
is just network latency (~20–100ms), not window duration.

In the pipeline this means `window_start` is `NULL` for every viewer's
first heartbeat, we genuinely don't know when their session began. We
chose `NULL` over fabricating a value, which is the honest decision, but
it is a real data quality gap.

The fix would be simple on the plugin side, record the timestamp when
each 30-second window **begins** and include it in the heartbeat:

```
timestampInfo: {
    windowStart: number   // ← when this 30s window started (new field)
    agent:       number   // when heartbeat was sent
    server:      number   // filled by backend on receipt
}
```

This would give the pipeline a true `window_start` for every heartbeat,
including the first one, enabling accurate session start times and more
precise session duration calculations in the Gold layer.

This is a small change on the collection side with meaningful impact on
analytics quality downstream.

---

### What I Would Do With More Time

Given more time and the right resources, I would approach this by:

- Reading the `hls.js` documentation to understand which events fire for
  buffering and quality changes
- Designing the telemetry object schema to match what the pipeline expects
- Collaborating with a frontend engineer who knows the browser SDK space
- Validating the output against the Bronze table schema to ensure end-to-end
  consistency
- Implementing the `windowStart` improvement described above

The assignment itself says *"it is not needed to fully implement the solution"*, so I chose to invest my time in delivering a honest Part 1 rather
than a shallow Part 2.
