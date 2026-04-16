# Project Principles

Design principles for this GA4 attribution dbt project. Follow these when
modifying, extending, or explaining any model. When a change conflicts with
a principle, update this file in the same PR and justify the deviation.

## 1. Activity Schema v2 is strict

The activity stream is the backbone of this project and it conforms to the
[Activity Schema v2 spec](https://www.activityschema.com/).

- **Top-level columns are ONLY the 10 canonical v2 columns:**
  `activity_id`, `ts`, `customer`, `activity`, `anonymous_customer_id`,
  `revenue_impact`, `link`, `feature_json`, `activity_occurrence`,
  `activity_repeated_at`.
- **Everything domain-specific lives in `feature_json`** — including
  `session_uid`, `source`, `medium`, `campaign`, `gclid`, `form_id`,
  `page_referrer`.
- Downstream models extract features with `JSON_VALUE(feature_json, '$.field')`.
- Adding a new activity type = one new feeder + one line in `activity_stream`.

## 2. Attribution — map to the canonical types, or label honestly

- `attribution__first_touch` and `attribution__last_touch` **are** canonical
  Activity Schema temporal joins (`First Before` / `Last Before`). Output
  grain: one row per conversion.
- `attribution__linear` and `attribution__position_based` are **CUSTOM** —
  they fan out to one row per (conversion × touchpoint) because multi-touch
  attribution needs per-touchpoint `credit_weight`. Label them in comments
  as "not a canonical temporal join."
- Every attribution model must satisfy:
  `SUM(credit_weight) GROUP BY conversion_id = 1.0`.
- Each attribution model is **self-contained** — no shared intermediate.
  The temporal self-join pattern is repeated deliberately so each file
  reads top-to-bottom.

## 3. Identity is at user grain only

- `anon_id` is the canonical user key. For now `anon_id = user_pseudo_id`
  (1:1). Cross-device or cookie-stitching goes in this model when available.
- `session_uid` is **not** identity — it's a touchpoint grain key and lives
  inside `feature_json`.
- `business_identity__person` and `business_identity__account` are
  structural placeholders. The shape is the teaching content.

## 4. Staging stays thin

- `stg_ga4__events` is a filter + flatten + clean layer. No deduplication,
  no business rules, no joins beyond `UNNEST(event_params)`.
- Dedupe at the activity layer, not here.

## 5. GA4 UTM coalesce — 3 tiers, in this order

```sql
COALESCE(
    NULLIF(session_traffic_source_last_click.source, '(not set)'),
    collected_traffic_source.manual_source,
    (event_params source)
)
```

- **NULLIF** strips GA4's `(not set)` sentinel so `COALESCE` can skip it.
- **Do NOT use the user-level `traffic_source`** — it's sticky (set once,
  never updates) and would contaminate session-level attribution for
  returning users.

## 6. Defensive SQL (BigQuery)

- `NULL = NULL` is `FALSE`. When joining on nullable keys like
  `source` / `medium`, wrap both sides in
  `IFNULL(x, '(direct)')` / `IFNULL(x, '(none)')`.
- Dedupe with `ROW_NUMBER() OVER (PARTITION BY ...) = 1`.
- Use `QUALIFY` to express "pick one row per partition" inline without
  wrapping the whole query in a CTE.

## 7. Folder and naming conventions

- Folders prefixed with index for DAG sort:
  `01_staging`, `02_activity`, `03_identity`, `04_attribution`, `05_analytics`.
- Model prefixes:
  - `stg_`                   — staging
  - `activity__`             — activity v2 feeders
  - `activity_stream`        — unified v2 stream
  - `business_identity__`    — identity models
  - `attribution__`          — attribution models
  - `analytics_`             — consumption marts
- dbt materialization by folder:
  - staging & activity  → `view`
  - identity, attribution, analytics → `table`

## 8. Workshop-facing style

- Comments explain **WHY**, not **WHAT**. The code tells you what; the
  comment tells you why we chose this approach.
- Prefer clarity over performance. This is teaching material.
- Each model should be individually readable top-to-bottom. No clever
  shared abstractions that force attendees to jump between files.
