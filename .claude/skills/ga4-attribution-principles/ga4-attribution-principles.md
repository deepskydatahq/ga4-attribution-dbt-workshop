---
name: ga4-attribution-principles
description: Enforces the design principles of the GA4 attribution dbt workshop project — strict Activity Schema v2, attribution conventions, identity/session separation, GA4 UTM coalesce, BigQuery gotchas. Use whenever adding or modifying any model in this repo.
---

Read `PRINCIPLES.md` at the repo root before touching any model. Key rules,
in priority order:

1. **Strict Activity Schema v2.** Only the 10 canonical columns at the top
   level of `activity__*` models and `activity_stream`. `session_uid`,
   `source`, `medium`, `campaign`, `gclid`, `form_id`, `page_referrer` all
   live in `feature_json`. Downstream uses `JSON_VALUE(feature_json, '$.field')`.

2. **Attribution labeling.** `first_touch` / `last_touch` are canonical
   `First Before` / `Last Before` (one row per conversion).
   `linear` / `position_based` are CUSTOM fan-outs (one row per
   conversion × touchpoint) — label them explicitly as not-canonical.
   Every attribution model must have `SUM(credit_weight) = 1.0` per
   `conversion_id`.

3. **Identity ≠ session.** `anon_id` is user-grain. `session_uid` is a
   grain key for touchpoints, inside `feature_json`. Never treat a session
   as an identity.

4. **Staging is thin.** Filter, flatten, clean only. No dedup, no joins
   beyond `UNNEST(event_params)`.

5. **UTM coalesce** in staging:
   `session_traffic_source_last_click` → `collected_traffic_source`
   → `event_params`. Use `NULLIF(x, '(not set)')` on the first tier.
   Never use user-level `traffic_source`.

6. **BigQuery NULL joins.** Wrap nullable keys in
   `IFNULL(x, '(direct)')` / `IFNULL(x, '(none)')` before comparing.

7. **Folder index = DAG order.** `01_staging`, `02_activity`,
   `03_identity`, `04_attribution`, `05_analytics`.

8. **Comments explain WHY.** This is workshop material — clarity over
   cleverness.

If a change conflicts with a principle, update `PRINCIPLES.md` in the same
PR and justify the deviation in the commit message.
