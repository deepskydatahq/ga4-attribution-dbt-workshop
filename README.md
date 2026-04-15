# GA4 Attribution — dbt Workshop

A hands-on dbt project that turns raw **Google Analytics 4 BigQuery Export**
data into a multi-touch attribution data model. Designed as workshop
material: each layer is small, commented, and self-contained so you can
walk through it step by step.

## What you'll learn

- How to tame the GA4 raw events schema — filtering, flattening
  `event_params`, and cleaning the three different traffic-source fields
  with a coalesce hierarchy.
- How to apply the **Activity Schema v2** pattern: one row per
  user activity, a unified activity stream, and temporal self-joins.
- How to express attribution as a **temporal join** problem — first touch
  and last touch map cleanly to the canonical "First Before" / "Last Before"
  relationships.
- Why multi-touch models (**linear**, **position-based**) break the
  canonical shape, and how to implement them with the same join mechanics.
- How to build **consumption-ready analytics marts** that let a marketer
  compare channels side-by-side under multiple attribution rules.

## Data model

```
source(ga4.events)
  └── stg_ga4__events            -- filter + flatten + UTM coalesce
        ├── activity__session_started   \
        ├── activity__form_submitted     }-- Activity Schema v2 feeders
        │        │
        │        └── activity_stream    -- unified v2 stream
        │             ├── attribution__first_touch     (First Before)
        │             ├── attribution__last_touch      (Last Before)
        │             ├── attribution__linear          (custom)
        │             └── attribution__position_based  (custom 40/20/40)
        │
        └── business_identity__anon    -- user_pseudo_id → anon_id
             ├── business_identity__person   (CRM placeholder)
             └── business_identity__account  (CRM placeholder)

  analytics_user        -- one row per user: dims + first/last touch (2 views)
  analytics_marketing   -- one row per channel: sessions, conversions,
                          unique converters under each attribution model
```

## Prerequisites

- Python 3.9+
- [dbt-bigquery](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup)
  (`pip install dbt-bigquery`)
- A Google Cloud project with a **GA4 BigQuery Export** linked
  ([GA4 linking guide](https://support.google.com/analytics/answer/9358801))
- `gcloud` authenticated: `gcloud auth application-default login`

## Quickstart

```bash
# 1. Install dependencies
pip install dbt-bigquery
dbt deps

# 2. Configure your BigQuery project
export GCP_PROJECT=your-gcp-project-id

# 3. Point at your GA4 dataset in dbt_project.yml vars, or override on the CLI:
dbt build --vars '{"ga4_project": "your-gcp-project", "ga4_dataset": "analytics_NNNNNN"}'
```

The `ga4_dataset` variable is the numeric dataset GA4 creates when you link
a property to BigQuery — typically `analytics_XXXXXXXXX`.

## Project layout

| Folder | Materialization | Purpose |
|--------|-----------------|---------|
| `models/staging/`     | view  | Clean GA4 events — one source of truth for downstream layers |
| `models/activity/`    | view  | Activity Schema v2 feeders + unified `activity_stream` |
| `models/identity/`    | table | Resolve `user_pseudo_id` to a canonical `anon_id` |
| `models/attribution/` | table | Four attribution models on top of the activity stream |
| `models/analytics/`   | table | Consumption marts for users and channels |

## Variables

Defined in `dbt_project.yml` and overridable on the CLI or in `profiles.yml`:

| Var | Default | Purpose |
|-----|---------|---------|
| `ga4_project` | `your-gcp-project` | GCP project containing the GA4 export |
| `ga4_dataset` | `analytics_NNNNNN` | GA4 BigQuery dataset id |

## Credit

Built as a workshop example for teaching GA4 attribution modeling in dbt.
Patterns borrowed from the public [Activity Schema v2](https://www.activityschema.com/)
specification.

## License

[MIT](./LICENSE)
