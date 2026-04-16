-- activity_stream.sql
--
-- The unified Activity Schema v2 stream. All activity feeders UNION-ed
-- into a single table with the strict v2 column contract. Downstream
-- models filter by `activity` and extract activity-specific fields from
-- feature_json with JSON_VALUE(feature_json, '$.field').
--
-- Canonical v2 columns only — no domain-specific fields at the top level.
-- Adding a new activity feeder is a one-line change in this model.

select
    activity_id,
    ts,
    customer,
    activity,
    anonymous_customer_id,
    revenue_impact,
    link,
    feature_json,
    activity_occurrence,
    activity_repeated_at
from {{ ref('activity__session_started') }}

union all

select
    activity_id,
    ts,
    customer,
    activity,
    anonymous_customer_id,
    revenue_impact,
    link,
    feature_json,
    activity_occurrence,
    activity_repeated_at
from {{ ref('activity__form_submitted') }}
