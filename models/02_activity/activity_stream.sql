-- activity_stream.sql
--
-- The unified Activity Schema v2 stream. All activity feeders UNION-ed
-- into a single table with the v2 column contract. Downstream attribution
-- and analytics models query the stream and filter by `activity`, rather
-- than picking individual feeder models.
--
-- This is the teaching payoff of activity schema: every kind of user
-- activity lives in one long table and looks the same, which means cross-
-- activity questions ("for each form_submitted, find the last session_started
-- before it") reduce to a self-join on this one table.
--
-- Adding a new activity (e.g., activity__page_viewed) is a one-liner here.

select
    activity_id,
    ts,
    customer,
    activity,
    anonymous_customer_id,
    session_uid,
    source,
    medium,
    campaign,
    gclid,
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
    session_uid,
    source,
    medium,
    campaign,
    gclid,
    revenue_impact,
    link,
    feature_json,
    activity_occurrence,
    activity_repeated_at
from {{ ref('activity__form_submitted') }}
