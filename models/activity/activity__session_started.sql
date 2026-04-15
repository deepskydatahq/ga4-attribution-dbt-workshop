-- activity__session_started.sql
--
-- Activity Schema v2 feeder for session_start events.
-- One row per deduplicated session start, shaped to the v2 column contract.
--
-- v2 column reference:
--   activity_id            — surrogate PK for this occurrence
--   ts                     — when the activity happened (TIMESTAMP)
--   customer               — resolved identity (anon_id for now; person_id when available)
--   activity               — the activity name ('session_started')
--   anonymous_customer_id  — raw cookie-level ID (user_pseudo_id) for when customer isn't resolved
--   revenue_impact         — nullable numeric, NULL for non-monetary activities
--   link                   — canonical URL for this activity (the landing page)
--   feature_json           — JSON blob of activity-specific features (page_referrer)
--   activity_occurrence    — Nth time THIS customer performed THIS activity (1 = first ever)
--   activity_repeated_at   — ts of the NEXT time this customer performs this activity (NULL for last)
--
-- Pragmatic addition (not strict v2, but hot for attribution): we also expose
-- session_uid and the UTM columns (source/medium/campaign/gclid) as top-level
-- columns so downstream attribution queries read cleanly instead of calling
-- JSON_VALUE on every row.

with session_starts as (

    select
        *,
        row_number() over (
            partition by session_uid
            order by event_timestamp asc
        ) as rn
    from {{ ref('stg_ga4__events') }}
    where event_name = 'session_start'

),

deduped as (

    select * from session_starts where rn = 1

),

joined as (

    -- Resolve customer identity by joining to the identity spine.
    select
        {{ dbt_utils.generate_surrogate_key(['s.session_uid', 's.event_timestamp']) }}  as activity_id,
        s.event_timestamp                                                                as ts,
        i.anon_id                                                                        as customer,
        cast('session_started' as string)                                                as activity,
        s.user_pseudo_id                                                                 as anonymous_customer_id,
        s.session_uid,
        s.source,
        s.medium,
        s.campaign,
        s.gclid,
        cast(null as float64)                                                            as revenue_impact,
        s.page_location                                                                  as link,
        to_json_string(struct(
            s.page_referrer as page_referrer
        ))                                                                               as feature_json
    from deduped s
    left join {{ ref('business_identity__anon') }} i
        on s.user_pseudo_id = i.user_pseudo_id

)

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
    -- v2 computed columns: per (customer, activity). Because this feeder only
    -- contains one activity type, (customer, activity) is equivalent to
    -- partitioning on customer alone here.
    row_number() over (partition by customer, activity order by ts asc)  as activity_occurrence,
    lead(ts)      over (partition by customer, activity order by ts asc) as activity_repeated_at

from joined
