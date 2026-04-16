-- analytics_user.sql
--
-- Consumption-ready user-level summary. One row per anonymous visitor.
--
-- This model brings together:
--   - business_identity__anon       → anon_id, first/last seen timestamps
--   - activity_stream (v2)          → session counts, first/last touch (ANY time),
--                                     conversion counts
--   - attribution__first/last_touch → first/last touch sources scoped to the
--                                     user's MOST RECENT conversion
--
-- Two perspectives on first/last touch coexist here on purpose:
--   first_touch_source              → user's earliest session ever
--   first_touch_conversion_source   → first-touch credited to their most recent conversion
--   last_touch_source               → user's most recent session ever
--   last_touch_conversion_source    → last-touch credited to their most recent conversion
--
-- For non-converters the conversion-scoped columns are NULL.
--
-- Workshop note: ARRAY_AGG with IGNORE NULLS + ORDER BY + LIMIT 1 is a
-- BigQuery pattern for picking the first (or last) non-null value in a group.

with identity as (

    -- Spine: one row per anonymous visitor
    select
        anon_id,
        user_pseudo_id,
        first_seen_at,
        last_seen_at
    from {{ ref('business_identity__anon') }}

),

sessions as (

    -- All session_started activities from the v2 stream.
    -- Strict v2 keeps source/medium inside feature_json — unpack them here.
    select
        customer                                as anon_id,
        ts,
        json_value(feature_json, '$.source')    as source,
        json_value(feature_json, '$.medium')    as medium
    from {{ ref('activity_stream') }}
    where activity = 'session_started'
      and customer is not null

),

session_stats as (

    -- Aggregate session metrics per user
    select
        anon_id,
        count(*) as num_sessions,

        -- First-touch: earliest session's source/medium
        array_agg(source ignore nulls order by ts asc limit 1)[safe_offset(0)]
            as first_touch_source,
        array_agg(medium ignore nulls order by ts asc limit 1)[safe_offset(0)]
            as first_touch_medium,

        -- Last-touch: latest session's source/medium
        array_agg(source ignore nulls order by ts desc limit 1)[safe_offset(0)]
            as last_touch_source,
        array_agg(medium ignore nulls order by ts desc limit 1)[safe_offset(0)]
            as last_touch_medium

    from sessions
    group by anon_id

),

conversions as (

    -- Count form submissions (conversions) per user from the v2 stream
    select
        customer as anon_id,
        count(*) as total_conversions
    from {{ ref('activity_stream') }}
    where activity = 'form_submitted'
      and customer is not null
    group by customer

),

first_touch_conv as (

    -- The first-touch credit for each user's MOST RECENT conversion.
    -- attribution__first_touch already has one row per conversion (the
    -- earliest preceding session). We pick the row tied to the user's
    -- latest conversion via QUALIFY.
    select
        customer as anon_id,
        source   as first_touch_conversion_source,
        medium   as first_touch_conversion_medium
    from {{ ref('attribution__first_touch') }}
    qualify row_number() over (
        partition by customer
        order by conversion_at desc
    ) = 1

),

last_touch_conv as (

    -- Last-touch credit for each user's most recent conversion.
    select
        customer as anon_id,
        source   as last_touch_conversion_source,
        medium   as last_touch_conversion_medium
    from {{ ref('attribution__last_touch') }}
    qualify row_number() over (
        partition by customer
        order by conversion_at desc
    ) = 1

)

-- Final output: one row per anonymous visitor
select
    id.anon_id,
    id.first_seen_at,
    id.last_seen_at,
    coalesce(ss.num_sessions, 0)       as num_sessions,
    coalesce(cv.total_conversions, 0)  as total_conversions,

    -- Activity-view first/last touch (any time, all users)
    ss.first_touch_source,
    ss.first_touch_medium,
    ss.last_touch_source,
    ss.last_touch_medium,

    -- Attribution-view first/last touch (most recent conversion, converters only)
    ftc.first_touch_conversion_source,
    ftc.first_touch_conversion_medium,
    ltc.last_touch_conversion_source,
    ltc.last_touch_conversion_medium

from identity id
left join session_stats ss
    on id.anon_id = ss.anon_id
left join conversions cv
    on id.anon_id = cv.anon_id
left join first_touch_conv ftc
    on id.anon_id = ftc.anon_id
left join last_touch_conv ltc
    on id.anon_id = ltc.anon_id
