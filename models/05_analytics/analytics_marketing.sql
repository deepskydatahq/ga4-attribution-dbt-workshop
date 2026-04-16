-- analytics_marketing.sql
--
-- Consumption-ready marketing channel summary. One row per source/medium pair.
--
-- Combines raw session & conversion counts with attributed conversions from
-- four attribution models (first-touch, last-touch, linear, position-based).
--
-- Workshop note: We use a sessions base CTE and LEFT JOIN each attribution
-- summary so that channels with sessions but zero conversions still appear.

with sessions as (

    -- Total sessions per source/medium — from the Activity Schema v2 stream.
    -- Strict v2 keeps source/medium inside feature_json — unpack first, then
    -- aggregate.
    select
        json_value(feature_json, '$.source')    as source,
        json_value(feature_json, '$.medium')    as medium,
        count(*) as total_sessions
    from {{ ref('activity_stream') }}
    where activity = 'session_started'
    group by 1, 2

),

conversions as (

    -- Raw conversions and unique converters per source/medium.
    select
        json_value(feature_json, '$.source')    as source,
        json_value(feature_json, '$.medium')    as medium,
        count(*)                  as total_conversions,
        count(distinct customer)  as total_unique_converters
    from {{ ref('activity_stream') }}
    where activity = 'form_submitted'
    group by 1, 2

),

first_touch as (

    -- First-touch model: credited conversions (SUM of credit_weight) and the
    -- number of distinct customers who had a first-touch on this channel.
    select
        source,
        medium,
        sum(credit_weight)        as first_touch_conversions,
        count(distinct customer)  as first_touch_unique_converters
    from {{ ref('attribution__first_touch') }}
    group by source, medium

),

last_touch as (

    select
        source,
        medium,
        sum(credit_weight)        as last_touch_conversions,
        count(distinct customer)  as last_touch_unique_converters
    from {{ ref('attribution__last_touch') }}
    group by source, medium

),

linear as (

    select
        source,
        medium,
        sum(credit_weight)        as linear_conversions,
        count(distinct customer)  as linear_unique_converters
    from {{ ref('attribution__linear') }}
    group by source, medium

),

position_based as (

    select
        source,
        medium,
        sum(credit_weight)        as position_based_conversions,
        count(distinct customer)  as position_based_unique_converters
    from {{ ref('attribution__position_based') }}
    group by source, medium

)

-- Final output: one row per source/medium channel.
-- For each attribution model we expose two metrics:
--   *_conversions          → SUM(credit_weight)   "how much credit?"
--   *_unique_converters    → COUNT(DISTINCT user) "how many distinct people?"
-- The ratio tells you whether the channel drives many conversions from
-- few power users or one each from many users.
select
    s.source,
    s.medium,
    s.total_sessions,
    coalesce(c.total_conversions, 0)                  as total_conversions,
    coalesce(c.total_unique_converters, 0)            as total_unique_converters,

    coalesce(ft.first_touch_conversions, 0)           as first_touch_conversions,
    coalesce(ft.first_touch_unique_converters, 0)     as first_touch_unique_converters,

    coalesce(lt.last_touch_conversions, 0)            as last_touch_conversions,
    coalesce(lt.last_touch_unique_converters, 0)      as last_touch_unique_converters,

    coalesce(ln.linear_conversions, 0)                as linear_conversions,
    coalesce(ln.linear_unique_converters, 0)          as linear_unique_converters,

    coalesce(pb.position_based_conversions, 0)        as position_based_conversions,
    coalesce(pb.position_based_unique_converters, 0)  as position_based_unique_converters

from sessions s
-- Note: we use IFNULL to handle NULL source/medium in joins, because
-- in BigQuery NULL = NULL evaluates to FALSE and would silently drop rows.
left join conversions c
    on ifnull(s.source, '(direct)') = ifnull(c.source, '(direct)')
    and ifnull(s.medium, '(none)') = ifnull(c.medium, '(none)')
left join first_touch ft
    on ifnull(s.source, '(direct)') = ifnull(ft.source, '(direct)')
    and ifnull(s.medium, '(none)') = ifnull(ft.medium, '(none)')
left join last_touch lt
    on ifnull(s.source, '(direct)') = ifnull(lt.source, '(direct)')
    and ifnull(s.medium, '(none)') = ifnull(lt.medium, '(none)')
left join linear ln
    on ifnull(s.source, '(direct)') = ifnull(ln.source, '(direct)')
    and ifnull(s.medium, '(none)') = ifnull(ln.medium, '(none)')
left join position_based pb
    on ifnull(s.source, '(direct)') = ifnull(pb.source, '(direct)')
    and ifnull(s.medium, '(none)') = ifnull(pb.medium, '(none)')
