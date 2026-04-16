-- activity__session_started.sql
--
-- Activity Schema v2 feeder for session_start events.
-- Strict v2: only canonical columns at the top level. Every activity-
-- specific attribute (including session_uid and the attribution UTMs)
-- lives inside feature_json and is extracted with JSON_VALUE downstream.

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

    select
        {{ dbt_utils.generate_surrogate_key(['s.session_uid', 's.event_timestamp']) }}  as activity_id,
        s.event_timestamp                                                                as ts,
        i.anon_id                                                                        as customer,
        cast('session_started' as string)                                                as activity,
        s.user_pseudo_id                                                                 as anonymous_customer_id,
        cast(null as float64)                                                            as revenue_impact,
        s.page_location                                                                  as link,
        to_json_string(struct(
            s.session_uid   as session_uid,
            s.source        as source,
            s.medium        as medium,
            s.campaign      as campaign,
            s.gclid         as gclid,
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
    revenue_impact,
    link,
    feature_json,
    row_number() over (partition by customer, activity order by ts asc)  as activity_occurrence,
    lead(ts)      over (partition by customer, activity order by ts asc) as activity_repeated_at

from joined
