-- activity__form_submitted.sql
--
-- Activity Schema v2 feeder for form_submit events — the conversion activity.
-- One row per form submission, shaped to the same v2 contract as every other
-- feeder so downstream queries can treat all activities uniformly.

with form_submits as (

    select
        *,
        row_number() over (
            partition by session_uid, event_timestamp
            order by event_timestamp asc
        ) as rn
    from {{ ref('stg_ga4__events') }}
    where event_name = 'form_submit'

),

deduped as (

    select * from form_submits where rn = 1

),

joined as (

    -- Resolve customer identity by joining to the identity spine.
    select
        {{ dbt_utils.generate_surrogate_key(['s.session_uid', 's.event_timestamp']) }}  as activity_id,
        s.event_timestamp                                                                as ts,
        i.anon_id                                                                        as customer,
        cast('form_submitted' as string)                                                 as activity,
        s.user_pseudo_id                                                                 as anonymous_customer_id,
        s.session_uid,
        s.source,
        s.medium,
        s.campaign,
        s.gclid,
        cast(null as float64)                                                            as revenue_impact,
        s.page_location                                                                  as link,
        to_json_string(struct(
            s.form_id as form_id
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
    row_number() over (partition by customer, activity order by ts asc)  as activity_occurrence,
    lead(ts)      over (partition by customer, activity order by ts asc) as activity_repeated_at

from joined
