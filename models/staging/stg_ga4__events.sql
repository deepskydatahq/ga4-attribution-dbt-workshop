with source as (

    select * from {{ source('ga4', 'events') }}

),

filtered as (

    select *
    from source
    where event_name in (
        'session_start',
        'form_submit',
        'form_start',
        'generated_lead',
        'file_download'
    )

),

flattened as (

    select
        -- Date & time
        parse_date('%Y%m%d', event_date)                          as event_date,
        timestamp_micros(event_timestamp)                         as event_timestamp,

        -- Event identity
        event_name,
        user_pseudo_id,

        -- Session ID from event_params
        (select value.int_value
         from unnest(event_params)
         where key = 'ga_session_id')                             as ga_session_id,

        -- Page context
        (select value.string_value
         from unnest(event_params)
         where key = 'page_location')                             as page_location,

        (select value.string_value
         from unnest(event_params)
         where key = 'page_referrer')                             as page_referrer,

        -- Form context
        (select value.string_value
         from unnest(event_params)
         where key = 'form_id')                                   as form_id,

        -- Attribution: 3-tier coalesce of traffic source fields.
        --
        -- GA4 BigQuery Export actually exposes FOUR traffic source fields, at different grains:
        --   1. traffic_source                       (USER-level)   — the user's FIRST-EVER acquisition; set once, never updates.
        --   2. session_traffic_source_last_click    (session-level) — last non-direct click attributed to this session.
        --   3. collected_traffic_source             (event-level)  — source as it was when the event was collected.
        --   4. event_params (source/medium/campaign) (event-level) — UTMs captured in the URL at event time.
        --
        -- We deliberately SKIP the user-level `traffic_source` here. If we coalesced it in, every
        -- session for a returning user would inherit their original acquisition source and we'd
        -- lose the ability to see what brought them back today. User-level first-touch is something
        -- we reconstruct downstream in analytics_user by ordering this user's sessions chronologically.
        --
        -- So our chain prefers session_last_click (most authoritative per session), then falls back
        -- to event-level signals. NULLIF strips GA4's '(not set)' sentinel so coalesce can skip it.
        coalesce(
            nullif(session_traffic_source_last_click.source, '(not set)'),
            collected_traffic_source.manual_source,
            (select value.string_value from unnest(event_params) where key = 'source')
        )                                                         as source,

        coalesce(
            nullif(session_traffic_source_last_click.medium, '(not set)'),
            collected_traffic_source.manual_medium,
            (select value.string_value from unnest(event_params) where key = 'medium')
        )                                                         as medium,

        coalesce(
            nullif(session_traffic_source_last_click.campaign, '(not set)'),
            collected_traffic_source.manual_campaign_name,
            (select value.string_value from unnest(event_params) where key = 'campaign')
        )                                                         as campaign,

        -- Click ID
        coalesce(
            collected_traffic_source.gclid,
            (select value.string_value from unnest(event_params) where key = 'gclid')
        )                                                         as gclid,

        -- Device & geo
        device.category                                           as device_category,
        geo.country                                               as country

    from filtered

)

select
    *,
    concat(
        user_pseudo_id,
        '-',
        coalesce(cast(ga_session_id as string), 'no-session')
    ) as session_uid
from flattened
