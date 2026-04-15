-- business_identity__anon
-- One row per anonymous visitor (user_pseudo_id).
-- Today anon_id = user_pseudo_id (1:1). In production, this is where
-- cross-device / cookie-stitching logic would collapse multiple
-- user_pseudo_ids into a single resolved anon_id.

with visitor_spine as (

    select
        user_pseudo_id,
        min(event_timestamp) as first_seen_at,
        max(event_timestamp) as last_seen_at
    from {{ ref('stg_ga4__events') }}
    group by user_pseudo_id

)

select
    -- 1:1 mapping for now; replace with resolution logic when available
    user_pseudo_id as anon_id,
    user_pseudo_id,
    first_seen_at,
    last_seen_at
from visitor_spine
