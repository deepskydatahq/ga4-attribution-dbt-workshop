-- attribution__position_based.sql
--
-- Position-based (40/20/40) attribution — CUSTOM implementation (not a
-- canonical Activity Schema temporal join).
--
-- Same reason as linear: the output is one row per (conversion × touchpoint),
-- not one row per conversion, because each touchpoint gets its own weighted
-- credit. We reuse the temporal self-join mechanics but the shape differs
-- from the 12 canonical types.
--
-- Weighting:
--   1 touch    → 1.0
--   2 touches  → 0.5 each
--   3+ touches → first 0.4, last 0.4, middle share 0.2 equally

with touchpoints as (

    select
        c.activity_id    as conversion_id,
        s.session_uid,
        c.customer,
        s.source,
        s.medium,
        s.campaign,
        s.ts             as activity_at,
        c.ts             as conversion_at,

        row_number() over (partition by c.activity_id order by s.ts asc)  as position_asc,
        row_number() over (partition by c.activity_id order by s.ts desc) as position_desc,
        count(*)     over (partition by c.activity_id)                    as touch_count

    from {{ ref('activity_stream') }} c
    inner join {{ ref('activity_stream') }} s
        on c.customer   = s.customer
        and s.ts        < c.ts
        and s.activity  = 'session_started'

    where c.activity    = 'form_submitted'
      and c.customer is not null

)

select
    conversion_id,
    session_uid,
    customer,
    source,
    medium,
    campaign,
    activity_at,
    conversion_at,
    case
        when touch_count   = 1 then 1.0
        when touch_count   = 2 then 0.5
        when position_asc  = 1 then 0.4
        when position_desc = 1 then 0.4
        else 0.2 / (touch_count - 2)
    end as credit_weight
from touchpoints
