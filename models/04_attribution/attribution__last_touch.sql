-- attribution__last_touch.sql
--
-- Last-touch attribution.
--
-- This IS a canonical Activity Schema temporal join: "Last Before".
-- Primary activity  = form_submitted (conversion)
-- Append  activity  = session_started (touchpoint)
-- Relationship      = Last Before — attach the most recent append for the
--                     same customer that happened before the primary.
--
-- Identical to first_touch except QUALIFY orders DESC to keep the latest
-- touchpoint. Output grain: one row per conversion (canonical shape).

select
    c.activity_id    as conversion_id,
    s.session_uid,
    c.customer,
    s.source,
    s.medium,
    s.campaign,
    s.ts             as activity_at,
    c.ts             as conversion_at,
    1.0              as credit_weight

from {{ ref('activity_stream') }} c
inner join {{ ref('activity_stream') }} s
    on c.customer   = s.customer
    and s.ts        < c.ts
    and s.activity  = 'session_started'

where c.activity    = 'form_submitted'
  and c.customer is not null

qualify row_number() over (
    partition by c.activity_id
    order by s.ts desc                     -- DESC = last touch
) = 1
