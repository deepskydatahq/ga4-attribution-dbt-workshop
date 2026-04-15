-- attribution__first_touch.sql
--
-- First-touch attribution.
--
-- This IS a canonical Activity Schema temporal join: "First Before".
-- Primary activity  = form_submitted (conversion)
-- Append  activity  = session_started (touchpoint)
-- Relationship      = First Before — attach the earliest append for the
--                     same customer that happened before the primary.
--
-- In SQL: a self-join on activity_stream with s.ts < c.ts as the temporal
-- predicate, then QUALIFY ROW_NUMBER() = 1 ordered ASC to keep the earliest.
-- Output grain: one row per conversion (canonical shape).

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
    and s.ts        < c.ts                 -- ← the temporal predicate
    and s.activity  = 'session_started'    -- ← the touchpoint activity

where c.activity    = 'form_submitted'     -- ← the conversion activity
  and c.customer is not null

qualify row_number() over (
    partition by c.activity_id
    order by s.ts asc                      -- ASC = first touch
) = 1
