-- attribution__linear.sql
--
-- Linear attribution — CUSTOM implementation (not a canonical Activity
-- Schema temporal join).
--
-- Why it's not canonical: all 12 Activity Schema temporal join types produce
-- ONE ROW PER PRIMARY (conversion). Multi-touch attribution needs to fan out
-- to one row per (conversion × touchpoint) pair so each touchpoint can carry
-- its own fractional credit_weight — a different output grain.
--
-- We reuse the same temporal join MECHANICS (self-join on activity_stream
-- with s.ts < c.ts), but skip the "pick one" step: every preceding session
-- survives, and credit_weight = 1.0 / touch_count. Weights sum to 1.0 per
-- conversion by construction.

select
    c.activity_id    as conversion_id,
    s.session_uid,
    c.customer,
    s.source,
    s.medium,
    s.campaign,
    s.ts             as activity_at,
    c.ts             as conversion_at,
    1.0 / count(*) over (partition by c.activity_id) as credit_weight

from {{ ref('activity_stream') }} c
inner join {{ ref('activity_stream') }} s
    on c.customer   = s.customer
    and s.ts        < c.ts
    and s.activity  = 'session_started'

where c.activity    = 'form_submitted'
  and c.customer is not null
