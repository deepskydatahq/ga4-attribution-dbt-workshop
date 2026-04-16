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
-- Because we follow strict v2, session_uid and the UTMs live inside
-- feature_json — we unpack them with JSON_VALUE in the SELECT.

select
    c.activity_id                                  as conversion_id,
    json_value(s.feature_json, '$.session_uid')    as session_uid,
    c.customer,
    json_value(s.feature_json, '$.source')         as source,
    json_value(s.feature_json, '$.medium')         as medium,
    json_value(s.feature_json, '$.campaign')       as campaign,
    s.ts                                            as activity_at,
    c.ts                                            as conversion_at,
    1.0                                             as credit_weight

from {{ ref('activity_stream') }} c
inner join {{ ref('activity_stream') }} s
    on c.customer   = s.customer
    and s.ts        < c.ts                         -- ← the temporal predicate
    and s.activity  = 'session_started'            -- ← the touchpoint activity

where c.activity    = 'form_submitted'             -- ← the conversion activity
  and c.customer is not null

qualify row_number() over (
    partition by c.activity_id
    order by s.ts asc                              -- ASC = first touch
) = 1
