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
    and s.ts        < c.ts
    and s.activity  = 'session_started'

where c.activity    = 'form_submitted'
  and c.customer is not null

qualify row_number() over (
    partition by c.activity_id
    order by s.ts desc                             -- DESC = last touch
) = 1
