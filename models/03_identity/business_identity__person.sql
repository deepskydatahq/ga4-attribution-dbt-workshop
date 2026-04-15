-- business_identity__person
-- Structural placeholder: maps anonymous visitors to known people.
--
-- In production, join Salesforce contacts here on email to resolve
-- anon_id -> person_id. For example:
--
--   select
--       c.id          as person_id,
--       a.anon_id,
--       c.email,
--       current_timestamp() as matched_at
--   from {{ ref('business_identity__anon') }} a
--   inner join {{ ref('stg_salesforce__contacts') }} c
--       on a.email = c.email

with placeholder as (

    select
        cast(null as string)    as person_id,
        cast(null as string)    as anon_id,
        cast(null as string)    as email,
        cast(null as timestamp) as matched_at

)

-- Return zero rows; the schema is the contract.
select *
from placeholder
where 1 = 0
