-- business_identity__account
-- Structural placeholder: maps known people to accounts / companies.
--
-- In production, join Salesforce accounts here on account_id to resolve
-- person_id -> account_id. For example:
--
--   select
--       a.id            as account_id,
--       c.person_id,
--       a.name          as account_name,
--       current_timestamp() as matched_at
--   from {{ ref('business_identity__person') }} c
--   inner join {{ ref('stg_salesforce__accounts') }} a
--       on c.account_id = a.id

with placeholder as (

    select
        cast(null as string)    as account_id,
        cast(null as string)    as person_id,
        cast(null as string)    as account_name,
        cast(null as timestamp) as matched_at

)

-- Return zero rows; the schema is the contract.
select *
from placeholder
where 1 = 0
