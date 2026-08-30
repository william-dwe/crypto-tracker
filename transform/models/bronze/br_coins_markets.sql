-- Bronze: raw payloads restricted to loads that actually completed.
--
-- Structurally 1:1 with the source — no renaming, casting or business logic,
-- which is silver's job. The one thing this layer adds is trust: dlt writes rows
-- before it marks a load successful, so an interrupted run can leave rows behind
-- whose `load_id` never reaches `status = 0`. Selecting straight from the dlt
-- table would silently include them.

select source.*
from {{ source('bronze', 'coins_markets_raw') }} as source
inner join {{ ref('br_completed_loads') }} as loads
    on loads.load_id = source._dlt_load_id
