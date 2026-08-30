-- Bronze: daily history payloads restricted to loads that actually completed.
-- Structurally 1:1 with the source; typing and dedupe happen in silver.

select source.*
from {{ source('raw', 'coin_market_chart_raw') }} as source
inner join {{ ref('br_completed_loads') }} as loads
    on loads.load_id = source._dlt_load_id
