-- The set of dlt load ids that finished successfully.
--
-- `_dlt_loads` is pipeline-internal bookkeeping in dlt's `raw` schema. This
-- model is the single, documented place where the rest of the project reads it,
-- so `status = 0` (dlt's "completed" code) is asserted once rather than being
-- repeated as a magic number across every bronze model.
--
-- Note the column names genuinely differ across the join: `_dlt_loads.load_id`
-- against `<table>._dlt_load_id`.

select
    load_id,
    schema_name,
    inserted_at as load_completed_at
from {{ source('raw', '_dlt_loads') }}
where status = 0
