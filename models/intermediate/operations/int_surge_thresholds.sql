select
    approx_quantiles(surge_multiplier, 100)[offset(95)] as p95_surge,
    approx_quantiles(surge_multiplier, 100)[offset(99)] as p99_surge,
    avg(surge_multiplier)                               as avg_surge,
    max(surge_multiplier)                               as max_surge,
    count(*)                                            as total_trips_analysed
from {{ ref('stg_trips') }}
where surge_multiplier is not null