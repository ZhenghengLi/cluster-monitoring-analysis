select idle.node as node,
    round(
        (total.max_time - total.min_time) / 60000.0 / 60./ 24,
        2
    ) as total_time,
    round(total.cnt / 60.0 / 24, 2) as record_time,
    round(idle.cnt / 60.0 / 24, 2) as fully_idle,
    round((total.cnt - idle.cnt) / 60.0 / 24, 2) as usage_time,
    round(cast(usage.val / 60.0 / 24 as NUMERIC), 2) as cpu_time,
    round(cast(busy.cnt / 60.0 / 24 as NUMERIC), 2) as fully_busy,
    round(cast(busy.cnt / usage.val as NUMERIC), 2) as busy_ratio
from (
        select node,
            count(*) as cnt
        from node_cpu_load
        where idle > 99.5
        GROUP by node
    ) as idle
    INNER JOIN (
        select node,
            sum(1 - idle / 100.) as cnt
        from node_cpu_load
        where idle < 0.5
        group by node
    ) as busy on idle.node = busy.node
    INNER JOIN (
        select node,
            sum(1 - idle / 100.) as val
        from node_cpu_load
        where idle <= 99.5
        group by node
    ) as usage on idle.node = usage.node
    INNER JOIN (
        select node,
            count(*) as cnt,
            min(time) as min_time,
            max(time) as max_time
        from node_cpu_load
        group by node
    ) as total on idle.node = total.node