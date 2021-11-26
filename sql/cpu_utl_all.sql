select idle.node as node,
    (total.max_time - total.min_time) / 60000.0 / 60./ 24 as total_time,
    total.cnt / 60.0 / 24 as record_time,
    idle.cnt / 60.0 / 24 as fully_idle,
    (total.cnt - idle.cnt) / 60.0 / 24 as usage_time,
    usage.val / 60.0 / 24 as cpu_time,
    busy.cnt / 60.0 / 24 as fully_busy,
    busy.cnt / usage.val as busy_ratio
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