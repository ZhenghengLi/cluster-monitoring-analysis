select total.node_busid as node_busid,
    round(
        (total.max_time - total.min_time) / 60000.0 / 60./ 24,
        2
    ) as total_time,
    round(total.cnt / 60.0 / 24, 2) as record_time,
    round(cast(usage.gpu_time / 60.0 / 24 as NUMERIC), 2) as gpu_time
from (
        select concat(node, '-', busid) as node_busid,
            count(*) as cnt,
            min(time) as min_time,
            max(time) as max_time
        from node_gpu_load
        group by node,
            busid
    ) as total
    INNER JOIN (
        select concat(node, '-', busid) as node_busid,
            busid,
            sum(gpu / 100.0) as gpu_time
        from node_gpu_load
        group by node,
            busid
    ) as usage on total.node_busid = usage.node_busid