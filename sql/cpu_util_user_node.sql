select per_node.user as user,
    per_node.node as node,
    round(cast(per_node.using_time as NUMERIC), 2) as using_time,
    round(cast(per_node.cpu_time as NUMERIC), 2) as cpu_time,
    round(
        cast(
            per_node.cpu_time / per_node.using_time as NUMERIC
        ),
        2
    ) as ratio,
    round(cast(all_node.using_time as NUMERIC), 2) as using_time_all,
    round(cast(all_node.cpu_time as NUMERIC), 2) as cpu_time_all,
    round(
        cast(
            all_node.cpu_time / all_node.using_time as NUMERIC
        ),
        2
    ) as ratio_all
FROM (
        select "user",
            count(*) / 60.0 / 24.0 as using_time,
            sum(cpu) / 100.0 / 60.0 / 24.0 as cpu_time
        from user_cpu_mem
        group by "user"
    ) as all_node
    INNER JOIN (
        select "user",
            node,
            count(*) / 60.0 / 24.0 as using_time,
            sum(cpu) / 100.0 / 60.0 / 24.0 as cpu_time
        from user_cpu_mem
        group by "user",
            node
    ) as per_node on all_node.user = per_node.user
where per_node.using_time >= 0.01
order by cpu_time_all desc,
    using_time_all desc,
    "user",
    node