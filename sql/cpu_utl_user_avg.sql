select usage.user as "user",
    round(usage.using_time_avg, 2) as using_time_avg,
    round(cast(usage.cpu_time_avg as NUMERIC), 2) as cpu_time_avg,
    round(
        cast(coalesce(busy.busy_time_avg, 0) as NUMERIC),
        2
    ) as busy_time_avg,
    round(
        cast(
            usage.cpu_time_avg / usage.using_time_avg as NUMERIC
        ),
        2
    ) as cpu_ratio,
    round(
        cast(
            coalesce(busy.busy_time_avg / usage.cpu_time_avg, 0) as NUMERIC
        ),
        2
    ) as busy_ratio
FROM (
        select "user",
            count(*) / 60.0 / 24.0 / 8.0 as using_time_avg,
            sum(cpu) / 100.0 / 60.0 / 24.0 / 8.0 as cpu_time_avg
        from user_cpu_mem
        group by "user"
        having count(*) / 60.0 / 24.0 / 8.0 >= 0.01
    ) as usage
    LEFT OUTER JOIN (
        select "user",
            sum(cpu) / 100.0 / 60.0 / 24.0 / 8.0 as busy_time_avg
        from user_cpu_mem
        where cpu > 95
        group by "user"
    ) as busy on usage.user = busy.user
order by cpu_time_avg desc,
    using_time_avg desc