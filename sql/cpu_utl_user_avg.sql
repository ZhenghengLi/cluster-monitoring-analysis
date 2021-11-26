select "user",
    round(cast(count(*) / 60.0 / 24.0 / 8.0 as NUMERIC), 2) as using_time,
    round(
        cast(sum(cpu) / 100.0 / 60.0 / 24.0 / 8.0 as NUMERIC),
        2
    ) as cpu_time
from user_cpu_mem
group by "user"
having count(*) / 60.0 / 24.0 / 8.0 >= 0.01
order by cpu_time desc,
    using_time desc