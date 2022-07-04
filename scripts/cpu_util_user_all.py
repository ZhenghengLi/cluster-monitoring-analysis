#!/usr/bin/env python3

import argparse
import time
from datetime import datetime
import psycopg2

parser = argparse.ArgumentParser(description='cpu usage statistics')

parser.add_argument("-H", dest="dbhost",
                    help="database host", default='vm.physky.org', type=str)
parser.add_argument("-P", dest="dbport",
                    help="database port", default=8862, type=int)
parser.add_argument("-n", dest="dbname",
                    help="database name", default='cluster_monitoring', type=str)
parser.add_argument("-u", dest="dbuser",
                    help="database user", default='cluster_monitoring_analysis', type=str)
parser.add_argument("-w", dest="dbpassword",
                    help="database password", default='cluster_monitoring_password', type=str)
parser.add_argument("-a", dest="begin_time",
                    help="begin time in unix time seconds", default=0, type=int)
parser.add_argument("-b", dest="end_time",
                    help="end time in unix time seconds", default=int(time.time()), type=int)
args = parser.parse_args()

timefmt = '%Y-%m-%dT%H:%M:%S'

conn = psycopg2.connect(host='vm.physky.org', port=8862, dbname='cluster_monitoring',
                        user='cluster_monitoring_analysis', password='cluster_monitoring_password')
cur = conn.cursor()

cur.execute("""
select min(time) / 1000 as min_time, max(time) / 1000 as max_time
from node_cpu_load
where time > %s and time < %s
""", (args.begin_time * 1000, args.end_time * 1000))

begin_time_s, end_time_s = cur.fetchone()
actual_period_m = end_time_s / 60 - begin_time_s / 60
begin_time = datetime.fromtimestamp(begin_time_s)
end_time = datetime.fromtimestamp(end_time_s)

print()
print('  query time range: %s => %s' %
      (datetime.fromtimestamp(args.begin_time).strftime(timefmt),
       datetime.fromtimestamp(args.end_time).strftime(timefmt)))
print(' actual time range: %s => %s (%.2f days)' %
      (begin_time.strftime(timefmt), end_time.strftime(timefmt), actual_period_m / 60.0 / 24.0))

cur.execute("""
select usage.user as "user",
    round(usage.using_time_all, 2) as using_time_all,
    round(cast(usage.cpu_time_all as NUMERIC), 2) as cpu_time_all,
    round(
        cast(coalesce(busy.busy_time_all, 0) as NUMERIC),
        2
    ) as fully_busy_all,
    round(
        cast(
            usage.cpu_time_all / usage.using_time_all as NUMERIC
        ),
        2
    ) as cpu_ratio,
    round(
        cast(
            coalesce(busy.busy_time_all / usage.cpu_time_all, 0) as NUMERIC
        ),
        2
    ) as busy_ratio
FROM (
        select "user",
            count(*) / 60.0 / 24.0 as using_time_all,
            sum(cpu) / 100.0 / 60.0 / 24.0 as cpu_time_all
        from user_cpu_mem
        where time > %(begin)s and time < %(end)s
        group by "user"
        having count(*) / 60.0 / 24.0 / 8.0 >= 0.01
    ) as usage
    LEFT OUTER JOIN (
        select "user",
            sum(cpu) / 100.0 / 60.0 / 24.0 as busy_time_all
        from user_cpu_mem
        where cpu > 95 and time > %(begin)s and time < %(end)s
        group by "user"
    ) as busy on usage.user = busy.user
order by cpu_time_all desc,
    using_time_all desc
""", {'begin': args.begin_time * 1000, 'end': args.end_time * 1000})

name_list = []
for desc in cur.description:
    name_list.append(desc[0])

print()
print('%10s %16s %15s %16s %15s %15s' % tuple(name_list))
print('%10s %16s %15s %16s %15s %15s' %
      ('', '(days)', '(days)', '(days)', '', ''))

for record in cur:
    print('%10s %16.2f %15.2f %16.2f %15.2f %15.2f' % record)
print()

cur.close()
conn.close()
