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
select idle.node as node,
    round(
        (total.max_time - total.min_time) / 60000.0 / 60./ 24,
        2
    ) as total_time,
    round(total.cnt / 60.0 / 24, 2) as record_time,
    round(idle.cnt / 60.0 / 24, 2) as fully_idle,
    round((total.cnt - idle.cnt) / 60.0 / 24, 2) as using_time,
    round(cast(usage.val / 60.0 / 24 as NUMERIC), 2) as cpu_time,
    round(cast(busy.cnt / 60.0 / 24 as NUMERIC), 2) as fully_busy,
    round(cast(busy.cnt / usage.val as NUMERIC), 2) as busy_ratio
from (
        select node,
            count(*) as cnt
        from node_cpu_load
        where idle > 99.5 and time > %(begin)s and time < %(end)s
        GROUP by node
    ) as idle
    INNER JOIN (
        select node,
            sum(1 - idle / 100.) as cnt
        from node_cpu_load
        where idle < 0.5 and time > %(begin)s and time < %(end)s
        group by node
    ) as busy on idle.node = busy.node
    INNER JOIN (
        select node,
            sum(1 - idle / 100.) as val
        from node_cpu_load
        where idle <= 99.5 and time > %(begin)s and time < %(end)s
        group by node
    ) as usage on idle.node = usage.node
    INNER JOIN (
        select node,
            count(*) as cnt,
            min(time) as min_time,
            max(time) as max_time
        from node_cpu_load
        where time > %(begin)s and time < %(end)s
        group by node
    ) as total on idle.node = total.node
""", {'begin': args.begin_time * 1000, 'end': args.end_time * 1000})

name_list = []
for desc in cur.description:
    name_list.append(desc[0])

print()
print('%7s %12s %12s %12s %12s %12s %12s %12s' % tuple(name_list))
print('%7s %12s %12s %12s %12s %12s %12s %12s' %
      ('', '(days)', '(days)', '(days)', '(days)', '(days)', '(days)', ''))
for record in cur:
    print('%7s %12.2f %12.2f %12.2f %12.2f %12.2f %12.2f %12.2f' % record)
print()

cur.close()
conn.close()
