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

conn = psycopg2.connect(host=args.dbhost, port=args.dbport, dbname=args.dbname,
                        user=args.dbuser, password=args.dbpassword)
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
select total.node_busid as node_busid,
    round(
        (total.max_time - total.min_time) / 60000.0 / 60./ 24,
        2
    ) as total_time,
    round(total.cnt / 60.0 / 24, 2) as record_time,
    round(cast(COALESCE(record.cnt, 0) / 60.0 / 24 as NUMERIC), 2) as using_time,
    round(cast(COALESCE(usage.gpu_time, 0) / 60.0 / 24 as NUMERIC), 2) as gpu_time,
    round(cast(COALESCE(usage.gpu_time / record.cnt, 0) as NUMERIC), 2) as ratio
from (
        select concat(node, '-', busid) as node_busid,
            count(*) as cnt,
            min(time) as min_time,
            max(time) as max_time
        from node_gpu_load
        where time > %(begin)s and time < %(end)s
        group by node,
            busid
    ) as total
    LEFT OUTER JOIN (
        select concat(node, '-', busid) as node_busid,
            busid,
            sum(gpu / 100.0) as gpu_time
        from node_gpu_load
        where time > %(begin)s and time < %(end)s
        group by node,
            busid
    ) as usage on total.node_busid = usage.node_busid
    LEFT OUTER JOIN (
        select concat(node, '-', busid) as node_busid,
            count(*) as cnt
        from node_gpu_load
        where gpu > 0 and time > %(begin)s and time < %(end)s
        group by node,
            busid
    ) as record on total.node_busid = record.node_busid
""", {'begin': args.begin_time * 1000, 'end': args.end_time * 1000})

name_list = []
for desc in cur.description:
    name_list.append(desc[0])

print()
print('%25s %12s %12s %12s %12s %12s' % tuple(name_list))
print('%25s %12s %12s %12s %12s %12s' %
      ('', '(days)', '(days)', '(days)', '(days)', ''))
for record in cur:
    print('%25s %12.2f %12.2f %12.2f %12.2f %12.2f' % record)
print()

cur.close()
conn.close()
