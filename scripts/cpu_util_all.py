#!/usr/bin/env python3

import argparse
import time
import psycopg2 as pg2

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
                    help="begin time", default=0, type=int)
parser.add_argument("-b", dest="end_time",
                    help="end time", default=(1000 * int(time.time())), type=int)
args = parser.parse_args()

print('current time:', args.end_time)

conn = pg2.connect(host='vm.physky.org', port=8862, dbname='cluster_monitoring',
                   user='cluster_monitoring_analysis', password='cluster_monitoring_password')

cur = conn.cursor()

cur.execute("""
select *
from node_cpu_load
where node = 'gpu04'
limit 10
""")
while True:
    res = cur.fetchone()
    if res:
        print(res)
    else:
        print(res)
        break
