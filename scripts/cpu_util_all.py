#!/usr/bin/env python3

import argparse
import time
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

conn = psycopg2.connect(host='vm.physky.org', port=8862, dbname='cluster_monitoring',
                        user='cluster_monitoring_analysis', password='cluster_monitoring_password')

args.begin_time *= 1000
args.end_time *= 1000

cur = conn.cursor()

cur.execute("""
select node, count(*)
from node_cpu_load
where time > %s and time < %s
group by node
""", (args.begin_time, args.end_time))

res = cur.fetchall()

print(res)
