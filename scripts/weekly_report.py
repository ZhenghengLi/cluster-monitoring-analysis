#!/usr/bin/env python3

import argparse
import time
import os
from datetime import datetime
import subprocess

parser = argparse.ArgumentParser(description='computing resource usage')

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
                    help="begin time in unix time seconds", default=(int(time.time() - 604800)), type=int)
parser.add_argument("-b", dest="end_time",
                    help="end time in unix time seconds", default=int(time.time()), type=int)
args = parser.parse_args()

script_dir = os.path.dirname(os.path.abspath(__file__))
cpu_util_all_script = os.path.join(script_dir, 'cpu_util_all.py')
gpu_util_all_script = os.path.join(script_dir, 'gpu_util_all.py')
cpu_util_user_node_script = os.path.join(script_dir, 'cpu_util_user_node.py')
mail_addr_file = os.path.join(script_dir, 'aux', 'mail_addresses.list')

cpu_util_all_output = subprocess.check_output(
    [cpu_util_all_script,
     '-H', args.dbhost,
     '-P', str(args.dbport),
     '-n', args.dbname,
     '-u', args.dbuser,
     '-w', args.dbpassword,
     '-a', str(args.begin_time),
     '-b', str(args.end_time)]).decode('utf-8')

gpu_util_all_output = subprocess.check_output(
    [gpu_util_all_script,
     '-H', args.dbhost,
     '-P', str(args.dbport),
     '-n', args.dbname,
     '-u', args.dbuser,
     '-w', args.dbpassword,
     '-a', str(args.begin_time),
     '-b', str(args.end_time)]).decode('utf-8')

cpu_util_user_node_output = subprocess.check_output(
    [cpu_util_user_node_script,
     '-H', args.dbhost,
     '-P', str(args.dbport),
     '-n', args.dbname,
     '-u', args.dbuser,
     '-w', args.dbpassword,
     '-a', str(args.begin_time),
     '-b', str(args.end_time)]).decode('utf-8')

message_str = ""
message_str += "\n==== CPU utilization of all nodes ===================================================================\n"
message_str += cpu_util_all_output
message_str += "\n==== GPU utilization of all nodes ===================================================================\n"
message_str += gpu_util_all_output
message_str += "\n==== CPU utilization of all users on all nodes ======================================================\n"
message_str += cpu_util_user_node_output

print(message_str)
