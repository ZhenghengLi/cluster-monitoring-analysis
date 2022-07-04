#!/usr/bin/env python3

import argparse
import time
import os
from smtplib import SMTP_SSL
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import subprocess
from typing import Sequence


def send_mail(user_163: str, passwd_163: str, to_addrs: Sequence[str], subject: str, message: str):
    smtp_host = 'smtp.163.com'
    smtp_port = 465
    smtp_user = user_163
    smtp_pass = passwd_163
    from_addr = user_163
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = ', '.join(to_addrs)
    msg['Subject'] = subject
    msg['Reply-to'] = 'lizhh1@shanghaitech.edu.cn'
    msg.attach(MIMEText('<pre style="font-family: monospace">' +
                        message + '</pre>', 'html'))
    server_ssl = SMTP_SSL(smtp_host, smtp_port)
    server_ssl.login(smtp_user, smtp_pass)
    server_ssl.sendmail(from_addr, to_addrs, msg.as_string())
    server_ssl.quit()


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
parser.add_argument("--user163", dest="user163",
                    help="email user of 163", type=str)
parser.add_argument("--pass163", dest="pass163",
                    help="email password of 163", type=str)
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
message_str += "\n==== CPU utilization of all nodes =================================================================\n"
message_str += cpu_util_all_output
message_str += "\n==== GPU utilization of all nodes =================================================================\n"
message_str += gpu_util_all_output
message_str += "\n==== CPU utilization of all users on all nodes ====================================================\n"
message_str += cpu_util_user_node_output

print(message_str)

mail_header = """
----
This email is automatically sent by a script written by Zhengheng Li (lizhh1@shanghaitech.edu.cn), 
which contains the computing resources utilization statistics of the demo machine (including 9 nodes) 
during the last one week. For realtime monitor, please visit <a src="https://cluster.physky.org/monitor">https://cluster.physky.org/monitor</a>
----
"""

if args.user163 and args.pass163:
    mail_list = [line.rstrip('\n') for line in open(mail_addr_file, 'r')]
    mail_message = mail_header + message_str
    send_mail(args.user163, args.pass163, mail_list,
              "Cluster Computing Resources Utilization Weekly Report", mail_message)
