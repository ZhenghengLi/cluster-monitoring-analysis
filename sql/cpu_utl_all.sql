select time,
    node,
    100 - idle,
    memory
from node_cpu_load
limit 30;