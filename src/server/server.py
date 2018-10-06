#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

import queue
import threading

from utils import simulate_data
from analysis import msg_analysis
from insert import insert

q1 = queue.Queue()
q2 = queue.Queue()

t1 = threading.Thread(target=simulate_data, args=(q1,))
t2 = threading.Thread(target=msg_analysis, args=(q1,q2))
t3 = threading.Thread(target=insert, args=([q2],))

t1.start()
t2.start()
t3.start()