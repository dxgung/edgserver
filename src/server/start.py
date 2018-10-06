#! /usr/bin/env python
# -*- coding:utf-8 -*-

# CreateDate:
# Author:

import multiprocessing
import queue

from .insert/worker import run
from .find/worker import run
p1 = multiprocessing.Process(target=run)
p2 = multiprocessing.Process(target=find_from_db)