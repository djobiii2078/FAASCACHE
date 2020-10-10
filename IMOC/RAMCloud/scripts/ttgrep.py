#!/usr/bin/env python

# Copyright (c) 2016 Stanford University
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""
Scan the time trace data in a log file; find all records containing
a given string, and output only those records, renormalized in time
so that the first record is at time 0.
Usage: ttgrep.py string file
"""

from __future__ import division, print_function
from glob import glob
from optparse import OptionParser
import math
import os
import re
import string
import sys

def scan(f, string):
    """
    Scan the log file given by 'f' (handle for an open file) and output
    all-time trace records containing string, with times renormalized
    relative to the time of the first matching record.
    """

    startTime = 0.0
    prevTime = 0.0
    writes = 0
    for line in f:
        match = re.match('.*TimeTrace.*printInternal.* '
                '([0-9.]+) ns \(\+ *([0-9.]+) ns\): (.*)',
                line)
        if not match:
            continue
        time = float(match.group(1))
        interval = float(match.group(2))
        event = match.group(3)
        if string not in event:
            continue
        if time < prevTime:
            # Time went backwards: there must be multiple time traces
            # in the log. Restart from a new time 0.
            startTime = time
            prevTime = time
        if startTime == 0.0:
            startTime = time
            prevTime = time
        print("%9.3f us (+%7.3f us): %s" % ((time - startTime)/1000.0,
                (time - prevTime)/1000.0, event))
        prevTime = time

if len(sys.argv) != 3:
    print("Usage: %s string logFile" % (sys.argv[0]))
    sys.exit(1)

scan(open(sys.argv[2]), sys.argv[1])