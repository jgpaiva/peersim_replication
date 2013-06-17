#!/usr/bin/env python

from __future__ import print_function
import numpy

alpha = 2.5
n = 100000
dist = numpy.random.zipf(alpha,100000)
s = ""

print("# zipf distribution with","alpha:",alpha,"n:",n,"max:",max(dist),"total:",sum(dist))

for i in sorted(dist):
    print(s + str(i),end="")
    s = ","
