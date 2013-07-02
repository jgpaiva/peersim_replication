#!/usr/bin/env python

from __future__ import print_function
import numpy

alpha = 2.5
n = 1000000
dist = numpy.random.zipf(alpha,n)
s = ""

print("# zipf distribution with","alpha:",alpha,"n:",n,"max:",max(dist),"total:",sum(dist))

for i in dist:
    print(s + str(i),end="")
    s = ","
