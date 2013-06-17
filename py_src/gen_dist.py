#!/usr/bin/env python

from __future__ import print_function
import numpy

alfa = 2.5
n = 100000
dist = numpy.random.zipf(alfa,100000)
s = ""

print("# zipf distribution with","alfa:",alfa,"n:",n,"max:",max(dist),"total:",sum(dist))

for i in dist:
    print(s + str(i),end="")
    s = ","
