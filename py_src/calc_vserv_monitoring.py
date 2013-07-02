#!/usr/bin/env python

from random import random as r
from operator import itemgetter

vservers=100
repl=6


for size in [(2**x)*100 for x in xrange(8)]:
    loads = []
    for i in xrange(size):
        loads.append(set())

    ids=[]
    for n in xrange(size):
        for v in xrange(vservers):
            ids.append((n,r()))

    ids.sort(key=itemgetter(1))

    for i in xrange(len(ids)):
        v,ident=ids[i]
        
        found = 0
        current = i
        while(found < repl-1):
            current = (current+1) %size
            mon,ident2 = ids[current]
            if mon != v:
                loads[v].add(mon)
                found+=1

    m = sum((len(x) for x in loads))/float(len(loads))

    print size,m
