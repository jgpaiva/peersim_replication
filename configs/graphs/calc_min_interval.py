#!/usr/bin/env python

from utils import get_numbers, transpose, plot_and_save, get_num_dict, expand_num_dict, plt
import glob
import fileinput
from sys import argv
import numpy as np

def avg_intervals(points,size=100):
    d = dict(zip(points['x'],points['y']))
    r = {'x':[],'y':[]} 
    minX = min(points['x'])
    maxX = max(points['x'])+1
    acc = 0
    cnt = 0
    for x in range(minX,maxX):
        cnt += 1
        acc += d[x]
        if cnt == size:
            r['x'].append(x)
            r['y'].append(acc/float(cnt))
            acc = 0
            cnt = 0
    return r

def sample(points,sample=100):
    r = {'x':[],'y':[]} 
    cnt = 0;
    for x,y in zip(points['x'],points['y']):
        cnt += 1
        if cnt == sample:
            r['x'].append(x)
            r['y'].append(y)
            cnt = 0
    return r

if __name__ == "__main__":
    import doctest
    doctest.testmod()

    f = argv[1]

    d = dict()
    for i in ["J","K","D","M"]:
        d[i] = {'x':[],'y':[]}
    s = {'x':[],'y':[]}

    m = float("inf")

    j = float("-inf")


    ns = []
    joins = 0
    leaves = 0
     
    with open(f+"/out.execution") as infile:
        for line in infile:
            if line.startswith("J") :
                _,t,n,k = line.split(" ")
                n=int(n)
                j = int(t)
                a = 27000
                if t < a:
                    continue
                joins += 1
                ns.append(n)

                
            if line.startswith("K"):
                _,t,n,k = line.split(" ")
                n=int(n)
                a = 27000
                if t < a:
                    continue
                leaves += 1
                t = int(t)
                ns.append(n)

                if m > t - j:
                    m = t - j

    print m
    print sum(ns)/float(len(ns))
    print joins+leaves
