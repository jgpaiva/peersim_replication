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

    for line in fileinput.input(f+"/out.execution"):
        if line.startswith("J") or line.startswith("K") or line.startswith("D") or line.startswith("M"):
            t,n,k = get_numbers(line)
            a = 27000
            if t < a:
                continue

            d[line[:1]]['x'].append(t)
            d[line[:1]]['y'].append(k)
            s['x'].append(t)
            s['y'].append(n)

    for i in d:
        print "sum for",i,sum(d[i]['y'])/1000,"x1000"
    for i in d:
        print "avg for",i,sum(d[i]['y'])/float(len(d[i]['y']))
    for i in d:
        print "std for",i,np.std(d[i]['y'])

