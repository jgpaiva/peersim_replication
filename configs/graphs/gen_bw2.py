#!/usr/bin/env python

from utils import get_numbers, transpose, plot_and_save, get_num_dict, expand_num_dict, plt
import glob
import fileinput
from sys import argv

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

    names = {"J":"Joins","K":"Leaves","D":"Divisions","M":"Merges"}

    for i in ["J","K","D","M"]:
        d[i] = {'x':[],'y':[]}
    s = {'x':[],'y':[]}

    for line in fileinput.input(f+"/out.execution"):
        if line.startswith("J") or line.startswith("M"):
            _,t,n,k = line.split(" ")
            t,n,k = map(int,[t,n,k])
            a = 500000
            if t < a:
                continue

            d[line[:1]]['x'].append(t-a)
            d[line[:1]]['y'].append(k)
            s['x'].append(t-a)
            s['y'].append(n)

    for i in ['J']:
        d[i] = sample(d[i],1000)

    s = sample(s)
    print "loaded data"

    fig = plt.figure()
    fig.set_size_inches(20,7)

    axS = fig.add_subplot(311)

    from matplotlib.ticker import EngFormatter
    formatter = EngFormatter(places=1)
    axS.yaxis.set_major_formatter(formatter)

    textsize = 11
    axS.plot(s['x'], s['y'], label='Size')
    axS.set_xlim(right=(1600000))
    axS.set_ylabel('#Nodes')
    axS.text(0.025, 0.95, 'Network size', va='top', transform=axS.transAxes, fontsize=textsize)

    print "axS done"


    for p,i in zip([312],["J"]):
        ax = fig.add_subplot(p,sharex=axS)
        ax.set_ylabel('#Keys Moved')
        formatter = EngFormatter(places=1)
        ax.yaxis.set_major_formatter(formatter)
        ax.plot(d[i]['x'], d[i]['y'], 'o', label=i, markersize=6)
        ax.text(0.025, 0.95, names[i], va='top', transform=ax.transAxes, fontsize=textsize)
        print "ax",i,"done"

    for p,i in zip([313],["M"]):
        ax = fig.add_subplot(p,sharex=axS)
        ax.set_ylabel('#Keys Moved')
        formatter = EngFormatter(places=1)
        ax.yaxis.set_major_formatter(formatter)
        ax.plot(d[i]['x'], d[i]['y'], 'o', label=i,markersize=6)
        ax.text(0.025, 0.95, names[i], va='top', transform=ax.transAxes, fontsize=textsize)
        print "ax",i,"done"

    ax.set_xlabel('Time (Seconds)')

    plt.savefig("bw.pdf",bbox_inches='tight')
