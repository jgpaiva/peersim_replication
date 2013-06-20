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

    for i in ['J','K']:
        d[i] = sample(d[i])

    s = sample(s)
    print "loaded data"

    fig = plt.figure()
    fig.set_size_inches(20,7)

    axS = fig.add_subplot(511)

    from matplotlib.ticker import EngFormatter
    formatter = EngFormatter(places=1)
    axS.yaxis.set_major_formatter(formatter)

    textsize = 9
    axS.plot(s['x'], s['y'], label='Size')
    axS.set_ylabel('#Nodes')
    axS.text(0.025, 0.95, 'Network size', va='top', transform=axS.transAxes, fontsize=textsize)

    print "axS done"


    for p,i in zip(range(512,514),["J","K"]):
        ax = fig.add_subplot(p,sharex=axS)
        ax.set_ylabel('#Keys')
        formatter = EngFormatter(places=1)
        ax.yaxis.set_major_formatter(formatter)
        ax.plot(d[i]['x'], d[i]['y'], label=i)
        ax.text(0.025, 0.95, i, va='top', transform=ax.transAxes, fontsize=textsize)
        print "ax",i,"done"

    for p,i in zip(range(514,516),["D","M"]):
        ax = fig.add_subplot(p,sharex=axS)
        ax.set_ylabel('#Keys')
        formatter = EngFormatter(places=1)
        ax.yaxis.set_major_formatter(formatter)
        ax.plot(d[i]['x'], d[i]['y'], 'o', label=i,markersize=10)
        ax.text(0.025, 0.95, i, va='top', transform=ax.transAxes, fontsize=textsize)
        print "ax",i,"done"

    ax.set_xlabel('time')

    plt.savefig("bw.pdf",bbox_inches='tight')
