#!/usr/bin/env python

import re
import fileinput
from sys import argv

N = float(15434)
K = float(100000)
R = float(6)
c = float(3.3838)
m = float(60)
ucm = float(84)
totaljoins = float(7419071)


def calc_message(d,mon,repl):
    bcm = R * ucm
    bcj = K * R / N * uct(d)
    acm = mon * ucm  
    acj = repl / totaljoins * uct(d)
    message = (N*acm + c*m*acj) / (N*bcm + c*m*bcj)
    return message

def calc_stuff():
    bcj = K * R / N
    return bcj*totaljoins
    
def uct(d):
    return ucm + d

r = re.compile("\d+")

def get_file_count(f):
    acc = 0
    with open(f+"/control.messagecost.weighted") as f:
        for line in f:
            if "," in line:
                f,s = line.split(",")
                acc += int(r.match(s).group())
    return float(acc)

a = re.compile("avg.*var")
b = re.compile("\d+\.\d+E-\d+|\d+\.\d+E\d+|\d+\.\d+")

def get_mon(f):
    with open(f+"/control.queries") as f:
        for line in f:
            if "mon:stats" in line:
                m1 = a.search(line).group()
                m2 = b.search(m1).group()
                rval = float(m2)
                return rval

def get_load(f):
    with open(f+"/control.queries") as f:
        for line in f:
            if line.startswith("load:Percentage for 50"):
                m1 = b.search(line).group()
                rval = float(m1)
                return rval

def get_mon_mul(f):
    with open(f+"/control.queries") as f:
        for line in f:
            if "multi_mon:stats" in line:
                m1 = a.search(line).group()
                m2 = b.search(m1).group()
                rval = float(m2)
                return rval

def get_load_mul(f):
    with open(f+"/control.queries") as f:
        for line in f:
            if line.startswith("multi_load:Percentage for 50"):
                m1 = b.search(line).group()
                rval = float(m1)
                return rval

def get_mon_vs(f):
    with open(f+"/control.queries") as f:
        for line in f:
            if "vnode_mon:stats" in line:
                m1 = a.search(line).group()
                m2 = b.search(m1).group()
                rval = float(m2)
                return rval

def get_load_vs(f):
    with open(f+"/control.queries") as f:
        for line in f:
            if line.startswith("vnode_load:Percentage for 50"):
                m1 = b.search(line).group()
                rval = float(m1)
                return rval
 

if __name__ == "__main__":
    import doctest
    doctest.testmod()

    #print calc_stuff()

    folders = []
    for i in range(1,len(argv)):
        folders.append(argv[i])

    count = dict()
    mon = dict()
    load = dict()
    for i in folders:
        count[i] = get_file_count(i)
        mon[i] = get_mon(i)
        load[i] = get_load(i)
        if i.split("/")[-1] == "chord_mp.txt":
            count[i+" vs"] = get_file_count(i)
            mon[i+" vs"] = get_mon_vs(i)
            load[i+" vs"] = get_load_vs(i)

            count[i+" mul"] = get_file_count(i)
            mon[i+" mul"] = get_mon_mul(i)
            load[i+" mul"] = get_load_mul(i)


    for i in count:
        print i.split("/")[-1]
        print mon[i]
        if i.split("/")[-1] == "chord_mp.txt":
            count[i] = totaljoins * K * R / N 
        r1 = calc_message(1024,mon[i],count[i])
        r2 = calc_message(1024*1024,mon[i],count[i])
        r3 = calc_message(1024*1024*1024,mon[i],count[i])
        r4 = load[i]
        print "& %2.2f & %2.2f & %2.2f & %.1f \\\\ \\hline"%(r1,r2,r3,0.5/r4)

