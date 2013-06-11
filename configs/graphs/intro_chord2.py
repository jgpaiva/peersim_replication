#!/usr/bin/env python

import matplotlib.pyplot as plt
import fileinput
import re

def tokenize(line):
    scanner = re.Scanner([
        (r"[0-9]+\.[0-9]+E[0-9]",    lambda scanner,token:("NUMBER", token)),
        (r"[0-9]+E[0-9]",    lambda scanner,token:("NUMBER", token)),
        (r"[0-9]+\.[0-9]+",    lambda scanner,token:("NUMBER", token)),
        (r"[0-9]+",    lambda scanner,token:("NUMBER", token)),
        (r".",None) # ignore everything else
        ])
    results,remainder = scanner.scan(line)
    return results

def read_file(filename):
    contents=[]
    for line in fileinput.input(filename):
        fill_contents(contents,line)
    contents = transpose(contents)
    return contents

def fill_contents(contents,line):
    read_line = []
    for t,val in tokenize(line):
        read_line.append(val)
    contents.append(read_line)

def transpose(matrix):
    return map(list, zip(*matrix))

def plot_and_save(out_file,*args):
    for i in args:
        plt.plot(i[0],i[1])
    plt.savefig(out_file)


f1 = read_file("chord.out")
f2 = read_file("vservers.out")


plt.xlabel("#Nodes")
plt.ylabel("#Monitored Nodes")

out_file = "intro_chord1.pdf"
plot_and_save(out_file,(f1[0],f1[3]),(f2[0],f2[3]))

plt.clf()
plt.cla()

############ graph 2 ############

##normalize load balancing
#f1[1] = [float(x)*2 for x in f1[1]]
#f2[1] = [float(x)*2 for x in f2[1]]

plt.xlabel("#Nodes")
plt.ylabel("% of nodes storing 50% of data")

plt.ylim(0,0.5)

out_file = "intro_chord2.pdf"
plot_and_save(out_file,(f1[0],f1[1]),(f2[0],f2[1]))
