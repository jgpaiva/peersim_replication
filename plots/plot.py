#!/usr/bin/env python

import argparse
import random
import matplotlib.pyplot as plt

def plotxy(f):
    lines = f.readlines()
    x = []
    y = []
    for l in lines:
        c_x,c_y =l.strip().split(' ')
        x.append(c_x)
        y.append(c_y)
    return x,y

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Plot a graph')
    parser.add_argument('files', metavar='files', type=argparse.FileType('r'), nargs='+',
                            help='files to take as input')
    args = parser.parse_args()

    #plt.xlabel('x axis label')
    #plt.ylabel('y axis label')

    for i in args.files:
        print "plotting for file ",i
        x,y=plotxy(i)
        plt.plot(x,y)

    plt.savefig('out.pdf')
