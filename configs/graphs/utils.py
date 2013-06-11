#!/usr/bin/env python

import matplotlib.pyplot as plt
import re
from itertools import izip as zip

scanner = re.Scanner([
        (r"[0-9]+\.[0-9]+E[0-9]",    lambda scanner,token:("FLOAT", token)),
        (r"[0-9]+E[0-9]",    lambda scanner,token:("FLOAT", token)),
        (r"[0-9]+\.[0-9]+",    lambda scanner,token:("FLOAT", token)),
        (r"[0-9]+",    lambda scanner,token:("INT", token)),
        (r".",None) # ignore everything else
        ])
def get_numbers(line):
    '''
    returns list of floats from a string
    >>> get_numbers("1 2 3 4 5 6")
    [1, 2, 3, 4, 5, 6]
    >>> get_numbers("1,2,3,4,5,6")
    [1, 2, 3, 4, 5, 6]
    >>> get_numbers("1a2b3c4d5e6")
    [1, 2, 3, 4, 5, 6]
    >>> get_numbers("kjhasdf_123")
    [123]
    >>> get_numbers("kjhasdf_123.12")
    [123.12]
    '''
    results,remainder = scanner.scan(line)
    retval = []
    for t,val in results:
        if t is "FLOAT":
            retval.append(float(val))
        elif t is "INT":
            retval.append(int(val))
    return retval

def transpose(matrix):
    '''
    transpose matrix
    >>> transpose([[1,2,3], [4,5,6], [7,8,9]])
    [[1, 4, 7], [2, 5, 8], [3, 6, 9]]
    '''
    return map(list, zip(*matrix))

def plot_and_save(out_file,*args):
    for i in args:
        t = sorted(zip(i[0],i[1]))
        x = [z for z,k in t]
        y = [k for z,k in t]
        if(len(i) == 2):
            plt.plot(x,y)
        elif(len(i) == 3):
            plt.plot(x,y,label=i[2])
    plt.legend(loc=0)
    plt.savefig(out_file)

def get_num_dict(line):
    '''
    transform a frequency string into a frequency dictionary
    >>> get_num_dict("(1,2),(3,4),(10,11)")
    {1: 2, 10: 11, 3: 4}
    '''
    tks = iter(get_numbers(line))
    retval = dict()
    for i, j in zip(tks,tks):
        retval[i] = j
    return retval

def expand_num_dict(d):
    '''
    expand a dictionary of frequencies into a list of entries
    >>> expand_num_dict({1:3,10:2})
    [1, 1, 1, 10, 10]
    '''

    retval = []
    for i,j in d.iteritems():
        for c in xrange(j):
            retval.append(i)
    return retval

if __name__ == "__main__":
    import doctest
    doctest.testmod()
