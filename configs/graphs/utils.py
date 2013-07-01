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
    >>> list(get_numbers("1 2 3 4 5 6"))
    [1, 2, 3, 4, 5, 6]
    >>> list(get_numbers("1,2,3,4,5,6"))
    [1, 2, 3, 4, 5, 6]
    >>> list(get_numbers("1a2b3c4d5e6"))
    [1, 2, 3, 4, 5, 6]
    >>> list(get_numbers("kjhasdf_123"))
    [123]
    >>> list(get_numbers("kjhasdf_123.12"))
    [123.12]
    '''
    results,remainder = scanner.scan(line)
    retval = []
    for t,val in results:
        if t is "FLOAT":
            yield float(val)
        elif t is "INT":
            yield int(val)

r = re.compile("\d+\.\d+E\d+|\d+E\d+|\d+\.\d+|\d+")
f = re.compile("\d+\.\d+E\d+|\d+\.\d+")
def get_numbers2(line):
    '''
    returns list of floats from a string
    >>> list(get_numbers2("1 2 3 4 5 6"))
    [1, 2, 3, 4, 5, 6]
    >>> list(get_numbers2("1,2,3,4,5,6"))
    [1, 2, 3, 4, 5, 6]
    >>> list(get_numbers2("1a2b3c4d5e6"))
    [1, 2, 3, 4, 5, 6]
    >>> list(get_numbers2("kjhasdf_123"))
    [123]
    >>> list(get_numbers("kjhasdf_123.12"))
    [123.12]
    '''
    m = r.finditer(line)
    for j in m:
        i=j.group()
        if f.match(i):
            yield float(i)
        else:
            yield int(i)

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
        elif(len(i) == 4):
            plt.plot(x,y,label=i[2],)
    plt.legend(loc=0)
    plt.savefig(out_file,bbox_inches='tight')

def prepare(xvals, yvals):
    t = sorted(zip(xvals,yvals))
    x = [z for z,k in t]
    y = [k for z,k in t]
    return {'x':x,'y':y}


def plot(out_file,*args):
    for options in args:
        x = options['x']
        y = options['y']
        del options['x']
        del options['y']

        plt.plot(x,y,**options)

    plt.legend(loc=0)
    plt.savefig(out_file,bbox_inches='tight')

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
