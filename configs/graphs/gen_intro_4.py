#!/usr/bin/env python

from utils import get_numbers, transpose, plot_and_save, get_num_dict, expand_num_dict, plt
import glob
import fileinput

def get_50_percent(d):
    '''
    get 50% load percentage from a dictionary
    >>> get_50_percent({1:1,2:2,3:1})
    0.5
    '''
    exp = sorted(expand_num_dict(d))
    total_load = sum(exp)
    nodes = float(len(exp))
    acc = 0
    for i,l in enumerate(reversed(exp)):
        acc += l
        if(acc) >= total_load/2:
            break
    i+=1
    return i/nodes
        
if __name__ == "__main__":
    import doctest
    doctest.testmod()

    folder = "../tmp/*"

    x_values1 = []
    x_values2 = []
    chord_loads = []
    best_loads = []
    for f in glob.glob(folder):
        if "chord" in f:
            loads = []
            for line in fileinput.input(f+"/control.queries"):
                if "loadFreqs" in line:
                    loads.append(line)
            if len(loads) < 1:
                print "coudln't find loads for " + f
                continue
            loads_d = []
            for i in loads:
                loads_d.append(get_num_dict(i))

            chord_loads.append(get_50_percent(loads_d[0]))

            x_values1.append(get_numbers(f)[0])

    for f in glob.glob(folder):
        if "best" in f:
            loads = []
            for line in fileinput.input(f+"/control.queries"):
                if "loadFreqs" in line:
                    loads.append(line)
            if len(loads) < 1:
                print "coudln't find loads for " + f
                continue
            loads_d = []
            for i in loads:
                loads_d.append(get_num_dict(i))

            best_loads.append(get_50_percent(loads_d[0]))

            x_values2.append(get_numbers(f)[0])

    plt.xlabel("#Nodes")
    plt.ylabel("% of nodes storing 50% of data")

    plt.ylim(0,0.5)

    out_file = "intro_4.pdf"
    plot_and_save(
            out_file,
            (x_values1,chord_loads,"Neighbour Replication"),
            (x_values2,best_loads,"Best Node Replication"))
