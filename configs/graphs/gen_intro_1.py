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

    to_match = "chord"

    x_values = []
    chord_loads = []
    vserver_loads = []
    for f in glob.glob(folder):
        if to_match in f:
            loads = []
            for line in fileinput.input(f+"/control.queries"):
                if "loadFreqs" in line:
                    loads.append(line)
            if len(loads) < 2:
                print "coudln't find loads for " + f
                continue
            loads_d = []
            for i in loads:
                loads_d.append(get_num_dict(i))

            chord_loads.append(get_50_percent(loads_d[0]))
            vserver_loads.append(get_50_percent(loads_d[1]))

            x_values.append(get_numbers(f)[0])

    plt.xlabel("#Nodes")
    plt.ylabel("% of nodes storing 50% of data")

    plt.ylim(0,0.5)

    out_file = "intro_1.pdf"
    plot_and_save(
            out_file,
            (x_values,chord_loads,"Neighbour Replication")
            ,(x_values,vserver_loads,"Virtual Servers"))
