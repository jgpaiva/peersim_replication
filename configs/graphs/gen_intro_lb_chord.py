#!/usr/bin/env python

from utils import get_numbers, transpose, get_num_dict, expand_num_dict, plt, prepare, plot
import glob
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

    plt.figure().set_size_inches(6.5,5)
    plt.xlabel("#Nodes")
    plt.ylabel("% of nodes storing 50% of data")

    from matplotlib.ticker import EngFormatter
    formatter = EngFormatter(places=0)
    plt.gca().xaxis.set_major_formatter(formatter)

    plt.ylim(0,0.5)
    plt.xlim(0,1000000)

    out_file = "intro_lb_chord.pdf"

    d1 = prepare(x_values,chord_values)
    d2 = prepare(x_values,vserver_loads)

    d1['label'] = 'Neighbor Replication'
    d1['linestyle'] = 'dashed'
    d2['label'] = "Virtual Servers"

    plot(out_file,d1,d2)
