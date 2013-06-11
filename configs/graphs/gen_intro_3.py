#!/usr/bin/env python

from utils import get_numbers, transpose, plot_and_save, get_num_dict, expand_num_dict, plt
import glob
import fileinput

if __name__ == "__main__":
    import doctest
    doctest.testmod()

    folder = "../tmp/*"

    x_values1 = []
    x_values2 = []
    chord_values = []
    best_values = []
    for f in glob.glob(folder):
        if "chord" in f or "best" in f:
            print "at",f
            count = 0
            for line in fileinput.input(f+"/control.messagecost.weighted"):
                ns = get_numbers(line)
                if len(ns) > 1:
                    count += int(ns[-1])

            nsize = get_numbers(f)[0]
            if "chord" in f:
                chord_values.append(count/nsize)
                x_values1.append(nsize)
            else:
                best_values.append(count/nsize)
                x_values2.append(nsize)


    plt.xlabel("#Nodes")
    plt.ylabel("Per-node Replication Maintenance Cost (KB)")

    plt.yscale('log')

    out_file = "intro_3.pdf"
    plot_and_save(
            out_file,
            (x_values1,chord_values,"Neighbour Replication"),
            (x_values2,best_values,"Best Node Replication"))
