#!/usr/bin/env python

from utils import get_numbers, transpose, plot_and_save, get_num_dict, expand_num_dict, plt
import glob
import fileinput

if __name__ == "__main__":
    import doctest
    doctest.testmod()

    folder = "../tmp/*"

    to_match = "chord"

    x_values = []
    chord_monitor = []
    vserver_monitor = []
    for f in glob.glob(folder):
        if to_match in f:
            monitor = []
            for line in fileinput.input(f+"/control.queries"):
                if "monitorFreqs" in line:
                    monitor.append(line)
            if len(monitor) < 2:
                print "coudln't find loads for " + f
                continue
            monitor_d = []
            for i in monitor:
                monitor_d.append(get_num_dict(i))

            t_d = expand_num_dict(monitor_d[0]) 
            chord_monitor.append(float(sum(t_d))/len(t_d))
            t_d = expand_num_dict(monitor_d[1]) 
            vserver_monitor.append(float(sum(t_d))/len(t_d))

            x_values.append(get_numbers(f)[0])

    plt.figure().set_size_inches(6.5,5)
    plt.xlabel("#Nodes")
    plt.ylabel("#Monitored Nodes")

    from matplotlib.ticker import EngFormatter
    formatter = EngFormatter(places=0)
    plt.gca().xaxis.set_major_formatter(formatter)

    plt.xlim(0,1000000)

    out_file = "intro_mon_chord.pdf"
    plot_and_save(
            out_file,
            (x_values,chord_monitor,"Neighbor Replication"),
            (x_values,vserver_monitor, "Virtual Servers"))
