#!/usr/bin/env python
# set style line 2 lt 7 lw 3 pt 7 ps 0.4 linecolor rgb "forest-green"
# set style line 4 lt 1 lw 3 pt 6 ps 0.8 linecolor rgb "dark-red"
# set style line 5 lt 0 lw 3 pt 6 ps 1.2 linecolor rgb "web-blue"
# set style line 1 lt 1 lw 4.0 pt 6 ps 1.3 linecolor rgb "dark-red"
# set style line 2 lt 1 lw 4.0 pt 6 ps 0.9 linecolor rgb "navy-blue"
# set style line 3 lt 1 lw 4.0 pt 6 ps 0.5 linecolor rgb "forest-green"
# set log y
# set yrange [8000:60000]
# set ytics("8000" 8000, "16000" 16000, "25000" 25000, "50000" 50000) nomirror font "Helvetica,13"
# set xtics("2" 2, "4" 4, "6" 6, "10" 10) nomirror font "Helvetica,13"
# set xlabel "#nodes" font "Helvetica,14"
# set ylabel "throughput (ops/s)" font "Helvetica,14"         
# set key top right font "Helvetica,13"

import sh

def draw_graph(filename,style,key,logx,logy,x_range,y_range,x_label,y_label,lines,in_data):
    c  = "set terminal pdf enhanced"
    c += "\n"
    c += "set output '" + filename + "'"
    c += "\n"
    c += "set style " + style
    c += "\n"
    c += "set size 1.0,1.0"
    c += "\n"
    c += "set key " + key
    c += "\n"
    if logx:
        c += "set log x"
        c += "\n"
    if logy:
        c += "set log y"
        c += "\n"
    if x_range is not "":
        c += "set xrange " + x_range
        c += "\n"
    if y_range is not "":
        c += "set yrange " + y_range
        c += "\n"
    if x_label is not "":
        c += "set xlabel '" + x_label + "'  font 'Helvetica,10'"
        c += "\n"
    if y_label is not "":
        c += "set ylabel '" + y_label + "'  font 'Helvetica,10'"
        c += "\n"

    c += lines
    c += "\n"
    c += "set font 'Helvetica,10'"
    c += "\n"

    c += in_data
    c += "\n"

    sh.gnuplot(_in=c)

filename="intro_chord1.pdf"
logx = True
logy = False
key = "center right"
style = "data lines"
x_range = "[8000:1024000]"
y_range = ""
x_label = "#Nodes"
y_label = "#Monitored Nodes"
lines = '''
set style line 1 lt 1 lw 4.0 pt 6 ps 1.3 linecolor rgb "red"
set style line 2 lt 1 lw 4.0 pt 6 ps 0.9 linecolor rgb "blue"
'''
in_data = '''plot "chord.out" using 1:4 title "Neighbour Replication" ls 1, "vservers.out" using 1:4 title "Virtual Servers" ls 2'''
draw_graph(filename,style,key,logx,logy,x_range,y_range,x_label,y_label,lines,in_data)

filename="intro_chord2.pdf"
logx = True
logy = False
key = "center right"
style = "data lines"
x_range = "[8000:1024000]"
y_range = "[0:0.5]"
x_label = "#Nodes"
y_label = "% of nodes storing 50% of data"
lines = '''
set style line 1 lt 1 lw 4.0 pt 6 ps 1.3 linecolor rgb "red"
set style line 2 lt 1 lw 4.0 pt 6 ps 0.9 linecolor rgb "blue"
'''
in_data = '''plot "chord.out" using 1:2 title "Neighbour Replication" ls 1, "vservers.out" using 1:2 title "Virtual Servers" ls 2'''
draw_graph(filename,style,key,logx,logy,x_range,y_range,x_label,y_label,lines,in_data)
