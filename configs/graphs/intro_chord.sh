#!/bin/sh
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

gnuplot << EOF

set terminal pdf enhanced
set output 'intro_chord1.pdf'


set style data lines
set size 1.0,1.0
set key center right
set log x
set xrange [8000:1024000]
set xlabel "#Nodes" font "Helvetica,10"
set ylabel "#Monitored Nodes" font "Helvetica,10"         

set style line 1 lt 1 lw 4.0 pt 6 ps 1.3 linecolor rgb "red"
set style line 2 lt 1 lw 4.0 pt 6 ps 0.9 linecolor rgb "blue"

set font "Helvetica,10"

plot "chord.out" using 1:4 title "Neighbour Replication" ls 1, "vservers.out" using 1:4 title "Virtual Servers" ls 2

EOF

gnuplot << EOF

set terminal pdf enhanced
set output 'intro_chord2.pdf'


set style data lines
set size 1.0,1.0
set key center right
set log x
met xrange [8000:1024000]
set xlabel "#Nodes" font "Helvetica,10"
set ylabel "% of nodes storing 50% of data" font "Helvetica,10"         

set style line 1 lt 1 lw 4.0 pt 6 ps 1.3 linecolor rgb "red"
set style line 2 lt 1 lw 4.0 pt 6 ps 0.9 linecolor rgb "blue"

set font "Helvetica,10"

plot "chord.out" using 1:2 title "Neighbour Replication" ls 1, "vservers.out" using 1:2 title "Virtual Servers" ls 2

EOF
