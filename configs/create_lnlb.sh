#!/bin/sh

mkdir -p lnlb_preemptive

for slice in 0.9 0.7 0.5 0.1 0.01 0.001
do
for aboveavg in 0.9 0.7 0.5 0.1 0.01 0.001
    do
        slice_cut=`echo "$slice" | grep --only-matching -P "(?<=0.).*"`
        aboveavg_cut=`echo "$aboveavg" | grep --only-matching -P "(?<=0.).*"`
        cat lnlb_preemptive_template.txt | sed "s/SLICE/$slice/" | sed "s/ABOVEAVG/$aboveavg/" > lnlb_preemptive/lnlb_preemp_${slice_cut}_${aboveavg_cut}
    done
done
