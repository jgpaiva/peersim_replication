#!/bin/sh

OUTFOLDER=gen/lnlb_preemptive
rm -r $OUTFOLDER 2>/dev/null
mkdir -p $OUTFOLDER

for slice in 0.8 0.9 0.99
do
    for aboveavg in 0.2 0.1 0.01
    do
        echo "creating $slice $aboveavg"
        slice_cut=`echo "$slice" | grep --only-matching -P "(?<=0.).*"`
        aboveavg_cut=`echo "$aboveavg" | grep --only-matching -P "(?<=0.).*"`
        cat lnlb_preemptive_template.txt | sed "s/SLICE/$slice/" | sed "s/ABOVEAVG/$aboveavg/" > $OUTFOLDER/lnlb_preemp_${slice_cut}_${aboveavg_cut}
    done
done
