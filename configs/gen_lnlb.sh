#!/bin/sh

OUTFOLDER=gen/lnlb_preemptive
rm -r $OUTFOLDER 2>/dev/null
mkdir -p $OUTFOLDER

for slice in 0.01 0.1 0.2 0.5 0.8 0.9 0.99
do
    for reliable in 0.01 0.1 0.2 0.5 0.8 0.9 0.99
    do
        echo "creating $slice $reliable"
        slice_cut=`echo "$slice" | grep --only-matching -P "(?<=0.).*"`
        reliable_cut=`echo "$reliable" | grep --only-matching -P "(?<=0.).*"`
        cat lnlb_preemptive_template.txt | sed "s/SLICETOKEN/$slice/" | sed "s/RELIABLETOKEN/$reliable/" > $OUTFOLDER/lnlb_preemp_${slice_cut}_${reliable_cut}
    done
done
