#!/bin/bash

#script to run batches of instances of peersim

FILENAME=/tmp/lkjasdkkjlnksdiunijncs.tmp

rm $FILENAME

for i in $@
do
    echo "$i" >> $FILENAME
done

vim $FILENAME

COUNTER=0
for i in `cat $FILENAME`
do
    TORUN=`echo "$i" | sed 's:.*/::'`

    date
    ./all_wait.sh $i runs/$TORUN &

    let COUNTER=COUNTER+1

    if [ $COUNTER == 8 ]
    then
        for job in `jobs -p`
        do
            echo waiting for $job
            wait $job
        done
        COUNTER=0
    fi
done

for job in `jobs -p`
do
    echo waiting for $job
    wait $job
done
