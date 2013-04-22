#!/bin/sh

FILENAME=/tmp/lkjasdkkjlnksdiunijncs.tmp

ls -F . | grep -v "\*$" | grep -v "/$" > $FILENAME | grep -v ".jar$"
vim $FILENAME

for i in `cat $FILENAME`
do
    ./run.sh $i tt/$i
done
