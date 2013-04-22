#!/bin/bash

OUTFOLDER=gen/motivation
rm -r $OUTFOLDER
mkdir -p $OUTFOLDER

for SIZE in 1000 2000 4000 8000 16000 32000 64000 120000
do
    echo creating $SIZE
    cat motivation_chord_template.txt | sed "s/SIZETOKEN/$SIZE/" > $OUTFOLDER/chord_$SIZE
    cat motivation_best_template.txt | sed "s/SIZETOKEN/$SIZE/" > $OUTFOLDER/best_$SIZE
done
