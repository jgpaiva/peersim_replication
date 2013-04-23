#!/bin/bash

OUTFOLDER=gen/motivation
rm -r $OUTFOLDER
mkdir -p $OUTFOLDER

for SIZE in 2000 4000 8000 16000 32000 64000 128000 256000 512000 1024000
do
    echo creating $SIZE
    cat motivation_chord_template.txt | sed "s/SIZETOKEN/$SIZE/" > $OUTFOLDER/chord_$SIZE
    cat motivation_best_template.txt | sed "s/SIZETOKEN/$SIZE/" > $OUTFOLDER/best_$SIZE
done
