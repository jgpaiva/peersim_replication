#!/bin/bash

#fail-fast bash
set -o errexit
set -o errtrace 
set -o nounset  

TMP=`mktemp`
cat $1 | grep -v "chord_2000" | grep -v "chord_4000" | sed "s:^tmp/chord_::" | sed "s:,: :g" | sort -g > $TMP
mv $TMP $1
