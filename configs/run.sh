#!/bin/bash

#script to run instances of peersim

BASEDIR=/home/jgpaiva/tidy

FILENAME=$1
FOLDERNAME=$2

if [ $# -ne 2 ]
then
  echo "Usage: `basename $0` {filename} {num iter}"
  exit 65
fi


mkdir -p $FOLDERNAME
cd $FOLDERNAME

rm -rf *
ln -s $BASEDIR/arguments `pwd`

cp $BASEDIR/$FILENAME .
FILENAME=`echo "$FILENAME" | sed 's:.*/::'`
echo "running $FILENAME"

java -Xmx2000M -ea -cp $BASEDIR/peersim.jar:$BASEDIR/libs/jep-2.3.0.jar:$BASEDIR/libs/djep-1.0.0.jar peersim.Simulator $BASEDIR/$FOLDERNAME/$FILENAME 2>execution.err 1>out.execution &
