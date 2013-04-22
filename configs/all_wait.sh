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
ln -s /home/jgpaiva/tidy/arguments `pwd`

cp /home/jgpaiva/tidy/$FILENAME .
FILENAME=`echo "$FILENAME" | sed 's:.*/::'`
echo "running $FILENAME"

java -Xmx2000M -ea -cp /home/jgpaiva/tidy/peersim.jar:/home/jgpaiva/tidy/libs/jep-2.3.0.jar:/home/jgpaiva/tidy/libs/djep-1.0.0.jar peersim.Simulator /home/jgpaiva/tidy/$FOLDERNAME/$FILENAME 2>execution.err 1>out.execution
#java -Xmx2000M  -cp /home/jgpaiva/peersim.jar:/home/jgpaiva/libs/jep-2.3.0.jar:/home/jgpaiva/libs/djep-1.0.0.jar peersim.Simulator /home/jgpaiva/$FOLDERNAME/$FILENAME 2>execution.err 1>out.execution &
cd ..
