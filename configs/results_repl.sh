for i in `ls tmp | grep -v "lnlb"` 
do 
    i="tmp/$i"
    RES1=`cat $i/control.queries | grep --only-matching -P "50 Per.*" | head -n 1 | grep --only-matching -P "(?<=\: ).*" 2>/dev/null`
    RES2=`cat $i/control.messagecost.weighted| grep "\d" | grep -P --only-matching "(?<=,)\d+" | j_sum 2>/dev/null`
    echo "$i,$RES1,$RES2"
done
