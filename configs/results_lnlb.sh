for i in `ls $1 | grep "lnlb"` 
do 
    i="$1/$i"
    SLICE=`echo "$i" | grep --only-matching -P "(?<=p_)\d+" | sed "s/^/0./"`
    ABOVE=`echo "$i" | grep --only-matching -P "(?<=\d_)\d+" | sed "s/^/0./"`
    RES1=`cat $i/control.queries | grep --only-matching -P "^load.*50 Per.*" | grep --only-matching -P "(?<=\: ).*" 2>/dev/null`
    RES2=`cat $i/control.messagecost.weighted| grep "\d" | grep -P --only-matching "(?<=,)\d+" | j_sum 2>/dev/null`
    echo "$SLICE,$ABOVE,$RES1,$RES2"
done
