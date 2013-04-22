echo "for non-lbk"
for i in `ls tt | grep preemp | grep -v lbk` 
do 
    i="tt/$i"
    SLICE=`echo "$i" | grep --only-matching -P "(?<=e_)\d+" | sed "s/^/0./"`
    ABOVE=`echo "$i" | grep --only-matching -P "(?<=\d_)\d+" | sed "s/^/0./"`
    RES1=`cat $i/control.queries2 | grep --only-matching -P "50 Per.*" | grep --only-matching -P "(?<=\: )..?.?.?.?.?" 2>/dev/null`
    RES2=`cat $i/control.messagecost.weighted| grep "\d" | grep -P --only-matching "(?<=,)\d+" | j_sum 2>/dev/null`
    echo "$SLICE,$ABOVE,$RES1,$RES2"
done

echo "for lbk"
for i in `ls tt | grep preemp | grep lbk` 
do 
    i="tt/$i"
    SLICE=`echo "$i" | grep --only-matching -P "(?<=e_)\d+" | sed "s/^/0./"`
    ABOVE=`echo "$i" | grep --only-matching -P "(?<=\d_)\d+" | sed "s/^/0./"`
    RES1=`cat $i/control.queries2 | grep --only-matching -P "50 Per.*" | grep --only-matching -P "(?<=\: ).?.?.?.?.?.?" 2>/dev/null`
    RES2=`cat $i/control.messagecost.weighted| grep "\d" | grep -P --only-matching "(?<=,)\d+" | j_sum 2>/dev/null`
    echo "$SLICE,$ABOVE,$RES1,$RES2"
done

echo "for lnlb"
for i in `ls tt | grep lnlb.txt` 
do 
    i="tt/$i"
    SLICE="0"
    ABOVE="0"
    RES1=`cat $i/control.queries2 | grep --only-matching -P "50 Per.*" | grep --only-matching -P "(?<=\: )..?.?.?.?.?" 2>/dev/null`
    RES2=`cat $i/control.messagecost.weighted| grep "\d" | grep -P --only-matching "(?<=,)\d+" | j_sum 2>/dev/null`
    echo "$SLICE,$ABOVE,$RES1,$RES2"
done
