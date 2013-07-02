for i in `ls tmp | grep "best\|surplus\|supersize\|scatter" | sort -g` 
do 
    i="tmp/$i"
    RES1=`cat $i/control.queries | grep --only-matching -P "50 Per.*" | head -n 1 | grep -oP "(?<=\: ).*" 2>/dev/null`
    RES2=`cat $i/control.messagecost.weighted| grep "\d" | grep -P --only-matching "(?<=,)\d+" | j_sum 2>/dev/null`
    RES3=`cat $i/control.queries | grep --only-matching -P "mon:stats.*" | head -n 1 | grep -oP "(?<=\: )\S+ \S+ \S+ \S+" | cut -d" " -f4`
    echo "$i,$RES1,$RES2,$RES3"
done
