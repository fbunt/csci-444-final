#!/bin/bash
while getopts f:t:i: opts; do
    case ${opts} in
        f) framedir=${OPTARG%/} ;;
        t) title=${OPTARG} ;;
        i) infile=${OPTARG} ;;
    esac
done

if [ ! -d "$framedir" ]; then
    echo "Creating frame dir: $framedir"
    mkdir -p "$framedir"
fi
if [ ! -f "$infile" ]; then
    echo "Input file not found: $infile"
    exit 1
fi

framefile="$framedir/frame_$title.ps"

if [ ! -f sst.cpt ]; then
    gmt makecpt -Cviridis -T-2/36/1 -Z > sst.cpt
fi

w=7i

gmt grdimage $infile -Rg -JKs180/$w -B+t"$title" -Csst.cpt -P -K > $framefile
gmt pscoast -Rg -JKs180/$w -Bafg -Dc -A5000 -Wthinnest -Gwhite -O -K >> $framefile
gmt psscale -Rg -JKs180/$w -DjMR+w4i/0.5+v+o-1i/0i -Csst.cpt -Ba6f -O >> $framefile
gmt psconvert $framefile -P -E500 -Tg -A1i -Z
