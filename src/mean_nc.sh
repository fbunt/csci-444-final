#!/bin/bash
while getopts o:i: opts; do
    case ${opts} in
        o) outdir=${OPTARG%/} ;;
        i) infile=${OPTARG} ;;
    esac
done

if [ ! -f "$infile" ]; then
    echo "Invalid input file: $infile"
    exit 1
fi
if [ ! -d "$outdir" ]; then
    echo "Creating out dir: $outdir"
    mkdir -p "$outdir"
fi


lat_range=-30.,30.
lon_range=0.,360.


fname=${infile##*/}
outfile="$outdir/$fname"
# NCO claims that output nc file can have same name as input but I
# have run into trouble with this in practice with many threads going.
# Thus, I am usning a temp file.
tmp="${outfile}.temp"

# Average over spatial range
ncwa -O -a lat,lon -d lat,$lat_range -d lon,$lon_range $infile $tmp || {
    rm $tmp 2> /dev/null
    exit 2
}
# Reset time as record dimension
ncks --mk_rec_dmn time $tmp $outfile || {
    rm $tmp 2> /dev/null
    exit 3
}
rm $tmp
echo "Done: $infile"
