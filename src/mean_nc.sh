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

ncwa -O -a lat,lon -d lat,$lat_range -d lon,$lon_range $infile $outfile && {
    echo "Done: $infile"
} || {
    exit 1
}
