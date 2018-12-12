#!/bin/bash
split=0
while getopts w:f:i:s opts; do
    case ${opts} in
        # strip trailing slashes
        w) workdir=${OPTARG%/} ;;
        f) framedir=${OPTARG%/} ;;
        i) infile=${OPTARG} ;;
        s) split=1 ;;
    esac
done

if [ ! -f "$infile" ]; then
    echo "Input file not found: $infile"
    exit 1
fi
if [ ! -d "$workdir" ]; then
    echo "Creating working dir: $workdir"
    mkdir -p "$workdir"
fi
if [ ! -d "$framedir" ]; then
    echo "Creating frame dir: $framedir"
    mkdir -p "$framedir"
fi


# Extract base name with no ext
tmp=${infile##*/}
base_name=${tmp%.*}

if [ $split -eq 0 ]; then
    prefix="$workdir/${base_name}"

    fname_nt="${prefix}_avg.nc"
    # Average over day
    ncwa -O -a time "$infile" "$fname_nt"
    tstamp=$(cdo -s showtimestamp "$fname_nt")
    bash $(dirname $0)/plotnc.sh -f "$framedir" -t "$tstamp" -i "$fname_nt"
    rm "$fname_nt"
else
    # Split into time slices (1 slice per file)
    cdo -s splitsel,1 "$infile" "$prefix"

    for i in {0..7}; do
        fname="${prefix}00000${i}.nc"
        # Final data file with no time dim
        fname_nt="${fname%.nc}_nt.nc"
        # GMT won't work with 3D data even if len(time) -> 1
        # Collapse time dimension (remove it) by averaging along time dim
        ncwa -O -a time "$fname" "$fname_nt"
        tstamp=$(cdo -s showtimestamp "$fname_nt")
        bash $(dirname $0)/plotnc.sh -f "$framedir" -t "$tstamp" -i "$fname_nt"
        rm "$fname" "$fname_nt"
    done
fi
echo "DONE: $infile"
