#!/bin/bash
while getopts w:f:i: opts; do
    case ${opts} in
        # strip trailing slashes
        w) workdir=${OPTARG%/} ;;
        f) framedir=${OPTARG%/} ;;
        i) infile=${OPTARG} ;;
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

prefix="$workdir/${base_name}_"

# Split into time splices (1 slice per file)
cdo -s splitsel,1 "$infile" "$prefix"

for i in {0..7}; do
    fname="${prefix}00000${i}.nc"
    # Final data file with no time dim
    fname_nt="${fname%.nc}_nt.nc"
    # GMT won't work with 3D data even if len(time) -> 1
    # Collapse time dimension (remove it) by averaging along time dim
    ncwa -O -a time "$fname" "$fname_nt"
    tstamp=$(cdo -s showtimestamp "$fname_nt")
    # trim whitespace
    tstamp=$(echo $tstamp | xargs)
    bash $(dirname $0)/plotnc.sh -f "$framedir" -t "$tstamp" -i "$fname_nt"
    rm "$fname" "$fname_nt"
done
echo "DONE: $infile"
