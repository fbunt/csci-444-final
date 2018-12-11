#!/bin/bash
while getopts w:f:d:j: opts; do
    case ${opts} in
        # strip trailing slashes
        w) workdir=${OPTARG%/} ;;
        f) framedir=${OPTARG%/} ;;
        d) datadir=${OPTARG%/} ;;
        j) njobs=${OPTARG} ;;
    esac
done

if [ ! -d "$workdir" ]; then
    echo "Creating working dir: $workdir"
    mkdir -p "$workdir"
fi
if [ ! -d "$framedir" ]; then
    echo "Creating frame dir: $framedir"
    mkdir -p "$framedir"
fi
if [ ! -d "$datadir" ]; then
    echo "Invalid data dir: $datadir"
    exit 1
fi


ls $datadir/*/*.nc | parallel -I% --max-args 1 --progress -j$njobs --joblog par.log \
    src/plotframes.sh -w $workdir -f $framedir -i % | tee -a my.log
