#!/bin/bash
resume=
while getopts d:j:o:r opts; do
    case ${opts} in
        d) datadir=${OPTARG%/} ;;
        o) outdir=${OPTARG%/} ;;
        j) njobs=${OPTARG} ;;
        r) resume="--resume" ;;
    esac
done

if [ ! -d "$datadir" ]; then
    echo "Invalid data dir: $datadir"
    exit 1
fi
if [ ! -d "$outdir" ]; then
    echo "Creating output dir: $outdir"
    mkdir -p "$outdir"
fi
if [ ! -d ./logs ]; then
    echo "Creating log dir"
    mkdir ./logs
fi


ls $datadir/*/*.nc | parallel -I% --max-args 1 --progress -j$njobs $resume --joblog logs/mean_par.log \
    src/mean_nc.sh -o $outdir -i % | tee -a logs/mean.log
