#!/bin/bash
while getopts f:o: opts; do
    case ${opts} in
        f) fps=${OPTARG} ;;
        o) out=${OPTARG} ;;
    esac
done


ffmpeg -framerate $fps -pattern_type glob -i 'plots/frames/*.png' \
        -c:v libx264 -pix_fmt yuv420p -vf scale=1280:-2 $out
