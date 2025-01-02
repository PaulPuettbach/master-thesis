#!/bin/bash
awk '/^echo / { name=$2; printf "mkdir %s | && cd %s | && ", name, name; print $1,$2,"|",$3,$4,$5,$6,$7,"|",$8,$9,$10,$11,$12,"| && cd .." } !/^echo /{ print $0 }' download-graphalytics-data-sets-r2.sh | column -t -s "|" > modified_download_graphs.sh
