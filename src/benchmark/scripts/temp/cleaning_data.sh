#!/bin/bash

directory="$1"

for file in "$directory"/*.csv; do
    filename=$(basename "$file")
    if [[ "$filename" == "merged_output.csv" ]]; then
        continue
    fi
    echo "Cleaning: $file"
    awk -F',' '{
        for (i=1; i<NF; i++) {
            printf "%s%s", $i, (i<NF-1 ? "," : "")
        }
        printf "\n"
    }' "$file" > "$file.tmp" && mv "$file.tmp" "$file"
done