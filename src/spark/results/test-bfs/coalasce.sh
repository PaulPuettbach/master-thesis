#!/bin/bash

OUTPUT_DIR="./output"
COMBINED_FILE="./combined_result"


# delte existing
if [ -f "$COMBINED_FILE" ]; then
  rm "$COMBINED_FILE"
fi

# Concatenate all part-* files, sort by the first column (vertex ID), and write to the combined output file
cat $OUTPUT_DIR/part-*| sort -n -k1,1  > "$COMBINED_FILE" 

# Confirm completion
if [ $? -eq 0 ]; then
  echo "Files concatenated and sorted into $COMBINED_FILE successfully."
else
  echo "Error occurred during concatenation and sorting."
fi