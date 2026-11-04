#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 input_dir_mr input_dir_parks output_dir"
    exit 1
fi

INPUT_DIR_MR=$1
INPUT_DIR_PARKS=$2
OUTPUT_DIR=$3

echo "Running Hive"
echo "Map Reduce results taken from: $INPUT_DIR_MR"
echo "Parks dictionary taken from: $INPUT_DIR_PARKS"
echo "Results stored in: $OUTPUT_DIR"

hadoop fs -mkdir -p $OUTPUT_DIR

beeline \
    -n "$(id -un)" \
    -u "jdbc:hive2://localhost:10000/default" \
    -hiveconf INPUT_MR="$INPUT_DIR_MR" \
    -hiveconf INPUT_PARKS="$INPUT_DIR_PARKS" \
    -hiveconf OUTPUT="$OUTPUT_DIR" \
    -f hive.hql

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "Finished successfully"
else
    echo "Error: ${EXIT_CODE}"
fi

exit $EXIT_CODE