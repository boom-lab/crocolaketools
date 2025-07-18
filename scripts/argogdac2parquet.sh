#!/usr/bin/env bash

# Quit on error
set -e
set -o pipefail

CONFIG_DIR="../crocolaketools/config"
yaml_file="${CONFIG_DIR}""/config.yaml"
argo_variants=(
  "PHY"
  "BGC"
)

# Loop through each variant of target CrocoLake directories
for var in "${argo_variants[@]}"; do

    IN_PATH=$(yq ".\"ARGO-GDAC_${var}\".input_path" "$yaml_file")
    IN_PATH=$(echo "$IN_PATH" | sed 's/^"//;s/"$//')
    CONFIG_DIR=$(realpath "${CONFIG_DIR}") # get absolute path to config directory
    IN_PATH="${CONFIG_DIR}/${IN_PATH}"
    if [ ! -d "$IN_PATH" ]; then
        echo "Error: Input path $IN_PATH does not exist."
        exit 1
    fi
    IN_PATH=$(realpath "${IN_PATH}")

    OUT_PATH=$(yq ".\"ARGO-GDAC_${var}\".outdir_pq" "$yaml_file")
    OUT_PATH=$(echo "$OUT_PATH" | sed 's/^"//;s/"$//')
    OUT_PATH="${CONFIG_DIR}/${OUT_PATH}"

    echo "Creating output $OUT_PATH directory if it does not exist"
    mkdir -p "$OUT_PATH"
    OUT_PATH=$(realpath "${OUT_PATH}")

    echo "Processing $var"
    echo "Input path: $IN_PATH"
    echo "Output path: $OUT_PATH"

    ## executing code
    argogdac2parquet \
        --db "$var" \
        --db_parquet "$OUT_PATH" \
        --gdac_index "$IN_PATH" \
        --db_nc "$IN_PATH" \
        --download "false"

done
