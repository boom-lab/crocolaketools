#!/usr/bin/env bash

# Quit on error
set -e
set -o pipefail

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
CONFIG_DIR=$(realpath "${SCRIPT_DIR}/../config")
yaml_file="${CONFIG_DIR}""/config.yaml"
argo_variants=(
  "PHY"
  "BGC"
)

# If variants are provided, use those instead
if [ $# -gt 0 ]; then
    # Clear default array
    argo_variants=()

    # Add each command-line argument to the array
    for arg in "$@"; do
        argo_variants+=("$arg")
    done
fi


# Loop through each variant of target CrocoLake directories
for var in "${argo_variants[@]}"; do

    IN_PATH=$(yq ".\"ARGO-GDAC_${var}\".input_path" "$yaml_file")
    IN_PATH=$(echo "$IN_PATH" | sed 's/^"//;s/"$//')
    if resolved_path=$(realpath "${CONFIG_DIR}/${IN_PATH}" 2>/dev/null); then
        IN_PATH="$resolved_path"
    else
        IN_PATH="${IN_PATH}"
    fi

    OUT_PATH=$(yq ".\"ARGO-GDAC_${var}\".outdir_pq" "$yaml_file")
    OUT_PATH=$(echo "$OUT_PATH" | sed 's/^"//;s/"$//')
    if resolved_path=$(realpath "${CONFIG_DIR}/${OUT_PATH}" 2>/dev/null); then
        OUT_PATH="$resolved_path"
    else
        OUT_PATH="${OUT_PATH}"
    fi

    echo "${var}|${IN_PATH}|${OUT_PATH}"

done
