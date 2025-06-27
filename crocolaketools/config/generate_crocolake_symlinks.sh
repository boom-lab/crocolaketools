#!/usr/bin/env bash

# Quit on error
set -e
set -o pipefail

# This script will create symbolic links to the most recent version of the
# parquet databases that form CrocoLake's data lake.

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
echo "Script directory: $SCRIPT_DIR"
yaml_file="${SCRIPT_DIR}""/config.yaml"
crocolake_variants=(
  "PHY"
  "BGC"
)
# If variants are provided, use those instead
if [ $# -gt 0 ]; then
    # Clear default array
    crocolake_variants=()

    # Add each command-line argument to the array
    for arg in "$@"; do
        crocolake_variants+=("$arg")
    done
fi

# Loop through each variant of target CrocoLake directories
for var in "${crocolake_variants[@]}"; do

    # Extract the `ln_path` for the CROCOLAKE group
    crocolake_ln=$(yq ".CROCOLAKE_"$var".ln_path" "$yaml_file")
    echo "ln_path for CROCOLAKE_$var: $crocolake_ln"
    # Check if CROCOLAKE ln_path exists
    if [ -z "$crocolake_ln" ]; then
      echo "CROCOLAKE ln_path not found in the YAML file."
      exit 1
    fi
    crocolake_ln=$(echo "$crocolake_ln" | sed 's/^"//;s/"$//')
    crocolake_path_ln="${SCRIPT_DIR}/${crocolake_ln}"
    if [ ! -d "$crocolake_path_ln" ]; then
      echo "Directory $crocolake_path_ln does not exist. Creating it..."
      mkdir -p $crocolake_path_ln
    fi
    if [[ "${crocolake_ln}" != /* ]]; then
        crocolake_ln=$(realpath "${SCRIPT_DIR}/${crocolake_ln}")
    else
        crocolake_ln=$(realpath "${crocolake_ln}")
    fi
    echo "ln_path for CROCOLAKE_$var: $crocolake_ln"

    echo "Removing all existing symbolic links in $crocolake_ln"
    find $crocolake_ln -type l -exec rm {} \;

    crocolake_out=$(yq ".CROCOLAKE_"$var".outdir_pq" "$yaml_file")
    crocolake_out=$(echo "$crocolake_out" | sed 's/^"//;s/"$//')
    if [[ "${crocolake_out}" != /* ]]; then
        crocolake_out=$(realpath "${SCRIPT_DIR}/${crocolake_out}")
    else
        crocolake_out=$(realpath "${crocolake_out}")
    fi
    echo "crocolake_out for CROCOLAKE_$var: $crocolake_out"

    echo "Creating folder $crocolake_ln if it does not exist"
    mkdir -p $crocolake_ln

    # Extract all `outdir_pq` entries from the YAML file
    yq --arg var "$var" '.[] | select(has("outdir_pq") and .db_type == $var) | .outdir_pq' "$yaml_file" | while read -r outdir_pq; do
      # Skip the CROCOLAKE outdir_pq itself

      # skip if it's the crocolake dir or the other db type (bgc/phy)
      if [[ "$outdir_pq" != *"ARGO-CLOUD"* ]] && [[ "$outdir_pq" != *"ARGO-GDAC"* ]] && [[ "$outdir_pq" != *"CROCOLAKE"* ]]; then
        echo "Processing outdir_pq: $outdir_pq"
        outdir_pq=$(echo "$outdir_pq" | sed 's/^"//;s/"$//')
        if [[ "${outdir_pq}" != /* ]]; then
          outdir_pq=$(realpath "${SCRIPT_DIR}/${outdir_pq}")
        else
          outdir_pq=$(realpath "${outdir_pq}")
        fi

        if [ "${outdir_pq: -1}" != "/" ]; then
          outdir_pq="${outdir_pq}/"
        fi

        # Create a symbolic link in the CROCOLAKE directory
        if [ -d "$outdir_pq" ]; then
          db_name=$(basename "$outdir_pq")
          echo "Creating symlink for $db_name in $crocolake_ln (points to: $outdir_pq)"
          ln -s "$outdir_pq" "$crocolake_ln/$db_name"
        else
          echo "Destination directory $outdir_pq does not exist. Skipping symlink creation. This usually happens if you have not generated this parquet dataset first."
        fi
      fi
    done
done
