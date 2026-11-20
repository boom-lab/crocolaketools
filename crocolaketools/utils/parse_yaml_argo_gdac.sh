#!/usr/bin/env bash

# Quit on error
set -e
set -o pipefail

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
. "${SCRIPT_DIR}/../config/resolve_config_dir.sh" || exit 1
yaml_file="${CONFIG_DIR}""/datasets.yaml"
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


# Print the value of ARGO-GDAC_<variant>.<field> as a normalised absolute path.
# Relative values resolve against CONFIG_DIR, absolute ones are used as they
# are, matching crocolaketools/config/config_paths.py. A missing or empty value
# is an error, rather than resolving to "${CONFIG_DIR}/null".
resolve_field() {
    local key="ARGO-GDAC_$1" field="$2" value
    value=$("$YQ" -r ".\"${key}\".${field} // \"\"" "$yaml_file")
    if [ -z "$value" ]; then
        echo "${key}.${field} is not set in ${yaml_file}." >&2
        return 1
    fi
    if [[ "$value" != /* ]]; then
        value="${CONFIG_DIR}/${value}"
    fi
    # -m tolerates paths that do not exist yet; -s keeps `current` symlinks.
    realpath -m -s "$value"
}

# Loop through each variant of target CrocoLake directories
for var in "${argo_variants[@]}"; do

    IN_PATH=$(resolve_field "$var" input_path)
    OUT_PATH=$(resolve_field "$var" outdir_pq)

    echo "${var}|${IN_PATH}|${OUT_PATH}"

done
