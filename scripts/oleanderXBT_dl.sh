#!/usr/bin/env bash

download_oleanderXBT \
    --start_year 2020 \
    --end_year 2025 \
    --save_to ./oleanderXBT_data/ \
    --threads 8 

# To use download_oleanderXBT with a list of URLs, provide your own oleanderXBT.txt file
# This file should contain one URL per line
download_oleanderXBT \
    --url_file oleanderXBT.txt \
    --save_to ./oleanderXBT_data/ \
    --threads 8 