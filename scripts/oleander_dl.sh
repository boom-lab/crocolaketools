#!/usr/bin/env bash

download_oleander \
    --start_year 2020 \
    --end_year 2025 \
    --save_to ./oleander_data/ \
    --threads 8 

# To use download_oleander with a list of URLs, provide your own oleander.txt file
# This file should contain one URL per line
download_oleander \
    --url_file oleander.txt \
    --save_to ./oleander_data/ \
    --threads 8 