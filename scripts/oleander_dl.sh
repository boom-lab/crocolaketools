#!/usr/bin/env bash

download_oleander \
    --start_year 2020 \
    --end_year 2025 \
    --save_to ./oleander_data/ \
    --threads 8 \

download_oleander \
    --url_file oleander.txt \
    --save_to ./oleander_data/ \
    --threads 8 \