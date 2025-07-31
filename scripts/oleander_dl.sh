#!/usr/bin/env bash

# Example script to download Oleander data for a range of years.

download_oleander \
    --start_year 2020 \
    --end_year 2024 \
    --destination ./data/oleander \
    --threads 8