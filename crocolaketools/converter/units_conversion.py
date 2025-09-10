#!/usr/bin/env python3

## @file units_conversion.py
#
#  Shared unit conversion functions and registry
#
## ## @author David Nady <davidnady4yad@gmail.com>
#
## @date Fri 05 Sep 2025

##########################################################################
import gsw
import warnings
##########################################################################

#------------------------------------------------------------------------------#
# Conversions
#------------------------------------------------------------------------------#

def micromol_L_to_micromol_kg(ddf, col):
    """Convert concentration from μmol/L to μmol/kg using seawater density.
    Requires columns 'ABS_SAL_COMPUTED' and 'CONSERVATIVE_TEMP_COMPUTED'.
    """

    # Ensure required derived columns exist (should be added if add_derived_vars=True)
    required_cols = ['ABS_SAL_COMPUTED', 'CONSERVATIVE_TEMP_COMPUTED']
    missing = [col for col in required_cols if col not in ddf.columns]
    if missing:
        raise ValueError(f"Missing required columns for {col} conversion: {missing}"
                          "Set add_derived_vars=True to enable unit conversion.")

    def convert_partition(df):
        # convert DOXY units in a single partition.
        sigma0 = gsw.density.sigma0(df['ABS_SAL_COMPUTED'], df['CONSERVATIVE_TEMP_COMPUTED'])
        correction_factor = sigma0 / 1000 + 1
        df['DOXY'] = df['DOXY'] / correction_factor
        return df

    print("Converting DOXY from μmol/L to μmol/kg")
    ddf = ddf.map_partitions(
        convert_partition, 
        meta=ddf._meta
    )

    return ddf

#------------------------------------------------------------------------------#
# ... Add new conversions here ...

#------------------------------------------------------------------------------#
# Conversion mapping dictionary
conversion_map = {
    "micromol/L2micromol/kg": micromol_L_to_micromol_kg,
}