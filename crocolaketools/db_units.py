#!/usr/bin/env python3

## @file db_units.py
#
# Units of CrocoLake's variables, used by crocolaketools.loader.Loader to add
# units to the parquet schema metadata.
#
## Moved from params.py in crocolakeloader (https://github.com/boom-lab/crocolakeloader.git)
#  Originally written by @author enrico <enrico.milanese@whoi.edu>

units = {}

units["CROCOLAKE_UNITS"] = {
    'DB_NAME': '-',
    'PLATFORM_NUMBER': '-',
    'CYCLE_NUMBER': '-',
    'LATITUDE': 'degrees_north',
    'LONGITUDE': 'degrees_east',
    'JULD': 'days since 1950-01-01 00:00:00 UTC',
    'DEPTH': 'meters',
    'PRES': 'decibar',
    'TEMP': 'degree_Celsius',
    'PSAL': 'PSU',
    'DOXY': 'micromole/kg',
    'BBP': 'm^-1',
    'TURBIDITY': 'NTU',
    'CP': 'm^-1',
    'CHLA': 'micrograms/m^3',
    'CDOM': 'ppb',
    'NITRATE': 'micromole/kg',
    'BISULFIDE': 'micromole/kg',
    'PH_IN_SITU_TOTAL': '-',
    'DOWN_IRRADIANCE': 'W/m^2/nm',
    'UP_IRRADIANCE': 'W/m^2/nm',
    'DOWNWELLING_PAR': 'microMoleQuanta/m^2/s',
    'SILICATE': 'micromole/kg',
    'PHOSPHATE': 'micromole/kg',
    'TCO2': 'micromole/kg',
    'TOT_ALKALINITY': 'micromole/kg',
    'CFC11': 'picomole/kg',
    'CFC12': 'picomole/kg',
    'CFC113': 'picomole/kg',
    'CCL4': 'picomole/kg',
    'SF6': 'femtomole/kg',
    'QC': '-',
    'DATA_MODE': '-',
    'ABS_SAL_COMPUTED': 'PSU',
    'CONSERVATIVE_TEMP_COMPUTED': 'degree_Celsius',
    'SIGMA1_COMPUTED': 'kg/m^3',
}
