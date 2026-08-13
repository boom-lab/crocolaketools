# This file summarizes the mapping between CrocoLake and SPOTS variables and their units.

| CrocoLake variable | SPOTS variable/source | CrocoLake units | SPOTS units | Notes |
######################################################################################
| DB_NAME | generated as "SPOTS" | N/A | N/A | Added by converter |
| PLATFORM_NUMBER | TimeSeriesSite | N/A | N/A | Time-series site name |
| LATITUDE | LATITUDE | degree_north | decimal degrees |  |
| LONGITUDE | LONGITUDE | degree_east | decimal degrees |  |
| PRES | CTDPRS | dbar | dbar |  |
| JULD | DATE + TIME | days since 1950-01-01 00:00:00 UTC | DATE as YYYYMMDD, TIME as HHMM | Converted by converter |
| TEMP | CTDTMP | degree_Celsius | degrees Celsius |  |
| PSAL | SALNTY if available, otherwise CTDSAL | psu | psu | Bottle salinity preferred; CTD salinity fallback |
| DOXY | OXYGEN | micromole/kg | micromole/kg |  |
| NITRATE | NITRAT | micromole/kg | micromole/kg |  |
| PH_IN_SITU_TOTAL | PH_TOT | dimensionless | unitless |  |
| SILICATE | SILCAT | micromole/kg | micromole/kg |  |
| PHOSPHATE | PHSPHT | micromole/kg | micromole/kg |  |
| TOT_ALKALINITY | ALKALI | micromole/kg | micromole/kg |  |
| `<PARAM>_QC` | `<PARAM>_FLAG_W` | N/A | unitless | Keep values with FLAG_W = 2 |
| `<PARAM>_ERROR` | `<PARAM>_A` or `<PARAM>_P` | same as parameter | same as parameter | Use accuracy if available, otherwise precision |