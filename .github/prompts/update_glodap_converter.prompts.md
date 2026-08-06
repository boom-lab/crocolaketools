---
agent: 'converter-builder'
description: 'Update support for GLODAP dataset'
---

# Update support for converting the GLODAP dataset.

Update the existing conveterGLODAP converter, do not create new modules.

Database information:
- Canonical DB name: GLODAP
- Database types: both
- Source format: CSV
- Source data path: /home/enrico/myWHOI/databases/GLODAP/v3.2026/
- Source files/pattern: GLODAPv3_Merged_Master_File.csv
- Demo data path: /home/enrico/myWHOI/CrocoLake/crocolaketools/crocolaketools/demo/demo_GLODAP/demo_GLODAP.csv
- Documentation path: /home/enrico/myWHOI/CrocoLake/crocolaketools/.tmp
- Supplementary documentation: NONE

Documentation findings:
- QC values retained: like existing converterGLODAP.py
- QC behavior: like existing converterGLODAP.py
- Missing/fill values: like existing converterGLODAP.py
- Units/conversions: verify from documentation
- Time format and timezone: verify from documentation
- Profile identity: like existing converterGLODAP.py
- Profile-number reset scope: like existing converterGLODAP.py
- Coordinate behavior within a profile: like existing converterGLODAP.py

Implementation requirements:
- Update glodap2parquet in scripts/ if necessary
- Update db_names.py and db_params.py if necessary
- config.yaml and config_cluster.yaml should not need be updated
- Ensure generate_crocolake_symlinks.sh includes the outputs.
- Add converter tests and source-to-Parquet integrity tests.

Validation requirements:
- Run: glodap2parquet --config
- Check output with Dask
- Report any uncertainty or blocked validation explicitly.


