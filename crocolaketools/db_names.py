#!/usr/bin/env python3

## @file db_names.py
#
# Canonical list of databases compatible with CrocoLake.
#
# Source of truth lives here in CrocoLakeTools; the copy in CrocoLakeLoader is
# Will be kept in sync via CI/CD ( details at boom-lab/crocolaketools#52 and
# boom-lab/crocolakeloader#9).
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com> 
#
## Everything is Adapted from params.py in crocolakeloader (https://github.com/boom-lab/crocolakeloader.git)
#  Originally written by enrico <enrico.milanese@whoi.edu>


databases = ["ARGO", "GLODAP", "SprayGliders", "CPR", "Saildrones", "OleanderXBT", "IOOS_GLIDERS"]