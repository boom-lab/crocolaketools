#!/usr/bin/env python3

## @file downloaderIOOSGliders.py
#
# Downloader for IOOS Glider DAC datasets from ERDDAP.
#
# Inherits directly from DownloaderERDDAP
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Sunday 26 June, 2026

################################################################################################
import logging
import os
from datetime import datetime
from typing import Optional

from crocolaketools.downloader.downloaderERDDAP import DownloaderERDDAP
from crocolaketools.utils.logger_configurator import configure_logging

_DELAYED_SUFFIX = "-delayed"
_LOG_FILE = "ioos_gliders_download.log"

IOOS_GLIDERS_SERVER_URL = "https://gliders.ioos.us/erddap"

# Full list of variables to request from ERDDAP.
# ref: https://crocolakedocs.readthedocs.io/en/latest/crocolake.html#crocolake-s-conventions
# These  variables were selected from avaiable variables in erddap server till date (7-7-26)
# Look at the variable_frequency.json refered to at PR #50,
# (https://github.com/boom-lab/crocolaketools/pull/50)

GLIDER_VARIABLES = [
    # *--- PARAM flags <PARAM> ---*
    # --- Navigation / metadata ---
    "latitude",
    "longitude",
    "precise_lat",
    "precise_lon",
    "depth",
    "pressure",
    "time",
    "precise_time",
    "profile_id",
    "trajectory",
    "wmo_id",
    "instrument_ctd",
    "u",
    "v",
    "lat_uv",
    "lon_uv",
    "time_uv",
    # --- PHY ---
    "temperature",                  # TEMP
    "salinity",                     # PSAL
    "conductivity",
    "density",
    # --- BGC ---
    # DOXY - dissolved oxygen 
    "dissolved_oxygen",             # DOXY
    "oxy4_oxygen",                  # DOXY (Aanderaa Optode 4 raw output)
    "oxygen",                       # DOXY (alternate name)
    "oxygen_concentration",         # DOXY (alternate name)
    "oxy3835_wphase_oxygen",        # DOXY (Aanderaa Optode 3835 wphase)
    "sci_oxy4_oxygen",              # DOXY (Slocum glider internal name)
    "oxy3835_oxygen",               # DOXY (Aanderaa Optode 3835)
    "sci_oxy3835_oxygen",           # DOXY (Slocum glider internal name)
    "sci_oxy4330f_oxygen",          # DOXY (Slocum glider internal name)
    "sci_rinkoII_DO",               # DOXY (JFE Advantech RinkoII sensor)
    # BBP470 - 470 nm particle backscattering 
    "beta_470nm",                   # BBP470
    "backscatter_470",              # BBP470 (alternate name)
    "optical_backscatter_470",      # BBP470 (alternate name)
    # BBP532 - 532 nm particle backscattering 
    "beta_532nm",                   # BBP532
    "backscatter_532",              # BBP532 (alternate name)
    "backscatter532",               # BBP532 (alternate name)
    # BBP700 - 700 nm particle backscattering
    "backscatter",                  # BBP700
    "beta_700nm",                   # BBP700 (alternate name)
    "backscatter_700",              # BBP700 (alternate name)
    "optical_backscatter_700",      # BBP700 (alternate name)
    "VBSC",                         # BBP700 (RBR glider name)
    "backscatter700",               # BBP700 (alternate name)
    # TURBIDITY
    "turbidity",                    # TURBIDITY
    "sci_c3sfl_turbidity",          # TURBIDITY (Slocum WET Labs C3 sensor)
    "sci_c3sfl_turbidty",           # TURBIDITY (typo variant in source data)
    # CHLA - chlorophyll-a 
    "chlorophyll",                  # CHLA
    "chlorophyll_a",                # CHLA (alternate name)
    "fluorescence",                 # CHLA (alternate name)
    "sci_c3sfl_chlorophyll",        # CHLA (Slocum WET Labs C3 sensor)
    "CPHL",                         # CHLA (RBR glider name)
    # CDOM 
    "CDOM",                         # CDOM
    "cdom",                         # CDOM (alternate name)
    "sci_flbbcd_cdom_units",        # CDOM (Slocum WET Labs ECO FLBBCD sensor)
    # PH_IN_SITU_TOTAL 
    "pH",                           # PH_IN_SITU_TOTAL
    "pHtot",                        # PH_IN_SITU_TOTAL (alternate name)
    # DOWN_IRRADIANCE 
    # OCR-504: channels 1-4 map to 380/412/443/490 nm (info source google)
    # OCR-507: channels 1-7, wavelength varies by instrument configuration
    "sci_ocr504I_irrad1",           # DOWN_IRRADIANCE380
    "sci_ocr504I_irrad2",           # DOWN_IRRADIANCE412
    "sci_ocr504I_irrad3",           # DOWN_IRRADIANCE443
    "sci_ocr504I_irrad4",           # DOWN_IRRADIANCE490
    "sci_ocr507i_irrad1",           # DOWN_IRRADIANCE 
    "sci_ocr507i_irrad2",           # DOWN_IRRADIANCE 
    "sci_ocr507i_irrad3",           # DOWN_IRRADIANCE 
    "sci_ocr507i_irrad4",           # DOWN_IRRADIANCE 
    # UP_RADIANCE
    # OCR-504R: channels 1-4 map to 380/412/443/490 nm
    "sci_ocr504R_rad1",             # UP_RADIANCE380
    "sci_ocr504R_rad2",             # UP_RADIANCE412
    "sci_ocr504R_rad3",             # UP_RADIANCE443
    "sci_ocr504R_rad4",             # UP_RADIANCE490
    # DOWNWELLING_PAR
    "PAR",                          # DOWNWELLING_PAR
    "bsipar_par",                   # DOWNWELLING_PAR (Biospherical/Licor sensor)
    "sci_bsipar_par",               # DOWNWELLING_PAR (Slocum internal name)
    "sci_FIRe_par",                 # DOWNWELLING_PAR (Satlantic FIRe sensor)
    "par",                          # DOWNWELLING_PAR (alternate name)
    "sci_whpar_par",                # DOWNWELLING_PAR (WET Labs sensor)
    "sci_satpar_par",               # DOWNWELLING_PAR (Satlantic sensor)
    # TOT_ALKALINITY
    "total_alkalinity",             # TOT_ALKALINITY
    # NITRATE
    "nitrate",                      # NITRATE
    "sci_suna_nitrate_um",          # NITRATE (raw SUNA sensor, µmol/L)
    "sci_suna_nitrate_mg",          # NITRATE (raw SUNA sensor, mg/L)
    "suna_nitrate_concentration",   # NITRATE (alternate name)

    # Not added (absent from the IOOS glider catalogue):
    # BISULFIDE, TCO2, SILICATE, PHOSPHATE, CCL4, SF6, CFC11/12/113,
    # DOWN_IRRADIANCE555, UP_RADIANCE555.
    #
    # DOWN_IRRADIANCE and UP_RADIANCE at other wavelengths ARE present via OCR
    # sensor channel variables (sci_ocr504I_irrad*, sci_ocr504R_rad*,
    # sci_ocr507i_irrad*) and are included.

    #------------------------------------------------------------------------

    # *--- QC flags <PARAM>_QC ---*
    # Navigation / metadata QC
    "depth_qc",                     # PRES_QC
    "pressure_qc",                  # PRES_QC (alternate name)
    "time_qc",                      # JULD_QC
    "lat_qc",                       # LATITUDE_QC
    "lon_qc",                       # LONGITUDE_QC
    "u_qc",                         # u_QC
    "v_qc",                         # v_QC
    "lat_uv_qc",                    # lat_uv_QC
    "lon_uv_qc",                    # lon_uv_QC
    "time_uv_qc",                   # time_uv_QC
    "profile_time_qc",              # JULD_QC 
    "profile_lat_qc",               # LATITUDE_QC 
    "profile_lon_qc",               # LONGITUDE_QC 
    # PHY QC
    "temperature_qc",               # TEMP_QC
    "salinity_qc",                  # PSAL_QC
    "conductivity_qc",              # conductivity_QC
    "density_qc",                   # density_QC
    # DOXY QC 
    "dissolved_oxygen_qc",          # DOXY_QC
    "oxygen_qc",                    # DOXY_QC (alternate name)
    "doxy_qc",                      # DOXY_QC (alternate name)
    "DO_qc",                        # DOXY_QC (alternate name)
    # BBP470 QC 
    "optical_backscatter_470_qc",   # BBP470_QC
    # BBP532 QC 
    "backscatter532_qc",            # BBP532_QC
    # BBP700 QC
    "backscatter_qc",               # BBP700_QC
    "optical_backscatter_700_qc",   # BBP700_QC (alternate name)
    "backscatter700_qc",            # BBP700_QC (alternate name)
    # CHLA QC 
    "chlorophyll_a_qc",             # CHLA_QC
    "chlorophyll_qc",               # CHLA_QC (alternate name)
    "fluorescence_qc",              # CHLA_QC (alternate name)
    # CDOM QC 
    "cdom_qc",                      # CDOM_QC
    "CDOM_qc",                      # CDOM_QC (alternate name)
    # PH_IN_SITU_TOTAL QC 
    "pHtot_qc",                     # PH_IN_SITU_TOTAL_QC
    # DOWNWELLING_PAR QC 
    "par_qc",                       # DOWNWELLING_PAR_QC
    # NITRATE QC 
    "nitrate_qc",                   # NITRATE_QC

]

class DownloaderIOOSGliders(DownloaderERDDAP):
    """
    Download IOOS Glider DAC delayed-mode datasets from ERDDAP.

    To add a downloader for another IOOS ERDDAP source (e.g. ATN),
    we need to create a new subclass of DownloaderERDDAP and set its own
    SERVER_URL, variable list, and get_dataset_url() constraints.
    """

    SERVER_URL: str = IOOS_GLIDERS_SERVER_URL
    PROTOCOL: str = "tabledap"
    RESPONSE_FORMAT: str = "parquet"

    def __init__(self, config: dict = None):
        if config is None:
            config = {}
        config.setdefault("db", "IOOS_GLIDERS")
        config.setdefault("db_type", "PHY")
        super().__init__(config)

        configure_logging(_LOG_FILE)

        self.delayed_only = config.get("delayed_only", True)
        # sync=True: compare server timestamps, re-download updated files
        # sync=False: download missing files only, no timestamp check
        self.sync = config.get("sync", False)

    def get_dataset_url(
        self,
        dataset_id: str,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ) -> str:
        """
        Build the tabledap parquet URL with per-dataset variable filtering.
        Queries the ERDDAP info endpoint first to find which variables this
        specific dataset actually carries, then intersects with GLIDER_VARIABLES.
        prevents 400 errors on datasets that lack some specific variables.
        Falls back to the full GLIDER_VARIABLES list if the info call fails.
        """
        available = self.get_dataset_variables(dataset_id)
        if available:
            requested = [v for v in GLIDER_VARIABLES if v in available]
            if not requested:
                logging.warning(
                    "%s: no GLIDER_VARIABLES overlap with dataset variables - "
                    "falling back to full list.", dataset_id
                )
                requested = GLIDER_VARIABLES
        else:
            # info endpoint failed; let ERDDAP decide (may 400 on missing vars)
            requested = GLIDER_VARIABLES

        self._erddap._dataset_id = dataset_id
        self._erddap.variables = requested
        self._erddap.constraints = {}

        constraints = {}
        if time_start is not None:
            constraints["time>="] = time_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        if time_end is not None:
            constraints["time<="] = time_end.strftime("%Y-%m-%dT%H:%M:%SZ")

        # merge in any extra constraints from config (spatial, variable-level, etc.)
        constraints.update(self.extra_constraints)
        
        # pass constraints explicitly, even as an empty dict, to avoid erddapy
        # falling back to self.constraints which may hold stale values from a
        # previous call
        return self._erddap.get_download_url(
            response=self.response_format,
            constraints=constraints if constraints else {},
        )

    def download(self) -> tuple:
        """
            Runs the incremental sync against the IOOS Glider DAC.
        """
        logging.info("Querying IOOS Glider DAC for delayed-mode dataset IDs...")

        try:
            dataset_ids = self.list_dataset_ids()
        except Exception as exc:
            logging.error("Failed to fetch dataset catalogue: %s", exc)
            return 0, 0

        if not dataset_ids:
            logging.info("No delayed-mode glider datasets found on the server.")
            return 0, 0

        logging.info("Found %d delayed-mode dataset(s).", len(dataset_ids))

        to_download, skipped_current, skipped_no_ts, skipped_bounds = \
            self._build_download_queue(dataset_ids)

        if self.dryrun:
            mode = (
                "sync mode (timestamp check)"
                if self.sync
                else "download-only mode (missing files)"
            )
            logging.info(
                "Dry run [%s]: %d file(s) would be downloaded, "
                "%d already current, %d outside constraint bounds, "
                "%d skipped (no server timestamp).",
                mode, len(to_download), skipped_current, skipped_bounds,
                skipped_no_ts,
            )
            for ds in to_download[:10]:
                logging.info("  %s.parquet", ds)
            if len(to_download) > 10:
                logging.info("  ... and %d more.", len(to_download) - 10)
            return len(to_download), 0

        if not to_download:
            logging.info(
                "Nothing to download (%d already current, "
                "%d outside constraint bounds).",
                skipped_current, skipped_bounds,
            )
            return 0, 0

        logging.info(
            "Downloading %d dataset(s) (%d already current, "
            "%d outside constraint bounds, "
            "%d skipped - no server timestamp)...",
            len(to_download), skipped_current, skipped_bounds, skipped_no_ts,
        )

        completed = 0
        failed = 0

        for i, dataset_id in enumerate(to_download, 1):
            local_path = self._local_path(dataset_id)
            logging.info("[%d/%d] %s", i, len(to_download), dataset_id)
            ok = self._download_one(dataset_id, local_path)
            if ok:
                completed += 1
            else:
                failed += 1

        logging.info(
            "Sync complete. Downloaded: %d, Failed: %d, Already current: %d.",
            completed, failed, skipped_current,
        )
        return completed, failed

    def _filter_datasets(self, dataset_ids: list) -> list:
        """
            filtering only delayed-mode dataset IDs (those ending with '-delayed').
        """
        if not self.delayed_only:
            return dataset_ids
        return [d for d in dataset_ids if d.endswith(_DELAYED_SUFFIX)]

    def _local_path(self, dataset_id: str) -> str:
        """
            Return the local parquet file path for `dataset_id`.
        """
        return os.path.join(self.input_path, f"{dataset_id}.parquet")

################################################################################################
if __name__ == "__main__":
    DownloaderIOOSGliders()