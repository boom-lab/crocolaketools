#!/usr/bin/env python3

## @file downloaderIOOSGliders.py
#
# Contains source code for interacting with the IOOS glider data ERDDAP SERVER..
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Sunday 26 June, 2026

################################################################################################
import logging
import os
from datetime import datetime
from typing import Optional

from crocolaketools.downloader.downloaderIOOS import DownloaderIOOS
from crocolaketools.utils.logger_configurator import configure_logging

_DELAYED_SUFFIX = "-delayed"
_LOG_FILE = "ioos_gliders_download.log"

GLIDER_VARIABLES = [
    "latitude",
    "longitude",
    "precise_lat",
    "precise_lon",
    "depth",
    "pressure",
    "time",
    "precise_time",
    "temperature",
    "salinity",
    "conductivity",
    "density",
    "u",
    "v",
    "lat_uv",
    "lon_uv",
    "time_uv",
    "profile_id",
    "trajectory",
    "wmo_id",
    "instrument_ctd",
]


class DownloaderIOOSGliders(DownloaderIOOS):
    """
        Download IOOS Glider DAC delayed-mode datasets from ERDDAP.
    """

    def __init__(self, config: dict = None):
        if config is None:
            config = {}
        config.setdefault("db", "IOOS_GLIDERS")
        config.setdefault("db_type", "PHY")
        super().__init__(config)

        configure_logging(_LOG_FILE)

        # sync=True: compare server timestamps, download new and updated files
        # sync=False: download missing files only, no timestamp check
        self.sync = config.get("sync", False)

    def get_dataset_url(
        self,
        dataset_id: str,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ) -> str:
        """
            Build the tabledap parquet URL with glider variable selection.
        """
        self._erddap._dataset_id = dataset_id
        self._erddap.variables = GLIDER_VARIABLES
        self._erddap.constraints = {}

        constraints = {}
        if time_start is not None:
            constraints["time>="] = time_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        if time_end is not None:
            constraints["time<="] = time_end.strftime("%Y-%m-%dT%H:%M:%SZ")

        # pass constraints explicitly, even as an empty dict, to avoid erddapy
        # falling back to self.constraints which may hold stale values from a
        # previous call
        return self._erddap.get_download_url(
            response=self.response_format,
            constraints=constraints if constraints else {},
        )

    def download(self) -> tuple:
        """
            Run the incremental sync against the IOOS Glider DAC.
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

        to_download, skipped_current, skipped_no_ts = self._build_download_queue(dataset_ids)

        if self.dryrun:
            mode = (
                "sync mode (timestamp check)"
                if self.sync
                else "download-only mode (missing files)"
            )
            logging.info(
                "Dry run [%s]: %d file(s) would be downloaded, "
                "%d already current, %d skipped (no server timestamp).",
                mode, len(to_download), skipped_current, skipped_no_ts,
            )
            for ds in to_download[:10]:
                logging.info("  %s.parquet", ds)
            if len(to_download) > 10:
                logging.info("  ... and %d more.", len(to_download) - 10)
            return len(to_download), 0

        if not to_download:
            logging.info(
                "All %d local file(s) are current. Nothing to download.",
                skipped_current,
            )
            return 0, 0

        logging.info(
            "Downloading %d dataset(s) (%d already current, "
            "%d skipped - no server timestamp)...",
            len(to_download), skipped_current, skipped_no_ts,
        )

        # _download_one handles chunking and retries with smaller windows on 413
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

    def _build_download_queue(self, dataset_ids: list) -> tuple:
        """
            Decide which datasets need downloading.

            Returns (to_download, skipped_current, skipped_no_ts).
        """
        to_download = []
        skipped_current = 0
        skipped_no_ts = 0

        if self.sync:
            logging.info(
                "Checking %d dataset(s) against server timestamps...",
                len(dataset_ids),
            )

        for i, dataset_id in enumerate(dataset_ids, 1):
            local_path = self._local_path(dataset_id)

            if self.overwrite:
                to_download.append(dataset_id)
                if self.sync:
                    logging.info(
                        "  [%d/%d] %s: overwrite - queued",
                        i, len(dataset_ids), dataset_id,
                    )
                continue

            local_exists = self._local_timestamp(local_path) is not None

            if not local_exists:
                to_download.append(dataset_id)
                if self.sync:
                    logging.info(
                        "  [%d/%d] %s: not found locally - queued",
                        i, len(dataset_ids), dataset_id,
                    )
                continue

            if not self.sync:
                skipped_current += 1
                continue

            # sync=True: compare timestamps and log the result for every dataset
            server_ts = self.get_server_timestamp(dataset_id)
            if server_ts is None:
                logging.warning(
                    "  [%d/%d] %s: no server timestamp - skipped",
                    i, len(dataset_ids), dataset_id,
                )
                skipped_no_ts += 1
                continue

            local_ts = self._local_timestamp(local_path)
            if server_ts > local_ts:
                logging.info(
                    "  [%d/%d] %s: server newer (%s > %s) - queued",
                    i, len(dataset_ids), dataset_id,
                    server_ts.date(), local_ts.date(),
                )
                to_download.append(dataset_id)
            else:
                logging.info(
                    "  [%d/%d] %s: up to date (%s) - skipped",
                    i, len(dataset_ids), dataset_id, local_ts.date(),
                )
                skipped_current += 1

        return to_download, skipped_current, skipped_no_ts

    def _filter_datasets(self, dataset_ids: list) -> list:
        """
            Keep only delayed-mode dataset IDs (those ending with '-delayed').
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
