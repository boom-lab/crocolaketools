#!/usr/bin/env python3

import logging
import os
from datetime import datetime
from typing import Optional

from crocolaketools.downloader.downloaderIOOS import DownloaderIOOS

_DELAYED_SUFFIX = "-delayed"

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

        # sync=True, comparing server timestamps, download new + updated files
        # sync=False, download missing files only (no timestamp check)
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

        # Pass constraints explicitly -even empty dict to override
        # self._erddap.constraints. When constraints is None, erddapy
        # falls back to self.constraints which may have stale values.
        return self._erddap.get_download_url(
            response=self.response_format,
            constraints=constraints if constraints else {},
        )

    def download(self) -> tuple:
        """
            Run the incremental sync against the IOOS Glider DAC.
        """
        logging.info("Querying IOOS Glider DAC for delayed-mode dataset IDs...")
        print("Querying IOOS Glider DAC for delayed-mode dataset IDs...")

        try:
            dataset_ids = self.list_dataset_ids()
        except Exception as exc:
            logging.error("Failed to fetch dataset catalogue: %s", exc)
            print(f"ERROR: Could not fetch dataset catalogue: {exc}")
            return 0, 0

        if not dataset_ids:
            print("No delayed-mode glider datasets found on the server.")
            return 0, 0

        print(f"Found {len(dataset_ids)} delayed-mode dataset(s).")

        # Build list of datasets that need downloading 
        to_download = []
        skipped_current = 0
        skipped_no_ts = 0

        if self.sync:
            print(
                f"\nChecking {len(dataset_ids)} dataset(s) against server timestamps..."
            )

        for i, dataset_id in enumerate(dataset_ids, 1):
            local_path = self._local_path(dataset_id)

            if self.overwrite:
                to_download.append(dataset_id)
                if self.sync:
                    print(f"  [{i}/{len(dataset_ids)}] {dataset_id}: overwrite - queued")
                continue

            local_exists = self._local_timestamp(local_path) is not None

            if not local_exists:
                to_download.append(dataset_id)
                if self.sync:
                    print(f"  [{i}/{len(dataset_ids)}] {dataset_id}: not found locally - queued")
                continue

            if not self.sync:
                # File exists, sync disabled - skip silently
                skipped_current += 1
                continue

            # sync=True, compare timestamps, print result for every dataset
            server_ts = self.get_server_timestamp(dataset_id)
            if server_ts is None:
                print(
                    f"  [{i}/{len(dataset_ids)}] {dataset_id}: "
                    f"no server timestamp - skipped"
                )
                logging.warning("No server timestamp for %s; skipping.", dataset_id)
                skipped_no_ts += 1
                continue

            local_ts = self._local_timestamp(local_path)
            if server_ts > local_ts:
                print(
                    f"  [{i}/{len(dataset_ids)}] {dataset_id}: "
                    f"server newer ({server_ts.date()} > {local_ts.date()}) - queued"
                )
                to_download.append(dataset_id)
            else:
                print(
                    f"  [{i}/{len(dataset_ids)}] {dataset_id}: "
                    f"up to date ({local_ts.date()}) - skipped"
                )
                skipped_current += 1

        # Dry-run branch
        if self.dryrun:
            mode = (
                "sync mode (timestamp check)"
                if self.sync
                else "download-only mode (missing files)"
            )
            print(
                f"\nDry run [{mode}]: {len(to_download)} file(s) would be "
                f"downloaded, {skipped_current} already current, "
                f"{skipped_no_ts} skipped (no server timestamp)."
            )
            for ds in to_download[:10]:
                print(f"  {ds}.parquet")
            if len(to_download) > 10:
                print(f"  ... and {len(to_download) - 10} more.")
            return len(to_download), 0

        if not to_download:
            print(
                f"All {skipped_current} local file(s) are current. "
                "Nothing to download."
            )
            return 0, 0

        print(
            f"\nDownloading {len(to_download)} dataset(s) "
            f"({skipped_current} already current, "
            f"{skipped_no_ts} skipped -no server timestamp)..."
        )

        # Download one dataset at a time 
        # Each dataset tries a full download first.
        # On 413, chunks are downloaded in parallel with staggered starts.
        completed = 0
        failed = 0

        for i, dataset_id in enumerate(to_download, 1):
            local_path = self._local_path(dataset_id)
            print(f"[{i}/{len(to_download)}] {dataset_id}")
            ok = self._download_one(dataset_id, local_path)
            if ok:
                completed += 1
            else:
                failed += 1

        print(
            f"\nSync complete. "
            f"Downloaded: {completed}, "
            f"Failed: {failed}, "
            f"Already current: {skipped_current}."
        )
        return completed, failed
#

    def _filter_datasets(self, dataset_ids: list) -> list:
        """Keep only delayed-mode IDs (suffix `-delayed`)."""
        if not self.delayed_only:
            return dataset_ids
        return [d for d in dataset_ids if d.endswith(_DELAYED_SUFFIX)]

    def _local_path(self, dataset_id: str) -> str:
        """Return the local parquet file path for `dataset_id`."""
        return os.path.join(self.input_path, f"{dataset_id}.parquet")


if __name__ == "__main__":
    DownloaderIOOSGliders()