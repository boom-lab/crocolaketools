#!/usr/bin/env python3

import logging
import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional, Tuple

import pandas as pd
import requests
from tqdm import tqdm

from crocolaketools.downloader.downloader import Downloader

from erddapy import ERDDAP
from erddapy.core.url import urlopen


ERDDAP_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"
ERDDAP_CONSTRAINT_FMT = "%Y-%m-%dT%H:%M:%SZ"
TMP_CHUNKS_DIR = "tmp_chunks"

# Retry settings for transient server errors on (503, 429, 500)
RETRY_STATUS_CODES = {429, 500, 503}

RETRY_BACKOFF = [15, 30, 60]  # seconds to wait before each retry
MAX_RETRIES = len(RETRY_BACKOFF)
# Chunk size in hours, tried in order when a chunk gets 413.
# It is used as constraints in erddap.
CHUNK_SCHEDULE_HOURS = [24, 12, 6]

# Streaming read settings for the first-byte gate.
HTTP_TIMEOUT = (30, 600)   # (connect_timeout, read_timeout) seconds
STREAM_CHUNK_SIZE = 8192  # 8 KiB per iter_content block


class DownloaderERDDAP(Downloader):

    SERVER_URL: str = ""
    PROTOCOL: str = "tabledap"
    RESPONSE_FORMAT: str = "parquet"

    def __init__(self, config: dict = None):
        if config is None:
            raise ValueError("No config argument provided to DownloaderERDDAP.")

        super().__init__(config)

        self.server_url = config.get("server_url", self.SERVER_URL)
        self.protocol = config.get("protocol", self.PROTOCOL)
        self.response_format = config.get("response_format", self.RESPONSE_FORMAT)

        self._erddap = ERDDAP(
            server=self.server_url,
            protocol=self.protocol,
        )

    def list_dataset_ids(self) -> list:
        """Return all dataset IDs from the ERDDAP catalogue."""
        self._erddap._dataset_id = "allDatasets"
        self._erddap.constraints = {}
        self._erddap.variables = None

        df = self._erddap.to_pandas()
        dataset_ids = df["datasetID"].tolist()

        if "allDatasets" in dataset_ids:
            dataset_ids.remove("allDatasets")

        return self._filter_datasets(dataset_ids)

    def get_dataset_url(
        self,
        dataset_id: str,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ) -> str:
        """
            This method build the download URL.
            overriden from base class to add variable selection.
        """
        self._erddap._dataset_id = dataset_id
        self._erddap.variables = None
        self._erddap.constraints = {}

        constraints = {}
        if time_start is not None:
            constraints["time>="] = time_start.strftime(ERDDAP_CONSTRAINT_FMT)
        if time_end is not None:
            constraints["time<="] = time_end.strftime(ERDDAP_CONSTRAINT_FMT)

        return self._erddap.get_download_url(
            response=self.response_format,
            constraints=constraints if constraints else {},
        )

    def get_server_timestamp(self, dataset_id: str) -> Optional[datetime]:
        """Fetch last-modified timestamp from the ERDDAP info endpoint."""
        url = self._erddap.get_info_url(dataset_id=dataset_id, response="csv")
        try:
            data = urlopen(url)
            df = pd.read_csv(data)
        except Exception as exc:
            logging.warning(
                "Could not fetch info for %s: %s", dataset_id, exc
            )
            return None

        nc_global = df[df["Variable Name"] == "NC_GLOBAL"]
        for attr in ("date_modified", "date_created", "date_issued"):
            row = nc_global[nc_global["Attribute Name"] == attr]
            if not row.empty:
                raw_ts = row["Value"].iloc[0]
                try:
                    dt = datetime.strptime(raw_ts, ERDDAP_TS_FMT)
                    return dt.replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    pass
        return None

    def download(self) -> tuple:
        """Orchestrate the sync. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement download().")

    def _download_file(
        self,
        url: str,
        local_path: str,
        on_first_byte: Optional[Callable[[], None]] = None,
    ) -> None:
        """Stream url to local_path, firing on_first_byte once data arrives.

        Overrides the base Downloader._download_file to support the
        first-byte gate. ERDDAP does its expensive work (reading source
        files, building the response in memory) BEFORE sending the first
        byte. Once bytes start flowing, that heavy phase is done, so we
        invoke on_first_byte() at that moment to release the next chunk.

        Downloads to a temporary file first and renames it only after the
        download completes successfully, so a failed/interrupted download
        never leaves a corrupt file on disk.
        """
        tmp_path = local_path + ".tmp"
        try:
            with requests.get(url, stream=True, timeout=HTTP_TIMEOUT) as response:
                response.raise_for_status()
                total_size = int(response.headers.get("content-length", 0))
                signaled = False
                with open(tmp_path, "wb") as fh, tqdm(
                    desc=os.path.basename(local_path),
                    total=total_size,
                    unit="iB",
                    unit_scale=True,
                    unit_divisor=1024,
                    leave=True,
                ) as bar:
                    for chunk in response.iter_content(chunk_size=STREAM_CHUNK_SIZE):
                        if not signaled and chunk:
                            # First byte received: heavy server-side phase is
                            # done. Release the gate so the next chunk can
                            # begin its heavy phase.
                            signaled = True
                            if on_first_byte is not None:
                                on_first_byte()
                        size = fh.write(chunk)
                        bar.update(size)
            os.replace(tmp_path, local_path)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    def _download_file_with_retry(
        self,
        url: str,
        local_path: str,
        on_first_byte: Optional[Callable[[], None]] = None,
    ) -> None:

        from requests.exceptions import ChunkedEncodingError
        from requests.exceptions import ConnectionError as RequestsConnectionError
        from requests.exceptions import ReadTimeout

        last_exc = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                self._download_file(url, local_path, on_first_byte=on_first_byte)
                return

            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status not in RETRY_STATUS_CODES:
                    raise
                last_exc = exc

            except (
                ChunkedEncodingError,   # wraps IncompleteRead
                RequestsConnectionError,
                ReadTimeout,
            ) as exc:
                # Network-level interruption, always retryable
                last_exc = exc

            # Retry with backoff
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF[attempt]
                status = last_exc.response.status_code if isinstance(last_exc, requests.exceptions.HTTPError) and last_exc.response is not None else None
                status_str = f" (HTTP {status})" if status else ""
                logging.warning(
                    "%s%s — retrying in %ds (attempt %d/%d): %s",
                    type(last_exc).__name__, status_str, wait,
                    attempt + 1, MAX_RETRIES, url,
                )
                tqdm.write(
                    f"  {type(last_exc).__name__}{status_str} — waiting {wait}s before retry "
                    f"({attempt + 1}/{MAX_RETRIES})..."
                )
                time.sleep(wait)

        raise last_exc

    def _download_one(self, dataset_id: str, local_path: str) -> bool:
        """Download one dataset to `local_path` using parallel chunking. """
        time_range = self._get_dataset_time_range(dataset_id)
        if time_range is None:
            logging.error(
                "%s: cannot determine time range for chunking.", dataset_id
            )
            return False

        t_start, t_end = time_range

        for chunk_hours in CHUNK_SCHEDULE_HOURS:
            windows = self._split_window(t_start, t_end, chunk_hours=chunk_hours)
            logging.info(
                "%s: %d x %dh chunk(s) (%s - %s).",
                dataset_id, len(windows), chunk_hours,
                t_start.date(), t_end.date(),
            )
            print(
                f"  {dataset_id}: {len(windows)} x {chunk_hours}h chunk(s)."
            )

            tmp_dir = os.path.join(self.input_path, TMP_CHUNKS_DIR, dataset_id)
            os.makedirs(tmp_dir, exist_ok=True)

            try:
                ok, got_413 = self._download_chunks_parallel(
                    dataset_id, windows, tmp_dir, local_path
                )
            finally:
                if os.path.exists(tmp_dir):
                    shutil.rmtree(tmp_dir)

            if ok:
                return True
            if not got_413:
                return False

            logging.warning(
                "%s: still getting 413 at %dh chunks, trying smaller.",
                dataset_id, chunk_hours,
            )

        logging.error(
            "%s: 413 at minimum chunk size (%dh). Cannot reduce further.",
            dataset_id, CHUNK_SCHEDULE_HOURS[-1],
        )
        return False

    def _download_chunks_parallel(
        self,
        dataset_id: str,
        windows: List[Tuple[datetime, datetime]],
        tmp_dir: str,
        local_path: str,
    ) -> Tuple[bool, bool]:
        """
            Download `windows` as parallel chunks with a first-byte gate.

            Multiple chunk transfers run in parallel, but only ONE chunk
            is allowed to be in its server-side "heavy" phase (reading
            source files + building the response) at a time. The gate for
            the next chunk is released only when the current chunk receives
            its first byte — i.e. when its heavy phase is finished and the
            server is just streaming bytes. This avoids stacking several
            memory-heavy ERDDAP requests at once, which is what triggers
            503 (server out of memory) on file-heavy datasets.
        """
        chunk_jobs = []
        for i, (ws, we) in enumerate(windows):
            chunk_path = os.path.join(
                tmp_dir, f"{dataset_id}_chunk_{i:04d}.parquet"
            )
            url = self._safe_get_url(dataset_id, time_start=ws, time_end=we)
            if url is not None:
                chunk_jobs.append((chunk_path, url))

        if not chunk_jobs:
            logging.error("%s: no valid chunk URLs.", dataset_id)
            return False, False

        # Gate starts at 0; the main loop blocks on acquire() until the
        # running chunk signals (via first byte, or via the safety release
        # for empty/0-byte responses) that the next chunk may begin.
        started = threading.Semaphore(0)

        def _download_and_signal(url: str, path: str) -> None:
            # Each chunk releases the gate at most once: either when its
            # first byte arrives (heavy phase done) or, as a safety net,
            # when the call returns having produced no bytes (e.g. an empty
            # 404 window never triggers on_first_byte, so we must release
            # here or the main loop would block forever).
            released = threading.Event()

            def _release_once() -> None:
                if not released.is_set():
                    released.set()
                    started.release()

            try:
                self._download_file_with_retry(
                    url, path, on_first_byte=_release_once
                )
            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status == 404:
                    # Report the empty window immediately, where it occurs,
                    # so it appears live during the download rather than all
                    # at the end when results are collected.
                    tqdm.write(
                        f"  {os.path.basename(path)}: empty window (no data) - skipped"
                    )
                raise   # re-raise so the collector loop counts it correctly
            finally:
                _release_once()

        chunk_files = []
        failed_chunks = 0
        empty_chunks = 0
        got_413 = False

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            future_to_path = {}
            for chunk_path, url in chunk_jobs:
                future = executor.submit(_download_and_signal, url, chunk_path)
                future_to_path[future] = chunk_path
                started.acquire()

            for future in as_completed(future_to_path):
                chunk_path = future_to_path[future]
                try:
                    future.result()
                    chunk_files.append(chunk_path)
                except requests.exceptions.HTTPError as exc:
                    status = exc.response.status_code if exc.response is not None else None
                    if status == 404:
                        # Empty window - no data for this period, not a failure.
                        # The live "skipped" line is printed by the worker
                        # (_download_and_signal) at the moment it occurs.
                        empty_chunks += 1
                        logging.info(
                            "%s: chunk %s returned 404 (empty window, no data) - skipping.",
                            dataset_id, os.path.basename(chunk_path),
                        )
                    elif status == 413:
                        got_413 = True
                        failed_chunks += 1
                        logging.warning(
                            "%s: chunk %s returned 413 - need smaller chunks.",
                            dataset_id, os.path.basename(chunk_path),
                        )
                    else:
                        failed_chunks += 1
                        logging.error(
                            "%s: chunk %s failed: %s",
                            dataset_id, os.path.basename(chunk_path), exc,
                        )
                except Exception as exc:
                    failed_chunks += 1
                    logging.error(
                        "%s: chunk %s failed: %s",
                        dataset_id, os.path.basename(chunk_path), exc,
                    )

        if not chunk_files:
            logging.error("%s: no chunks downloaded.", dataset_id)
            return False, got_413

        if failed_chunks:
            #  will not merge some data windows (chunks) are missing.
            # In such scenerio we can try with smaller chunks (if 413) or mark as failed.
            logging.error(
                "%s: %d/%d chunk(s) failed - aborting merge. "
                "No partial file written.",
                dataset_id, failed_chunks, len(chunk_jobs),
            )
            return False, got_413

        chunk_files.sort()
        if empty_chunks:
            logging.info(
                "%s: %d empty window(s) skipped (404, no data).",
                dataset_id, empty_chunks,
            )
            tqdm.write(
                f"  {dataset_id}: {empty_chunks} empty window(s) skipped, "
                f"merging {len(chunk_files)} chunk(s)..."
            )
        else:
            tqdm.write(f"  {dataset_id}: merging {len(chunk_files)} chunk(s)...")
        try:
            dfs = [pd.read_parquet(f) for f in chunk_files]
            merged = pd.concat(dfs, ignore_index=True)
            if "time" in merged.columns:
                merged = merged.sort_values("time").reset_index(drop=True)
            merged.to_parquet(local_path, index=False)
        except Exception as exc:
            logging.error("%s: merge failed: %s", dataset_id, exc)
            return False, got_413

        return True, got_413

    def _get_dataset_time_range(
        self,
        dataset_id: str,
    ) -> Optional[Tuple[datetime, datetime]]:
        """Return (min_time, max_time) from NC_GLOBAL of the info endpoint."""
        url = self._erddap.get_info_url(dataset_id=dataset_id, response="csv")
        try:
            data = urlopen(url)
            df = pd.read_csv(data)
        except Exception as exc:
            logging.warning(
                "Could not fetch time range for %s: %s", dataset_id, exc
            )
            return None

        nc_global = df[df["Variable Name"] == "NC_GLOBAL"]

        def _parse(attr: str) -> Optional[datetime]:
            row = nc_global[nc_global["Attribute Name"] == attr]
            if row.empty:
                return None
            raw = row["Value"].iloc[0]
            for fmt in (
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%d",
            ):
                try:
                    return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            return None

        t_start = _parse("time_coverage_start")
        t_end   = _parse("time_coverage_end")

        if t_start is None or t_end is None:
            logging.warning("%s: time_coverage_start/end missing.", dataset_id)
            return None

        return t_start, t_end

    def _filter_datasets(self, dataset_ids: list) -> list:
        """Return dataset_ids unchanged. overriden from base class."""
        return dataset_ids

    def _safe_get_url(
        self,
        dataset_id: str,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ) -> Optional[str]:
        """Build the download URL, returning None on failure."""
        try:
            return self.get_dataset_url(dataset_id, time_start, time_end)
        except Exception as exc:
            logging.warning(
                "Could not build URL for %s: %s - skipping.", dataset_id, exc
            )
            return None

    @staticmethod
    def _split_window(
        t_start: datetime,
        t_end: datetime,
        chunk_hours: int = 24,
    ) -> List[Tuple[datetime, datetime]]:
        """Split [t_start, t_end] into windows of *chunk_hours* hours."""
        windows = []
        delta = timedelta(hours=chunk_hours)
        current = t_start
        while current < t_end:
            window_end = min(current + delta, t_end)
            windows.append((current, window_end))
            current = window_end
        return windows

    @staticmethod
    def _local_timestamp(local_path: str) -> Optional[datetime]:
        """Return filesystem mtime as UTC datetime, or None."""
        if not os.path.isfile(local_path):
            return None
        mtime = os.path.getmtime(local_path)
        return datetime.fromtimestamp(mtime, tz=timezone.utc)