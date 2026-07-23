#!/usr/bin/env python3

## @file downloaderERDDAP.py
#
# Contains base class for Interacting with ERDDAP servers
# (uses erddapy package internally and inherited from base downloader class).
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Sunday 26 June, 2026

################################################################################################
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
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ReadTimeout
from tqdm import tqdm

from crocolaketools.downloader.downloader import Downloader

from erddapy import ERDDAP
from erddapy.core.url import urlopen


ERDDAP_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"
TMP_CHUNKS_DIR = "tmp_chunks"

# Retry settings for transient server errors on (503, 429, 500)
RETRY_STATUS_CODES = {429, 500, 503}

RETRY_BACKOFF = [15, 30, 60]  # seconds to wait before each retry
MAX_RETRIES = len(RETRY_BACKOFF)
# tried in descending order; if a chunk returns 413, the next size down is used
CHUNK_SCHEDULE_HOURS = [24, 12, 6]

HTTP_TIMEOUT = (30, 600)   # (connect_timeout, read_timeout) seconds
STREAM_CHUNK_SIZE = 8192  # 8 KiB per iter_content block


class FirstByteGate:
    """
        Limits how many chunk requests can be in ERDDAP's build phase at once.

        ERDDAP loads all source files into memory before sending the first byte,
        so overlapping requests hit memory limits fast and cause 503s.
        Once a chunk starts streaming, the next one can begin its build phase.
    """

    def __init__(self):
        # starts at 0 so the first wait() blocks until explicitly released
        self._sem = threading.Semaphore(0)

    def wait(self):
        """
            Block until the current chunk signals it has started streaming.
        """
        self._sem.acquire()

    def release(self):
        self._sem.release()

    def make_callback(self) -> Callable:
        """
            Return a callable that releases the gate exactly once.
        """
        released = threading.Event()

        def _once():
            if not released.is_set():
                released.set()
                self.release()

        return _once


class NoOpGate:
    """
        Drop-in replacement for FirstByteGate that never blocks.

        Used when gated_parallel_download=False: all chunk requests fire
        immediately, with concurrency bounded only by num_threads.
    """

    def wait(self):
        pass

    def make_callback(self) -> Callable:
        def _noop():
            pass
        return _noop


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
        # if False, chunk requests fire without the first-byte gate (see NoOpGate)
        self.gated_parallel_download = config.get("gated_parallel_download", True)

        # Optional user-defined constraints from config.yaml.
        # Supports all 7 ERDDAP tabledap operators: =, !=, =~, <, <=, >, >=
        # time>= / time<= clamp the chunking window in _download_one.
        # All other constraints are passed to ERDDAP on every chunk request.
        _c = config.get("constraints", {})
        self.time_start        = _c.pop("time>=", None)
        self.time_end          = _c.pop("time<=", None)
        self.extra_constraints = _c

        self._erddap = ERDDAP(
            server=self.server_url,
            protocol=self.protocol,
        )

    def list_dataset_ids(self) -> list:
        """
            Return all dataset IDs from the ERDDAP catalogue.
        """
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
            Build the download URL for a dataset with optional time constraints.
            Subclasses override this to add variable selection or extra constraints.
        """
        self._erddap._dataset_id = dataset_id
        self._erddap.variables = None
        self._erddap.constraints = {}

        constraints = {}
        if time_start is not None:
            constraints["time>="] = time_start.strftime(ERDDAP_TS_FMT)
        if time_end is not None:
            constraints["time<="] = time_end.strftime(ERDDAP_TS_FMT)

        constraints.update(self.extra_constraints)

        return self._erddap.get_download_url(
            response=self.response_format,
            constraints=constraints if constraints else {},
        )

    def get_server_timestamp(self, dataset_id: str) -> Optional[datetime]:
        """
            Fetch last-modified timestamp from the ERDDAP info endpoint.
        """
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

    def get_dataset_variables(self, dataset_id: str) -> set:
        """
            Return the set of variable names available in `dataset_id` by
            querying the ERDDAP info endpoint.
            
            It will return empty set on failure so callers can decide whether
            to proceed or skip.
        """
        url = self._erddap.get_info_url(dataset_id=dataset_id, response="csv")
        try:
            data = urlopen(url)
            df = pd.read_csv(data)
            return set(df[df["Row Type"] == "variable"]["Variable Name"].tolist())
        except Exception as exc:
            logging.warning(
                "%s: could not fetch variable list: %s", dataset_id, exc
            )
            return set()

    def download(self) -> tuple:
        """
            Orchestrate the sync. Must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement download().")

    def _build_download_queue(self, dataset_ids: list) -> tuple:
        """
            Decide which datasets need downloading.
            Returns (to_download, skipped_current, skipped_no_ts).
            Subclasses may override this if they need different queue-building logic.
            default implementation covers the common case:
            - overwrite=True -> queue everything
            - sync=False   ->   queue only files missing locally
            - sync=True    ->   compare server vs local timestamps
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

            # sync=True: compare timestamps
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

    def _download_file(
        self,
        url: str,
        local_path: str,
        on_first_byte: Optional[Callable[[], None]] = None,
    ) -> None:
        """
            Stream url to local_path, calling on_first_byte once the first data chunk arrives.

            ERDDAP builds the full response in memory before sending anything, so the
            first byte means the heavy work is done and the next chunk can start its
            build. Uses a .tmp file so an interrupted download never corrupts the output.
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
        """
            Wraps _download_file with retry logic for transient HTTP and network errors.
        """
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
                ChunkedEncodingError,
                RequestsConnectionError,
                ReadTimeout,
            ) as exc:
                last_exc = exc

            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF[attempt]
                status = last_exc.response.status_code if isinstance(last_exc, requests.exceptions.HTTPError) and last_exc.response is not None else None
                status_str = f" (HTTP {status})" if status else ""
                logging.warning(
                    "%s%s - retrying in %ds (attempt %d/%d): %s",
                    type(last_exc).__name__, status_str, wait,
                    attempt + 1, MAX_RETRIES, url,
                )
                tqdm.write(
                    f"  {type(last_exc).__name__}{status_str} - waiting {wait}s before retry "
                    f"({attempt + 1}/{MAX_RETRIES})..."
                )
                time.sleep(wait)

        raise last_exc

    def _download_one(self, dataset_id: str, local_path: str) -> bool:
        """
            Download one dataset to `local_path` using parallel chunking.
        """
        time_range = self._get_dataset_time_range(dataset_id)
        if time_range is None:
            logging.error(
                "%s: cannot determine time range for chunking.", dataset_id
            )
            return False

        t_start, t_end = time_range

        if self.time_start is not None:
            user_t_start = datetime.strptime(self.time_start, ERDDAP_TS_FMT).replace(tzinfo=timezone.utc)
            t_start = max(t_start, user_t_start)
        if self.time_end is not None:
            user_t_end = datetime.strptime(self.time_end, ERDDAP_TS_FMT).replace(tzinfo=timezone.utc)
            t_end = min(t_end, user_t_end)

        if t_start >= t_end:
            logging.info(
                "%s: dataset time range is outside the requested "
                "constraint window - skipping.",
                dataset_id,
            )
            return True

        for chunk_hours in CHUNK_SCHEDULE_HOURS:
            windows = self._split_window(t_start, t_end, chunk_hours=chunk_hours)
            logging.info(
                "%s: %d x %dh chunk(s) (%s - %s).",
                dataset_id, len(windows), chunk_hours,
                t_start.date(), t_end.date(),
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
            Download `windows` as parallel chunks using a first-byte gate.

            Multiple chunks stream in parallel, but only one is in ERDDAP's heavy
            build phase at a time. See FirstByteGate.
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

        if self.gated_parallel_download:
            gate = FirstByteGate()
        else:
            gate = NoOpGate()
            logging.info(
                "%s: gated_parallel_download=False - chunks requested without "
                "the first-byte gate.", dataset_id,
            )

        def _worker(url: str, path: str, on_gate_release: Callable) -> None:
            try:
                self._download_file_with_retry(
                    url, path, on_first_byte=on_gate_release
                )
            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status == 404:
                    tqdm.write(
                        f"  {os.path.basename(path)}: empty window (no data) - skipped"
                    )
                raise
            finally:
                on_gate_release()

        chunk_files = []
        failed_chunks = 0
        empty_chunks = 0
        got_413 = False

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            future_to_path = {}
            for chunk_path, url in chunk_jobs:
                on_gate_release = gate.make_callback()
                future = executor.submit(_worker, url, chunk_path, on_gate_release)
                future_to_path[future] = chunk_path
                gate.wait()

            for future in as_completed(future_to_path):
                chunk_path = future_to_path[future]
                try:
                    future.result()
                    chunk_files.append(chunk_path)
                except requests.exceptions.HTTPError as exc:
                    status = exc.response.status_code if exc.response is not None else None
                    if status == 404:
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

        return self._merge_chunks(chunk_files, local_path, dataset_id), got_413

    def _merge_chunks(
        self,
        chunk_files: List[str],
        local_path: str,
        dataset_id: str,
    ) -> bool:
        """
            Concatenate sorted chunk parquet files into a single output file.
        """
        try:
            dfs = [pd.read_parquet(f) for f in chunk_files]
            merged = pd.concat(dfs, ignore_index=True)
            if "time" in merged.columns:
                merged = merged.sort_values("time").reset_index(drop=True)
            merged.to_parquet(local_path, index=False)
            return True
        except Exception as exc:
            logging.error("%s: merge failed: %s", dataset_id, exc)
            return False

    def _get_dataset_time_range(
        self,
        dataset_id: str,
    ) -> Optional[Tuple[datetime, datetime]]:
        """
            Return (min_time, max_time) from NC_GLOBAL of the info endpoint.
        """
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
        """
            Default is no operation. Subclasses override this to filter the list.
        """
        return dataset_ids

    def _safe_get_url(
        self,
        dataset_id: str,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ) -> Optional[str]:
        """
            Build the download URL, returning None on failure.
        """
        try:
            return self.get_dataset_url(dataset_id, time_start, time_end)
        except Exception as exc:
            logging.warning(
                "Could not build URL for %s: %s - skipping.", dataset_id, exc
            )
            return None

    def _local_path(self, dataset_id: str) -> str:
        """
            Return the local file path for `dataset_id`.
            Subclasses override to set filename and extension.
        """
        raise NotImplementedError("Subclasses must implement _local_path().")

    @staticmethod
    def _split_window(
        t_start: datetime,
        t_end: datetime,
        chunk_hours: int = 24,
    ) -> List[Tuple[datetime, datetime]]:
        """
            Split [t_start, t_end] into windows of *chunk_hours* hours.
        """
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
        """
            Return filesystem mtime as UTC datetime, or None.
        """
        if not os.path.isfile(local_path):
            return None
        mtime = os.path.getmtime(local_path)
        return datetime.fromtimestamp(mtime, tz=timezone.utc)

################################################################################################