#!/usr/bin/env python3
import argparse

from crocolaketools.downloader.downloaderIOOSGliders import DownloaderIOOSGliders


def download_ioos_gliders(
    config: dict = None,
    overwrite: bool = None,
    dryrun: bool = None,
    num_threads: int = None,
    sync: bool = None,
) -> tuple:
    """
        Run the IOOS Glider DAC incremental sync.
    """
    if config is None:
        config = {"db": "IOOS_GLIDERS", "db_type": "PHY"}

    # only override config.yaml values if explicitly passed via CLI/API;
    # None means "not set, let config.yaml decide"
    if overwrite is not None:
        config["overwrite"] = overwrite
    if dryrun is not None:
        config["dryrun"] = dryrun
    if sync is not None:
        config["sync"] = sync
    if num_threads is not None:
        config["num_threads"] = num_threads

    downloader = DownloaderIOOSGliders(config=config)
    return downloader.download()


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Incrementally sync IOOS Glider DAC delayed-mode datasets from "
            "gliders.ioos.us/erddap to the local path configured in config.yaml."
        )
    )
    parser.add_argument(
        "--config",
        action="store_true",
        default=False,
        help="Use config.yaml defaults for server_url, input_path, and other settings.",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        default=False,
        dest="sync",
        help=(
            "Compare server timestamps against local files and re-download "
            "updated datasets. Overrides sync setting in config.yaml."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Re-download all files, even those already present and current.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        dest="dryrun",
        help="Print a download summary without fetching any files.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Number of parallel download threads. "
            "If not set, uses the value from config.yaml (num_threads)."
        ),
    )

    args = parser.parse_args()

    config = {"db": "IOOS_GLIDERS", "db_type": "PHY"}

    # only pass flags that were explicitly set; don't override config.yaml
    # with argparse defaults
    download_ioos_gliders(
        config=config,
        overwrite=args.overwrite if args.overwrite else None,
        dryrun=args.dryrun if args.dryrun else None,
        sync=args.sync if args.sync else None,
        num_threads=args.threads,
    )


if __name__ == "__main__":
    main()
