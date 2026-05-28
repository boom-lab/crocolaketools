import argparse
import functools
from datetime import datetime

from crocolaketools.downloader.downloaderIOOSGliders import DownloaderIOOSGliders

print=functools.partial(print,flush=True)


def download_ioos_gliders(
    config=None,
    overwrite:bool=False,
    dryrun:bool=False,
    num_threads:int=4,
) -> tuple:
    
    """
        Run the IOOS Glider DAC incremental sync.
        Returns (completed,failed) counts from the downloader.
    """

    if config is not None:
        config["overwrite"]=overwrite
        config["dryrun"]=dryrun
        config["num_threads"]=num_threads

    downloader=DownloaderIOOSGliders(config=config)
    return downloader.download()


def main():
    parser=argparse.ArgumentParser(
        description=(
            "Incrementally sync IOOS Glider DAC delayed-mode datasets from "
            "gliders.ioos.us/erddap to the local path configured in config.yaml"
        )
    )


    parser.add_argument(
        "--config",
        action="store_true",
        default=False,
        help="Use config.yaml defauls for server_url, input_path and other settings."

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
        help="Print a download summary without fetching any files."
    )

    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        metavar="N",
        help="Number of parallel download threads (default:4)."

    )

    args=parser.parse_args()

    config = None if args.config else {"db":"IOOS_GLIDERS","db_type":"PHY"}

    download_ioos_gliders(
        config=config,
        overwrite=args.overwrite,
        dryrun=args.dryrun,
        num_threads=args.threads,
    )


if __name__=="__main__":
    print(datetime.now())
    main()
    print("download_ioos_gliders.py executed successfully\n")
    print(datetime.now())