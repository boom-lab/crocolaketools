import logging
import os

from crocolaketools.downloader.downloaderIOOS import DownloaderIOOS

IOOS_GLIDERS_SERVER="https://gliders.ioos.us/erddap"

_DELAYED_SUFFIX="-delayed"


# Target variables that we want to have in our downloaded datasets.
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
        # Incremental sync layer.
        # Returns (completed,failed) count.
    """

    SERVER_URL=IOOS_GLIDERS_SERVER
    PROTOCOL="tabledap"
    RESPONSE_FORMAT="parquet"

    def __init__(self,config:dict=None):

        if config is None:
            config={}
        
        config.setdefault("db","IOOS_GLIDERS")
        config.setdefault("db_type","PHY")
        super().__init__(config)

    
    def get_dataset_url(self, dataset_id:str)->str:
        """
            Returns the download URL for a specific dataset_id.
            Overridden base class method.
        """
        self._erddap._dataset_id=dataset_id
        self._erddap.constraints={}
        self._erddap.variables=GLIDER_VARIABLES
        return self._erddap.get_download_url(response=self.response_format)

    
    def download(self)->tuple:

        logging.info("Querying IOOS Glider DAC for delayed-mode dataset IDs..!")

        print("Querying IOOS Glider DAC for delayed-mode dataset IDs...")

        try:
            dataset_ids=self.list_dataset_ids()
        except Exception as e:
            logging.error("Failed to fetch dataset catalogue: %s",e)
            print(f"Error: Could not fetch dataset catalogue: {e}")
            return 0,0
        
        if not dataset_ids:
            print("No delayed-mode gliders dataset found on the server")

        print(f"Found {len(dataset_ids)} delayed-mode datasets(s).")
        

        # Downloader queue
        url_path_pairs=[]
        skipped_current=0 # Skipped dataset counter that is already upto date.
        skipped_no_ts=0 # Skipped dataset counter that didn't had a server timestamp.

        for dataset_id in dataset_ids:
            ext="parquet" if self.response_format=="parquet" else "nc"
            local_path=os.path.join(self.input_path,f"{dataset_id}.{ext}")

            #  when overwrite=True, always redownload

            if self.overwrite:
                url=self._safe_get_url(dataset_id)
                if url:
                    url_path_pairs.append((url,local_path))
                continue

            local_ts=self._local_timestamp(local_path)
            if local_ts is None:
                # No local file yet.
                url=self._safe_get_url(dataset_id)
                if url:
                    url_path_pairs.append((url,local_path))
                continue

            server_ts=self.get_server_timestamp(dataset_id)

            if server_ts is None:
                logging.warning(
                    "No server timestamp for %s; skipping.",dataset_id
                )
                skipped_no_ts+=1
                continue
            
            if server_ts>local_ts:
                logging.info(
                    "%s: server newer (%s > %s), queuing.",
                    dataset_id,
                    server_ts.isoformat(),
                    local_ts.isoformat(),
                )
                url= self._safe_get_url(dataset_id)
                if url:
                    url_path_pairs.append((url,local_path))
            else:
                logging.debug("%s: local copy is current, skipping.",dataset_id)
                skipped_current+=1

            

            # Dry run branch
            if self.dryrun:
                print(
                    f"\nDry run:\n{len(url_path_pairs)} files(s) would be downloaded."
                    f"\n{skipped_current} already current. " 
                    f"\n{skipped_no_ts} skipped due to (no server timestamp). "
                )

                for _,local_path in url_path_pairs[:10]:
                    print(f"{os.path.basename(local_path)}")
                
                if len(url_path_pairs)>10:
                    print(f" ... and {len(url_path_pairs)-10} more.")

                return len(url_path_pairs),0
            
            # Downloaded.
            if not url_path_pairs:
                print(
                    f"All {skipped_current} local files are current."
                    "Nothing to download"
                )
                return 0, 0
            
        
            print(
                f"\nDownloading {len(url_path_pairs)} datasets "
                f"({skipped_current} already current, "
                f"{skipped_no_ts} skipped due to (no server timestamp)"
            )

            completed,failed= self.download_parallel(
                url_path_pairs,
                num_threads=self.num_threads,
                dryrun=False
            )

            print(
                f"\nSync completed. "
                f"Downloaded: {completed}, Failed: {failed}, "
                f"Already current: {skipped_current}."
            )
            
            return completed,failed
        

    
    def _filtered_datasets(self,dataset_ids:list)->list:
        """Returns dataset IDs which are delayed mode."""
        if not self.delayed_only:
            return dataset_ids
        
        return [d for d in dataset_ids if d.endswith(_DELAYED_SUFFIX)]
    
    def _safe_get_url(self,dataset_id:str):

        """
            Build the download URL for `dataset_id` (completely string based, no network involved).

            Retruns None on failure. 
        """
        try:
            return self.get_dataset_url(dataset_id)
        except Exception as e:
            logging.warning(
                "Could not URL for %s: %s - skipping",dataset_id,e
            )
            return None


if __name__=="__main__":
    DownloaderIOOSGliders()