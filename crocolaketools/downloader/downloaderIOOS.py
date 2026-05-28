import logging
import os
from datetime import datetime,timezone
from typing import Optional
import pandas as pd

from crocolaketools.downloader.downloader import Downloader

from erddapy import ERDDAP
from erddapy.core.url import urlopen


_ERDDAP_TS_FMT="%Y-%m-%dT%H:%M:%SZ"


class DownloaderIOOS(Downloader):

    """Base class for all IOOS ERDDAP downloaders"""

    SERVER_URL:str="https://gliders.ioos.us/erddap" #IOOS ERDDAP server url
    PROTOCOL:str="tabledap" # `tabledap`` as we are downloading parquets.
    RESPONSE_FORMAT:str="parquet"

    # Constructor
    def __init__(self,config:dict=None):

        if config is None:
            config={}
        
        config.setdefault("db","IOOS_GLIDERS")
        config.setdefault("db_type","PHY")
        super().__init__(config)
        


        # overriding class-level defaults with config.yml entries.
        self.server_url=config.get("server_url",self.SERVER_URL)
        self.protocol=config.get("protocol",self.PROTOCOL)
        self.response_format=config.get("response_format",self.RESPONSE_FORMAT)
        self.delayed_only=config.get("delayed_only",True)
        
        # Initializing ERDDAP client
        self._erddap=ERDDAP(
            server=self.server_url,
            protocol=self.protocol,
        )
    
    def list_dataset_ids(self)->list:
        """
            Returns the dataset ids available in the ERDDAP server.
        """

        self._erddap._dataset_id="allDatasets"
        self._erddap.constraints={}
        self._erddap.variables=None
        

        df=self._erddap.to_pandas()
        dataset_ids=df["datasetID"].tolist()

        # removing the meta-dataset
        if "allDatasets" in dataset_ids:
            dataset_ids.remove("allDatasets")

        return self._filtered_datasets(dataset_ids)
    
    def get_dataset_url(self,dataset_id:str)->str:
        """
            Returns the download URL for a specific dataset_id
        """
        self._erddap._dataset_id=dataset_id
        self._erddap.constraints={}
        self._erddap.variables=None

        return self._erddap.get_download_url(response=self.response_format)


    def get_server_timestamp(self,dataset_id:str)->Optional[datetime]:
        """
            Fetching last modified timestamp from the erddap server for a specific dataset_id
        """    
        url=self._erddap.get_info_url(dataset_id=dataset_id,response="csv")

        try:
            data=urlopen(url)
            df=pd.read_csv(data)

        except Exception as e:
            logging.warning(
                "Could not fetch info endpoint for %s: %s",dataset_id,e
            )
            return None
        
        nc_global=df[df["Variable Name"]=="NC_GLOBAL"]
        for attr in ("date_modified","date_created","date_issued"):
            row=nc_global[nc_global["Attribute Name"]==attr]
            if not row.empty:
                raw_ts=row["Value"].iloc[0]
                try:
                    dt=datetime.strptime(raw_ts,_ERDDAP_TS_FMT)
                    return dt.replace(tzinfo=timezone.utc)
                except (ValueError,TypeError):
                    logging.debug(
                        "Could not parse timestamp %r for %s",raw_ts,dataset_id
                    )

        return None
    
    def download(self)->tuple:
        """
            Peforms the sync.
            It will be implemented through polymorphism in a subclass.
        """
        raise NotImplementedError("Subclass should implement download()")
    
    def _filtered_datasets(self,dataset_ids:list)->list:
        """
            leveraged for filtering dataset. (ex: `delayed_mode` datasets)
            By default, no filters are applied. 
            This method is modified in subclass through polymorphism according to the requirements.
        """

        return dataset_ids
    
    @staticmethod
    def _local_timestamp(local_path:str)->Optional[datetime]:
        """ Returns filesystem mtime of `local_path` as a UTC datetime or None if the file does not exist."""

        if not os.path.isfile(local_path):
            return None
        
        mtime=os.path.getmtime(local_path)
        return datetime.fromtimestamp(mtime,tz=timezone.utc)