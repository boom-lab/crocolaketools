#!/usr/bin/env python3

## @file downloader_saildrones.py
#
#
## @author Alieldin Alaa <alieldinalaa04@gmail.com>
#
## @date Wed 18 Mar 2026

##########################################################################
import time
from crocolaketools.downloader import saildrones_tools as st
from crocolaketools.downloader.downloader import Downloader
##########################################################################

class DownloaderSaildrones(Downloader):
    """class DownloaderSaildrones: methods to generate mirror of Saildrones
    files (missions 1-7) from ERDDAP
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self):
        return

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

    def saildrones_download(self, outdir_nc, search_for, id_prefix, dryrun_flag):

        start_time = time.time()
        print("Downloading Saildrones from ERDDAP...")
        
        st.saildrones_erddap(
            save_to=outdir_nc,
            dryrun=dryrun_flag,
            verbose=True,
            checktime=True,
            search_for=search_for,
            id_prefix=id_prefix
        )

        print("done.")
        elapsed_time = time.time() - start_time
        print("Time to download Saildrones database: " + str(elapsed_time))
        
        return

##########################################################################

if __name__ == "__main__":
    DownloaderSaildrones().saildrones_download(outdir_nc='.', dryrun_flag=False)

