#!/usr/bin/env python

import sys
import os

# For parsing files
from glob import glob

from database import ProjectUtils, DatasetUtils

# This script will add files to the database.  It can do pattern matching
# to find all of the files, but needs some implementation checked
# For counting how many events per file, etc.


# includes needed for opening a file to count number of events:
import ROOT
from ROOT import TFile

def main(top_level_dir, pattern_to_match, dataset_name):

    proj_utils = ProjectUtils()
    ds_utils   = DatasetUtils()

    # First step, create a dataset:

    if dataset_name in proj_utils.list_datasets():
        raise Exception("Can't create dataset with name {0}, already exists".format(dataset_name))
    else:
        print "Would have created dataset {0}".format(dataset_name)
        # proj_utils.create_dataset(dataset_name)


    # Collect the names of all the files:
    _file_list = glob(top_level_dir + pattern_to_match)
    print _file_list

    for _file in _file_list:
        n_events = get_events_per_file(_file)
        size = os.path.getsize(self.out_dir + self.output_file)
        jobid = -1
        print "Would have declared file {0} with the following info:".format(_file)
        print "  -- filename:\t{0}".format(_file)
        print "  -- type:\t    {0}".format(0)
        print "  -- nevents:\t {0}".format(n_events)
        print "  -- jobid:\t   {0}".format(jobid)
        print "  -- size:\t    {0}".format(size)

        # ds_utils.declare_file(dataset=dataset_name,
        #                       filename = _file,
        #                       ftype = 0,
        #                       nevents = n_events,
        #                       jobid = jobid,
        #                       size=size)


def get_events_per_file(file_name):
    return TFile(file_name).Get("EVENT").GetEntries()


if __name__ == '__main__':
    main(top_level_dir="/n/holylfs/LABS/guenette_lab/data/NEXT/NEXTNEW/MC/Calibration/nexus/NEXT_v1_00_05/nexus_v5_02_08/Tl/nexusfiles/",
         pattern_to_match="nexus_NEW_NEXT_v1_00_05_Tl_DISK_CLOSE_TO_ANODE_7bar_5Mev*.next",
         dataset_name="NEW_Tl_disk_close_to_anode_7bar_5mev_nexus")