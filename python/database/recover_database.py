#!/usr/bin/env python

import glob

from database import DBUtil

# This script traverses a list of files and recovers the sqlite3 database
# It's not meant as a permanent solution, just a quick fix to a problem
# that should go away soon.

def recover_stage(top_dir, stage_name, dataset_name,
                  root_pattern, ana_pattern, job_id
                  db_file):


    # walk through the directory list
    list_of_directories = glob.glob(top_dir + "/{0}_*/".format(job_id))
    print(list_of_directories[:10])

    # declare_file(self, dataset, filename, location, stage, status, nevents, ftype):


if __name ==  "__main__":

    top_dir = "/n/holylfs/LABS/guenette_lab/data/production/mcc8.6/single_pion/generation/"
    stage_name = "generation"
    dataset_name = "mcc8.6_single_pion"
    root_pattern = "prod_pions_0-2.0GeV_isotropic_uboone*_reco2.root"
    ana_pattern = "reco_stage_2_hist.root"
    job_id = 36597655
    db_file = "/n/holylfs/LABS/guenette_lab/data/production/mcc8.6/single_pion/work/mcc8.6_single_pion.db"

    recover_stage(top_dir=top_dir, stage_name=stage_name,
        dataset_name=dataset_name, root_pattern = root_pattern,
        ana_pattern = ana_pattern, job_id = job_id,
        db_file = db_file)