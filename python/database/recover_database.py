#!/usr/bin/env python

import glob
import os

from database import DBUtil

# This script traverses a list of files and recovers the sqlite3 database
# It's not meant as a permanent solution, just a quick fix to a problem
# that should go away soon.

def recover_stage(top_dir, stage_name, dataset_name,
                  root_pattern, ana_pattern, job_id,
                  db_file):

    db = DBUtil(db_file)

    # walk through the directory list
    print("Search pattern: " + top_dir + "/{0}_*/".format(job_id))
    list_of_directories = glob.glob(top_dir + "/{0}.*/".format(job_id))
    print(list_of_directories[:10])

    for _dir in list_of_directories:
        # Get the root files and ana files:
        root_file = glob.glob(_dir + root_pattern)
        ana_file  = glob.glob(_dir + ana_pattern)

        if len(root_file) < 1:
            print "Continuing"
            continue
        else:
            root_file = root_file[0]

        db.declare_file(dataset=dataset_name, filename=os.path.basename(root_file),
                        location=_dir, stage=stage_name, status = 0, ftype=0, nevents=100)


        if len(ana_file) < 1:
            print "Continuing"
            continue
        else:
            ana_file = ana_file[0]
        db.declare_file(dataset=dataset_name, filename=os.path.basename(ana_file),
                        location=_dir, stage=stage_name, status = 0, ftype=1, nevents=100)

        break

    # declare_file(self, dataset, filename, location, stage, status, nevents, ftype):


if __name__ ==  "__main__":

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
