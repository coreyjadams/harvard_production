#!/usr/bin/env python
import os
import sys

from config import ProjectConfig
from utils import JobRunner
from database import DBUtil

def main(config_file, stage, db_file):
    print("Creating Project Config Object")
    project = ProjectConfig(config_file)
    print("Config created, setup larsoft ...")
    project.larsoft().setup_larsoft()
    runner = JobRunner(project = project, stage=project.stage(stage))
    db = DBUtil(db_file)
    runner.prepare_job(db_util=db)
    runner.run_job(db_util=db)
    return

if __name__ == '__main__':
    print "Begining script execution."
    # Command line arguments need to be yml file and stage
    config_file = sys.argv[1]
    stage = sys.argv[2]
    db_file = sys.argv[3]
    main(config_file, stage, db_file)
