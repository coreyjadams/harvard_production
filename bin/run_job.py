#!/usr/bin/env python
import os
import sys

from config import ProjectConfig
from utils import JobRunner
from database import DBUtil

def main(config_file, stage):
    print("Creating Project Config Object")
    project = ProjectConfig(config_file)
    print("Config created, setup larsoft ...")
    project.larsoft().setup_larsoft()
    runner = JobRunner(project = project, stage=project.stage(stage))
    runner.prepare_job()

    job_id = "{0}_{1}".format(os.environ['SLURM_ARRAY_JOB_ID'] + os.environ['SLURM_ARRAY_TASK_ID'])

    runner.run_job(job_id)
    return

if __name__ == '__main__':
    print "Begining script execution."
    # Command line arguments need to be yml file and stage
    config_file = sys.argv[1]
    stage = sys.argv[2]
    main(config_file, stage)
