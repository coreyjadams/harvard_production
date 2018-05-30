#!/usr/bin/env python
import os
import sys

from config import ProjectConfig
from utils  import RunnerTypes

def main(config_file, stage):
    print("Creating Project Config Object")
    project = ProjectConfig(config_file)
    print("Config created, setup software ...")
    project.software().setup()

    runner_class = RunnerTypes()[project.software()['type']]
    runner = runner_class(project = project, stage=project.stage(stage))
    print("Preparing job ...")
    runner.prepare_job()

    job_id = "{0}_{1}".format(os.environ['SLURM_ARRAY_JOB_ID'], os.environ['SLURM_ARRAY_TASK_ID'])
    print("Job ID is {0}".format(job_id))
    print("Running job ...")
    runner.run_job(job_id)

    job_array = int(os.environ['SLURM_ARRAY_JOB_ID'])
    runner.finish_job(job_array)
    return

if __name__ == '__main__':
    print "Begining script execution."
    # Command line arguments need to be yml file and stage
    config_file = sys.argv[1]
    stage = sys.argv[2]
    main(config_file, stage)
