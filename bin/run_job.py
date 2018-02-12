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
    runner.run_job()
    return

if __name__ == '__main__':
    print "Begining script execution."
    # Command line arguments need to be yml file and stage
    config_file = sys.argv[1]
    stage = sys.argv[2]
    main(config_file, stage)
