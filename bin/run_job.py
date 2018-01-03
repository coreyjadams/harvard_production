#!/usr/bin/env python
import os
import sys

from config import ProjectConfig
from utils import JobRunner
from database import DBUtil

def main(config_file, stage, db_file):
    project = ProjectConfig(config_file)
    project.larsoft().setup_larsoft()
    runner = JobRunner(project = project, stage=project.stage(stage))
    runner.prepare_job()
    runner.run_job()
    if runner.check_job():
        db_util = DBUtil(db_file)
        runner.declare_output(db_util)
    return

if __name__ == '__main__':
    # Command line arguments need to be yml file and stage
    config_file = sys.argv[1]
    stage = sys.argv[2]
    db_file = sys.argv[3]
    main(config_file, stage, db_file)