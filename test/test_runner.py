import os

from config import ProjectConfig
from utils import JobRunner

def main():
    project = ProjectConfig('example_project.yml')
    project.larsoft().setup_larsoft()
    a = JobRunner(stage=project.stage('generation'))
    a.run_job()
    a.check_job()
    # Informational stuff:
    print ""
    return

if __name__ == '__main__':
    main()