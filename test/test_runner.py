import os

from config import ProjectConfig
from utils import JobRunner
from database import DBUtil

def main():
    project = ProjectConfig('example_project.yml')
    project.larsoft().setup_larsoft()
    db_file = project['top_dir'] +'/work/' + project['name'] + '.db'
    a = JobRunner(project = project, stage=project.stage('generation'), db_util = DBUtil(db_file))
    a.prepare_job()
    a.run_job()
    return

if __name__ == '__main__':
    main()