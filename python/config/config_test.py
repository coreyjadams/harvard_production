import subprocess
import os

from ProjectConfig import ProjectConfig

if __name__ == "__main__":
    proj = ProjectConfig('example_project.yml')

    # Set up larsoft:
    proj.larsoft().setup_larsoft()

    # Run a test job:
    subprocess.call(['lar', '-c', 'prodsingle_sbnd.fcl'], env=dict(os.environ))