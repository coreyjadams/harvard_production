import os

import yaml

from config import ProjectConfig
from utils import ProjectHandler
from database import DBUtil

# This script tests the multistage input/output flow.
# It generates fake data (empty files) based on the configuration

def main():
    # parse the configuration file:
    config_file = '/home/cadams/Harvard-Production/yml-configs/bnb_plus_cosmics-multistage-test.yml'
    config = ProjectConfig(config_file)

    # Print out the stages to run over:
    for name, stage in config.stages.iteritems():
        handler = ProjectHandler(config_file, action='status', stage=name)
        handler.build_directory()
        print stage
        print stage.has_input()

        #Make sure output directory exists:
        out_dir = stage.output_directory()
        handler.make_directory(out_dir)

        # Generate fake output for this stage:
        for i in range(stage.n_jobs()):
            for fcl in stage.fcl():
                fcl = os.path.basename(fcl)
                _f = out_dir + '/{0}_fake_{1}.root'.format(fcl, i)
            with open(_f, 'w') as file:
                pass
            # Declare the file to the database:


            break


if __name__ == '__main__':
    main()