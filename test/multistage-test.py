import os

import yaml

from config import ProjectConfig
from utils import ProjectHandler
from database import DBUtil

# This script tests the multistage input/output flow.
# It generates fake data (empty files) based on the configuration

def part1():
    # parse the configuration file:
    config_file = '/home/cadams/Harvard-Production/yml-configs/bnb_plus_cosmics-multistage-test.yml'
    config = ProjectConfig(config_file)

    # Print out the stages to run over:
    for name, stage in config.stages.iteritems():
        # Create a handler to check files and such
        handler = ProjectHandler(config_file, action='status', stage=name)
        handler.build_directory()
        handler.first_stage_check()
        db = handler.project_db

        print('Faking stage {0}'.format(stage.name))
        print('  Input specified? {0}'.format( stage.has_input()))

        #Make sure output directory exists:
        out_dir = stage.output_directory()
        handler.make_directory(out_dir)

        # Generate fake output for this stage:
        for i in range(stage.n_jobs()):
            for fcl in stage.fcl():
                fcl = os.path.basename(fcl)
                _f  = '/{0}_fake_{1}.root'.format(fcl, i)

            # Input
            if stage.has_input():
                input_files, locations = stage.get_next_files(1, db)

                print('Input files are: \n  {0}'.format(input_files))

            # Processing
            # Nothing actually happens
            # Output

            with open(out_dir + _f, 'w') as file:
                pass

            # Declare the file to the database:
            db.declare_file(filename=_f,
                            dataset=stage.output_dataset(),
                            location=out_dir,
                            stage=name,
                            status=0,
                            nevents=10,
                            ftype=0)

            # Mark the input files as consumed:
            if stage.has_input():
                stage.finalize(input_files, db)

def part2():


    # parse the configuration file:
    config_file = '/home/cadams/Harvard-Production/yml-configs/bnb_plus_cosmics-multistage-test_part2.yml'
    config = ProjectConfig(config_file)


    # Print out the stages to run over:
    for name, stage in config.stages.iteritems():
        handler = ProjectHandler(config_file, action='status', stage=name)
        handler.build_directory()
        handler.first_stage_check()

        db = handler.project_db

        print('Faking stage {0}'.format(stage.name))
        print('  Input specified? {0}'.format( stage.has_input()))

        #Make sure output directory exists:
        out_dir = stage.output_directory()
        handler.make_directory(out_dir)

        # Generate fake output for this stage:
        for i in range(stage.n_jobs()):
            for fcl in stage.fcl():
                fcl = os.path.basename(fcl)
                _f  = '/{0}_fake_{1}.root'.format(fcl, i)

            # Input
            if stage.has_input():
                input_files, locations = stage.get_next_files(1, db)

            print input_files

            # Processing
            # Nothing actually happens
            # Output

            with open(out_dir + _f, 'w') as file:
                pass

            # Declare the file to the database:
            db.declare_file(filename=_f,
                            dataset=stage.output_dataset(),
                            location=out_dir,
                            stage=name,
                            status=0,
                            nevents=10,
                            ftype=0)

            # Mark the input files as consumed:
            if stage.has_input():
                stage.finalize(input_files, db)



def main():
    part1()
    part2()

if __name__ == '__main__':
    main()