import os, sys
import subprocess
import time
import sqlite3
import glob


from database import DatasetReader, ProjectReader
from config import ProjectConfig

from utils import ProjectHandler

# This script checks the production of each set of files.  If it is finished,
# It calculates the passing rate of events and stores other useful information

# If it is not finished, it submits the jobs.


def move_files_to_neutrino(yml_name, stage):

    pr = ProjectReader()
    dr = DatasetReader()

    remote_host = 'cadams@neutrinos1.ific.uv.es'
    local_top_directory  = '/n/holylfs02/LABS/guenette_lab/data/NEXT/NEXTNEW/MC/OtherForTransfer/'
    remote_top_directory = '/lustre/neu/data4/NEXT/NEXTNEW/MC/Other/NEXUS_NEXT_v1_03_01/'


    # We want to copy, for every project here (76 + Xenon, eventually)
    # The files, configurations, and logs.

    # The remote directory structure should be:
    # remote_top_directory/nexus/{element}/{region}/output
    # remote_top_directory/nexus/{element}/{region}/log
    # remote_top_directory/nexus/{element}/{region}/config

    # The config files, logs, and output all live in the same directory.  So, what this script does
    # is to generate a list files to/from for transfering.  It creates symbolic links to
    # the local files in the right directory structure as needed on neutrinos.

    # The way this is done is to generate a file that will be used for creating symlinks
    # A process is spawned to make the links
    # Finally, a job is submitted to do the rsync command.

    with open('transfer_protocol.txt', 'w') as _trnsf:

        # Read in the yml file:
        pc = ProjectConfig(yml_name)
        stage = pc.stage(stage)
        dataset = stage.output_dataset()
        output_dir = stage.output_directory()

        core_config = stage.config()

        destination = "{top}/hdst/config/core_config".format(
            top     = local_top_directory,
        )
        trnsf_str = "{}\t{}\n".format(core_config, destination)


        # Get the log files, config files, and output files
        command_match = '/*command.txt'

        log_match = '/*.log'

        output_file_list = dr.list_file_locations(dataset)

        for _file in output_file_list:
            _file = _file[0]
            base = os.path.basename(_file)
            destination = "{top}/hdst/output/{base}".format(
                top     = local_top_directory,
                base    = base
            )

            # find the file index:
            index = (base.split('_')[3])

            trnsf_str = "{}\t{}\n".format(_file, destination)
            _trnsf.write(trnsf_str)

            directory = os.path.dirname(_file)

            # Get the command files:
            cmd = glob.glob(directory + command_match)[0]
            base = os.path.basename(cmd)
            base = base.replace('.txt', '_{}.txt'.format(index))
            destination = "{top}/hdst/config/{base}".format(
                top     = local_top_directory,
                base    = base
            )
            trnsf_str = "{}\t{}\n".format(cmd, destination)
            _trnsf.write(trnsf_str)


            # Get the log files:
            logs = glob.glob(directory + log_match)
            for log in logs:
                base = os.path.basename(log)
                base = base.replace('.log', '_{}.log'.format(index))
                destination = "{top}/hdst/log/{base}".format(
                    top     = local_top_directory,
                    base    = base
                )
                trnsf_str = "{}\t{}\n".format(log, destination)
                _trnsf.write(trnsf_str)

    print "Done making transfer list, creating symbolic links"

    with open('transfer_protocol.txt', 'r') as _trnsf:
        for line in _trnsf.readlines():
            original, destination = line.rstrip('\n').split('\t')

            destdir = os.path.dirname(destination)
            try:
                os.makedirs(destdir)
            except:
                pass
            try:
                os.symlink(original, destination)
            except:
                pass

    print "Beginning file transfer."

    with cd(local_top_directory):

        command = ['rsync', '-rvL', 'nexus', 'cadams@neutrinos1.ific.uv.es:/lustre/neu/data4/NEXT/NEXTNEW/MC/Other/NEXUS_NEXT_v1_03_01/']

        proc = subprocess.Popen(command,
                                stdout = subprocess.PIPE,
                                stderr = subprocess.PIPE,
                                env = dict(os.environ))

        retval=proc.poll()

        # the loop executes to wait till the command finish running
        stdout=''
        stderr=''
        while retval is None:
            time.sleep(1.0)
            # while waiting, fetch stdout (including STDERR) to avoid crogging the pipe
            for line in iter(proc.stdout.readline, b''):
                stdout += line
            for line in iter(proc.stderr.readline, b''):
                stderr += line
            # update the return value
            retval = proc.poll()

        return_code = proc.returncode

        if return_code != 0:
            raise Exception("Failed")

        else:
            print stdout


if __name__ == '__main__':
    move_files_to_neutrino('/n/holylfs02/LABS/guenette_lab/production/yml-configs/next/new_bkg_sim/next_new_hdst.yml', stage='evtmxr')

