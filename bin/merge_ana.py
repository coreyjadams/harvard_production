#!/usr/bin/env python
import os
import sys
import argparse
import subprocess
import json
import time
from collections import OrderedDict

from database import ProjectReader
from database import DatasetReader

def main():


    parser = argparse.ArgumentParser(description='Analysis file merger')
    parser.add_argument('-p', '--project',
        required=True, help='Project to merge analysis files for')
    parser.add_argument('-o', '--output',
        required=True, help='Top Level output directory for this merging.')
    parser.add_argument('-s', '--split',
        required=False, help="Dictionary format for how to split the data, order matters.",
        type=str)
    parser.add_argument('-t','--type',required=False,
        default=1,type=int,help="Type of file to merge (0=output, 1=ana) (default:1)")
    parser.add_argument('--script', required=False,
        help="Optional script to run merging with, defaults to \'hadd\'",
        type=str, default='hadd')


    args = parser.parse_args()
    if args.split is not None:
        split = json.loads(args.split, object_pairs_hook = OrderedDict)
    else:
        split = {'merge' : -1}

    if args.type not in [0, 1]:
        raise Exception("Type {} not supported, please use 0 (output) or 1 (ana)".format(args.type))

    if not os.path.isdir(args.output):
        try:
            os.makedirs(args.output)
        except Exception as e:
            print "Could not make output directory"
            raise e

    # splitting = {'dev'   : 1,
    #              'train' : 7000,
    #              'val'   : 1000,
    #              'test'  : -1b
    merge(args.project,
          output_directory=args.output,
          file_splitting_dict=split,
          _type=args.type,
          script=args.script)

    return

def merge(project, output_directory, file_splitting_dict, script='hadd', _type=1):

    # Get the list of projects, number of files (ana and non-ana), number of
    # events (ana and non-ana), and disk usage, and parents

    dataset_reader = DatasetReader()

    # Get all the files in this project:
    file_list = dataset_reader.select(project, select_string='filename, nevents', limit=None, type=_type)

    print('Total number of events to merge: {0}'.format(
        dataset_reader.sum(
            dataset=project,
            target='nevents',
            type=_type)))

    keys = iter(file_splitting_dict.keys())
    files_by_key = dict()
    events_by_key = dict()

    current_key = keys.next()
    current_total = 0

    for name, nevents in file_list:
        if current_key not in files_by_key:
            files_by_key.update({current_key : []})
            events_by_key[current_key] = 0
        files_by_key[current_key].append(name)
        events_by_key[current_key] += nevents
        current_total += nevents
        if current_total > file_splitting_dict[current_key] and file_splitting_dict[current_key] != -1:
            events_by_key[current_key] = current_total
            current_total = 0
            current_key = keys.next()

    for key in file_splitting_dict.keys():
        if key not in files_by_key:
            raise Exception("Key {0} not found in the output key list.  Please adjust the event counts and try again.".format(key))


    for key in files_by_key.keys():
        command = []
        command.append(script)
        file_extension = os.path.splitext(files_by_key[key][0])[1]
        output_file_name = "{0}_{1}{2}".format(project,key,file_extension)
        command.append(output_file_name)
        for _file in files_by_key[key]:
            command.append(_file)

        print('Work dir: {0}'.format(output_directory))
        print('Output file: {0}'.format(output_file_name))
        proc = subprocess.Popen(command,
                                cwd = output_directory,
                                stdout = subprocess.PIPE,
                                stderr = subprocess.PIPE,
                                env=os.environ)

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
            raise Exception("Script ended with return code {0}".format(return_code))



if __name__ == '__main__':
    main()
