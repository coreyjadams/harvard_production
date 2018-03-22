#!/usr/bin/env python
import os
import sys
import argparse
import subprocess
import json
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
    parser.add_argument('--script', required=False,
        help="Optional script to run merging with, defaults to \'hadd\'",
        type=str, default='hadd')


    args = parser.parse_args()
    print args
    if args.split is not None:
        split = json.loads(args.split, object_pairs_hook = OrderedDict)
    else:
        split = {'merge' : -1}

    try:
        os.makedirs(args.output)
    except Exception as e:
        print "Could not make output directory"
        raise e

    # splitting = {'dev'   : 1,
    #              'train' : 7000,
    #              'val'   : 1000,
    #              'test'  : -1}
    merge(args.project,
          output_directory=args.output,
          file_splitting_dict=split,
          script=args.script)

    return

def merge(project, output_directory, file_splitting_dict, script='hadd'):

    # Get the list of projects, number of files (ana and non-ana), number of
    # events (ana and non-ana), and disk usage, and parents

    dataset_reader = DatasetReader()

    # Get all the files in this project:
    file_list = dataset_reader.select(project, select_string='filename, nevents', limit=None, type=1)

    print dataset_reader.sum(
            dataset=project,
            target='nevents',
            type=1)

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
        file_extension = os.path.splittext(files_by_key[key][0])[1]
        output_file_name = "{0}_{1}{2}".format(project,key,file_extension)
        command.append(output_file_name)
        for _file in files_by_key[key]:
            command.appent(_file)

        print "Work dir: " + output_directory
        print command

        # proc = subprocess.Popen(command,
        #                         cwd = output_directory,
        #                         stdout = subprocess.PIPE,
        #                         stderr = subprocess.PIPE,
        #                         env=os.environ)

        # retval=proc.poll()
        # # the loop executes to wait till the command finish running
        # stdout=''
        # stderr=''
        # while retval is None:
        #     time.sleep(1.0)
        #     # while waiting, fetch stdout (including STDERR) to avoid crogging the pipe
        #     for line in iter(proc.stdout.readline, b''):
        #         stdout += line
        #     for line in iter(proc.stderr.readline, b''):
        #         stderr += line
        #     # update the return value
        #     retval = proc.poll()


        # return_code = proc.returncode

    # Pick files and events to go into each list

    # for project in projects:
    # project = project[0]
    # print project
    #     row = table.tr

    #     row.td("{0}".format(project))

    #     project_id      = project_reader.dataset_ids(project)
    #     row.td("{0}".format(project_id))

    #     file_count      = dataset_reader.count_files(dataset=project, type=0)
    #     row.td("{0}".format(file_count))

    #     file_count_ana  = dataset_reader.count_files(dataset=project, type=1)
    #     row.td("{0}".format(file_count_ana))

    #     event_count     = dataset_reader.sum(dataset=project,target='nevents',type=0)
    #     row.td("{0}".format(event_count))

    #     event_count_ana = dataset_reader.sum(dataset=project,target='nevents',type=1)
    #     row.td("{0}".format(event_count_ana))

    #     disk_usage      = dataset_reader.sum(dataset=project,target='size',type=0)
    #     row.td("{0}".format(bytes_2_human_readable(disk_usage)))

    #     disk_usage_ana  = dataset_reader.sum(dataset=project,target='size',type=1)
    #     row.td("{0}".format(bytes_2_human_readable(disk_usage_ana)))

    #     parents         = project_reader.direct_parents(dataset_id=project_id)
    #     row.td("{0}".format(parents))

    # with open("harvard_projects_summary.html", "w") as html_file:
    #     html_file.write(str(h))


if __name__ == '__main__':
    main()
