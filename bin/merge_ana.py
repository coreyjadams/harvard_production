#!/usr/bin/env python
import os
import sys
import argparse
import subprocess

from database import ProjectReader
from database import DatasetReader

def main():
    splitting = {'dev'   : 1,
                 'train' : 7000,
                 'val'   : 1000,
                 'test'  : -1}
    merge('sbnd_dl_NC_cosmics_larcv',
          '/n/holylfs/LABS/guenette_lab/data/production/testmerge/',
          splitting)
    return

def merge(project, output_directory, file_splitting_dict):

    # Get the list of projects, number of files (ana and non-ana), number of
    # events (ana and non-ana), and disk usage, and parents

    dataset_reader = DatasetReader()

    header = dataset_reader.metadata_header(project)
    print header

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
        files_by_key[current_key].append(name)
        current_total += nevents
        if current_total > file_splitting_dict[current_key]:
            events_by_key[current_key] = current_total
            current_total = 0
            current_key = keys.next()

    print files_by_key
    print events_by_key

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
