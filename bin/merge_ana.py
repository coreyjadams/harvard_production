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

def merge(project, output_directory, file_splitting_dict):

    # Get the list of projects, number of files (ana and non-ana), number of
    # events (ana and non-ana), and disk usage, and parents

    dataset_reader = DatasetReader()


    # Get all the files in this project:
    def select(self, dataset, select_string='*', limit=None, **kwargs):
    file_list = dataset_reader.select(project, select_string='*', limit=5, type=1)

    print file_list

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
