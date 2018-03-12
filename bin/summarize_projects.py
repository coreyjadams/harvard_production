#!/usr/bin/env python
import os
import sys

import html

from database import ProjectReader
from database import DatasetReader

def main():

    # Get the list of projects, number of files (ana and non-ana), number of
    # events (ana and non-ana), and disk usage, and parents

    dataset_reader = DatasetReader()
    project_reader = ProjectReader()

    # projects = project_reader.list_datasets()
    projects = ("test1", "test2")
    h = html.HTML()

    table = h.table(border='1')
    header = table.tr
    header.th("Project")
    header.th("ID")
    header.th("File Count")
    header.th("File Count (Ana)")
    header.th("Event Count")
    header.th("Event Count (Ana)")
    header.th("Disk Usage")
    header.th("Disk Usage (Ana)")
    header.th("Parents")

    for project in projects:
        row = table.tr

        row.td("{0}".format(project))

        project_id      = project_reader.dataset_ids(project)
        row.td("{0}".format(project_id))

        file_count      = dataset_reader.count_files(dataset=project, type=0)
        row.td("{0}".format(file_count))

        file_count_ana  = dataset_reader.count_files(dataset=project, type=1)
        row.td("{0}".format(file_count_ana))

        event_count     = dataset_reader.sum(dataset=project,target='nevents',type=0)
        row.td("{0}".format(event_count))

        event_count_ana = dataset_reader.sum(dataset=project,target='nevents',type=1)
        row.td("{0}".format(event_count_ana))

        disk_usage      = dataset_reader.sum(dataset=project,target='size',type=0)
        row.td("{0}".format(disk_usage))

        disk_usage_ana  = dataset_reader.sum(dataset=project,target='size',type=1)
        row.td("{0}".format(disk_usage_ana))

        parents         = project_reader.direct_parents(dataset_id=project_id)
        row.td("{0}".format(parents))

    with open("harvard_projects_summary.html", "w") as html_file:
        html_file.write(str(h))


if __name__ == '__main__':
    main()
