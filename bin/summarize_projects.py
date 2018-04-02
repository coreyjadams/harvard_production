#!/usr/bin/env python
import os
import sys

import html

from database import ProjectReader
from database import DatasetReader

def bytes_2_human_readable(number_of_bytes):
    if number_of_bytes is None:
        return "0 B"
    if number_of_bytes < 0:
        raise ValueError("!!! number_of_bytes can't be smaller than 0 !!!")

    step_to_greater_unit = 1024.

    number_of_bytes = float(number_of_bytes)
    unit = 'bytes'

    if (number_of_bytes / step_to_greater_unit) >= 1:
        number_of_bytes /= step_to_greater_unit
        unit = 'KB'

    if (number_of_bytes / step_to_greater_unit) >= 1:
        number_of_bytes /= step_to_greater_unit
        unit = 'MB'

    if (number_of_bytes / step_to_greater_unit) >= 1:
        number_of_bytes /= step_to_greater_unit
        unit = 'GB'

    if (number_of_bytes / step_to_greater_unit) >= 1:
        number_of_bytes /= step_to_greater_unit
        unit = 'TB'

    precision = 1
    number_of_bytes = round(number_of_bytes, precision)

    return str(number_of_bytes) + ' ' + unit

def main():

    # Get the list of projects, number of files (ana and non-ana), number of
    # events (ana and non-ana), and disk usage, and parents

    dataset_reader = DatasetReader()
    project_reader = ProjectReader()

    projects = project_reader.list_datasets()
    # projects = ("test1", "test2")
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

    total_file_count      = 0
    total_file_count_ana  = 0
    total_event_count     = 0
    total_event_count_ana = 0
    total_disk_usage      = 0
    total_disk_usage_ana  = 0

    for project in projects:
	project = project[0]
	print project
        row = table.tr

        row.td("{0}".format(project))

        project_id      = project_reader.dataset_ids(project)
        row.td("{0}".format(project_id))

        file_count      = dataset_reader.count_files(dataset=project, type=0)
        row.td("{0}".format(file_count))
        if file_count is not None:
            total_file_count += file_count

        file_count_ana  = dataset_reader.count_files(dataset=project, type=1)
        row.td("{0}".format(file_count_ana))
        if file_count_ana is not None:
            total_file_count_ana += file_count_ana

        event_count     = dataset_reader.sum(dataset=project,target='nevents',type=0)
        row.td("{0}".format(event_count))
        if event_count is not None:
            total_event_count += event_count

        event_count_ana = dataset_reader.sum(dataset=project,target='nevents',type=1)
        row.td("{0}".format(event_count_ana))
        if event_count_ana is not None:
            total_event_count_ana += event_count_ana

        disk_usage      = dataset_reader.sum(dataset=project,target='size',type=0)
        row.td("{0}".format(bytes_2_human_readable(disk_usage)))
        if disk_usage is not None:
            total_disk_usage += disk_usage

        disk_usage_ana  = dataset_reader.sum(dataset=project,target='size',type=1)
        row.td("{0}".format(bytes_2_human_readable(disk_usage_ana)))
        if disk_usage_ana is not None:
            total_disk_usage_ana += disk_usage_ana

        parents         = project_reader.direct_parents(dataset_id=project_id)
        row.td("{0}".format(parents))

    row = table.tr(style="font-weight:bold")
    row.td("Total:")
    row.td("-")
    row.td("{0}".format(total_file_count))
    row.td("{0}".format(total_file_count_ana))
    row.td("{0}".format(total_event_count))
    row.td("{0}".format(total_event_count_ana))
    row.td("{0}".format(bytes_2_human_readable(total_disk_usage)))
    row.td("{0}".format(bytes_2_human_readable(total_disk_usage_ana)))
    row.td("-")

    with open("harvard_projects_summary.html", "w") as html_file:
        html_file.write(str(h))


if __name__ == '__main__':
    main()
