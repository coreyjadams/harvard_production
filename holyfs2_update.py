#!/usr/bin/env python
import os
import sys

from database import ProjectReader
from database import DatasetReader

from database.connect_db import admin_connection, read_connection, write_connection

def alter_dataset(dataset):

    # Steps:
    # Create a new column in the table (bigsize)
    # For each entry in the table, add the correct byte size to the big int column (bigsize)
    # Delete the original size column (size)
    # Rename the bigsize colum to size

    print "Updating dataset {0}".format(dataset)

    table_name = "{0}_metadata".format(dataset)

    location_update_sql = '''
        UPDATE {table}
        SET filename = REPLACE(filename, 'holylfs', 'holylfs02')
    '''.format(table_name)

    with admin_connection("/n/home00/cadams/mysqldb") as conn:
        conn.execute(location_update_sql)


def main():
    project_reader = ProjectReader()

    projects = project_reader.list_datasets()
    for project in projects:
        project = project[0]
        print project
        if project == "sbnd_dl_numuCC_larcv":
            print project
            alter_dataset(project)


if __name__ == "__main__":
    main()
