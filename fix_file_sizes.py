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

    # Check the existence of the column:
    bigint_creation_sql = '''
        ALTER TABLE {table}
        ADD bigsize BIGINT NOT NULL;
    '''.format(table=table_name)

    with admin_connection("/n/home00/cadams/mysqldb") as conn:
        conn.execute(bigint_creation_sql)

    # Select all the files and ids:
    selection_sql = '''
        SELECT id,filename
        FROM {table}
        WHERE bigsize=0
    '''.format(table=table_name)

    with read_connection("/n/home00/cadams/mysqldb") as conn:
        conn.execute(selection_sql)
        files = conn.fetchall()

    size_update_sql = '''
        UPDATE {table}
        SET bigsize=%s
        WHERE id=%s
    '''.format(table=table_name)

    with write_connection("/n/home00/cadams/mysqldb") as conn:
        for _id, _file in files:
            # Get the correct file size:
            size = os.path.getsize(_file)
            tup = (size, _id)
            conn.execute(size_update_sql, tup)


    # Delete the column 'size':
    deletion_sql = '''
        ALTER TABLE {table}
        DROP COLUMN size
    '''.format(table=table_name)

    # Rename bigsize into size:
    rename_sql = '''
        ALTER TABLE {table}
        CHANGE bigsize size BIGINT
    '''.format(table=table_name)

    with admin_connection("/n/home00/cadams/mysqldb") as conn:
        conn.execute(deletion_sql)
        conn.execute(rename_sql)


def main():
    project_reader = ProjectReader()

    projects = project_reader.list_datasets()
    for project in projects:
        if project != "bnb_plus_cosmics_mcc86_reco2":
            print project
            # alter_dataset(project)


if __name__ == "__main__":
    main()
