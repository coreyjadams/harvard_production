#!/usr/bin/env python
import os
import sys

from database import ProjectReader
from database import DatasetReader

from database.connect_db import admin_connection, read_connection

def alter_dataset(dataset):

    # Steps:
    # Create a new column in the table (bigsize)
    # For each entry in the table, add the correct byte size to the big int column (bigsize)
    # Delete the original size column (size)
    # Rename the bigsize colum to size

    table_name = "{0}_metadata".format(dataset)

    # Check the existence of the column:
    bigint_creation_sql = '''
        ALTER TABLE {table}
        ADD bigsize BIGINT NOT NULL;
    '''.format(table=table_name)

    # with admin_connection("/n/home00/cadams/mysqldb") as conn:
    #     conn.execute(bigint_creation_sql)

    # Select all the files and ids:
    selection_sql = '''
        SELECT id,filename
        FROM {table}
        WHERE bigsize=0
    '''.format(table=table_name)

    with read_connection("/n/home00/cadams/mysqldb") as conn:
        conn.execute(selection_sql)
        files = conn.fetchall()

    print files


def main():
    alter_dataset("bnb_plus_cosmics_mcc86_reco2_test")


if __name__ == "__main__":
    main()
