#!/usr/bin/env python

from MySQLdb import Error

from connect_db import admin_connection

# This script will access the database and create the tables
#  - dataset_master_index
#  - dataset_master_consumption
# See the scheme.md file for more information

def initialize_master_tables(password_file):

    dataset_master_index_sql = """
        CREATE TABLE IF NOT EXISTS dataset_master_index (
            id          INTEGER     NOT NULL AUTO_INCREMENT,
            dataset     VARCHAR(50) NOT NULL UNIQUE,
            created     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        ); """

    dataset_master_consumption_sql = """
        CREATE TABLE IF NOT EXISTS dataset_master_consumption (
            id     INTEGER  NOT NULL AUTO_INCREMENT,
            input  INTEGER  NOT NULL,
            output INTEGER  NOT NULL,
            FOREIGN KEY(input)  REFERENCES dataset_master_index(id) ON UPDATE CASCADE,
            FOREIGN KEY(output) REFERENCES dataset_master_index(id) ON UPDATE CASCADE,
            PRIMARY KEY (id)
        ); """

    dataset_master_metadata_sql = """
        CREATE TABLE IF NOT EXISTS dataset_master_metadata (
            id          INTEGER  NOT NULL AUTO_INCREMENT,
            experiment  VARCHAR(50),
            project     VARCHAR(50),
            subproject  VARCHAR(50),
            FOREIGN KEY (id) REFERENCES dataset_master_index(id) ON UPDATE CASCADE,
            PRIMARY KEY (id)
        ); """

    with admin_connection(password_file) as conn:
        # try:
        #     conn.execute(dataset_master_index_sql)
        # except Error as e:
        #     print e
        #     print "Could not create master index table"
        # try:
        #     conn.execute(dataset_master_consumption_sql)
        # except Error as e:
        #     print "Could not create master consumption table"
        try:
            conn.execute(dataset_master_metadata_sql)
        except Error as e:
            print e
            print "Could not create master metadata table"

    print "Initialization complete."

if __name__ == "__main__":
    initialize_master_tables('/n/home00/cadams/mysqldb')
