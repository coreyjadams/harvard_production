# Schema for database organization for this production software

## Some basics

The production model generally runs in stages: many identical jobs run in a stage, and either they take no input, or the all take similar input from a previous stage or project.  The model here is that each stage of a project is a **dataset** which gets stored in the database.

### Dataset Tables

Therefore, a dataset has several tables associated directly with it:
 - dataset_metadata
 - dataset_search

The search table is minimal and contains the following information only:
 - primary key (**unique across the entire database**)
 - Secondary keys:
   - filename (full path, must be **unique**)
   - run identification number

The metadata table has fuller information about the dataset, and contains the following columns:
 - primary key (**unique across the entire database**)
 - Secondary keys:
   - filename (full path, must be unique)
   - run identification number
 - file type (output, analysis, etc)
 - number of events
 - size (GB)


### Dataset Index

There is a table for managing the datasets themselves.  This has one entry per dataset created, which can only be created interactively.  When a dataset is created in this table, it is also needed to create the two dataset tables above (search and metadata).

Contents of the dataset index:
 - primary key (**unique across the entire database**)
 - dataset name (**unique**)
 - whether this project has a parent or not
 - whether this project has a daughter or not
 - creation timestamp
 - last updated timestamp

### Dataset Consumption

Necessarily, there is also a dataset_hierarchy table.  This table has one entry per transaction, such as consuming a dataset for input into another project.  For one-to-one consumption (one dataset is input to another dataset) there is only one entry in this table, however many-to-one consumption is allowed.  Therefore, one output dataset can have as input N datasets, which will correspond to N entries in this table.  The structure of the table is:
- primary key
- input dataset key
- output dataset key

From this table, the full family tree of any given dataset can be reconstructed.

### File Consumption

When one (or more) dataset is input to another dataset, the input files need to be managed to ensure each file is delivered correctly.  Many-to-many consumption is not allowed, for neither datasets nor files.  When one project consumes other projects, several things happen:
 1. The dataset index table is updated to mark the input projects as having daughters
 2. The dataset index table is updated to add the new project, and indicate it has a parent
 3. The dataset consumption table is updated to mark the parentage of input and output projects
 4. A new table, \[input_project_name\]_consumption is created, which manages the delivery of files to the new project from the old project(s).
 
 The consumption table is created for a particular project, and the first step is to populate the table with all possible input files.  This step defines the table columns to be:
 - input file primary key
 - input file's project primary key
 - input file location
 - flag marking consumption status of this file (0 = not consumed, 1 = yielded for consumption, 2 = confirmed consumption)
 - output file's primary key
 
If the file consumption pattern is many-to-one, each input file will have a row in this table.

# Project Flow
In general, the creation of a new project (with the --submit command)  will do the following things:
 1. Update the dataset table
 2. Update the consumption table, if necessary
 3. Create the dataset_search and dataset_metadata tables
 4. If necessary, create and initialize the dataset_consumption table.
 
As a project runs, each worker node will need to insert or update the dataset_search, dataset_metadata, and dataset_consumption tables.  Updating the dataset_search and dataset_metadata tables is done for every project, always, and updating the consumption table only occurs for projects consuming another project.

# Utilities

This set of python scripts provides utilities to interact with and read the dataset.  There are several important classes:

## ProjectUtils.py
This class manages the master project table.  It does not run from worker nodes.  This class can create new datasets, mark one dataset as consuming other(s), and delete datasets (including their individual search, metadata, and consumption tables).

## DatasetUtils.py
This class manages individual datasets but can not delete the datasets from the database itself.  It can delete entries from tables, reset or reinitialize a consumption table, or clear all entries from a dataset.  **Deleting entries from a dataset does not delete the specified files from disk.**

## ProjectReader.py
This class offers a read-only view of datasets.  It can list available datasets, dataset heirachy and show creation/update times.

## DatasetReader.py
This class offers a read-only view of datasets.  It can list files from a dataset (based on filetype, consumtion status, etc), or show a file's family tree,  etc.
 
