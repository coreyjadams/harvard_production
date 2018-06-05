# Schema for database organization for this production software

## Some basics

The production model generally runs in stages: many identical jobs run in a stage, and either they take no input, or the all take similar input from a previous stage or project.  The model here is that each stage of a project is a **dataset** which gets stored in the database.

Additionally, for datasets requiring multiple jobs (which includes makeup jobs, and therefore is the default) a table is created to keep track of the jobid of running jobs.  This is handled with a "campaign" table for each dataset, and therefore includes a CampaignUtils and CampaignReader class.

### Dataset Tables

Therefore, a dataset has a table associated directly with it:
 - dataset_metadata

The table contains the following information:
 - primary key (**unique**) (is a foreign key for metadata ID)
 - run identification number
 - filename (full path, must be unique)
 - file type (output=0, analysis=1, etc)
 - number of events
 - creation time
 - creation JOB ID
 - size (GB) of output file

### Dataset Job Accounting

A dataset has a table associated with it:
 - dataset_job_accounting

This table has the following information stored:
 - Primary job id (array id) of every job submitted to produce files into this dataset
 - Number of jobs sumbitted for this job id
 - Number of successfully completed jobs for this job id
 - Number of failed jobs for this job id?
 - Number of running or pending jobs for this job id
 - Number of jobs with unknown status for this job id
 - Work directory for the jobs running in this job

When the "check" command is run, the campaign table is updated.  When the makeup command is issued, new entries are added to the table.

### Dataset Index

There is a table for managing the datasets themselves.  This has one entry per dataset created, which can only be created interactively.  When a dataset is created in this table, it is also needed to create the two dataset tables above (search and metadata).

Table name is dataset_master_index

Contents of the dataset index:
 - primary key (**unique**)
 - dataset name (secondary key) (**unique**)
 - creation timestamp
 - last updated timestamp

### Dataset Consumption

Table name is dataset_master_consumption

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
 4. A new table, `[input_project_name`]\_consumption is created, which manages the delivery of files to the new project from the old project(s).

The consumption table is created for a particular project, and the first step is to populate the table with all possible input files.  This step defines the table columns to be:
 - index (primary key)
 - input file primary key (foreign key)
 - input file's project primary key (foreign key)
 - flag marking consumption status of this file (0 = not consumed, 1 = yielded for consumption, 2 = confirmed consumption)
 - output file's primary key (foreign key)

The input file location is notably missing here.  Since the location is already stored above and is a long 500 character field, it's not duplicated.  The output file's project's primary key is not included since that relationship is one-to-one.

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
This class manages the master project table.  It does not run from worker nodes.  This class can create new datasets, mark one dataset as consuming other(s), and delete datasets (including their individual search, metadata, and consumption tables).  This class initializes consumption tables.

Available utilities (see class for more details):
 - create dataset (initiliazes dataset_metadata and, if parents != None, consumption table)
    - Implemented, tested
 - delete dataset (includes consumtion, not recursive, daughter datasets are orphaned)
 - ?

## DatasetUtils.py
This class manages individual datasets but can not delete the datasets from the database itself.  It can delete entries from tables, reset or reinitialize a consumption table (mark all failed files as not consumed), or clear all entries from a dataset.  **Deleting entries from a dataset does not delete the specified files from disk.**

Available utilities (see class for more details):
 - declare file (if parents != None, updates consumption table)
    - Implemented, tested
 - delete a file
    - Implemented, tested
 - yield files for consumption (modifies consumption table)
    - implemented, tested
 - finalize consumption of files
    - implemented, tested
 - Add a job ID to the campaign table
    - implemented, untested
 - Update a job ID to the campaign table
    - unimplemented
 - declare a job completed
    - implemented, untested

## ProjectReader.py
This class offers a read-only view of datasets.  It can list available datasets, dataset heirachy and show creation/update times.

Available utilities (see class for more details):
 - list all datasets
 - get ids of datasets from names
    - Implemented, positive tested
 - list parentage of dataset
    - Implemented, untested
 - list daughters of dataset (direct daughters only)
    - Implemented, untested



## DatasetReader.py
This class offers a read-only view of datasets.  It can list files from a dataset (based on filetype, consumtion status, etc), or show a file's family tree,  etc.

Available utilities (see class for more details):
 - get file ids from names
    - Implemented, tested
 - general function to unpack args into a WHERE clause
    - implemented, tested
 - COUNT number of files in dataset
    - implemented, tested
    - is an arbitrary count function
 - SUM number of events in dataset
    - implemented, tested
    - is an arbitrary sum function, used below too
 - SUM total disk usage by dataset
    - implemeneted as above
 - Get the number of jobs run for this dataset so far.
    - unimplemented
