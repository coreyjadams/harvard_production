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
   - filename (full path, must be unique)
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
 - dataset name
 - parent project
