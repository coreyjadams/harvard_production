#!/usr/bin/env python

import sys
import os


from database import ProjectUtils, DatasetUtils
from database import ProjectReader

def main():

    proj_utils  = ProjectUtils()
    ds_utils    = DatasetUtils()

    projects = proj_utils.list_datasets()

    for project in projects:
        print project



if __name__ == '__main__':
    main()