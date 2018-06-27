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
        name = project[0]

        if 'bnb' in name:
            experiment = 'uboone'
        elif 'ubxsec' in name:
            experiment = 'uboone'
        elif 'NEW' in name:
            experiment = 'next'
        elif 'sbnd' in name:
            experiment = 'sbnd'

        if experiment == 'uboone':
            project = 'mcc8'
            subproject = 'none'
            if 'marco' in name:
                subproject = 'ubxsec'

        elif experiment == 'sbnd':
            project = 'dl_samples'
            if 'larsoft' in name:
                subproject = 'larsoft'
            if 'larcv' in name:
                subproject = 'larcv'
            if 'anatree' in name:
                subproject = 'anatree'

        elif experiment == 'next':
            project = 'calibration'
            if 'detsim' in name:
                subproject = 'detsim'
            if 'rwf' in name:
                subproject = 'rwf'
            if 'nexus' in name:
                subproject = 'nexus'
            if 'pmaps' in name:
                subproject = 'pmaps'

        print name
        print '  ' + experiment
        print '  ' + project
        print '  ' + subproject



if __name__ == '__main__':
    main()