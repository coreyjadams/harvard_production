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

        experiment = None
        project = None
        subproject = None
        _slice = None

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
            _slice = 'none'
            if 'marco' in name:
                subproject = 'ubxsec'

        elif experiment == 'sbnd':
            project = 'dl_samples'
            if 'larsoft' in name:
                subproject = 'larsoft'
            elif 'larcv' in name:
                subproject = 'larcv'
            elif 'anatree' in name:
                subproject = 'anatree'

            if 'nueCC_cosmics' in name:
              _slice = 'nueCC_cosmics'
            elif 'nueCC' in name:
              _slice = 'nueCC'
            elif 'numuCC_cosmics' in name:
              _slice = 'numuCC_cosmics'
            elif 'numuCC' in name:
              _slice = 'numuCC'
            elif 'NC_cosmics' in name:
              _slice = 'NC_cosmics'
            elif 'NC' in name:
              _slice = 'NC'
            elif 'cosmics' in name:
              _slice = 'cosmics'


        elif experiment == 'next':
            project = 'calibration'
            if 'detsim' in name:
                subproject = 'detsim'
            elif 'rwf' in name:
                subproject = 'rwf'
            elif 'nexus' in name:
                subproject = 'nexus'
            elif 'pmaps' in name:
                subproject = 'pmaps'

            if 'Cs' in name:
              _slice = 'cs'
            elif 'Tl' in name:
              _slice = 'tl'


        print name
        print '  ' + experiment
        print '  ' + project
        print '  ' + subproject
        print '  ' + _slice



if __name__ == '__main__':
    main()