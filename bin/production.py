#!/usr/bin/env python

import argparse
import sys

from utils import ProjectHandler

def main():

    parser = argparse.ArgumentParser(description='Python based production system.')
    parser.add_argument('-y', '--yml',
        required=True, help='YML configuration file for the project')
    parser.add_argument('-s', '--stage',
        required=False, help='Which stage to run')
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument('--status',
                         action='store_const',
                         dest='action',
                         const='status',
                         help="Check the status of a stage")
    actions.add_argument('--check',
                         action='store_const',
                         dest='action',
                         const='check',
                         help="Check the outputs of a stage")
    actions.add_argument('--clean',
                         action='store_const',
                         dest='action',
                         const='clean',
                         help="Check the outputs of a stage")
    actions.add_argument('--submit',
                         action='store_const',
                         dest='action',
                         const='submit',
                         help="Submit a stage for processing")
    actions.add_argument('--makeup',
                         action='store_const',
                         dest='action',
                         const='makeup',
                         help="Submit makeup jobs for a stage")
    actions.add_argument('--statistics',
                         action='store_const',
                         dest='action',
                         const='statistics',
                         help="Query database for job statistics")

    args = parser.parse_args()


    # Create a project handler object :
    handler = ProjectHandler(config_file=args.yml, action=args.action, stage=args.stage)

    handler.act()

if __name__ == '__main__':
    main()
