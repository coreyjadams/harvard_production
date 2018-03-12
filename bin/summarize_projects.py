import os
import sys

from utils import ProjectReader
from utils import DatasetReader

def main():

    # Get the list of projects, number of files (ana and non-ana), number of
    # events (ana and non-ana), and disk usage, and parents

    dataset_reader = DatasetReader()
    project_reader = ProjectReader()

    projects = project_reader.list_datasets()
    print projects

    pass

if __name__ == '__main__':
    main()