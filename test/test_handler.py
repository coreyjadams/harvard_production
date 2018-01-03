import os

from utils import ProjectHandler

def main():
    a = ProjectHandler(config='/home/cadams/harvard_production/example_project.yml', stage='generation', action='submit')
    a.act()
    return

if __name__ == '__main__':
    main()