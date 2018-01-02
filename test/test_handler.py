import os

from utils import JobHandler

def main():
    a = JobHandler(config='example_project.yml', stage='generation', action='submit')
    a.act()
    return

if __name__ == '__main__':
    main()