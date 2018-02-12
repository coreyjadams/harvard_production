import yaml
import MySQLdb as mysql

def create_connection(host, username, password):
    """ create a database connection to the SQLite database
        specified by db_file
    :return: Connection object or None
    """
    try:
        conn = mysql.connect(host=host,                 # your host, usually db-guenette_neutrinos.rc.fas.harvard.edu
                             user=username,             # your username
                             passwd=password,           # your password
                             db='guenette_neutrinos',   # name of the data base
                             autocommit=False)          # Prevent automatic commits
        return conn
    except mysql.Error as e:
        print(e)

    return None

def read_connection(password_file):
    host = 'db-guenette_neutrinos.rc.fas.harvard.edu'
    username = 'guenette_read'
    with open(password_file, 'r') as _y:
        password = yaml.load(_y)[username]

    return create_connection(host=host, username=username, password=password)

def write_connection(password_file):
    host = 'db-guenette_neutrinos.rc.fas.harvard.edu'
    username = 'guenette_write'
    with open(password_file, 'r') as _y:
        password = yaml.load(_y)[username]

    return create_connection(host=host, username=username, password=password)

def admin_connection(password_file):
    host = 'db-guenette_neutrinos.rc.fas.harvard.edu'
    username = 'guenette_admin'
    with open(password_file, 'r') as _y:
        password = yaml.load(_y)[username]

    return create_connection(host=host, username=username, password=password)