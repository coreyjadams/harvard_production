
from ConfigException import ConfigException
from database import DatasetReader

class StageConfigException(ConfigException):
    ''' Custom exception for stages'''

    def __init__(self, key = None, name=None):
        message = 'Error Configuring Stage'
        if key is not None:
            message += ': Missing keyword {0} in stage {0}'.format(key, name)
        super(StageConfigException, self).__init__(message)

class StageConfig(object):
    '''
    Stage Configuration Object
    Stores the information from the larsoft configuration and includes
    helpful functions
    '''
    def __init__(self, yml_dict, name, previous_stage=None):
        super(StageConfig, self).__init__()
        required_keys=['fcl','n_jobs','events_per_job','input','output']
        required_subkeys={'input'  : ['dataset'],
                          'output' : ['dataset', 'location', 'anaonly']}
        for key in required_keys:
            if key not in yml_dict:
                raise StageConfigException(key, name)
            # Check for required subkeys:
            if key in required_subkeys.keys():
                for subkey in required_subkeys[key]:
                    if subkey not in yml_dict[key]:
                        raise StageConfigException(subkey, "{0}/{1}".format(name,key))

        self.name = name
        self.yml_dict = yml_dict


    def __getitem__(self, key):
        '''return objects

        Returns yml parameters, and returns defaults for some parameters if they
        are not specified in the file

        Arguments:
            key {key} -- dictionary key, typcially string

        Returns:
            object -- value for the key
        '''
        # List of defaults:
        if key not in self.yml_dict:
            if key == 'memory':
                return 4000
            if key == 'time':
                return '06:00:00'
            if key == 'ana_name':
                return 'hist'
        return self.yml_dict[key]

    def output_directory(self):
        out = self.yml_dict['output']['location']
        if not out.endswith('{0}/'.format(self.name)):
            out += '/{0}/'.format(self.name)
        return out

    def output_file(self):
        '''
        Return the output file for this job
        '''
        if not self.has_input():
            return

    def get_next_files(self, n, db=None):
        '''
        Function to interface with file interface tools
        and fetch files.  Returns absolute paths
        '''

        # If the input is none, we return None:
        if self.yml_dict['input']['dataset'] == 'none':
            return None

        else:
            if db is None:
                raise Exception("Can not list next files if no database available.")
            # Otherwise, access the data base and consume files:
            results = db.yield_files(dataset = self.yml_dict['input']['dataset'],
                                     stage   = self.yml_dict['input']['stage'],
                                     ftype=0, max_n_files=n)
            # Unpack the results:

            files = [x[2] for x in results]
            locations = [x[3] for x in results]

            full_paths = [path + name for path, name in zip(locations, files) ]

            return full_paths

    def finalize(self, input_files, db=None):
        '''Mark input files as finalized

        Take the input files and call db.consume_files
        to finish marking these files from 'in-progress'
        to finished

        Arguments:
            input_files {[type]} -- [description]
            db {[type]} -- [description]
        '''
        # Verify input is iterable:
        try:
            _ = (x for x in input_files)
        except:
            raise Exception('Input files must be a list of files to finalize')

        if db is None:
            raise Exception("Can not finalize files if no database available.")

        db.consume_files(dataset = self.yml_dict['input']['dataset'],
                         files   = input_files,
                         stage   = self.yml_dict['input']['stage'],
                         ftype   = 0)

    def total_output_events(self):
        '''Compute the number of output events expected

        If events per job is set, use that * njobs, otherwise
        if there is an input dataset count home many input events
        '''
        if 'target_events' in self.yml_dict:
            return int(self['target_events'])
        if self['events_per_job'] > 0:
            return int(self['n_jobs']) * int(self['events_per_job'])
        else:
            if self['input']['dataset'] == 'none' or self['input']['dataset'] == None:
                return None
            dr = DatasetReader()
            return dr.sum(dataset=self['input']['dataset'], target='nevents', type=0)


    def n_jobs(self):
        '''
        Return the number of jobs to launch for this stage
        '''
        return int(self.yml_dict['n_jobs'])

    def concurrent_jobs(self):
        '''
        Return the maximum number of jobs to run concurrently

        If this parameter is not specified in the yml configuration,
        it is set to n_jobs by default.
        '''
        if 'max_concurrent_jobs' in self.yml_dict:
            return int(self.yml_dict['max_concurrent_jobs'])
        else:
            return self.n_jobs()

    def events_per_job(self):
        '''
        Return the number of events to process per job
        '''

        if int(self.yml_dict['events_per_job']) != -1:
            return int(self.yml_dict['events_per_job'])
        else:
            return None

    def n_files(self):
        '''
        Return the number of files to process in a single job, default is one
        '''

        if 'n_files' in self.yml_dict['input']:
            return int(self.yml_dict['input']['n_files'])
        return 1

    def fcl(self):
        '''
        Return the fcl file for this stage.
        '''
        return self.yml_dict['fcl']

    def has_input(self):
        '''
        Return whether this stage has input or not
        '''
        if self.yml_dict['input']['dataset'] == 'none':
            return False
        else:
            return True

    def output_dataset(self):
        '''return the output dataset for this particular stage
        '''
        return self.yml_dict['output']['dataset']

    def input_dataset(self):
        if isinstance(self.yml_dict['input']['dataset'], str):
            if self.yml_dict['input']['dataset'] == 'none':
                return None
            if self.yml_dict['input']['dataset'] == 'None':
                return None
            return [self.yml_dict['input']['dataset'],]
        else:
            return self.yml_dict['input']['dataset']