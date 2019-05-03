'''
Configurations
Note: Reads all the yml from selector and makes it accessible for all the modules
Author: Sreekanth Mahesala
'''

import yaml
from os import listdir
from os.path import isfile, join

conf_store = None
arg_store = None
project_file = None

def save_args_configs(args, config_path):
    '''
    Save arguments and load configurations
    :param args: Arguments - dict
    :param config_path: Configuration path
    :return: None
    '''
    global arg_store, conf_store, project_file
    args['runId'] = clean_runId(args['runId']) if args['runId'] is not None else 'testing'
    arg_store = save(args)
    conf_store = None
    conf_store = save(__load_configs(config_path))
    return True


def clean_runId(runId):
    '''
    Clean runId
    :param runId: runId
    :return: cleaned runId
    '''
    return runId.replace(':', '_').replace('.', '-').replace('+', '-')


def project_config():
    '''
    Key for project configuration
    :return: string
    '''
    if project_file:
        return project_file

    if arg_store:
        if arg_store()['project_file']:
            global project_file
            project_file = arg_store()['project_file']
            return project_file
    return 'project_databricks'


def __load_configs(config_path):
    '''
    Load configurations
    :param selector: Selector file, defaulted to selector-dev.yml
    :return: Dict
    '''
    all_files = [{yFile.split('.')[0]:join(config_path, yFile)} for yFile in listdir(config_path)
                 if isfile(join(config_path, yFile)) & yFile.lower().endswith('.yml')]

    configs = dict()
    for dic in all_files:
        for key in dic:
            typ = key
            file_path = dic[key]
            configs[typ] = __yaml_open_file(file_path)

    if project_config() not in configs:
        raise Exception('Project config file [{}] is missing'.format(project_config()))

    if get_args('environment'):
        configs[project_config()]['env'] = get_args('environment')

    return configs


def save(inputs):
    '''
    Save closure
    :param inputs:
    :return:
    '''
    def closure():
        return inputs
    return closure


def get_config(keys, typ=None):
    '''
    Get config from yaml file
    Example: get_config(['store', 'recos'], 'etl')
    :param keys: List of tokens
    :param typ: Configuration type
    :param force: Force load before getting config
    :return: value
    '''
    global conf_store
    if not typ:
        typ = project_config()

    if conf_store:
        result = __yaml_find(conf_store()[typ], keys)
        print 'Config of type {typ} with keys {keys}: {result}'.format(typ=typ, keys=keys, result=result)
        return result


def get_args(keys):
    '''
    Get input arguments
    :param keys:
    :return:
    '''
    return arg_store()[keys]


def get_tbl(name):
    '''
    Get standard tables as a dataframe
    :param name: Table name
    :param tbl_typ: Type of the table, Defaulted to hdl
    :return: Spark dataframe
    '''

    table = get_config(('tables', name), 'tables')
    if table is None:
        raise Exception(
            'Table ' + name +' not found in tables config')

    return table


def is_production():
    '''
    Is the environment production
    :return:
    '''
    return get_config('env') == 'prod'


def hive_schema():
    '''
    Get hive schema config
    :return:
    '''
    return get_config(('hive', 'schema'))


def prj_hive_schema():
    return hive_schema()


def __yaml_open_file(file_location, func='r'):
    """
    Open and parse yaml file

    :param file_location: string
    :param func: function read/log_write
    :return: file stream
    """
    with open(file_location, func) as stream:
        result = yaml.load(stream)

    return result


def __get_value(obj, tokens,i):
    for i in range(i, len(tokens)):
        obj = obj.get(tokens[i])
    return obj


def __yaml_find(yml_data, tokens = []):
    """
    Find key in parsed yaml file
    :param yml_data: yaml_data stream
    :param tokens: list of keys, eg- (hive,schema)
    :return: value
    """
    tokens = [tokens] if isinstance(tokens, str) else tokens

    if len(tokens) > 1:
        return __get_value(yml_data[tokens[0]], tokens, 1)
    elif len(tokens) == 1:
        return yml_data[tokens[0]]
    else:
        return []
