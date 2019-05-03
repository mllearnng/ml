from pyperso.libraries import configs, logger

def _get_model(key):
    '''
    Get the values from config

    :param keys: Keys
    :param oppid: Opp Id
    :return:
    '''
    keys = 'models' if key is None else ['models',key]
    values = configs.get_config(keys,'mdl')
    logger.info('Opp Id config - {} : {}'.format(keys, values))
    return values

def get_value(mdl, key):
    '''
    Get value from model.yml

    :param mdl: model name
    :param key: key value
    :return: string
    '''
    return _get_model(mdl)[key]


def get_d90_value(key):
    '''

    :param key:
    :return:
    '''
    return get_value('nba_90',key)


def get_rf_value(key):
    '''

    :param key:
    :return:
    '''
    return get_value('nba_rf', key)


def get_pa_value(key):
    '''

    :param key:
    :return:
    '''
    return get_value('nba_pa', key)


def get_model_file(program, calib_model=False):
    '''
    Get the model file based on the RDL vs Cloud
    :param program: Program type eg: nba_d90, nba_rf etc.
    :param calib_model: Is it a regular model or claibration model, defaulted to False
    :return: File name from mdl.yml
    '''
    if calib_model:
        return get_value(program, 'db_calib_model')
    else:
        return get_value(program, 'db_model')
