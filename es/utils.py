import requests
import json
import pandas as pd
import logging
import yaml
from typing import Dict, Union


def _convert_to_df(res: Union[requests.Response, Dict]) -> pd.DataFrame:
    """
    Convers response content (it should be json first) to Pandas dataframe
    :param res: Requests Response or Dict
    :return: pandas Dataframe
    """
    if not isinstance(res, dict):
        d = json.loads(res.content.decode())
    else:
        d = res
    col_names = list()
    col_types = list()
    for col in d['columns']:
        col_names.append(col['name'])
        col_types.append(col['type'])
    df = pd.DataFrame(d['rows'], columns=col_names)
    return df


def _get_logger(logger_name: str) -> logging.Logger:
    """
    Setup of the logger for the function
    :param logger_name: name of the logger
    :return: logging.Logger that will work for this function
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def _read_config(path: str) -> Dict:
    """
    Reads yaml config
    :param path: Path of config
    :return: Dictionary with config
    """
    with open(path) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    return config


def get_dict(res: requests.Response) -> Dict:
    """
    Converts requests content to dictionary
    :param res: Requests response
    """
    return json.loads(res.content.decode())
