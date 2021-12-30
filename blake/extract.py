"""Provide extraction functions.
Currently support GET from URL or local file.
"""

import io
import logging
import pandas as pd
import requests

logger = logging.getLogger(__name__)

def csv_from_get_request(url):
    """Extracts a data text string accessible with a GET request.
    Parameters
    ----------
    url : str
        URL for the extraction endpoint, including any query string
    Returns
    ----------
    DataFrame
    """
    r = requests.get(url, timeout=5)
    data = r.content.decode('utf-8')
    df =  pd.read_csv(io.StringIO(data), low_memory=False)
    return df

def csv_from_local(path):
    """Extracts a csv from local filesystem.
    Parameters
    ----------
    path : str
    Returns
    ----------
    DataFrame
    """
    return pd.read_csv(path, low_memory=False)

