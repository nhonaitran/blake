"""Provides optional transform functions for different data sources."""

import logging
import pandas as pd

logger = logging.getLogger(__name__)

def _fips_cleaner(code):
    """Standardizes county FIPS codes as 5-digit strings.
    Parameters
    ----------
    code : pandas.Series object
      A series containing FIPS codes as string, int, or float type.

    Returns
    ----------
    pandas.Series
    """
    return code.astype(str).str.extract('(^[^/.]*).*', expand=False).str.zfill(5)

def nyt_cases_counties(df):
    """Transforms NYT county-level COVID data"""
    # Cast date as datetime
    df['date'] = pd.to_datetime(df['date'])
    # Drop records with county = 'Unknown' or no FIPs code
    df = df.loc[(df['county'] != 'Unknown') & (df['fips'].notnull())].copy()
    # Store FIPS codes as standard 5 digit strings
    df['fips'] = _fips_cleaner(df['fips'])
    # Drop FIPs that are not part of US states, cast deaths to int
    df = df.loc[df['fips'].str.slice(0,2) <= '56'].copy()
    df['deaths'] = df['deaths'].astype(int)
    return df


