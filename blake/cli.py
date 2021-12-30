"""This is an example of a simple ETL pipeline for loading data into bit.io.
This example omits many best practices (e.g. logging, error handling,
parameterizatin + config files, etc.) for the sake of a brief, minimal example.
"""

import logging
import os
import sys

from argparse import ArgumentParser

import blake.extract
import blake.transform
import blake.validate
import blake.load


# Set up logging
logging.basicConfig(
    format='%(asctime)s %(name)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

PG_CONN_STRING = os.getenv('PG_CONN_STRING')

def parse_args(args):
    parser = ArgumentParser()

    parser.add_argument('--src',
                        '-s',
                        type=str,
                        dest='src',
                        help="URL for source data extraction")
    parser.add_argument('--dest',
                        '-d',
                        type=str,
                        dest='dest',
                        help="Fully-qualified table for load into")
    parser.add_argument('--local_src',
                        '-l',
                        action="store_true",
                        dest='local_src',
                        default=False,
                        help="Optional flag to be more personal")
    parser.add_argument('--validate_data',
                        '-v',
                        action="store_true",
                        dest='validate_data',
                        default=False,
                        help="Optional flag, True if data validation should be run.")
    return parser.parse_args(args)

def run(src, dest, local_src, validate_data, options):
    """Executes ETL pipeline for a single table.

    Args:
        src (str): URL for source data extraction.
        dest (str): Fully-qualified table for load data into
        local_src (boolean): True if src is path to a local csv file.
        validate_data (boolean): True if data validation should be run.

    Returns:
        None
    """
    # EXTRACT data
    logger.info('Starting extract...')
    if local_src:
        df = blake.extract.csv_from_local(src)
    else:
        df = blake.extract.csv_from_get_request(src)

    # TRANSFORM data
    if 'name' in options:
        if hasattr(blake.transform, options['name']):
            logger.info(f"Starting transform with {options['name']}...")
            df = getattr(blake.transform, options['name'])(df)
        else:
            raise ValueError("Specified transformation name not found.")
    else:
        logger.info(f"No transformation specified, skipping to validation step.")

    # VALIDATE data
    if ('name' in options) and validate_data:
        if hasattr(blake.validate, options['name']):
            logger.info(f"Starting data validation with {options['name']}...")
            tests = getattr(blake.validate, options['name'])
            if not blake.validate.test_data(df, tests):
                raise Exception('Data validation failed, terminating ETL.')
        else:
            raise ValueError("Specified test suite not found.")
    else:
        logger.info(f"No data validation specified, skipping to load step.")

    # LOAD data
    logger.info(f"Loading data to bit.io...")
    blake.load.to_table(df, dest, PG_CONN_STRING)
    logger.info(f"Data loaded to bit.io.")

def main():
    args = parse_args(sys.argv[1:])
    logger.info('Starting ETL...')
    run(args.src, args.dest, args.local_src, args.validate_data, args.options)

if __name__ == '__main__':
    main()