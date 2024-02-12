import argparse
import csv
import sys
from datetime import datetime
from io import StringIO
import logging
import logging.config
from fetchdata import fetchInputData
 
logging.config.fileConfig('temp.conf')
logger = logging.getLogger('dataflylogger')


if __name__ == "__main__":
    '''
    parser = argparse.ArgumentParser(
        description="Python implementation of the Data-fly algorithm. Finds a k-anonymous "
                    "representation of a table.")
    parser.add_argument("--private_table", "-pt", required=True,
                        type=str, help="Path to the CSV table to K-anonymize.")
    parser.add_argument("--quasi_identifier", "-qi", required=True,
                        type=str, help="Names of the attributes which are Quasi Identifiers.",
                        nargs='+')
    parser.add_argument("--domain_gen_hierarchies", "-dgh", required=True,
                        type=str, help="Paths to the generalization files (must have same order as "
                                       "the QI name list.",
                        nargs='+')
    parser.add_argument("-k", required=True,
                        type=int, help="Value of K.")
    parser.add_argument("--output", "-o", required=True,
                        type=str, help="Path to the output file.")
    args = parser.parse_args()
    '''
    try:

        start = datetime.now()
        logger.info('Data-fly alogorithm started at:  %s',start)
        logger.info('Calling fetchdata...')
        fetchInputData()
        logger.info('fetching the data completed..')
        logger.info('generalisation of the QI\' started...')
        
        
        
    except:
        print('some error')