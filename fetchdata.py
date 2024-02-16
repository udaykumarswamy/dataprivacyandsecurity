import numpy as np
import pandas as pd
from ucimlrepo import fetch_ucirepo 
import logging
import logging.config

 
logging.config.fileConfig('temp.conf')
logger = logging.getLogger('dataflylogger')

def fetchInputData():
    '''
    This is the method to fetch the input data
    using ucimlrepo library and does the manipulation 
    on the noisy data and create the different sub-groups
    on the data based upon the criteria of >50K and <=50K
    '''
   
    # fetch dataset 
    logger.info('fetching the data started...')
    adultData = fetch_ucirepo(id=2) 
    logger.info('fetching the data completed...')
    # data (as pandas dataframes) 
    rawDataFeatures = adultData.data.features 
    rawDataTarget = adultData.data.targets 
   

    '''
    now lets manipulate the data and save the intented data
    '''
    #lets understand the each attribute of QI's
    logger.info(np.char.center(' age unique values',100,'*'))
    logger.info(np.sort(rawDataFeatures['age'].unique()))
    logger.info(np.char.center(' education unique values',100,'*'))
    logger.info(rawDataFeatures['education'].unique())
    logger.info(np.char.center(' marital-status unique values',100,'*'))
    logger.info(rawDataFeatures['marital-status'].unique())
    logger.info(np.char.center(' race unique values',100,'*'))
    logger.info(rawDataFeatures['race'].unique())

    #sensitive attribute
    logger.info(np.char.center(' this is sensitive attribute ',100,'*'))
    logger.info(np.char.center(' occupation unique values ',100,'*'))
    logger.info(rawDataFeatures['occupation'].unique())

    logger.info('manipulation started...')
    '''
    as the target data has some extra noisy values we are clearing 
    it in below
    '''
    rawDataTarget['income']=pd.DataFrame(rawDataTarget['income'].replace('<=50K.','<=50K').replace('>50K.','>50K'))

    #now lets remove unwanted columns and lets keep the QI's and sensitive
    adultData = rawDataFeatures.merge(rawDataTarget, how='inner',left_index=True, right_index=True)
    adultData = adultData[['age','education','marital-status','race','income','occupation']]
    adultData.to_csv('dataset/orginal.csv')
    logger.info('manipulation completed...')
    logger.info('file creation for less than 50k...')
    
    #less than 50k
    lessThanFifty = adultData[adultData['income'] == '<=50K']
    lessThanFifty.to_csv('dataset/lessThanFifty.csv')
    logger.info('file creation for greater than 50k...')
    
    #greater than 50k
    greaterThanFifty = adultData[adultData['income'] == '>50K']
    greaterThanFifty.to_csv('dataset/greaterThanFifty.csv')
    logger.info('Input file creation has been completed...')

