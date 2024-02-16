import argparse
import csv
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from io import StringIO
import logging
import logging.config
from generalizeQi import mergeInputFileDataFile
from generalizeQi import genearliseData
from fetchdata import fetchInputData
from anonymise import anonymize_data
from anonymise import Distortion_And_Precision_Calculation
from anonymise import anonymize_with_Entropy_l
from anonymise import checkRecursiveDiversity
 
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
        logger.info('fetchdata the data started...')
        fetchInputData()
        logger.info('fetching the data completed..')
        
        logger.info('megerging both files started...')
        merged_output_file = 'generalised/merged_input_file.csv'
        mergeInputFileDataFile(merged_output_file)
        logger.info('megergeing both files completed...')
        
        logger.info('generalisation of the QI\' started...')
        #here we are taking all the hirarchy at the level-1
        #this condition is satisfied after 
        output_file = 'final/complete_anonymised_1b.csv'
        genearliseData(1,1,1,1,merged_output_file,output_file)
        logger.info('generalisation of the QI\' completed...')
        
        logger.info('k-anonymisation for the k1=10 and k2=5..started')
        dataFrame = pd.read_csv(output_file)
        k1=10
        k2=5
        #marital-status
        anonymised_output = 'final/k510_anonymised_1b.csv'
        k_min_anonymised_data=anonymize_data(dataFrame,k1,k2)
        k_min_anonymised_data = k_min_anonymised_data[['age', 'education', 'marital-status', 'race','occupation']]
        k_min_anonymised_data.to_csv(anonymised_output,index=False)
        logger.info('k-anonymisation for the k1=10 and k2=5..completed')
        
        logger.info('1-c calcuate the distortion and precision for the 1b started...')
        manageHierarchy = {'age': [1,4], 'education': [1,3], 'marital-status': [1,3], 'race': [1,3]}
        quasibasedIdentifiers=['age', 'education', 'marital-status', 'race']
        anonymized_data = pd.read_csv(anonymised_output)
        original_generalised = pd.read_csv(output_file)
        distortion,precision=Distortion_And_Precision_Calculation(quasibasedIdentifiers,manageHierarchy,anonymized_data,original_generalised)
        logger.info(np.char.center('1-C distortion and precision',80,'*'))
        logger.info('Distortion: %s',str(distortion))
        logger.info('Precision: %s',str(precision))
        logger.info('1-c  calcuate the distortion and precision for the 1b completed...')
        
        logger.info('2-a heuristic algorithm for entropy l-diversity started...')
        dataFrame = pd.read_csv(anonymised_output)
        output_file_l_diverse = 'final/entropy_l_diverse_2a.csv'
        k=5
        l=3
        entropy_l_diverse = anonymize_with_Entropy_l(dataFrame,k,l)
        entropy_l_diverse = entropy_l_diverse[['age', 'education', 'marital-status', 'race','occupation']]
        entropy_l_diverse.to_csv(output_file_l_diverse,index=False)
        logger.info('2-a heuristic algorithm for entropy l-diversity completed...')
        
        logger.info('2-b heuristic algorithm for Recursive(c,l)-diversity started...')
        c=2
        l=3
        k=5
        #here take the anonymised file for- 5
        output_file_recursive = 'final/recursive_2_b.csv'
        dataframe = pd.read_csv(anonymised_output)
        #genearliseData(1,2,1,1) 
        recursive_data=checkRecursiveDiversity(c,l,k,dataframe)
        recursive_data = recursive_data[['age', 'education', 'marital-status', 'race','occupation']]
        recursive_data.to_csv(output_file_recursive,index=False)
        logger.info('2-b heuristic algorithm for Recursive(c,l)-diversity completed...')
        
        
        logger.info('2-c k = 5, l = 3 for (a), and calculate the distortion and precision of the output started...')
        manageHierarchy = {'age': [1,4], 'education': [1,3], 'marital-status': [1,3], 'race': [1,3]}
        quasibasedIdentifiers=['age', 'education', 'marital-status', 'race']
        anonymized_data = pd.read_csv(output_file_l_diverse)
        o_g_path = pd.read_csv(output_file)
        distortion,precision=Distortion_And_Precision_Calculation(quasibasedIdentifiers,manageHierarchy,anonymized_data,o_g_path)
        logger.info(np.char.center('2-C distortion and precision',80,'*'))
        logger.info('Distortion: %s',str(distortion))
        logger.info('Precision: %s',str(precision))
        logger.info('2-c k = 5, l = 3 for (a), and calculate the distortion and precision of the output completed...')
        
        
        logger.info('2-D k = 5, l = 3 c = 0.5, 1, 2 for (b), and calculate the distortion and precision of the output started..')
        c_list = [0.5,1,2]
        for c in c_list:
            k=5
            l=3
            if(c == 0.5):
                logger.info('recursive l-divesity for c= %s, started..',str(c))
                manageHierarchy=  {'age': [2,4], 'education': [1,3], 'marital-status': [2,3], 'race': [2,3]}
                logger.info('generalize the data according to satisfaction..started')
                output_file_c='final/generalised_for_c_0_5.csv'
                genearliseData(1,2,2,2,merged_output_file,output_file_c)
                logger.info('generalize the data according to satisfaction..completed')
                logger.info('annonymisation for k-5 is started...')
                dataFrame = pd.read_csv(output_file_c)
                anony_output = 'final/kanonymised_for_c_0_5.csv'
                anonymised_data=anonymize_data(dataFrame,k,10)
                anonymised_data.to_csv(anony_output,index=False)
                logger.info('annonymisation for k-5 is completed...')
                logger.info('recursive l-diversity on data started...')
                dataframe = pd.read_csv(anony_output)
                recursive_output=checkRecursiveDiversity(c,l,k,dataframe)
                recursive_output = recursive_output[['age', 'education', 'marital-status', 'race','occupation']]
                recursive_output_data = 'final/reccursive_data_for_c_0_5.csv'
                recursive_output.to_csv(recursive_output_data,index=False)
                logger.info('recursive l-diversity on data completed...')
                logger.info('calculating the distortion and precision for c-0.5 started...')      
                anonymized_data = pd.read_csv(recursive_output_data)
                original_generalised = pd.read_csv(output_file_c)          
                distortion,precision=Distortion_And_Precision_Calculation(quasibasedIdentifiers,manageHierarchy,anonymized_data,original_generalised)
                logger.info(np.char.center('2-D-a k = 5,l = 3, c = 0.5 distortion and precision',80,'*'))
                logger.info('Distortion: %s',str(distortion))
                logger.info('Precision: %s',str(precision))
                logger.info('calculating the distortion and precision for c-0.5 completed...')    
            elif(c==1):
                logger.info('recursive l-divesity for c= %s, started..',str(c))
                logger.info('generalize the data according to satisfaction..started')
                manageHierarchy=  {'age': [2,4], 'education': [1,3], 'marital-status': [2,3], 'race': [2,3]}
                output_file_c='final/generalised_for_c_1.csv'
                genearliseData(1,2,2,2,merged_output_file,output_file_c)
                logger.info('generalize the data according to satisfaction..completed')
                logger.info('annonymisation for k-5 is started...')
                dataFrame = pd.read_csv(output_file_c)
                anony_output = 'final/kanonymised_for_c_1.csv'
                anonymised_data=anonymize_data(dataFrame,k,10)
                anonymised_data.to_csv(anony_output,index=False)
                logger.info('annonymisation for k-5 is completed...')
                logger.info('recursive l-diversity on data started...')
                dataframe = pd.read_csv(anony_output)
                recursive_output=checkRecursiveDiversity(c,l,k,dataframe)
                recursive_output = recursive_output[['age', 'education', 'marital-status', 'race','occupation']]
                recursive_output_data = 'final/reccursive_data_for_c_1.csv'
                recursive_output.to_csv(recursive_output_data,index=False)
                logger.info('recursive l-diversity on data completed...')
                logger.info('calculating the distortion and precision for c-1 started...')      
                anonymized_data = pd.read_csv(recursive_output_data)
                original_generalised = pd.read_csv(output_file_c)          
                distortion,precision=Distortion_And_Precision_Calculation(quasibasedIdentifiers,manageHierarchy,anonymized_data,original_generalised)
                logger.info(np.char.center('2-D-a k = 5,l = 3, c = 1 distortion and precision',80,'*'))
                logger.info('Distortion: %s',str(distortion))
                logger.info('Precision: %s',str(precision))
                logger.info('calculating the distortion and precision for c-1 completed...')   
            elif(c==2):
                logger.info('recursive l-divesity for c= %s, started..',str(c))
                logger.info('generalize the data according to satisfaction..started')
                manageHierarchy=  {'age': [2,4], 'education': [1,3], 'marital-status': [2,3], 'race': [2,3]}
                output_file_c='final/generalised_for_c_2.csv'
                genearliseData(1,2,2,2,merged_output_file,output_file_c)
                logger.info('generalize the data according to satisfaction..completed')
                logger.info('annonymisation for k-5 is started...')
                dataFrame = pd.read_csv(output_file_c)
                anony_output = 'final/kanonymised_for_c_2.csv'
                anonymised_data=anonymize_data(dataFrame,k,10)
                anonymised_data.to_csv(anony_output,index=False)
                logger.info('annonymisation for k-5 is completed...')
                logger.info('recursive l-diversity on data started...')
                dataframe = pd.read_csv(anony_output)
                recursive_output=checkRecursiveDiversity(c,l,k,dataframe)
                recursive_output = recursive_output[['age', 'education', 'marital-status', 'race','occupation']]
                recursive_output_data = 'final/reccursive_data_for_c_2.csv'
                recursive_output.to_csv(recursive_output_data,index=False)
                logger.info('recursive l-diversity on data completed...')
                logger.info('calculating the distortion and precision for c-1 started...')      
                anonymized_data = pd.read_csv(recursive_output_data)
                original_generalised = pd.read_csv(output_file_c)          
                distortion,precision=Distortion_And_Precision_Calculation(quasibasedIdentifiers,manageHierarchy,anonymized_data,original_generalised)
                logger.info(np.char.center('2-D-c k = 5,l = 3, c = 2 distortion and precision',80,'*'))
                logger.info('Distortion: %s',str(distortion))
                logger.info('Precision: %s',str(precision))
                logger.info('calculating the distortion and precision for c-2 completed...')    
    except Exception as e:
        print('Error while running the data-fly',e)