import pandas as pd
import math
import logging
import logging.config
from scipy.stats import entropy

logging.config.fileConfig('temp.conf')
logger = logging.getLogger('dataflylogger')


def anonymize_data(df, k1, k2):
    '''
    This method will anonymise the data
    on criteria of k1=10 and k2=5
    returns the anonymised data.
    '''
    anonymized_data = pd.DataFrame()
    for salary_group in df['income'].unique():
        if salary_group == '<=50K':
            k = k1
        else:
            k = k2

        for _, group in df[df['income'] == salary_group].groupby(['age', 'education', 'marital-status', 'race']):
            if len(group) >= k:
                logger.info('k-anonyminity is  satisfied so considering this group...group size: %s and k value is: %s',str(len(group)),str(k))
                anonymized_data = pd.concat([anonymized_data, group.sample(len(group))])
    
            else:
                logger.info('k-anonyminity is not satisfied so ignoring this group...group size: %s and k value is: %s',str(len(group)),str(k))
                #anonymized_data = pd.concat([anonymized_data,group])

    return anonymized_data


def anonymize_with_Entropy_l(df,k,l_value):
    '''
    Here we will be checking the Entropy-l diversity
    The method will group the QI's and check for the annonyminty
    and once that is achived will check the entopy
    of the sensitive data in group if it is greater than the
    >= log(l) then will consider that group and 
    again check the k-annonyminty critera if , then data
    will be moved further else it will be discarded.
    returns annonymised data.
    '''
    anonymized_data = pd.DataFrame()
    l_criteria = l_value  # Minimum number of unique sensitive values for diversity
    for _, group in df.groupby(['age', 'education', 'marital-status', 'race']):
            if len(group) >= k:
                freq_count_of_sensitive_data = group['occupation'].value_counts()
                #entropy of the distribution of the sensitive values in each equalence class
                enTropy = entropy(freq_count_of_sensitive_data / group.shape[0], base=2)
                
                log_value=math.log(l_criteria)
                if(enTropy >= log_value):  
                    if(len(group) >= k):
                        logger.info('Criteria satisfied standard_divation Entroy-l group size: %s, Entropy-l: %s',str(len(group)),str(enTropy))                  
                        sample_size = max(k,len(group))
                        sampled_rows = group.sample(sample_size, replace=False)
                        anonymized_data = pd.concat([anonymized_data,sampled_rows ])
                    else:
                        logger.info('annonyminsty is lost so will not consider this..so continued..group size: %s',str(len(group)))
                        continue
                else:
                    logger.info('Insufficient records to achieve l-diversity standard_divation: %s and Entroy-l: %s ',str(log_value),str(enTropy))
                    continue
            else:
                logger.info('annonyminsty is lost so will not consider this..so continued..group size: %s',str(len(group)))
                continue
    return anonymized_data


def check_c_2_diversity(r_values, c):
    '''
    check recursive diversity if l<2
    return boolean flag
    '''
    return r_values[0] < c * sum(r_values[1:])


def check_recursive_c_l_diversity(r_values, c, l):
    '''
    This method will check the recursive diversity if l > 2, 
    we say that a q* -block satisfies recursive (c, l)-diversity 
    if we can eliminate one possible sensitive value in the block and still have a
    (c, l−1)-diverse block.
    returns , boolean flag
    '''
    flag = False  # Initialize flag variable to False
    for i in range(0, len(r_values)):
        if i == 0:
            # Remove the second most frequent sensitive value
            r_prime = r_values[2:]  # Remove the second most frequent sensitive value
            #r_prime = r_prime[1:] 
            if r_values[1] < c * sum(r_prime):
                flag = True 
                return flag# Set flag to True if condition is met
        else:
            # Remove one sensitive value except the most frequent one
            r_prime = r_values[:i]   # Remove one sensitive value
            r_prime = r_values[i+1:]
            r_prime = [x - r_values[0] for x in r_prime]  # Subtract the count of the most frequent sensitive value
            if r_values[0] < c * sum(r_prime):
                flag = True  # Set flag to True if condition is met
                return flag

    return flag


# Step 4: Calculate counts and apply the diversity checks
def checkRecursiveDiversity(c,l,k,dataframe):
    does_not_count = 0
    satisfy_count = 0
    group_not_satisfy_k=0
    
    anonymised = pd.DataFrame()
    grouped_data = dataframe.groupby(['age', 'education', 'race', 'marital-status'])
    for _, group in dataframe.groupby(['age', 'education', 'race', 'marital-status']):
        r_values = group['occupation'].value_counts().values
        if len(group) >= k:
            if(l==2):
                if not check_c_2_diversity(r_values, c):
                    logger.info("Does not satisfy (c, 2)-diversity")
                else:
                    logger.info('it satisfies (c, l)-diversity')
                    anonymised = pd.concat([anonymised,group.sample(len(group)) ])
                    
            if(l>2):
                if not check_recursive_c_l_diversity(r_values, c, l):
                    logger.info("Does not satisfy recursive (c, l)-diversity (l > 2)")
                    print(group)
                    does_not_count +=1

                else:
                    anonymised = pd.concat([anonymised,group.sample(len(group))])
                    satisfy_count +=1
                    logger.info('it satisfies recursive (c, l)-diversity (l > 2) condition')
        else:
            logger.info('group not satisfying k')
            group_not_satisfy_k +=1
            
    logger.info('group size: %s',str(len(grouped_data)))
    logger.info('dose_not_count: %s',str(does_not_count))
    logger.info('satisfy: %s',str(satisfy_count))
    logger.info('k-not satisfy: %s',str(group_not_satisfy_k))
    return anonymised
        




#Formula for Calculating of Distortion and Precision
def Distortion_And_Precision_Calculation(quasibasedIdentifiers,manageHierarchy,anonymized_data,original_generalised):
    '''
    This method will calculate the distortion and precision
    of the generalised data and post anonymised data
    returns --> distortion,  precision
    '''
    distortion = 0
    precision = 0
    inter_sum = 0
   
    for k in range(0,len(quasibasedIdentifiers)):
        currentLevel = manageHierarchy.get(quasibasedIdentifiers[k])[0]
        maxLevel=manageHierarchy.get(quasibasedIdentifiers[k])[1]
        inter_sum = inter_sum+(currentLevel/maxLevel)
   
    distortion = inter_sum/len(quasibasedIdentifiers)
    
    #precision = num of recs that kept their og values divided by total num of recs
    '''
    sum_value = ((manageHierarchy.get('age')[0]/manageHierarchy.get('age')[1])+
                    (manageHierarchy.get('education')[0]/manageHierarchy.get('education')[1])+
                    (manageHierarchy.get('martial-status')[0]/manageHierarchy.get('martial-status')[1])+
                    (manageHierarchy.get('race')[0]/manageHierarchy.get('race')[1])) '''
                    
    PNA = len(anonymized_data)
    PN = len(original_generalised)
    PT = len(anonymized_data)

    NA = len(quasibasedIdentifiers)  

    depth_age = manageHierarchy.get('age')[1]
    depth_education = manageHierarchy.get('education')[1]
    depth_marital_status = manageHierarchy.get('marital-status')[1]
    depth_race = manageHierarchy.get('race')[1]

    sum_depth = depth_age + depth_education + depth_marital_status + depth_race

    precision = 1 - (PNA / PN) * (1 / (PT * NA)) * sum_depth

    return distortion,  precision



