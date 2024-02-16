import pandas as pd
import math
import logging
import logging.config
from scipy.stats import entropy

logging.config.fileConfig('temp.conf')
logger = logging.getLogger('dataflylogger')


#1-b
def anonymize_data(df, k1, k2):
    anonymized_data = pd.DataFrame()
    for salary_group in df['income'].unique():
        if salary_group == '<=50K':
            k = k1
        else:
            k = k2

        for _, group in df[df['income'] == salary_group].groupby(['age', 'education', 'marital-status', 'race']):
            if len(group) >= k:
                print('in first block',len(group),'k value:',k)
                anonymized_data = pd.concat([anonymized_data, group.sample(len(group))])
    
            else:
                print('in second block',len(group),'k value:',k)
                #anonymized_data = pd.concat([anonymized_data,group])

    return anonymized_data

#2a
def anonymize_with_Entropy_l_diverse(df, k1, k2,l_value):

   
    anonymized_data = pd.DataFrame()
    l_criteria = l_value  # Minimum number of unique sensitive values for diversity
    for salary_group in df['income'].unique():
        if salary_group == '<=50K':
            k = k1
        else:
            k = k2
            
        for _, group in df[df['income'] == salary_group].groupby(['age', 'education', 'marital-status', 'race']):
            if len(group) >= k:
                freq_count_of_sensitive_data = group['occupation'].value_counts()
                standard_diviation=freq_count_of_sensitive_data.std()
                enTropy = entropy(freq_count_of_sensitive_data / group.shape[0], base=2)
                #entropy of the distribution of the sensitive values in each equalence class
                entropy_value=math.log(l_criteria)
                if(enTropy >= math.log(l_criteria)):  
                    logger.info('Criteria satisfied standard_divation: ',standard_diviation,'Entroy-l: ',entropy_value)                  
                    if(len(group) >= k):
                        sample_size = max(k,len(group))
                        sampled_rows = group.sample(sample_size, replace=False)
                        anonymized_data = pd.concat([anonymized_data,sampled_rows ])
                    else:
                        logger.info('annonyminsty is lost so will not consider this..so continued..group size: ')

                        continue
                else:
                    logger.info('Insufficient records to achieve l-diversity standard_divation: ''Entroy-l: ')
                    continue
            else:
                logger.info('Insufficient records to achieve l-diversity standard_divation: ''Entroy-l: ')

                continue
    return anonymized_data


#2a modified 
def anonymize_with_Entropy_l_modifed(df, k,l_value):
    '''
    Here we will be checking the Entropy-l diversity
    The method will group the QI's and check for the annonyminty
    and once that is achived will check the stadard-divaiation
    of the sensitive data in group if it is greater than the
    entropy -->log(l) then will consider that group and 
    again check the k-annonyminty critera if passed then data
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
                    logger.info('Criteria satisfied standard_divation Entroy-l')                  
                    if(len(group) >= k):
                        sample_size = max(k,len(group))
                        sampled_rows = group.sample(sample_size, replace=False)
                        anonymized_data = pd.concat([anonymized_data,sampled_rows ])
                    else:
                        logger.info('annonyminsty is lost so will not consider this..so continued..group size: ')

                        continue
                else:
                    logger.info('Insufficient records to achieve l-diversity standard_divation: ''Entroy-l: ')
                    continue
            else:
                logger.info('Insufficient records to achieve l-diversity standard_divation: ''Entroy-l: ')

                continue
    return anonymized_data

#2b
dataframe = pd.read_csv('final/anony_data_minimumk.csv')

def check_c_2_diversity(r_values, c):
    return r_values[0] < c * sum(r_values[1:])

def check_recursive_c_l_diversity(r_values, c, l):
    temp = False  # Initialize temp variable to False
    for i in range(0, len(r_values)):
        if i == 0:
            # Remove the second most frequent sensitive value
            r_prime = r_values[1:]  # Remove the second most frequent sensitive value
            if r_values[1] < c * sum(r_prime):
                temp = True  # Set temp to True if condition is met
        else:
            # Remove one sensitive value except the most frequent one
            r_prime = r_values[:i]   # Remove one sensitive value
            r_prime = r_values[i+1:]
            r_prime = [x - r_values[0] for x in r_prime]  # Subtract the count of the most frequent sensitive value
            if r_values[0] < c * sum(r_prime):
                temp = True  # Set temp to True if condition is met

    return temp
# Step 3: Group the dataframe by quasi-identifiers and sensitive attribute
grouped_data = dataframe.groupby(['age', 'education', 'race', 'marital-status'])


l=3
c=2
# Step 4: Calculate counts and apply the diversity checks
def checkRecursiveDiversity():
    for name, group in grouped_data:
        r_values = group['occupation'].value_counts().values
        if(l==2):
            if not check_c_2_diversity(r_values, c):
                print("Does not satisfy (c, 2)-diversity")
            else:
                print('it satisfies (c, l)-diversity')
        if(l>2):
            if not check_recursive_c_l_diversity(r_values, c, l):
                print("Does not satisfy recursive (c, l)-diversity (l > 2)")
            else:
                print('it satisfies recursive (c, l)-diversity (l > 2) condition')
        
#checkRecursiveDiversity()



#1-b,c and 2-a,b,c -->hirarchy
manageHierarchy = {'age': [1,4], 'education': [1,3], 'martial-status': [1,3], 'race': [1,3]}

#Formula for Calculating of Distortion and Precision
def Distortion_And_Precision_Calculation(quasibasedIdentifiers, level,original):
    distortion = 0
    precision = 0
    inter_sum = 0
    for k in range(0,len(quasibasedIdentifiers)):
        currentLevel = manageHierarchy.get(quasibasedIdentifiers[k])[0]
        maxLevel=manageHierarchy.get(quasibasedIdentifiers[k])[1]
        inter_sum = inter_sum+(currentLevel/maxLevel)
   
    distortion = inter_sum/len(quasibasedIdentifiers)
    #precision = round(1 - distortion,2)
    #precision = num of recs that kept their og values divided by total num of recs
    sum_value = ((manageHierarchy.get('age')[0]/manageHierarchy.get('age')[1])+
                    (manageHierarchy.get('education')[0]/manageHierarchy.get('education')[1])+
                    (manageHierarchy.get('martial-status')[0]/manageHierarchy.get('martial-status')[1])+
                    (manageHierarchy.get('race')[0]/manageHierarchy.get('race')[1]))
    precision = 1-(sum_value/4)
    return distortion,  precision


def Generalization(quasibasedIdentifiers, level):
    for p in range(0, len(quasibasedIdentifiers)):
        if(quasibasedIdentifiers[p] not in ['age', 'education', 'martial-status', 'race']):
            return('The passed value is not recognized as a Quasi Identifier.')
    if(len(quasibasedIdentifiers) != len(level)):
        return('Unable to obtain the correct level, ensure that the number of QuasiIdentifiers and Levels is the same')
    for p in range(0,len(quasibasedIdentifiers)):
        for q in range(0,masked[quasibasedIdentifiers[p]].count()):
            masked[quasibasedIdentifiers[p]][q] =Distortion_And_Precision_Calculation(quasibasedIdentifiers[p],
             level[p])
    distortion_precision = Distortion_And_Precision_Calculation(quasibasedIdentifiers, level)
    print(distortion_precision)
    

quasiIdentifiers=['age', 'education', 'martial-status', 'race']    
#combined = pd.read_csv('anonymised/complete_anonymised.csv')
combined = pd.read_csv('final/complete_anonymised.csv')

#return_df=anonymize_data(combined,10,5) #<=50 --> 10 >=50k --> 5
#return_df.to_csv('final/anony_data_minimumk.csv',index=False)
original = pd.read_csv('final/anony_data_minimumk.csv')
masked = pd.read_csv('anonymised/anony_data.csv')
#sample_distortion()
#print(return_df.head())
distortion,prision=Distortion_And_Precision_Calculation(['age', 'education', 'martial-status', 'race'], ['age', 'education', 'martial-status', 'race'],original)
print('distortion:',distortion)
print('prisition:',prision)

#k-->5 l-->3 satisfies 2-a and 2-c
#l_diverse_modified = anonymize_with_Entropy_l_modifed(combined,5,3)
#l_diverse_modified.to_csv('final/l_diverse_modified.csv',index=False)
#l_diverse=anonymize_with_Entropy_l_diverse(combined,10,5,5)      
#l_diverse.to_csv('final/l_diverse.csv',index=False)
