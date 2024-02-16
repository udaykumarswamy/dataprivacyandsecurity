import csv
import pandas as pd
import numpy as np
import logging
import logging.config



logging.config.fileConfig('temp.conf')
logger = logging.getLogger('dataflylogger')

manageHierarchy = {'age': 3, 'education': 3, 'martial-status': 2, 'race': 2}

#greaterThanFifty
#lessThanFifty
input_file = 'dataset/greaterThanFifty.csv'
output_file ='anonymised/completed_anonymised.csv'
def anonymiseData():
    logger.info('annonymisation of the education started...')
    anonymize_education_csv(input_file,'anonymised/new_education.csv',1) #prev-1
    #anonymize_education_csv(input_file,output_file,1)
    logger.info('annonymisation of the education completed...')
    logger.info('annonymisation of the marital-status started...')
    anonymize_maritalstatus_csv(input_file,'anonymised/new_marital-status.csv',1) 
    #anonymize_maritalstatus_csv(input_file,output_file,2)
    logger.info('annonymisation of the marital-status completed...')
    logger.info('annonymisation of the age started...')
    anonymize_age_csv(input_file,'anonymised/new_age.csv',1) 
    #anonymize_age_csv(input_file,output_file,2)
    logger.info('annonymisation of the age completed...')
    logger.info('annonymisation of the race started...')
    anonymize_race_csv(input_file,'anonymised/new_race.csv',2) 
    #anonymize_race_csv(input_file,output_file,1)
    logger.info('annonymisation of the race completed...')
    #now will combine all the files to one
    combineAll()



def combineAll():
    logger.info('combining all the data started...')
    ageDataFrame = pd.read_csv('anonymised/new_age.csv')
    raceDataFrame = pd.read_csv('anonymised/new_race.csv')
    maritalStatusDataFrame = pd.read_csv('anonymised/new_marital-status.csv')
    educationDataFrame = pd.read_csv('anonymised/new_education.csv')
    main_data = pd.read_csv(input_file)
    result_df = pd.concat([ageDataFrame['age'],educationDataFrame['education'] , maritalStatusDataFrame['marital-status'],raceDataFrame['race'],main_data['income'],main_data['occupation']], axis=1)    
    if( 'greaterThanFifty' in input_file):
        result_df.to_csv('anonymised/final_data_for_greaterfifty.csv', index=False)
    else:
        result_df.to_csv('anonymised/final_data_for_lessfifty.csv', index=False)

def mergeFinalDataFile():
    gFifty = 'anonymised/final_data_for_greaterfifty.csv'
    lFifty = 'anonymised/final_data_for_lessfifty.csv'
    df1 = pd.read_csv(gFifty)
    df2 = pd.read_csv(lFifty)
    appended_df = pd.concat([df1, df2], ignore_index=True)
    appended_df.to_csv('anonymised/complete_anonymised.csv', index=False)


         
        
def anonymize_race_csv(input_file, output_file, level):
    '''
    this function will anonymise the race columns
    according to the level provide and creates the .csv file
    '''
    with open(input_file, 'r', newline='') as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    for row in rows:
        race = row['race']
        row['race'] = raceAnonymisation('race', race, level)
        
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
      
    
    
def anonymize_age_csv(input_file, output_file, level):
    '''
    this function will anonymise the age columns
    according to the level provide and creates the .csv file
    '''
    with open(input_file, 'r', newline='') as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    for row in rows:
        age = row.get('age')
        row['age'] = ageAnonymisation('age', age, level)
        
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        


def anonymize_education_csv(input_file, output_file, level):
    # Read the input file and store its contents
    with open(input_file, 'r', newline='') as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)
    
    # Modify the education column in the stored rows
    for row in rows:
        education = row.get('education')
        row['education'] = educationAnonymisation('education', education, level)
       
    
    # Write the modified contents to the output file
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def anonymize_maritalstatus_csv(input_file, output_file, level):
    # Read the input file and store its contents
    with open(input_file, 'r', newline='') as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)
    
    # Modify the marital status column in the stored rows
    for row in rows:
        marital_status = row.get('marital-status')
        row['marital-status'] = maritalStatusAnonymisation('marital-status', marital_status, level)
   
    
    # Write the modified contents to the output file
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    
def raceAnonymisation(key,value,level):
     #race hirarchy
    #level-0
    if(key=='race' and level==0):
        return value
    #level-1
    elif(key=='race' and (level==1 and value in ['White' ,'Black', 'Asian-Pac-Islander', 'Amer-Indian-Eskimo' ,'Other'])):
        return 'Person'
    #level-2
    elif(key=='race' and (level==2 or level>2)):
        return '*'
   

def ageAnonymisation(key, value, level):
    #age hirarchy
    #level-0
    #value = int(value)
   # Check if value is valid and convert it to an integer
    if value == None:
        return '*'
     # Convert value to integer
    
    value = int(value)
    
    #level-0
    if(key=='age' and level==0):
        return value
    #level-1
    elif(key=='age' and (value <=19 and level==1)):
        return '0-19'
    elif(key=='age' and (value >=20 and value <=39 and level==1)):
        return '20-39'
    elif(key=='age' and (value >=40 and value <=59 and level==1)):
        return '40-59'
    elif(key=='age' and (value >=60 and value <=79 and level==1)):
        return '60-79'
    elif(key=='age' and (value >=80 and  level==1)):
        return '>=80'
    #level-2
    elif(key=='age' and (value <=19 and level==2)):
        return '<60'
    elif(key=='age' and (value > 19 and  value < 60 and level==2)):
        return '<60'
    elif(key=='age' and (value > 60 and value<=79 and level==2)):
        return '>=60'
    elif(key=='age' and (value >= 80 and level==2)):
        return '>=80'
    #level-3
    elif(key=='age' and level==3):
        return '*'




def educationAnonymisation(key,value,level):
    # Hierarchy of Education
    # Hirarchy of education level == 0
 
    if(value == None):
        return '*'
    if(key=='education' and level==0):
        return value
    #level-1
    elif(key=='education' and (level==1 and value in['Preschool','1st-4th','5th-6th','7th-8th','9th','10th','11th','12th'])):
        return 'PreSchool'
    elif(key=='education' and (level==1 and value in['Bachelors','Some-college','Prof-school'])):
        return 'College'
    elif(key=='education' and (level==1 and value in ['Assoc-voc','Assoc-acdm'])):
        return 'Assoc'
    elif(key=='education' and (level==1 and value in ['HS-grad','Masters','Doctorate'])):
        return 'Higher Education'
    #level-2
    elif(key=='education' and level==2):
        return 'education'
    
    #level-3
    elif(key=='education' and (level==3 or level>3)):
        return '*'
     
def maritalStatusAnonymisation(key,value,level):
    #Hierarchy of marital-status
    #leve-0
    if(key=='marital-status' and level==0):
        return value
    #level-1
    elif(key=='marital-status' and level==1 and value in ['Married-civ-spouse','Married-AF-spouse','Married-spouse-absent']):
        return 'Married-spouse'
    elif(key=='marital-status' and level==1 and value in ['Never-married']):
        return 'Never-Married'
    elif(key=='marital-status' and level==1 and value in ['Divorced','Widowed','Separated']):
        return 'Seperated'
    #level-2
    elif(key=='marital-status' and level==2 and value in ['Married-spouse','Never-Married','Seperated']):
        return 'Marital-status'
    #level-3
    elif(key=='marital-status' and (level==3 or level>3)):
        return '*'
    
    
#anonymiseData()
mergeFinalDataFile()
