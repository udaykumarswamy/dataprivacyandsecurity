import csv
import pandas as pd
import numpy as np
import logging
import logging.config



logging.config.fileConfig('temp.conf')
logger = logging.getLogger('dataflylogger')


input_file = 'dataset/greaterThanFifty.csv'
output_file ='anonymised/completed_anonymised_education.csv'
def anonymiseData():
    logger.info('annonymisation of the education started...')
    anonymize_education_csv(input_file,'anonymised/education.csv',2)
    #anonymize_education_csv(input_file,output_file,1)
    logger.info('annonymisation of the education completed...')
    logger.info('annonymisation of the marital-status started...')
    anonymize_maritalstatus_csv(input_file,'anonymised/marital-status.csv',2)
    #anonymize_maritalstatus_csv(input_file,output_file,2)
    logger.info('annonymisation of the marital-status completed...')
    logger.info('annonymisation of the age started...')
    anonymize_age_csv(input_file,'anonymised/age.csv',2)
    #anonymize_age_csv(input_file,output_file,2)
    logger.info('annonymisation of the age completed...')
    logger.info('annonymisation of the race started...')
    anonymize_race_csv(input_file,'anonymised/race.csv',2)
    #anonymize_race_csv(input_file,output_file,1)
    logger.info('annonymisation of the race completed...')
    
    



def anonymize_education_csv(input_file, output_file, level):
    with open(input_file, 'r+', newline='') as infile, open(output_file, 'w+', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        rows = list(reader)
        
        for row in rows:
            education = row.get('education')
            print(education)
            row['education'] = educationAnonymisation('education', education, level)
            writer.writerow(row)
 
'''
def anonymize_education_csv(input_file, output_file, level):
    this function will anonymise the education columns
    according to the level provide and creates the .csv file
    with open(input_file, 'r', newline='') as file:
        reader = csv.DictReader(file)
        fieldnames = reader.fieldnames 
        rows = list(reader)

    for row in rows:
        education = row.get('education')
        row['education'] = educationAnonymisation('education', education, level)
        
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)  
'''     
    
   
def educationAnonymisation(key,value,level):
      # Hierarchy of Education
    # Hirarchy of education level == 0
    
    if(value == None):
        return '*'
    if(key=='education' and level==0):
        return value
    #level-1
    elif(key=='education' and level==1 and value in['preschool','1st-12th']):
        return 'PreSchool'
    elif(key=='education' and level==1 and value in['Bachelors','Some-college','Prof-school']):
        return 'College'
    elif(key=='education' and level==1 and value in ['Assoc-voc','Assoc-acdm']):
        return 'Assoc'
    elif(key=='education' and level==1 and value in ['HS-grad','Masters','Doctorate']):
        return 'Higher Education'
    #level-2
    elif(key=='education' and level==2):
        return 'education'
    
    #level-3
    elif(key=='education' and (level==3 or level>3)):
        return '*'


'''
def anonymize_maritalstatus_csv(input_file, output_file, level):
    
    #this function will anonymise the marital-status columns
    #according to the level provide and creates the .csv file
    with open(input_file, 'r', newline='') as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    for row in reader:
        maritalstatus = row['marital-status']
        row['marital-status'] = maritalStatusAnonymisation('marital-status', maritalstatus, level)
        
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)  
 '''
def anonymize_maritalstatus_csv(input_file, output_file, level):
    with open(input_file, 'r+', newline='') as infile, open(output_file, 'w+', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        
        for row in reader:
            maritalStatus = row['marital-status']
            row['marital-status'] = educationAnonymisation('marital-status', maritalStatus, level)
            writer.writerow(row)   
            
               
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


def anonymize_race_csv(input_file, output_file, level):
    '''
    this function will anonymise the race columns
    according to the level provide and creates the .csv file
    '''
    with open(input_file, 'r', newline='') as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    for row in reader:
        race = row['race']
        row['race'] = raceAnonymisation('race', race, level)
        
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)
   
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

def anonymize_age_csv(input_file, output_file, level):
    '''
    this function will anonymise the age columns
    according to the level provide and creates the .csv file
    '''
    with open(input_file, 'r', newline='') as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    for row in reader:
        age = row['age']
        row['age'] = ageAnonymisation('age', age, level)
        
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        
def ageAnonymisation(key, value, level):
    #age hirarchy
    #level-0
    #value = int(value)
   # Check if value is valid and convert it to an integer
    if value == None:
        return '*'
     # Convert value to integer
    try:
        value = int(value)
    except ValueError:
        return '*' 
    
    if(key=='age' and level==0):
        return value
    #level-1
    elif(key=='age' and (value <=19 and level==1)):
        return '<=19'
    elif(key=='age' and (value >=20 and value <=39 and level==1)):
        return '20-39'
    elif(key=='age' and (value >=40 and value <=59 and level==1)):
        return '40-59'
    elif(key=='age' and (value >=60 and value <=79 and level==1)):
        return '50-79'
    elif(key=='age' and (value >=80 and  level==1)):
        return '>=80'
    #level-2
    elif(key=='age' and (value<=19 and level==2)):
        return '<=19'
    elif(key=='age' and (value<=79 and level==2)):
        return '<=79'
    elif(key=='age' and (value>=80 and level==2)):
        return '>=80'
    #level-3
    elif(key=='age' and level==3):
        return '*'

anonymiseData()

'''age_ranges = [
    {"age_start": 1, "age_end": 19, "level": 4, "description": "Children"},
    {"age_start": 20, "age_end": 39, "level": 4, "description": "Adults"},
    {"age_start": 40, "age_end": 59, "level": 4, "description": "Seniors"},
]

# Write data to CSV
with open("age_generalization.csv", "w") as file:
    writer = csv.DictWriter(file, fieldnames=["age_start", "age_end", "level", "description"])
    writer.writeheader()
    writer.writerows(age_ranges)
'''   
'''
def Generalization(quasibasedIdentifiers, level):

    for p in range(0, len(quasibasedIdentifiers)):
        if(quasibasedIdentifiers[p] not in ['age','education','marital-status','race']):
            return('The passed value is not recognized as a Quasi Identifier')

    if(len(quasibasedIdentifiers) != len(level)):
        return('Unable to obtain the correct level, ensure that the number of QuasiIdentifiers and Levels is the same')

    for p in range(0,len(quasibasedIdentifiers)):
        for q in range(0,Table[quasibasedIdentifiers[p]].count()):
            Table[quasibasedIdentifiers[p]][q] = Distortion_And_Precision_Calculation(quasibasedIdentifiers[p],
            Table[quasibasedIdentifiers[p]][q] ,level[p])
manageHierarchy = {'age': 3, 'education': 3, 'martial-status': 2, 'race': 2}
'''