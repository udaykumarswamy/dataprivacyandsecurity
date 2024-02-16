import pandas as pd



def check_c_2_diversity(r_values, c):
    return r_values[0] < c * sum(r_values[1:])

def check_recursive_c_l_diversity(r_values, c, l):
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
# Step 3: Group the dataframe by quasi-identifiers and sensitive attribute
#anony_data_minimumk
#complete_anonymised_for_rec
dataframe = pd.read_csv('final/anony_data_minimum510k.csv')




#genearliseData(education,maritalstatus,age,race)
#genearliseData(2,2,1,2) ---> for 2-d
#for c-->0.5 genearliseData(1,1,2,1)
k=5
l=3
c=0.5 #2->3 1-->3 0.5->3  
#c=2 genearliseData(1,2,1,1) satisfies
#c=1 genearliseData(1,2,2,2) satisfies
#c=0.5 genearliseData(1,2,2,2) satisfies

# Step 4: Calculate counts and apply the diversity checks
def checkRecursiveDiversity(c,l,k):
    dose_not_count = 0
    satisfy_count = 0
    group_not_satisfy_k=0
    anonymised = pd.DataFrame()
    grouped_data = dataframe.groupby(['age', 'education', 'race', 'marital-status'])
    for _, group in dataframe.groupby(['age', 'education', 'race', 'marital-status']):
        r_values = group['occupation'].value_counts().values
        if len(group) >= k:
            if(l==2):
                if not check_c_2_diversity(r_values, c):
                    print("Does not satisfy (c, 2)-diversity")
                else:
                    print('it satisfies (c, l)-diversity')
                    anonymised = pd.concat([anonymised,group.sample(len(group)) ])
                    
            if(l>2):
                if not check_recursive_c_l_diversity(r_values, c, l):
                    print("Does not satisfy recursive (c, l)-diversity (l > 2)")
                    print(group)
                    dose_not_count +=1

                else:
                    anonymised = pd.concat([anonymised,group.sample(len(group))])
                    satisfy_count +=1
                    print('it satisfies recursive (c, l)-diversity (l > 2) condition')
        else:
            print('group not satisfying k')
            group_not_satisfy_k +=1
            
    print('group size:',len(grouped_data))
    print('dose_not_count:',dose_not_count)
    print('satisfy:',satisfy_count)
    print('k-not satisfy:',group_not_satisfy_k)
    return anonymised
        
anonymized_data=checkRecursiveDiversity(c,l,k)
if(c==0.5):
    anonymized_data.to_csv('final/recur/c_0_5.csv',index=False)
elif(c==1):
    anonymized_data.to_csv('final/recur/c_1.csv',index=False)
elif(c==2):
        anonymized_data.to_csv('final/recur/c_2.csv',index=False)

