from copy import copy
from itertools import count
import snowflake.snowpark as sf
import numpy as np
import pandas as pd
from fuzzywuzzy import process as fwProcess 
# import rapidfuzz
from rapidfuzz import fuzz , process
import time

class SFdatabase:

    secret_keys = {
        'user':'SVC_PYTHONIDE', 
        'password':'Welcome12345', 
        'account':'ti97672.east-us-2.azure', 
        'warehouse': 'DEV', 
        'database':'DEV_PROVIDER_REGISTRY_CLONE_GALAXE', 
        'schema':'DBO'
        }

    def __init__(self):
        # self.session = session
        self.session = sf.Session.builder.configs(self.secret_keys).create()

    def read_DB_Data(self, sm_records='', pr_records='', state='',selectType='' ,providerType='',):
          if (state):
            # print (state)
            if (providerType):           
                # print(providerType)
                if (selectType):    
                    # print(selectType)       
                    if(selectType=='1'):
                        if(providerType=='1'):
                        # Provider registry
                            self.pr_df = self.session.sql(
                                f"SELECT {pr_records} PROVIDER.PROVIDERID, FIRSTNAME, LASTNAME, PROVIDERADDRESSID, STREET1, STREET2, CITY, ZIPCODE FROM PROVIDER \
                                    INNER JOIN PROVIDERADDRESS ON PROVIDER.PROVIDERID = PROVIDERADDRESS.PROVIDERID  \
                                    WHERE State='{state}' AND PROVIDERTYPE = {providerType}"
                                ).toPandas()
                            # print(self.pr_df)
                            self.pr_df['ZIPCODE_5']=np.where(self.pr_df['ZIPCODE'].fillna('').astype(str).str.len() <= 5, 
                                   self.pr_df['ZIPCODE'],
                                   self.pr_df['ZIPCODE'].astype(str).str[:5])

                            self.pr_df['NAME_PR'] = self.pr_df['FIRSTNAME'] + ' ' + self.pr_df['LASTNAME']
                            self.pr_df['ADDRESS_PR'] = self.pr_df['STREET1'].fillna('')+' '+self.pr_df['STREET2'].fillna('') +' '+self.pr_df['CITY'].fillna('') +' '+self.pr_df['ZIPCODE_5'].fillna('') 
                           
                            self.pr_df['DETAILS'] = self.pr_df['FIRSTNAME'] + ' ' + self.pr_df['LASTNAME']+' '+self.pr_df['STREET1'].fillna('')+' '+self.pr_df['STREET2'].fillna('') +' '+self.pr_df['CITY'].fillna('') +' '+self.pr_df['ZIPCODE_5'].fillna('') 
                            self.pr_df['DETAILS'].astype('string[pyarrow]')
                            self.pr_df.set_index('DETAILS',inplace =False)
                            self.pr_df.dropna(subset=['DETAILS'], how='any', inplace=True)
                            # print(self.pr_df)
                            # print("PR DF Size",len(self.pr_df))

                            self.sm_df = self.session.sql(
                            f"SELECT {sm_records} IMPORTID, FIRSTNAME ,LASTNAME, Address, Suite, City, Zip FROM DBT_RMURAHARISETTY.STANDAREDMAPPER \
                                WHERE PROVIDERID IS NULL AND State='{state}' AND PROVIDERTYPE = {providerType}  \
                                AND IMPORTID NOT IN (SELECT IMPORTID FROM PROVIDERSTANDARDMAPPER)\
                                AND IMPORTID NOT IN (SELECT IMPORTID FROM PROB_SM_USED_DATA)"
                            ).toPandas()   
                            self.sm_df_sf=  self.sm_df['IMPORTID'].copy()  
                            # print("SM DF Size",len(self.sm_df))
                            self.sm_df['ZIP_5']=np.where(self.sm_df['ZIP'].fillna('').astype(str).str.len() <= 5, 
                                   self.sm_df['ZIP'],
                                   self.sm_df['ZIP'].astype(str).str[:5])
                            
                            self.sm_df['NAME_SM']=self.sm_df['FIRSTNAME'] + ' ' + self.sm_df['LASTNAME']  
                            self.sm_df['ADDRESS_SM'] = self.sm_df['ADDRESS'].fillna('')+' '+self.sm_df['SUITE'].fillna('')+' '+self.sm_df['CITY'].fillna('')+' '+self.sm_df['ZIP_5'].fillna('')
                                           
                            self.sm_df['DETAILS'] = self.sm_df['FIRSTNAME'] + ' ' + self.sm_df['LASTNAME']+' '+self.sm_df['ADDRESS'].fillna('')+' '+self.sm_df['SUITE'].fillna('')+' '+self.sm_df['CITY'].fillna('')+' '+self.sm_df['ZIP_5'].fillna('')
                            self.sm_df['DETAILS'].astype('string[pyarrow]')
                            # self.sm_df.set_index('DETAILS',inplace =False)            
                            self.sm_df.dropna(subset=['DETAILS'], how='any', inplace=True)
                            # print(self.sm_df)
                            # self.sm_df_sf                           
                            # print(self.sm_df_sf)
                          
                        if(providerType=='2'):
                        # Provider registry
                            self.pr_df = self.session.sql(
                                f"SELECT {pr_records} PROVIDER.PROVIDERID, FACILITYNAME, PROVIDERADDRESSID, STREET1, STREET2, CITY, ZIPCODE FROM PROVIDER \
                                    INNER JOIN PROVIDERADDRESS ON PROVIDER.PROVIDERID = PROVIDERADDRESS.PROVIDERID  \
                                    WHERE State='{state}' AND PROVIDERTYPE = {providerType}"
                                ).toPandas()
                            # print(self.pr_df)
                            self.pr_df['ZIPCODE_5']=np.where(self.pr_df['ZIPCODE'].fillna('').astype(str).str.len() <= 5, 
                                   self.pr_df['ZIPCODE'],
                                   self.pr_df['ZIPCODE'].astype(str).str[:5])
                            self.pr_df['NAME_PR'] = self.pr_df['FACILITYNAME'] 
                            self.pr_df['ADDRESS_PR'] = self.pr_df['STREET1'].fillna('')+' '+self.pr_df['STREET2'].fillna('') +' '+self.pr_df['CITY'].fillna('') +' '+self.pr_df['ZIPCODE_5'].fillna('') 
                           
                            self.pr_df['DETAILS'] = self.pr_df['FACILITYNAME'].fillna('')+ self.pr_df['STREET1'].fillna('')+' '+self.pr_df['STREET2'].fillna('') +' '+self.pr_df['CITY'].fillna('') +' '+self.pr_df['ZIPCODE_5'].fillna('') 
                            self.pr_df['DETAILS'].astype('string[pyarrow]')
                            self.pr_df.set_index('DETAILS',inplace =False)
                            self.pr_df.dropna(subset=['DETAILS'], how='any', inplace=True)

                            self.sm_df = self.session.sql(
                            f"SELECT {sm_records} IMPORTID, FACILITYNAME, Address, Suite, City, Zip FROM DBT_RMURAHARISETTY.STANDAREDMAPPER \
                                WHERE PROVIDERID IS NULL AND State='{state}' AND PROVIDERTYPE = {providerType}  \
                                AND IMPORTID NOT IN (SELECT IMPORTID FROM PROVIDERSTANDARDMAPPER) \
                                AND IMPORTID NOT IN (SELECT IMPORTID FROM PROB_SM_USED_DATA)"
                            ).toPandas()
                            self.sm_df['NAME_SM']=self.sm_df['FACILITYNAME']
                            self.sm_df['ZIP_5']=np.where(self.sm_df['ZIP'].fillna('').astype(str).str.len() <= 5, 
                                   self.sm_df['ZIP'],
                                   self.sm_df['ZIP'].astype(str).str[:5])
                            self.sm_df['ADDRESS_SM'] = self.sm_df['ADDRESS'].fillna('')+' '+self.sm_df['SUITE'].fillna('')+' '+self.sm_df['CITY'].fillna('')+' '+self.sm_df['ZIP_5'].fillna('')
                              
                            self.sm_df['DETAILS'] = self.sm_df['FACILITYNAME'].fillna('') +' '+self.sm_df['ADDRESS'].fillna('')+' '+self.sm_df['SUITE'].fillna('')+' '+self.sm_df['CITY'].fillna('')+' '+self.sm_df['ZIP_5'].fillna('')
                            self.sm_df['DETAILS'].astype('string[pyarrow]')
                            self.sm_df.set_index('DETAILS',inplace =False)            
                            self.sm_df.dropna(subset=['DETAILS'], how='any', inplace=True)
                            # print(self.sm_df)
                    
                    else:
                        print("Enter SelectType")   
                else:
                    print("Enter ProviderType")
            else:
                print("Enter State") 

    
    def matchDetails(self, selectType='',providerType=''):
        if(selectType=='1'):
            if(providerType=='1') :
                self.DataSetID=1
            else:                        
                self.DataSetID=2   
                       
        CONFIDENCE_SCORE=50
        self.final_df=self.Rapid_Merge(self.sm_df, self.pr_df, 'DETAILS', 'DETAILS', CONFIDENCE_SCORE)
        df_filter = self.final_df[self.final_df['matches'].isna() == False]
        print(df_filter)
        if not df_filter.empty:
            y = zip(df_filter['matches'], df_filter['DETAILS'])
            x = dict(zip( df_filter['matches'],df_filter['DETAILS']))
            self.pr_df['DETAILS_bkp']= self.pr_df['DETAILS'].replace(x)
            combined_df = pd.merge(self.final_df, self.pr_df, how='left', left_on='DETAILS', right_on='DETAILS_bkp')
           
            combined_df['DETAILS_SM'] = combined_df['DETAILS_bkp']
            combined_df['MATCHES'] = combined_df['matches']
            combined_df.drop(
                axis=1 ,
                columns=['matches', 'FIRSTNAME_x','LASTNAME_x','FIRSTNAME_y','LASTNAME_y',
                'CITY_x','CITY_y','DETAILS_bkp','DETAILS_x','DETAILS_y','ADDRESS','SUITE','STREET1','STREET2','ZIP_5','ZIPCODE_5'], 
                inplace=True
                )           

            # print(sm_df_PROVIDERSTANDARDMAPPER)  
            combined_df.dropna(subset=['PROVIDERID'], how='any', inplace=True)
            combined_df['CONTAINERID'] = 2
            combined_df['DATASETID'] = self.DataSetID
            combined_df['CONFIDENCE_SCORE_NAME'] = 0  
            combined_df['CONFIDENCE_SCORE_ADDRESS'] = 0  
            combined_df['STATUS'] = 0
            combined_df['MATCHEDON'] = 0
            print(combined_df)
            self.final_df = combined_df

    def Update_Score(self):
         if not self.final_df.empty:
            self.final_df['CONFIDENCE_SCORE_NAME']= \
            self.final_df.apply(lambda x:fuzz.token_sort_ratio(x["NAME_SM"].upper(), x["NAME_PR"].upper()),axis=1)
    
            self.final_df['CONFIDENCE_SCORE_ADDRESS']= \
            self.final_df.apply(lambda x:fuzz.ratio(x["ADDRESS_SM"].upper(), x["ADDRESS_PR"].upper()),axis=1)


    def Rapid_Merge(self, df_1, df_2, key1, key2, threshold, limit=1):
        """
        :param df_1: the left table to join
        :param df_2: the right table to join
        :param key1: key column of the left table
        :param key2: key column of the right table
        :param threshold: how close the matches should be to return a match, based on Levenshtein distance
        :param limit: the amount of matches that will get returned, these are sorted high to low
        :return: dataframe with boths keys and matches
        """
        s = df_2[key2].tolist()
        # m = df_1[key1].apply(lambda x: rapidfuzz.fuzz.ratio(x, s))
        # df_1['matches'] = m        
        # print(df_1['matches'])

        m = df_1[key1].apply(lambda x: process.extract(x, s,limit=limit))       
        df_1['matches'] = m        
        print(m) 
        if not df_1['matches'].empty:      
            m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
            t2 = df_1['matches'].apply(lambda x: ', '.join([ str(i[1]) for i in x if i[1] >= threshold]))
         #  m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
            df_1['matches'] = m2
            df_1['CONFIDENCE_SCORE'] = t2      
        return df_1

    def toSnowflake_PSM(self):
        if not self.final_df.empty:
            self.session.write_pandas(self.final_df, 'PROVIDERSTANDARDMAPPER')

    def toSnowflake_Pro_Used_SMDATA(self):
        # self.sm_df.drop(                            
        #                     columns=['matches','FIRSTNAME','LASTNAME','CITY','ADDRESS','SUITE','ZIP','DETAILS'],
        #                     inplace=True,
        #                     axis=1                        
        #                     )
        # print("Prob Used DF")
        # print(self.sm_df)
        if not self.sm_df_sf.empty:              
                self.session.write_pandas(pd.DataFrame(self.sm_df_sf), 'PROB_SM_USED_DATA')        


    def close(self):
        self.session.close()
    
    def log_sf(session, operation, msg):
        session.sql(f"INSERT INTO LOGS (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), ''{operation}'', ''{msg}'')").collect()
        return 'New log created'


if __name__=='__main__':
    start = time.time()
    db = SFdatabase()
    chunkSize=100
    print(f'Read Start time: {float((time.time() - start)) / 60} minutes')
    print('Reading data...')
    for providerType in range(1, 2, 1):
        # states = db.session.sql(f"SELECT DISTINCT STATE FROM DBT_RMURAHARISETTY.STANDAREDMAPPER WHERE PROVIDERID IS NULL ").toPandas()
        providerType = str(providerType)
        states = db.session.sql(
                            f"SELECT  DISTINCT STATE  FROM DBT_RMURAHARISETTY.STANDAREDMAPPER \
                                WHERE PROVIDERID IS NULL  AND PROVIDERTYPE = '{providerType}'  \
                                AND IMPORTID NOT IN (SELECT IMPORTID FROM PROVIDERSTANDARDMAPPER)\
                                AND IMPORTID NOT IN (SELECT IMPORTID FROM PROB_SM_USED_DATA) \
                                ORDER BY STATE"
                            ).toPandas()  

        states = states['STATE']  

        # print (states)
        for state in states:
            print("State is ::",state)
            print("PT is :",providerType)           
            db.read_DB_Data(f'', '', state, '1', providerType)          
            pr_row=len(db.pr_df)
            if (pr_row>0) :
                rows = db.session.sql(f"SELECT COUNT(*) FROM DBT_RMURAHARISETTY.STANDAREDMAPPER \
                WHERE PROVIDERID IS NULL AND STATE = '{state}' AND PROVIDERTYPE = '{providerType}'\
                AND IMPORTID NOT IN (SELECT IMPORTID FROM PROB_SM_USED_DATA )").toPandas()
                print(rows)
                rows = rows['COUNT(*)']              
                batches = int(rows / chunkSize) + 1               
                operation="Details"
                msg="Total PR Data ::"+str(pr_row)+" Total SM Data ::"+ str(rows)+" Batch Size ::"+ str(batches) +"  "+ " Chunk Size ::"+str(chunkSize)+" State ::"+str(state)+" ProviderType ::"+str(providerType)
                print(msg)
                db.session.sql(f"INSERT INTO LOGS (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
                for i in range(1, batches, 1):        
                    selecttype='1'
                    providerType = str(providerType)
                    operation="Iteration Number"
                    msg=i
                    print(i)
                    db.session.sql(f"INSERT INTO LOGS (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
                    operation="Read Data"
                    msg="Start"
                    db.session.sql(f"INSERT INTO LOGS (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
                    db.read_DB_Data(f'TOP {chunkSize}', '', state, selecttype, providerType)   
                    operation="Matching Data"
                    msg="Start"
                    print("Matching Data Start")
                    db.session.sql(f"INSERT INTO LOGS (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
                    db.matchDetails(selecttype,providerType) 
                    db.Update_Score()
                    # db.Rapid_Merge_Row_Level(db.final_df,"NAME_SM","NAME_PM",50,1) 
                    # db.toSnowflake_Pro_Used_SMDATA() 
                    operation="Write to Snowflake"
                    msg="Start"
                    print("Write to  Snowflake Start")
                    db.session.sql(f"INSERT INTO LOGS (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()                
                    db.toSnowflake_PSM()     
                    db.toSnowflake_Pro_Used_SMDATA()               
                    msg="END"
                    db.session.sql(f"INSERT INTO LOGS (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
    db.close()