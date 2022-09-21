from copy import copy
from tabnanny import check
import snowflake.snowpark as sf
import pandas as pd

from fuzzywuzzy import fuzz, process
from snowflake.snowpark.functions import udtf
from snowflake.snowpark.functions import table_function

from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
from snowflake.snowpark import Window


import rapidfuzz
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
        self.source = ''
        self.bestMatchScore = 0
        self.matchedVal = ''

    def process(self,threshold, sourceVal, checkVal):
        self.source = sourceVal
        tempScore = fuzz.ratio(sourceVal, checkVal)
        if tempScore> threshold:
            if tempScore >= self.bestMatchScore:
                self.bestMatchScore = tempScore
                self.matchedVal = checkVal
            yield None
        
    def end_partition(self):
        yield (self.source, self.bestMatchScore, self.matchedVal)

    
    def read_DB_Data(self, sm_records='', pr_records='', state='',selectType='' ,providerType='',check=True):
          if (state):
            if (providerType):           
                if (selectType):    
                    if(selectType=='1'):
                        if(providerType=='1'):
                            self.pr_df = self.session.sql(
                                f"SELECT PROVIDER.PROVIDERID, FIRSTNAME, LASTNAME, PROVIDERADDRESSID, STREET1, STREET2, CITY, ZIPCODE,STATE FROM PROVIDER \
                                    INNER JOIN PROVIDERADDRESS ON PROVIDER.PROVIDERID = PROVIDERADDRESS.PROVIDERID  \
                                    WHERE State='{state}' AND PROVIDERTYPE = {providerType}"
                                ).toPandas()
                           
                            if not self.pr_df.empty :
                                # print(self.pr_df)
                                self.pr_df['DETAILS_PR'] = self.pr_df['FIRSTNAME'] + ' ' + self.pr_df['LASTNAME']+' '+self.pr_df['STREET1'].fillna('')+' '+self.pr_df['STREET2'].fillna('') +' '+self.pr_df['CITY'].fillna('') +' '+self.pr_df['ZIPCODE'].fillna('') 
                                self.pr_df['DETAILS_PR'].astype('string[pyarrow]')
                                self.pr_df.set_index('DETAILS_PR',inplace =False)
                                self.pr_df.dropna(subset=['DETAILS_PR'], how='any', inplace=True)  

                                if  check==False :  
                                    print(self.pr_df)
                                    self.session.write_pandas(self.pr_df, 'PROVIDER_DATAFRAME',auto_create_table=True)
                                    self.df_PROVIDER=self.session.table('PROVIDER_DATAFRAME',)
                                    print('Snowpark DF')
                                    print(self.df_PROVIDER.toPandas())
                                                             
                        if not self.pr_df.empty :
                            self.sm_df = self.session.sql(
                            f"SELECT {sm_records} IMPORTID, FIRSTNAME ,LASTNAME, Address, Suite, City, Zip,STATE FROM DBT_RMURAHARISETTY.STANDAREDMAPPER \
                                WHERE PROVIDERID IS NULL AND State='{state}' AND PROVIDERTYPE = {providerType}  \
                                AND IMPORTID NOT IN (SELECT IMPORTID FROM PROVIDERSTANDARDMAPPER_SDF)\
                                AND IMPORTID NOT IN (SELECT IMPORTID FROM PROB_SM_USED_DATA_SDF)"
                                ).toPandas()   
                           
                            if not self.sm_df.empty:
                                # print(self.sm_df)                                        
                                self.sm_df['DETAILS_SM'] = self.sm_df['FIRSTNAME'] + ' ' + self.sm_df['LASTNAME']+' '+self.sm_df['ADDRESS'].fillna('')+' '+self.sm_df['SUITE'].fillna('')+' '+self.sm_df['CITY'].fillna('')+' '+self.sm_df['ZIP'].fillna('')
                                self.sm_df['DETAILS_SM'].astype('string[pyarrow]')
                                self.sm_df.set_index('DETAILS_SM',inplace =False)            
                                self.sm_df.dropna(subset=['DETAILS_SM'], how='any', inplace=True)  
                                # self.SnowParkDF= self.pr_df[['DETAILS_PR']].copy()
                                # self.SnowParkDF['DETAILS_SM']= self.sm_df[['DETAILS_SM']].copy()
                                if  check==False :  
                                    print(self.sm_df)
                                    self.session.write_pandas(self.sm_df, 'STANDAREDMAPPER_DATAFRAME',auto_create_table=True)
                                    self.df_STANDARDMAPPER=self.session.table('STANDAREDMAPPER_DATAFRAME',)
                                    print('Snowpark DF')
                                    print(self.df_STANDARDMAPPER.toPandas())
                                

                                # if  check==False :  
                                #     print(self.SnowParkDF)
                                #     self.session.write_pandas(self.SnowParkDF, 'SNOWPARK_DATAFRAME',auto_create_table=True)
                                #     self.df_snowpark=self.session.table('SNOWPARK_DATAFRAME',)
                                #     print('Snowpark DF')
                                #     print(self.df_snowpark.toPandas())
                                                       
                    
    def MatchDetails_SnowPark_DF(self,Threshhold=50, selectType='',providerType=''):
        print('MatchDetails_SnowPark_DF')
        crossJoinedData = self.df_STANDARDMAPPER.select("DETAILS_SM").crossJoin(self.df_PROVIDER.select("DETAILS_PR"))  
        crossJoinedData.create_or_replace_view("CROSS_JOINED_Fuzzy_UDTF")
        
        fuzzy_wuzzy_udtf = table_function("fuzzywuzzy_value_udtf")
        result = crossJoinedData.join_table_function(fuzzy_wuzzy_udtf(crossJoinedData.col("DETAILS_PR"), crossJoinedData.col("DETAILS_SM")).over(partition_by="DETAILS_SM",order_by="DETAILS_SM"))
        result_pd=result.select(F.col("SOURCE"), F.col("SCORE"), F.col("MATCHED")).to_pandas()  
        print(result_pd)
        result_pd.sort_values(by='SCORE', ascending=False)
        self.session.write_pandas(result_pd, 'PROVIDERREGISTRY_MATCHED_UDTF')
        drop = self.session.sql(f"Delete from STANDAREDMAPPER_DATAFRAME")
        drop.collect()  
        drop = self.session.sql(f"Delete from PROVIDER_DATAFRAME")
        drop.collect()


if __name__=='__main__':
    start = time.time()
    db = SFdatabase()
    chunkSize=1000  
    for providerType in range(1, 2, 1):
        providerType = str(providerType)
        states = db.session.sql(
                            f"SELECT  DISTINCT STATE  FROM DBT_RMURAHARISETTY.STANDAREDMAPPER \
                                WHERE PROVIDERID IS NULL  AND PROVIDERTYPE = '{providerType}'  \
                                AND IMPORTID NOT IN (SELECT IMPORTID FROM PROVIDERSTANDARDMAPPER_SDF)\
                                AND IMPORTID NOT IN (SELECT IMPORTID FROM PROB_SM_USED_DATA_SDF) \
                                ORDER BY STATE"
                            ).toPandas()  

        states = states['STATE']  
        print(states)
        for state in states:
                    
            db.read_DB_Data(f'', '', state, '1', providerType,True)          
            pr_row=len(db.pr_df)
            if (pr_row>0) :
                rows = db.session.sql(f"SELECT COUNT(*) FROM DBT_RMURAHARISETTY.STANDAREDMAPPER \
                WHERE PROVIDERID IS NULL AND STATE = '{state}' AND PROVIDERTYPE = '{providerType}'\
                AND IMPORTID NOT IN (SELECT IMPORTID FROM PROB_SM_USED_DATA_SDF )").toPandas()

                rows = len(db.sm_df)  #rows['COUNT(*)']              
                batches = int(rows / chunkSize) + 1               
                operation="Details"
                msg="Total PR Data ::"+str(pr_row)+" Total SM Data ::"+ str(rows)+" Batch Size ::"+ str(batches) +"  "+ " Chunk Size ::"+str(chunkSize)+" State ::"+str(state)+" ProviderType ::"+str(providerType)
                db.session.sql(f"INSERT INTO LOGS_SDF (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
                for i in range(1, batches, 1):        
                    selecttype='1'
                    providerType = str(providerType)
                    operation="Iteration Number"
                    msg=i
                    db.session.sql(f"INSERT INTO LOGS_SDF (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
                    operation="Read Data"
                    msg="Start"
                    db.session.sql(f"INSERT INTO LOGS_SDF (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
                    db.read_DB_Data(f'TOP {chunkSize}', '', state, selecttype, providerType,False)   
                    operation="Matching Data"
                    msg="Start"
                    db.session.sql(f"INSERT INTO LOGS_SDF (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
                    db.MatchDetails_SnowPark_DF(50)                                
                    msg="END"
                    db.session.sql(f"INSERT INTO LOGS_SDF (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
    db.close()
