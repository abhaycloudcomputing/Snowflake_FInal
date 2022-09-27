
from copy import copy
from email import message
from tabnanny import check
import snowflake.snowpark as sf
import pandas as pd
import numpy as np

from fuzzywuzzy import fuzz, process
from snowflake.snowpark.functions import udtf
from snowflake.snowpark.functions import table_function

from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
from snowflake.snowpark import Window
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

    def Read_DB_Data(self, state='' ,providerType='',check=True):
        if (state):
            if (providerType):  
                # DATA for Given State and Provider Type 1 
                if(providerType=='1'):
                    self.pr_df = self.session.sql(
                        f"SELECT  PROVIDER.PROVIDERID, FIRSTNAME, LASTNAME, PROVIDERADDRESSID, STREET1, STREET2, CITY, ZIPCODE,STATE FROM PROVIDER \
                            INNER JOIN PROVIDERADDRESS ON PROVIDER.PROVIDERID = PROVIDERADDRESS.PROVIDERID  \
                            WHERE State='{state}' AND PROVIDERTYPE = {providerType}"
                        ).toPandas()
                    
                    if not self.pr_df.empty :
                        # print(self.pr_df)
                        self.pr_df['ZIPCODE_5']=np.where(self.pr_df['ZIPCODE'].fillna('').astype(str).str.len() <= 5, 
                            self.pr_df['ZIPCODE'],
                            self.pr_df['ZIPCODE'].astype(str).str[:5])

                        self.pr_df['NAME_PR'] = self.pr_df['FIRSTNAME'] + ' ' + self.pr_df['LASTNAME']
                        self.pr_df['ADDRESS_PR'] = self.pr_df['STREET1'].fillna('')+' '+self.pr_df['STREET2'].fillna('') +' '+self.pr_df['CITY'].fillna('') +' '+self.pr_df['ZIPCODE_5'].fillna('') 
                    
                        self.pr_df['DETAILS_PR'] = self.pr_df['FIRSTNAME'] + ' ' + self.pr_df['LASTNAME']+' '+self.pr_df['STREET1'].fillna('')+' '+self.pr_df['STREET2'].fillna('') +' '+self.pr_df['CITY'].fillna('') +' '+self.pr_df['ZIPCODE_5'].fillna('') 
                        self.pr_df['DETAILS_PR'].astype('string[pyarrow]')
                        self.pr_df.set_index('DETAILS_PR',inplace =False)
                        self.pr_df.dropna(subset=['DETAILS_PR'], how='any', inplace=True)
                        self.pr_df.dropna(subset=['ADDRESS_PR'], how='any', inplace=True)
                        print(len(self.pr_df))
                        if not self.pr_df.empty:     
                            if  check==False :  
                                print('Dataframe DF pr_df')
                                print(len(self.pr_df))                            
                                self.session.write_pandas(self.pr_df, 'PROVIDER_DATAFRAME',auto_create_table=True)
                                self.df_PROVIDER=self.session.table('PROVIDER_DATAFRAME',)
                                print('Snowpark Table DF')
                                print(self.df_PROVIDER.toPandas())
                                print(len(self.df_PROVIDER.toPandas()))                                                             
                    
                        self.sm_df = self.session.sql(
                        f"SELECT TOP 200 IMPORTID, FIRSTNAME ,LASTNAME, Address, Suite, City, Zip,STATE FROM DBT_RMURAHARISETTY.STANDAREDMAPPER \
                            WHERE PROVIDERID IS NULL AND State='{state}' AND PROVIDERTYPE = {providerType}  \
                            AND IMPORTID NOT IN (SELECT IMPORTID FROM PROVIDERSTANDARDMAPPER_SDF)\
                            AND IMPORTID NOT IN (SELECT IMPORTID FROM PROB_SM_USED_DATA_SDF)"
                            ).toPandas()   
                    
                        if not self.sm_df.empty:    
                            self.sm_df_matched=  self.sm_df['IMPORTID'].copy()                                                                       
                            self.sm_df['ZIP_5']=np.where(self.sm_df['ZIP'].fillna('').astype(str).str.len() <= 5, 
                            self.sm_df['ZIP'],
                            self.sm_df['ZIP'].astype(str).str[:5])                                
                            self.sm_df['NAME_SM']=self.sm_df['FIRSTNAME'] + ' ' + self.sm_df['LASTNAME']  
                            self.sm_df['ADDRESS_SM'] = self.sm_df['ADDRESS'].fillna('')+' '+self.sm_df['SUITE'].fillna('')+' '+self.sm_df['CITY'].fillna('')+' '+self.sm_df['ZIP_5'].fillna('')
                            self.sm_df['DETAILS_SM'] = self.sm_df['FIRSTNAME'] + ' ' + self.sm_df['LASTNAME']+' '+self.sm_df['ADDRESS'].fillna('')+' '+self.sm_df['SUITE'].fillna('')+' '+self.sm_df['CITY'].fillna('')+' '+self.sm_df['ZIP_5'].fillna('')
                            self.sm_df['DETAILS_SM'].astype('string[pyarrow]')
                            self.sm_df.set_index('DETAILS_SM',inplace =False)
                            self.sm_df.dropna(subset=['DETAILS_SM'], how='any', inplace=True)
                            self.sm_df.dropna(subset=['ADDRESS_SM'], how='any', inplace=True)

                        if not self.sm_df.empty: 
                            if  check==False :                                 
                                print('Dataframe sm_df')
                                print(len(self.sm_df))                               
                                self.session.write_pandas(self.sm_df, 'STANDAREDMAPPER_DATAFRAME',auto_create_table=True)
                                self.df_STANDARDMAPPER=self.session.table('STANDAREDMAPPER_DATAFRAME')
                                print('Snowpark Table DF')
                                print(self.df_STANDARDMAPPER.toPandas())
                                print(len(self.df_STANDARDMAPPER.toPandas()))
                # DATA for Given State and Provider Type 1 
                if(providerType=='2'):
                    self.pr_df = self.session.sql(
                        f"SELECT  PROVIDER.PROVIDERID,FACILITYNAME, PROVIDERADDRESSID, STREET1, STREET2, CITY, ZIPCODE,STATE FROM PROVIDER \
                            INNER JOIN PROVIDERADDRESS ON PROVIDER.PROVIDERID = PROVIDERADDRESS.PROVIDERID  \
                            WHERE State='{state}' AND PROVIDERTYPE = {providerType}"
                        ).toPandas()
                    
                    if not self.pr_df.empty :

                        # print(self.pr_df)
                        self.pr_df['ZIPCODE_5']=np.where(self.pr_df['ZIPCODE'].fillna('').astype(str).str.len() <= 5, 
                            self.pr_df['ZIPCODE'],
                            self.pr_df['ZIPCODE'].astype(str).str[:5])

                        self.pr_df['NAME_PR'] = self.pr_df['FACILITYNAME'] 
                        self.pr_df['ADDRESS_PR'] = self.pr_df['STREET1'].fillna('')+' '+self.pr_df['STREET2'].fillna('') +' '+self.pr_df['CITY'].fillna('') +' '+self.pr_df['ZIPCODE_5'].fillna('') 
                    
                        self.pr_df['DETAILS_PR'] = self.pr_df['FACILITYNAME'] +self.pr_df['STREET1'].fillna('')+' '+self.pr_df['STREET2'].fillna('') +' '+self.pr_df['CITY'].fillna('') +' '+self.pr_df['ZIPCODE_5'].fillna('') 
                        self.pr_df['DETAILS_PR'].astype('string[pyarrow]')
                        self.pr_df.set_index('DETAILS_PR',inplace =False)
                        self.pr_df.dropna(subset=['DETAILS_PR'], how='any', inplace=True)
                        self.pr_df.dropna(subset=['ADDRESS_PR'], how='any', inplace=True)

                        if  check==False :  
                            print('Dataframe DF pr_df')
                            print(len(self.pr_df))
                            print(self.pr_df)
                            self.session.write_pandas(self.pr_df, 'PROVIDER_DATAFRAME',auto_create_table=True)
                            self.df_PROVIDER=self.session.table('PROVIDER_DATAFRAME',)
                            print('Snowpark Table DF')
                            print(self.df_PROVIDER.toPandas())
                            print(len(self.df_PROVIDER.toPandas()))                                                             
                    
                        self.sm_df = self.session.sql(
                        f"SELECT IMPORTID, FACILITYNAME Address, Suite, City, Zip,STATE FROM DBT_RMURAHARISETTY.STANDAREDMAPPER \
                            WHERE PROVIDERID IS NULL AND State='{state}' AND PROVIDERTYPE = {providerType}  \
                            AND IMPORTID NOT IN (SELECT IMPORTID FROM PROVIDERSTANDARDMAPPER_SDF)\
                            AND IMPORTID NOT IN (SELECT IMPORTID FROM PROB_SM_USED_DATA_SDF)"
                            ).toPandas()   
                        self.sm_df_matched=  self.sm_df['IMPORTID'].copy() 

                        if not self.sm_df.empty:                                                                          
                            self.sm_df['ZIP_5']=np.where(self.sm_df['ZIP'].fillna('').astype(str).str.len() <= 5, 
                            self.sm_df['ZIP'],
                            self.sm_df['ZIP'].astype(str).str[:5])                                
                            self.sm_df['NAME_SM']=self.sm_df['FACILITYNAME'] 
                            self.sm_df['ADDRESS_SM'] = self.sm_df['ADDRESS'].fillna('')+' '+self.sm_df['SUITE'].fillna('')+' '+self.sm_df['CITY'].fillna('')+' '+self.sm_df['ZIP_5'].fillna('')
                            self.sm_df['DETAILS_SM'] = self.sm_df['FACILITYNAME'] +' '+self.sm_df['ADDRESS'].fillna('')+' '+self.sm_df['SUITE'].fillna('')+' '+self.sm_df['CITY'].fillna('')+' '+self.sm_df['ZIP_5'].fillna('')
                            self.sm_df['DETAILS_SM'].astype('string[pyarrow]')
                            self.sm_df.dropna(subset=['DETAILS_SM'], how='any', inplace=True)
                            self.sm_df.dropna(subset=['ADDRESS_SM'], how='any', inplace=True)

                            if  check==False :  
                                print(self.sm_df)
                                print('Dataframe sm_df')
                                print(len(self.sm_df))
                                print(self.sm_df)
                                self.session.write_pandas(self.sm_df, 'STANDAREDMAPPER_DATAFRAME',auto_create_table=True)
                                self.df_STANDARDMAPPER=self.session.table('STANDAREDMAPPER_DATAFRAME',)
                                print('Snowpark Table DF')
                                print(self.df_STANDARDMAPPER.toPandas())
                                print(len(self.df_STANDARDMAPPER.toPandas()))
                        

    def MatchDetails_SnowPark_DF(self,Threshhold=''):
        if not self.sm_df.empty:             
                if not self.pr_df.empty :   
                    if(providerType=='1') :
                        self.DataSetID=1
                    else:                        
                        self.DataSetID=2 

                    crossJoinedData = self.df_STANDARDMAPPER.select("DETAILS_SM").crossJoin(self.df_PROVIDER.select("DETAILS_PR"))  
                    crossJoinedData.create_or_replace_view("CROSS_JOINED_Fuzzy_UDTF")
                    
                    fuzzy_wuzzy_udtf = table_function("fuzzywuzzy_value_udtf")
                    result = crossJoinedData.join_table_function(fuzzy_wuzzy_udtf(crossJoinedData.col("DETAILS_PR"), crossJoinedData.col("DETAILS_SM"))\
                        .over(partition_by="DETAILS_PR",order_by="DETAILS_PR"))

                    result_pd=result.select(result.SOURCE.alias("DETAILS_PR"),result.SCORE.alias('CONFIDENCE_SCORE'), result.MATCHED.alias('DETAILS_SM'))\
                        .filter(result.SCORE>Threshhold).to_pandas()
                        
                    # self.df_Combined =result_pd.merge(self.pr_df,on='DETAILS_PR').merge(self.sm_df,on='DETAILS_SM')

                    self.final_df =result_pd.merge(self.df_PROVIDER.toPandas(),on='DETAILS_PR').merge(self.df_STANDARDMAPPER.toPandas(),on='DETAILS_SM')\
                        .drop_duplicates().sort_values(by='CONFIDENCE_SCORE', ascending=False)

                    dropsm = self.session.sql(f"DROP TABLE STANDAREDMAPPER_DATAFRAME")
                    dropsm.collect()  

                    droppr = self.session.sql(f"DROP TABLE PROVIDER_DATAFRAME")
                    droppr.collect()

                    print(self.final_df)
                    self.final_df.drop(
                    axis=1 ,
                    columns=['FIRSTNAME_x','LASTNAME_x','FIRSTNAME_y','LASTNAME_y',
                    'CITY_x','CITY_y','STATE_x','STATE_y','ADDRESS','SUITE','STREET1','STREET2','ZIP_5','ZIPCODE_5'], 
                    inplace=True
                    )     
                    print(self.final_df)
                    self.final_df.dropna(subset=['PROVIDERID'], how='any', inplace=True)
                    self.final_df['CONTAINERID'] = 2
                    self.final_df['DATASETID'] = self.DataSetID                     
                    self.final_df['STATUS'] = 0
                    self.final_df['MATCHEDON'] = 0
                    print(self.final_df)

    def Update_OthersScore(self):
        if not self.sm_df.empty:             
                if not self.pr_df.empty :
                        if not self.final_df.empty:
                            self.final_df['NAME_CONFIDENCE_SCORE']= \
                            self.final_df.apply(lambda x:fuzz.token_sort_ratio(x["NAME_SM"].upper(), x["NAME_PR"].upper()),axis=1)
                    
                            self.final_df['ADDRESS_CONFIDENCE_SCORE']= \
                            self.final_df.apply(lambda x:fuzz.ratio(x["ADDRESS_SM"].upper(), x["ADDRESS_PR"].upper()),axis=1)

    def toSnowflake_PSM(self):
       if not self.sm_df.empty:             
                if not self.pr_df.empty :
                        if not self.final_df.empty:
                         self.session.write_pandas(self.final_df, 'PROVIDERSTANDARDMAPPER_SDF')

    def toSnowflake_Pro_Used_SMDATA(self):       
        if not self.sm_df_matched.empty:              
                self.session.write_pandas(pd.DataFrame(self.sm_df_matched), 'PROB_SM_USED_DATA_SDF')         

if __name__=='__main__':
    start = time.time()
    db = SFdatabase()    
    threshold=80
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
                    
            db.Read_DB_Data( state, providerType,True)          
            pr_row=len(db.pr_df)
            if (pr_row>0) :
                sm_row = len(db.sm_df)
                operation="Details"
                msg="Total PR Data ::"+str(pr_row)+" Total SM Data ::"+ str(sm_row)+" State ::"+str(state)+" ProviderType ::"+str(providerType)
                db.session.sql(f"INSERT INTO LOGS_SDF (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()               
                providerType = str(providerType)               
                operation="Read Data"
                msg="Start"
                db.session.sql(f"INSERT INTO LOGS_SDF (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
                db.Read_DB_Data(state,  providerType,False)   
                operation="Matching Data"
                msg="Start"
                db.session.sql(f"INSERT INTO LOGS_SDF (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
                db.MatchDetails_SnowPark_DF(threshold)   
                db.Update_OthersScore()                            
                msg="END"
                db.session.sql(f"INSERT INTO LOGS_SDF (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
                operation="Write to Snowflake"
                msg="Start"
                print("Write to  Snowflake Start")
                db.session.sql(f"INSERT INTO LOGS (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()                
                db.toSnowflake_PSM()
                db.toSnowflake_Pro_Used_SMDATA()
                msg="end"
                print("Write to  Snowflake END")
                db.session.sql(f"INSERT INTO LOGS (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()                

    db.close()