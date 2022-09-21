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
                            pro_df_snowpark=self.session.table('PROVIDER')
                            pradd_df_snowpark=self.session.table('PROVIDERADDRESS')  
                            pr_df_Snowpark_All=pro_df_snowpark.join(pradd_df_snowpark,pro_df_snowpark.col('PROVIDERID')==pradd_df_snowpark.col('PROVIDERID'))\
                                .select(pro_df_snowpark.PROVIDERID.alias('PROVIDERID'),'*').\
                                filter(pradd_df_snowpark.col('STATE')==state).filter(pro_df_snowpark.col('providerType')==providerType)

                            self.pr_df_Snowpark_Selected=pr_df_Snowpark_All.\
                                select(pr_df_Snowpark_All.col('FIRSTNAME'),pr_df_Snowpark_All.col('LASTNAME'),pr_df_Snowpark_All.col('PROVIDERID'),\
                                F.concat( pr_df_Snowpark_All.col('FIRSTNAME'),pr_df_Snowpark_All.col('LASTNAME')).alias('NAME'),\
                                F.concat( pr_df_Snowpark_All.col('FIRSTNAME'), pr_df_Snowpark_All.col('LASTNAME'),pr_df_Snowpark_All.col('STREET1'),\
                                pr_df_Snowpark_All.col('STREET2'),pr_df_Snowpark_All.col('CITY'),pr_df_Snowpark_All.col('ZIPCODE').substr(1,5)).alias('DETAILS_PR'),\
                                F.concat(pr_df_Snowpark_All.col('STREET1'),pr_df_Snowpark_All.col('STREET2'),pr_df_Snowpark_All.col('CITY'),pr_df_Snowpark_All.col('ZIPCODE').substr(1,5)).alias('ADDRESS_PR'),\
                                pr_df_Snowpark_All.col('ZIPCODE'))
                           
                            if self.pr_df_Snowpark_Selected.count() >0:                              
                                # self.pr_df_Snowpark_Selected=self.pr_df_Snowpark_Selected.select('*').filter(self.pr_df_Snowpark_Selected.col('DETAILS_PR')!='NULL')
                                self.pr_df_Snowpark_Selected.dropna(subset=['DETAILS_PR'], how='any')   
                                                             
                                if self.pr_df_Snowpark_Selected.count()>0 :
                                    stdmap_df_snowpark=self.session.table('DBT_RMURAHARISETTY.STANDAREDMAPPER')
                                    prostdmap_df_snowpark=self.session.table('PROVIDERSTANDARDMAPPER')
                                    probuseddata_df_snowpark=self.session.table('PROB_SM_USED_DATA')

        
                                    stdmap_df_snowpark=stdmap_df_snowpark.select('*').\
                                        filter(stdmap_df_snowpark.col('PROVIDERID').isNull()).\
                                        filter(stdmap_df_snowpark.col('STATE')==state).\
                                        filter(stdmap_df_snowpark.col('providerType')==providerType).\
                                        filter(~stdmap_df_snowpark.col('IMPORTID').isin(prostdmap_df_snowpark.select(prostdmap_df_snowpark.IMPORTID))).\
                                        filter(~stdmap_df_snowpark.col('IMPORTID').isin(probuseddata_df_snowpark.select(probuseddata_df_snowpark.IMPORTID)))

                                    self.sm_df_snowpark_Selected=stdmap_df_snowpark.select(stdmap_df_snowpark.col('IMPORTID'),\
                                        F.concat( stdmap_df_snowpark.col('FIRSTNAME'), stdmap_df_snowpark.col('LASTNAME')).alias('NAME_SM'),\
                                        F.concat( stdmap_df_snowpark.col('FIRSTNAME'), stdmap_df_snowpark.col('LASTNAME'),stdmap_df_snowpark.col('ADDRESS'),\
                                        stdmap_df_snowpark.col('SUITE'),stdmap_df_snowpark.col('CITY'),stdmap_df_snowpark.col('ZIP').substr(1,5)).alias('DETAILS_SM'),\
                                        F.concat(stdmap_df_snowpark.col('ADDRESS'),stdmap_df_snowpark.col('SUITE'),stdmap_df_snowpark.col('CITY'),stdmap_df_snowpark.col('ZIP').substr(1,5)).alias('ADDRESS_SM'),\
                                        stdmap_df_snowpark.col('ZIP'))
                                
                                    if  self.sm_df_snowpark_Selected.count()>0:                               
                                        self.sm_df_snowpark_Selected.dropna(subset=['DETAILS_SM'], how='any')

                                # print(self.sm_df_snowpark_Selected.toPandas())
                                # print(self.pr_df_Snowpark_Selected.toPandas())
                               
                                                       
                    
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
            pr_row=db.pr_df_Snowpark_Selected.count()
            if (pr_row>0) :
                rows = db.sm_df_snowpark_Selected.count()  #rows['COUNT(*)']              
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
                    # db.MatchDetails_SnowPark_DF(50)                                
                    msg="END"
                    db.session.sql(f"INSERT INTO LOGS_SDF (TIME, OPERATION, LOG) VALUES (CURRENT_TIMESTAMP(3), '{operation}', '{msg}')").collect()
    db.close()
