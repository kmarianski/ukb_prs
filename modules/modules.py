import dxpy
import dxdata
import pandas as pd
import pyspark
import pyspark.pandas as ps
import re
from distutils.version import LooseVersion

print("quick start:")
print("")
print("analysis = Analysis()")
print("df = analysis.retrieve_data(covs) #covs must be a dictionary e.g. 'eid': 'IID'")

class Analysis:
    def __init__(self):
        conf = pyspark.SparkConf().set("spark.kryoserializer.buffer.max", '1024m')
        self.sc = pyspark.SparkContext(conf=conf)
        self.spark = pyspark.sql.SparkSession(self.sc)

        dispensed_dataset = dxpy.find_one_data_object(
            typename="Dataset", 
            name="app*.dataset", 
            folder="/", 
            name_mode="glob")
        dispensed_dataset_id = dispensed_dataset["id"]
        self.dataset = dxdata.load_dataset(id=dispensed_dataset_id)
        self.participant = self.dataset['participant']

    def fields_for_id(self, field_id):
        field_id = str(field_id)
        fields = self.participant.find_fields(name_regex=r'^{}(_i\d+)?(_a\d+)?$'.format(field_id))
        return sorted(fields, key=lambda f: LooseVersion(f.name))

    def retrieve_fields(self, covs):
        fields = []
        for field in covs.keys():
            try:
                fields.append(self.fields_for_id(field)[0])
            except:
                continue      
        
        return fields

    def retrieve_data(self, covs, cohort=None):
        if cohort == None:
            filter_sql = None
        else:
            filter_sql = cohort.sql
            
        fields = self.retrieve_fields(covs)
        df = self.participant.retrieve_fields(fields=fields, filter_sql = filter_sql, engine=dxdata.connect(
            dialect="hive+pyspark", 
            connect_args=
            {
                'config':{'spark.kryoserializer.buffer.max':'512m','spark.sql.autoBroadcastJoinThreshold':'-1'}
                     
            })).toPandas()
        # df.rename(columns=covs, inplace=True)
        # df = df.to_pandas()
        # print(f'shape of the df: {df.shape}')
        return df