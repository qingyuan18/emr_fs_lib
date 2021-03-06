import json
import datetime
import importlib.util
import numpy as np
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType
from pyspark.sql import SparkSession, DataFrame
from pyspark.rdd import RDD
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.functions import struct, concat, col, lit
import logging
from emr_fs.engine.spark.hudi_engine import HudiEngine


class FeatureStoreSparkEngine:

    APPEND = "append"
    OVERWRITE = "overwrite"

    def __init__(self,mode):
        self._spark_session = SparkSession.builder.appName("emr_feature_store app").master("yarn").config("spark.submit.deployMode","client")\
                                                   .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")\
                                                   .enableHiveSupport().getOrCreate()
        self._spark_context = self._spark_session.sparkContext
        self._spark_session.conf.set("hive.exec.dynamic.partition", "true")
        self._spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        self._spark_session.conf.set("spark.sql.hive.convertMetastoreParquet", "false")
        self.logger = logging
        self._mode = mode


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if self._mode=="local":
           self._spark_session.stop()
        return True

    def executeSql(self,sqlStr):
        df=self._spark_session.sql(sqlStr)
        retInfo = ""
        for line in df.collect():
            retInfo = retInfo+line["info_value"] + ","
        self.logger.info("executeSql result:"+retInfo)
        return retInfo

    def create_feature_store(self,name, desc, location):
        sql="create database if not exists @emr_feature_store@ comment 'emr_feature_store for sagemaker' location '@DBLocation@';".replace("@emr_feature_store@",name).replace("@DBLocation@",location)
        if desc is not None:
           sql.replace("emr_feature_store for sagemaker",desc)
        print("sql=="+sql)

        self._spark_session.sql(sql)

        self.logger.info("created emr feature store: "+name)

    def get_feature_group(self,feature_store_name,feature_group_name):
        sql = "show create table "+feature_store_name+ "."+feature_group_name+" AS SERDE"
        df=self._spark_session.sql(sql)
        feature_group_info = ""
        for line in df.collect():
            feature_group_info = feature_group_info+line["createtab_stmt"] + "\n"
        self.logger.info("get feature groups:"+feature_group_info)
        return feature_group_info

    def register_feature_group(self,
                                 feature_store_name,feature_group_name, desc,
                                 feature_unique_key,
                                 feature_partition_key):
        try:
          sql = "alter table  "+feature_store_name+".@feature_group_nm@ set tblproperties ('feature_unique_key'='@feature_unique_key@')".replace("@feature_group_nm@",feature_group_name).replace("@feature_unique_key@",feature_unique_key)
          self._spark_session.sql(sql)
          sql = "alter table  "+feature_store_name+".@feature_group_nm@ set tblproperties ('feature_partition_key'='@feature_partition_key@')".replace("@feature_group_nm@",feature_group_name).replace("@feature_partition_key@",feature_partition_key)
          df=self._spark_session.sql(sql)
          self.logger.warn("register emr feature group "+feature_group_name + "in "+ feature_store_name)
        except  Exception as e:
          print(str(e))
          self.logger.error(str(e))


    def create_feature_group(self,
                                 feature_store_name,feature_group_name, desc,
                                 feature_unique_key,
                                 feature_partition_key,
                                 features,location):

        self._spark_session.sql("use "+feature_store_name+";")
        sql="""CREATE EXTERNAL TABLE @feature_group_nm@(
          _hoodie_commit_time string,
          _hoodie_commit_seqno string,
          _hoodie_record_key string,
          _hoodie_partition_path string,
          _hoodie_file_name string,
          @features@)
        PARTITIONED BY (
          @feature_partitions@)
        ROW FORMAT SERDE
          'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT
          'org.apache.hudi.hadoop.HoodieParquetInputFormat'
        OUTPUTFORMAT
          'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION '@location@'
        TBLPROPERTIES (@tableProps@)"""

        tableProps="'feature_unique_key'='"+feature_unique_key+"',"
        tableProps=tableProps+"'feature_partition_key'='"+feature_partition_key+"'"
        columns=""
        for featureKey in features:
           columns=columns+featureKey+" "+features[featureKey]+",\n"
        columns=columns[:-2]
        sql=sql.replace("@features@",columns)
        sql=sql.replace("@tableProps@",tableProps)
        sql=sql.replace("@feature_partitions@",feature_partition_key)
        sql=sql.replace("@feature_group_nm@",feature_group_name)
        sql=sql.replace("@location@",location)
        try:
           df=self._spark_session.sql(sql)
           self.logger.info("create emr feature group "+feature_group_name + "in "+ feature_store_name+" result:")
           for result in df.collect():
               self.logger.info(result)
        except  Exception as e:
           print(str(e))
           self.logger.error(str(e))



    def show(self,full_query,lines):
        try:
          if lines != 0:
             self._spark_session.sql(full_query).show(lines)
          else :
             self._spark_session.sql(full_query).show()
        except  Exception as e:
             print(str(e))
             self.logger.error(str(e))

    def query(self,full_query):
        return self._spark_session.sql(full_query)


    def append_features(self, feature_store_name, feature_group_name, new_feature_key,new_feature_key_type):
        sql = "alter table "+feature_store_name+"."+feature_group_name+" add columns ("+new_feature_key+" "+new_feature_key_type+")"
        print("sql==="+sql)
        try:
           df=self._spark_session.sql(sql)
           self.logger.info("add new_feature :"+new_feature_key+":"+new_feature_key_type+" in "+feature_group_name)
        except Exception as e:
           raise FeatureStoreException(
                           "Error add new feature:" + str(e)
                       )

    def save_s3_dataset(
            self,
            feature_store_name,
            feature_group_name,
            feature_group_location,
            source_s3_location,
            mode,
            operation,
            feature_unique_key,
            feature_partition_key,
            features
        ):
            hudi_engine = HudiEngine(feature_store_name,feature_group_name,self._spark_context,self._spark_session)
            hudi_options = hudi_engine._setup_hudi_write_opts(operation, primary_key=feature_unique_key,partition_key=feature_partition_key,pre_combine_key=feature_partition_key)
            print(hudi_options)
            dataframe = self._spark_session.read.format("csv").option("header", "true").load(source_s3_location)
            try:
            ### change column type based on features mapping######
                for feature in features:
                    dataframe=dataframe.withColumn(feature._name,dataframe[feature._name].cast(feature._type))
                print(dataframe.printSchema())
                dataframe.write.format("org.apache.hudi").options(**hudi_options).mode(mode).save(feature_group_location)
            except  Exception as e:
                raise FeatureStoreException(
                    "Error writing to offline feature group :" + str(e)
                )


    def save_dataframe(
        self,
        feature_group_name,
        feature_group_location,
        dataframe,
        operation,
        feature_unique_key,
        feature_partition_key
    ):
        hudi_engine = HudiEngine(feature_group,self._spark_context,self._spark_session)
        hudi_options = hudi_engine._setup_hudi_write_opts(operation, primary_key=feature_unique_key,partition_key=feature_partition_key,pre_combine_key=feature_partition_key)
        try:
            dataframe.write.format("hudi").options(**hudi_options).mode("append").save(feature_group_location)
        except Exception as e:
            raise FeatureStoreException(
                "Error writing to offline feature group :" + str(e)
            )



    #def _sychronize_online_feature_group(self, feature_group_name, dataframe, sagemaker_fs):
    #   pass





