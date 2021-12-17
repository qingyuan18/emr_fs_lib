import numpy
import datetime
import re
from emr_fs.func.transformation_function import TransformationFunction
from emr_fs.engine.spark.feature_store_spark_engine import FeatureStoreSparkEngine
from emr_fs.common.util import *
from emr_fs.feature_group import FeatureGroup
from emr_fs.feature import Feature



class FeatureStore:
    def __init__(
        self,
        featurestore_name,
        s3_store_path,
        featurestore_description,
    ):
        self._name = featurestore_name
        self._description = featurestore_description
        self._s3_store_path = s3_store_path


    def connect_to_feature_store(self,feature_store_name):
        with FeatureStoreSparkEngine() as engine:
             feature_store_info = engine.executeSql("desc database "+feature_store_name)
             self._name = feature_store_info.split(",")[0]
             otherInfo = feature_store_info.split(",")[1:]
             for info in otherInfo:
                 if "//" in info:
                    self._s3_store_path = info
        return self



    def get_feature_group(self, feature_group_name):
        feature_group = None
        feature_group_info=""
        with FeatureStoreSparkEngine() as engine:
            feature_group_info =  engine.get_feature_group(self._name,feature_group_name)
        feature_unique_key = ""
        feature_partition_key = ""
        features =[]
        feature_group_info=feature_group_info.replace("\\n","").replace("\n","")
        matchObjs = re.findall(r'[(](.*?)[)]', feature_group_info)
        for matchObj in matchObjs:
            print("here1=="+matchObj)
            if "," in matchObj and " " in matchObj:
               #tableColumns
               tableColumns = matchObj.split(",")
               for column in tableColumns:
                   if "hoodie" in column:
                       continue
                   print("here0=="+column)
                   feature_name = column.split(" ")[0].replace("`","").replace("'","")
                   print("here1=="+column.split(" "))
                   feature_type = column.split(" ")[1]
                   feature = Feature(feature_group_name,feature_name,feature_type)
                   features.append(feature)
            elif "," in matchObj and "=" in matchObj:
               tablePros=matchObj.split(",")
               for property in tablePros:
                    if 'feature_unique_key' in property:
                       feature_unique_key=property.split("=")[1]
                    elif 'feature_partition_key' in property:
                       feature_partition_key=property.split("=")[1]
            elif " " in matchObj:
               feature_name = column.split(" ")[0].replace("`","").replace("'","")
               feature_type = column.split(" ")[1]
               feature = Feature(feature_group_name,feature_name,feature_type)
               features.append(feature)
        feature_group = FeatureGroup(self,feature_group_name,"",feature_unique_key,feature_partition_key,features)
        return feature_group




    def create_feature_store(self):
        """Create a feature store db in hive metadata."""
        #with FeatureStoreHiveEngine(emr_master_node) as engine:
        #   engine.create_feature_store(
        #        master_node=self._emr_master_node,
        #        name=self._name,
        #        desc=self._description,
        #        location=self._s3_store_path)
        with FeatureStoreSparkEngine() as engine:
            engine.create_feature_store(self._name,self._description,self._s3_store_path)
        return FeatureStore(self._name,self._description,self._s3_store_path)


    def register_feature_group(
        self,
        feature_group_name,
        desc,
        feature_unique_key,
        feature_partition_key
    ):
       """register a feature group metadata object in a exsiting hudi table.
            # Returns
                `FeatureGroup`. The feature group metadata object.
       """
       #with FeatureStoreHiveEngine(emr_master_node) as engine:
       #      engine.register_feature_group(feature_store_name=self.feature_store_name,feature_group_name, desc,
       #           feature_unique_key,feature_unique_key_type,
       #           feature_partition_key,feature_partition_key_type,
       #           feature_normal_keys)
       #            )
       with FeatureStoreSparkEngine() as engine:
             engine.register_feature_group(self._name,feature_group_name, desc,
                  feature_unique_key,
                  feature_partition_key)
             return get_feature_group(self._name, feature_group_name)




    def create_feature_group(
        self,
        feature_group_name,
        desc,
        feature_unique_key,
        feature_partition_key,
        feature_keys
    ):
        """Create a feature group metadata object.
        # Returns
            `FeatureGroup`. The feature group metadata object.
        """
        #with FeatureStoreHiveEngine(emr_master_node) as engine:
        #     engine.create_feature_group(feature_store_name=self.feature_store_name,feature_group_name, desc,
        #           feature_unique_key,feature_unique_key_type,
        #           feature_partition_key,feature_partition_key_type,
        #           feature_normal_keys)
        #    )
        with FeatureStoreSparkEngine() as engine:
             engine.create_feature_group(self._name,feature_group_name, desc,
                          feature_unique_key,
                          feature_partition_key,
                          feature_keys,
                          self._s3_store_path+"/"+feature_group_name)
        features = pares_features(feature_group_name,feature_keys)
        print("created feature group:"+feature_group_name)
        return FeatureGroup(self,feature_group_name,desc,feature_unique_key,feature_partition_key,features)







    @property
    def featurestore_name(self):
        """Name of the feature store."""
        return self._featurestore_name


    @property
    def s3_store_path(self):
        """Description of the feature store."""
        return self._s3_store_path


