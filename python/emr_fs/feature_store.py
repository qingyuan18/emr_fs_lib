import numpy
import datetime
import re
from func.transformation_function import TransformationFunction
from engine.spark import FeatureStoreSparkEngine


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



    def get_feature_group(self, name: str):
        with FeatureStoreSparkEngine() as engine:
            feature_group_info =  engine.get_feature_group(name)
            feature_unique_key = ""
            feature_partition_key = ""
            features = []

            matchObj = re.findall(r'[(](.*?)[)]', feature_group_info)[0]
            tableColumns = matchObj[0]
            tablePros = matchObj[1]

            for property in tablePros:
                if 'feature_unique_key' in property:
                   feature_unique_key=property.split("=")[1]
                elif 'feature_partition_key' in property:
                   feature_partition_key=property.split("=")[1]

            for column in tableColumns:
                name = column.split(" ")[0]
                type = column.split(" ")[1]
                feature = Feature(name,type)
                features.append(feature)

            feature_group = FeatureGroup(self,name,"",feature_unique_key,feature_partition_key,features)
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
                   engine.create_feature_store(
                        name=self._name,
                        desc=self._description,
                        location=self._s3_store_path)
        return FeatureStore(name,desc,location)


    def register_feature_group(
        self,
        feature_group_name: str,
        desc: str = "",
        feature_unique_key: str,
        feature_partition_key: str,
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
             engine.register_feature_group(feature_store_name=self.feature_store_name,feature_group_name, desc,
                  feature_unique_key,feature_unique_key_type,
                  feature_partition_key,feature_partition_key_type,
                  feature_normal_keys)
                   )



    def create_feature_group(
        self,
        feature_group_name: str,
        desc: str = "",
        feature_unique_key: str,
        feature_partition_key: str,
        feature_keys:  = {}
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
        with FeatureStoreSparkEngine(emr_master_node) as engine:
                    engine.create_feature_group(feature_store_name=self.feature_store_name,feature_group_name, desc,
                          feature_unique_key,feature_unique_key_type,
                          feature_partition_key,feature_partition_key_type,
                          feature_normal_keys)
                   )
        features = pares_features(feature_keys)
        return FeatureGroup(feature_group_name,desc,feature_unique_key,feature_partition_key,features)







    @property
    def featurestore_name(self):
        """Name of the feature store."""
        return self._featurestore_name


    @property
    def s3_store_path(self):
        """Description of the feature store."""
        return self._s3_store_path


