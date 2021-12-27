import numpy as np
from emr_fs.engine.spark.feature_store_spark_engine import FeatureStoreSparkEngine
from emr_fs.func.query import Query
from emr_fs.feature import Feature



class FeatureGroup:
    def __init__(self, feature_store,feature_group_name,feature_group_desc,feature_unique_key,feature_partition_key,features):
        self._feature_store = feature_store
        self._feature_group_name = feature_group_name
        self._feature_group_desc = feature_group_desc
        self._feature_unique_key = feature_unique_key
        self._feature_partition_key = feature_partition_key
        self._features = features
        self._query = None


    def delete(self):
        with FeatureStoreSparkEngine(emr_master_node) as engine:
             engine.delete(self.feature_group_name)

    def select_all(self):
        """Select all features in the feature group and return a query object.
        The query can be used to construct joins of feature groups or create a
        training dataset immediately.
        # Returns
            `Query`. A query object with all features of the feature group.
        """
        if self._query is None:
          self._query = Query(self._feature_store,self,'spark',None)
        self._query.select_all()
        return self._query

    def timeQuery(self,beginTimeStamp,endTimeStamp):
        if self._query is None:
           self._query = Query(self._feature_store,self,'spark',None)
           self._query.timeQuery(beginTimeStamp,endTimeStamp)
        return self._query

    def select(self, features):
       """Select a subset of features of the feature group and return a query object.
       """
       if self._query is None:
          self._query = Query(self._feature_store,self,'spark',None)
       filtedFeatures=[]
       for feature_name in features:
          for feature in self._features:
             if feature._name == feature_name:
                filtedFeatures.append(feature)
       return self._query.select(filtedFeatures)

    def ingestion(self,dataframe):
       """use spark engine(which will use hudi engine internal) to ingest  into feature group"""
       feature_group_location = self._feature_store._s3_store_path+"/"+self._feature_group_name+"/"
       with FeatureStoreSparkEngine() as engine:
            engine.save_dataframe(
                           self._feature_group_name,
                           feature_group_location,
                           dataframe,
                           "append",
                           self._feature_unique_key,
                           self._feature_partition_key)

    def ingestion(self,source_dataset_location,mode):
       """use spark engine(which will use hudi engine internal) to ingest  into feature group"""
       feature_group_location = self._feature_store._s3_store_path+"/"+self._feature_group_name+"/"
       print("feature_partition_key:"+self._feature_partition_key)
       print("feature_unique_key:"+self._feature_unique_key)
       with FeatureStoreSparkEngine() as engine:
            engine.save_s3_dataset(
                           self._feature_store._name,
                           self._feature_group_name,
                           feature_group_location,
                           source_dataset_location,
                           mode,
                           "upsert",
                           self._feature_unique_key,
                           self._feature_partition_key)

    def print_info(self):
        print("*************feature group details*************")
        print("feature store name:"+self._feature_store._name)
        print("feature group name:"+self._feature_group_name)
        print("feature_unique_key:"+self._feature_unique_key)
        print("feature_partition_key:"+self._feature_partition_key)
        print("all features:")
        for feature in self._features:
            print("   "+feature._name +":"+feature._type)
        print("***********************************************")

    def create_training_dataset(self,
            name ,
            data_format,
            startDt,
            endDt,
            outputLoc ):
        if self._query is None:
            self._query = Query(self._feature_store,self,'spark',None)
        self._query.create_training_dataset(name,data_format,startDt,endDt,outputLoc)

    def add_feature(self, new_feature_name,new_feature_type):
        new_feature = Feature(self._feature_group_name,new_feature_name,new_feature_type)
        with FeatureStoreSparkEngine() as engine:
            engine.append_features(self._feature_store._name, self._feature_group_name, new_feature_name,new_feature_type)
        self._features.append(new_feature)
        print("added new feature:"+new_feature_name+" "+new_feature_type)

    def get_feature(self, name: str):
        """Retrieve a `Feature` object from the schema of the feature group.
        Returns:
            [type]: [description]
        """
        try:
            return self._features.__getitem__(name)
        except KeyError:
            raise FeatureStoreException(
                f"'FeatureGroup' object has no feature called '{name}'."
            )





    @property
    def primary_key(self):
        """List of features building the primary key."""
        return self._primary_key

    @primary_key.setter
    def primary_key(self, new_primary_key):
        self._primary_key = [pk.lower() for pk in new_primary_key]
