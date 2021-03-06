import sys
import numpy
import datetime
sys.path.append("./func/")
sys.path.append("./engine/")
sys.path.append("./common/")
from emr_fs.func.transformation_function import TransformationFunction
#from emr_fs.common.logger import *
import logging
from emr_fs.feature_group import FeatureGroup
from emr_fs.feature_store import FeatureStore

class Client:
    _instance = None

    @classmethod
    def create(cls,engine_mode):
        if cls._instance is None:
            cls._instance = cls(engine_mode)
        return cls._instance

    def __init__(self,engine_mode):
        self._feature_group = None
        self._feature_store = None
        self._engine_mode = engine_mode
        self.logger = logging

    def create_feature_store(self,name,s3_location_path,desc):
        self._feature_store = FeatureStore(name,s3_location_path,desc,self._engine_mode)
        self._feature_store.create_feature_store()
        return self._feature_store

    def connect_to_feature_store(self,name):
        self._feature_store = FeatureStore(None,None,None,self._engine_mode).connect_to_feature_store(name)
        return self._feature_store

    def get_feature_group(self,name):
        if self._feature_store is None:
           self.logger.error("not connected to a feature_store")
           return None
        else:
           return self._feature_store.get_feature_group(name)

    def create_feature_group(self,name,desc,feature_unique_key,feature_partition_key,features):
         if self._feature_store is None:
            self.logger.error("not connected to a feature_store")
            return None
         else:
            return self._feature_store.create_feature_group(name,desc,feature_unique_key,feature_partition_key,features)
