import sys
import numpy
import datetime
sys.path.append("./func/")
sys.path.append("./engine/")
sys.path.append("./common/")
from emr_fs.func.transformation_function import TransformationFunction
from emr_fs.common.logger import *
from emr_fs.feature_group import FeatureGroup
from emr_fs.feature_store import FeatureStore

class Client(object):
    _instance = None

    def __new__(cls, *args, **kw):
         if cls._instance is None:
            cls._instance = object.__new__(cls, *args, **kw)
         return cls._instance

    def __init__(self):
        self._feature_group = None
        self._feature_store = None
        self.logger = Log("console")

    def create_feature_store(self,name,s3_location_path,desc):
        self._feature_store = FeatureStore(name,s3_location_path,desc)
        self._feature_store.create_feature_store()
        return self._feature_store

    def connect_to_feature_store(self,name):
        self._feature_store = FeatureStore(name,None,None)
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
