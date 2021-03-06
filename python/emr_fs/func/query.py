import json
import sys
from emr_fs.func.transformation_function import TransformationFunction
#sys.path.append("../")
from emr_fs.engine.spark.feature_store_spark_engine import FeatureStoreSparkEngine




class Query:
    def __init__(
        self,
        feature_store,
        feature_group,
        engine_type,
        emr_master_node,
        engine_mode
    ):
        self._feature_store = feature_store
        self._feature_group = feature_group
        self._join_feature_groups = []
        self._join_feature_group_keys = {}
        self._features = []
        self._sqlSelect = "select "
        self._sqlJoin = ""
        self._sqlWhere = ""
        if engine_type == "spark":
           self._engine = FeatureStoreSparkEngine(engine_mode)
        else:
           self._engine=FeatureStoreHiveEngine(emr_master_node)

    def select_all(self):
        """select all the feature group dataset.
        """
        self._features=[]
        for feature in self._feature_group._features:
             self._features.append(feature)
        return self

    def select(self,features=[]):
        """select subset of the feature group dataset.
        """
        self._features=[]
        for feature in features:
            self._features.append(feature)
        return self


    def pareseSql(self):
        #####select sction #######################
        full_query="select "
        for feature in self._features:
            if feature._feature_group_name == self._feature_group._feature_group_name:
                full_query = full_query + self._feature_group._feature_store._name+"."+self._feature_group._feature_group_name+"."+feature._name + ","
        if len(self._join_feature_groups)!= 0:
            for  join_feature_group in self._join_feature_groups:
                for feature in self._features:
                  if feature._feature_group_name == join_feature_group._feature_group_name:
                      full_query = full_query + join_feature_group._feature_store._name+"."+join_feature_group._feature_group_name +\
                                              "."+feature._name + ","
        full_query=full_query[:-1]
        full_query = full_query + " from " + self._feature_group._feature_store._name+"."+self._feature_group._feature_group_name
        #####join section##########################
        if len(self._join_feature_groups)!= 0:
            for  join_feature_group in self._join_feature_groups:
                full_query = full_query+ " left join "+join_feature_group._feature_store._name+"."+join_feature_group._feature_group_name +\
                                         " on "+self._feature_group._feature_store._name+"."+self._feature_group._feature_group_name + "." + \
                                         self._feature_group._feature_unique_key +"="+join_feature_group._feature_store._name+"."+join_feature_group._feature_group_name+"."+self._join_feature_group_keys[join_feature_group._feature_group_name]
                full_query = full_query + ","
            full_query=full_query[:-1]
        ######where section ######################
        if self._sqlWhere != "":
            full_query=full_query+" where "+self._sqlWhere
        print("full_query sql is:"+full_query)
        return full_query


    def show(self,lines:int):
        """Show the first N rows of the Query.
        """
        full_query = self.pareseSql()
        return self._engine.show(full_query,lines)



    def create_training_dataset(self,
            name,
            data_format,
            startDt,
            endDt,
            outputLoc):
        self.timeQuery(startDt,endDt)
        full_query = self.pareseSql()
        df = self._engine.query(full_query)
        transfer = TransformationFunction(self,outputLoc)
        transfer.save(df,data_format)



    def join(self,query,join_key):
        """join other query
        # Arguments
            query: another query instance.
            join_key:feature key to join each other.
        """
        self._join_feature_groups.append(query._feature_group)
        self._join_feature_group_keys[query._feature_group._feature_group_name]=join_key
        for join_feature in query._features:
            self._features.append(join_feature)
        if query._sqlWhere !="":
           self._sqlWhere = self._sqlWhere+" and " + query._sqlWhere
        return self

    def timeQuery(self,begin_timestamp,end_timestamp):
        self._sqlWhere = " "+self._feature_group._feature_group_name+"._hoodie_commit_time>='"+begin_timestamp + "' and "+self._feature_group._feature_group_name+"._hoodie_commit_time <='"+end_timestamp+"'"
        return self



