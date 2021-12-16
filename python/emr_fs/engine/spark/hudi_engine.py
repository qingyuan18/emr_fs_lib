


class HudiEngine:

    def __init__(
        self,
        feature_store_name,
        feature_group_name,
        spark_context,
        spark_session,
    ):
        print("here0===")
        self._feature_store_name=feature_store_name
        self._feature_group_name = feature_group_name
        self._spark_context = spark_context
        self._spark_session = spark_session
        self.HIVE_NON_PARTITION_EXTRACTOR_CLASS_OPT_VAL = "org.apache.hudi.hive.NonPartitionedExtractor"
        self.HUDI_COPY_ON_WRITE = "COPY_ON_WRITE"
        self.HUDI_BULK_INSERT = "bulk_insert"
        self.HUDI_INSERT = "insert"
        self.HUDI_UPSERT = "upsert"
        self.HUDI_QUERY_TYPE_OPT_KEY = "hoodie.datasource.query.type"
        self.HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot"
        self.HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental"
        self.HUDI_BEGIN_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.begin.instanttime"
        self.HUDI_END_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.end.instanttime"
        self.PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class"
        self.PAYLOAD_CLASS_OPT_VAL = "org.apache.hudi.common.model.EmptyHoodieRecordPayload"
        self.HUDI_WRITE_INSERT_DROP_DUPLICATES = "hoodie.datasource.write.insert.drop.duplicates"
        self.HUDI_SCHEMA_MERGE = "hoodie.mergeSchema"
        self.HUDI_SPARK_FORMAT = "org.apache.hudi"
        self.HUDI_TABLE_NAME = "hoodie.table.name"
        self.HUDI_TABLE_STORAGE_TYPE = "hoodie.datasource.write.storage.type"
        self.HUDI_TABLE_OPERATION = "hoodie.datasource.write.operation"
        self.HUDI_RECORD_KEY = "hoodie.datasource.write.recordkey.field"
        self.HUDI_PARTITION_FIELD = "hoodie.datasource.write.partitionpath.field"
        self.HUDI_PRECOMBINE_FIELD = "hoodie.datasource.write.precombine.field"
        self.HUDI_HIVE_SYNC_ENABLE = "hoodie.datasource.hive_sync.enable"
        self.HUDI_HIVE_SYNC_TABLE = "hoodie.datasource.hive_sync.table"
        self.HUDI_HIVE_SYNC_DB = "hoodie.datasource.hive_sync.database"
        self.HUDI_HIVE_SYNC_JDBC_URL = "hoodie.datasource.hive_sync.jdbcurl"
        self.HUDI_HIVE_SYNC_PARTITION_FIELDS = "hoodie.datasource.hive_sync.partition_fields"
        self.HUDI_HIVE_SYNC_SUPPORT_TIMESTAMP = "hoodie.datasource.hive_sync.support_timestamp"
        self.HUDI_KEY_GENERATOR_OPT_KEY = "hoodie.datasource.write.keygenerator.class"
        self.HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL = "org.apache.hudi.keygen.CustomKeyGenerator"
        self.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY = "hoodie.datasource.hive_sync.partition_extractor_class"
        self.DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL = "org.apache.hudi.hive.MultiPartKeysValueExtractor"





    def _setup_hudi_write_opts(self, operation, primary_key,partition_key,pre_combine_key):
        hudi_options = {
            self.HUDI_PRECOMBINE_FIELD: pre_combine_key,
            self.HUDI_RECORD_KEY: primary_key,
            self.HUDI_PARTITION_FIELD: partition_key,
            self.HUDI_TABLE_NAME: self._feature_group_name,
            self.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY: self.DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL,
            self.HUDI_HIVE_SYNC_ENABLE: "true",
            self.HUDI_HIVE_SYNC_TABLE: self._feature_group_name,
            self.HUDI_HIVE_SYNC_DB: self._feature_store_name,
            self.HUDI_HIVE_SYNC_PARTITION_FIELDS: partition_key,
            self.HUDI_TABLE_OPERATION: operation,
            self.HUDI_SCHEMA_MERGE:"true"
        }
        return hudi_options

    def _setup_hudi_read_opts(self, start_timestamp, end_timestamp):

        hudi_options = {
            self.HUDI_QUERY_TYPE_OPT_KEY: self.HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL,
            self.HUDI_BEGIN_INSTANTTIME_OPT_KEY: start_timestamp,
            self.HUDI_END_INSTANTTIME_OPT_KEY: end_timestamp,
        }
        return hudi_options



