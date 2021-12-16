from pyspark.mllib.util import MLUtils
from emr_fs.common import *
from pyspark.context import SparkContext, SparkConf


class TransformationFunction:
   def __init__(
        self,
        query,
        s3_target_path
   ):
        self._query = query
        self._s3_target_path = s3_target_path
        self.logger = Log("file")



   def save(self, df, format='tfrecords',path="s3://"):
        """
            transformate the feature store dataset into output format and landing to target location.
        """
        if path is not None:
            if format == 'tfrecords':
                path=path+".tfrecords"
                df.write.format("tfrecord").option("recordType", "Example").mode("overwrite").save(path)
            elif format == 'libsvm':
                path=path+".libsvm"
                convDf = df.rdd.map(lambda line: LabeledPoint(line[0],[line[1:]]))
                MLUtils.saveAsLibSVMFile(convDf, path)
            elif format == 'csv':
                path=path+".csv"
                df.write.format("csv").mode("overwrite").save(path)



