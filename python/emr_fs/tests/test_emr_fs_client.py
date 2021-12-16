import sys
from emr_fs.feature_store import FeatureStore
from emr_fs.client import Client

if __name__ == '__main__':
   client=Client()
   #test create feature store
   #emr_fs01 = client.create_feature_store("emr_feature_store","s3://emrfssampledata/emr_feature_store/","emr feature store test")
   #test connect to feature store
   emr_fs01 = client.connect_to_feature_store("emr_feature_store")
   print(emr_fs01._s3_store_path)

   #test create feature group
   #features01={"customer_id":"string","city_code":"string","state_code":"string","country_code":"string","dt":"string","identify_code":"string"}
   #emr_fg01 = client.create_feature_group("customer_base","","customer_id","dt",features01)
   emr_fg01 = emr_fs01.get_feature_group("customer_base")

   #features02 = {"customer_id":"string",	"age":"string","diabetes":"string","ejection_fraction":"string",	"high_blood_pressure":"string","platelets":"string","sex":"string","smoking":"init","DEATH_EVENT":"string","dt":"timestamp"}
   #emr_fg02 = client.create_feature_group("customer_advance","","customer_id","dt",features02)

   #test feature group ingestion
   #source_feature_group_dataset = "s3://emrfssampledata/feature_store_stringroduction_custs.csv"
   #emr_fg01.ingestion(source_feature_group_dataset)
   #source_feature_group_dataset = "s3://emrfssampledata/feature_store_customer_advance.csv"
   #emr_fg02.ingestion(source_feature_group_dataset)

   #test query feature group
   emr_fg01.select_all().show(0)
   #emr_fs01.select(["customer_id","city_code","state_code"]).show(5)
   #test feature group time travel query
   #emr_fs01.timeQuery("20211201000000","20211216000000").show(0)
   #test join
   #emr_fs01.select(["customer_id","city_code","state_code"]).join(emr_fg02.select_all()).show(5)
   #test train dataset retrive
   #emr_fg02.create_training_dataset(name = "userProfile dataset",\
   #            data_format = "tfrecord",\
   #            startDt="20211201000000",\
   #            endDt= "20211216000000",\
   #            outputLoc = "s3://emrfssampledata/traindataset/output.tfrecord/")







