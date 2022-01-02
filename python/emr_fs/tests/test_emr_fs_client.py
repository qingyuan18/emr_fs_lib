import sys
from emr_fs.feature_store import FeatureStore
from emr_fs.client import Client

if __name__ == '__main__':
   client=Client.create("kernal")
   #test create feature store
   emr_fs01 = client.create_feature_store("emr_feature_store","s3://emrfssampledata/emr_feature_store/","emr feature store test")
   #test connect to feature store
   #emr_fs01 = client.connect_to_feature_store("emr_feature_store")


   #test create feature group
   features01={"customer_id":"string","city_code":"string","state_code":"string","country_code":"string","dt":"string"}
   emr_fg01 = client.create_feature_group("customer_base","","customer_id","dt",features01)
   #emr_fg01 = emr_fs01.get_feature_group("customer_base")
   emr_fg01.print_info()

   features02 = {"customer_id":"string",	"age":"string","diabetes":"string","ejection_fraction":"string",	"high_blood_pressure":"string","platelets":"string","sex":"string","smoking":"string","DEATH_EVENT":"string","dt":"string"}
   emr_fg02 = client.create_feature_group("customer_advance","","customer_id","dt",features02)
   #emr_fg02= emr_fs01.get_feature_group("customer_advance")
   emr_fg02.print_info()

   #test feature group ingestion
   source_feature_group_dataset = "s3://emrfssampledata/feature_store_customer_base.csv"
   emr_fg01.ingestion(source_feature_group_dataset,"overwrite")
   source_feature_group_dataset = "s3://emrfssampledata/feature_store_customer_advance.csv"
   emr_fg02.ingestion(source_feature_group_dataset,"overwrite")

   #test add feature
   #emr_fg01.add_feature("identify_code","int")
   #update_feature_group_dataset = "s3://emrfssampledata/feature_store_customer_base_update.csv"
   #emr_fg01.ingestion(update_feature_group_dataset,"append")

   #test query feature group
   #emr_fg01.select_all().show(0)
   #emr_fg01.select(["customer_id","city_code","state_code","identify_code"]).show(0)
   #test feature group time travel query
   #emr_fg01.select_all().timeQuery("20211201000000","20211216000000").show(0)
   #test join
   emr_fg01.select(["customer_id","city_code","state_code"]).join(emr_fg02.select(["age","diabetes"]),"customer_id").show(5)
   #test train dataset retrive
   #emr_fg02.select_all().create_training_dataset(name = "userProfile dataset",\
   #            data_format = "tfrecord",\
   #            startDt="20211201000000",\
   #            endDt= "20211228000000",\
   #            outputLoc = "s3://emrfssampledata/traindataset/output")
   #emr_fg01.select(["customer_id","city_code","state_code","identify_code"]).join(emr_fg02.select(["age","diabetes"]),"customer_id")\
   #                .create_training_dataset(name = "userProfile dataset",\
   #                                             data_format = "libsvm",\
   #                                             startDt="20211201000000",\
   #                                             endDt= "20211231000000",\
   #                                             outputLoc = "s3://emrfssampledata/traindataset/output")







