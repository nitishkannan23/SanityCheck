"""
This module contains the configuration required for spark and also the path of all the files which need to be load

"""


#Below are the paths for all the files.


transaction_file_path = "gs://group-on/Manoj/jobrun/Sept27/Transaction/valid/TransactionValidProfectusFormat_28sept_718pm.csv"
hierarchy_file_path = ["gs://group-on/day-wise/18-sept-2018/data-science/18-sept-category"]
calender_file_path = ['gs://shopko-bucket/gpn_cal_check']
store_file_path = ['gs://shopko-data/profectus-format/store-data']
competitor_file_path = ['gs://data-science-profectus/19-sept-2018/competition/']
onlinedata_file_path = ['gs://group-on/Manoj/jobrun/Transaction26sept']
inventory_file_path = ['gs://group-on/day-wise/18-sept-2018/inventory_category_join']

constraint_file_path = "gs://shopko-bucket/integrationv1/data/sanityconstraints.csv"
output_file_path = "gs://shopko-bucket/integrationv1/outputdata/sanitycheck/Sanity_Groupon.csv"
# "demo"
# "N-C-SHOPKO-FALL-1-2-55-45-145-143-7121-712160-B SWEET"
#SPARK CONFIGURATIONS:

readmode="PERMISSIVE"
repartitions  = 20*3
min_executors = '25'
init_executor = '1000'
executor_memory = '2586m'
executor_cores = '2'
