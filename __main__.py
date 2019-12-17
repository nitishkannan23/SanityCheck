"""
Owner :- Anendra Gupta
last change:- 3/16/2018
"""
import traceback

"""
This module is where all the calls of the functions will happen. Functions which has been written n other files of this module
will get called from here.

"""

import ops
from pyspark.sql.functions import *
from pyspark.sql import *
import config
from pyspark import SparkContext
import logging
#setting up the logging configuration here.
logging.basicConfig(filename='/tmp/SanityChecks.log',level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

#Creating a spark context/session
sc = SparkContext()
spark = SparkSession(sc)
#print(sc.getConf().getAll())


#Function call for transaction sanity checks ops.transaction_data_sanity_checks(spark)
try:
    logger.info('Sanity Checks for Transaction data is in progress...')
    ops.transaction_data_sanity_checks(spark)
    logger.info('Transaction Sanity Finished.....')
except Exception as e:
    print(e, traceback.format_exc())
    logger.error(e,exc_info=True)

#Function call for Calender sanity checks

# try:
#     logger.info('Sanity Checks for Calender data is in progress...')
#     ops.calender_data_sanity_checks(spark)
#     logger.info('Calender Sanity Finished.....')
# except Exception as e:
#     print(e, traceback.format_exc())
#     logger.error(e,exc_info=True)
#
# #Function call for Inventory sanity checks
#
# try:
#     logger.info('Sanity Checks for Inventory data is in progress...')
#     ops.inventory_data_sanity_checks(spark)
#     logger.info('Inventory Sanity Finished.....')
# except Exception as e:
#     print(e, traceback.format_exc())
#     print(e)
#     logger.error(e,exc_info=True)
#
# #Function call for category sanity checks
#
# try:
#     logger.info('Sanity Checks for Category data is in progress...')
#     ops.category_data_sanity_checks(spark)
#     logger.info('Category Sanity Finished.....')
# except Exception as e:
#     print(e, traceback.format_exc())
#     print(e)
#     #logger.error(e,exc_info=True)
#
# #Function call for competitor sanity checks
# try:
#     logger.info('Sanity Checks for Competitor data is in progress...')
#     ops.competior_data_sanity_checks(spark)
#     logger.info('Category Sanity Finished.....')
# except Exception as e:
#     print(e, traceback.format_exc())
#     logger.error(e,exc_info=True)
#
# #Function call for Store sanity checks
#
# try:
#     logger.info('Sanity Checks for Store data is in progress...')
#     ops.store_data_sanity_checks(spark)
#     logger.info('Store Sanity Finished.....')
# except Exception as e:
#     print(e, traceback.format_exc())
#     logger.error(e,exc_info=True)
#
# #Function call for Online sanity checks
# try:
#     logger.info('Sanity Checks for Online data is in progress...')
#     ops.online_data_sanity_checks(spark)
#     logger.info('Online Sanity Finished.....')
# except Exception as e:
#     print(e, traceback.format_exc())
#     logger.error(e,exc_info=True)

#Writing the final result into a csv file.
#print(ops.sanity_result)
if not ops.sanity_result.empty:
    spark.createDataFrame(ops.sanity_result).coalesce(1).write.mode('overwrite').csv(config.output_file_path, header=True)

	
# ops.sanity_result.to_csv("sanity_result.csv", encoding='utf-8')