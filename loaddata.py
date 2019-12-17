"""
This module is to load the data from path available in :obj:`SanityCheck.config` modules
"""

#All the requried import statements
import config
import schema
import pandas as pd

#Function to load the calender data
def get_calender_data(spark):

    """
    :param spark: spark session
    :return: calender DataFrame
    """
    return spark.read.csv(config.calender_file_path,
                                 # dateFormat=config.dateFormat,
                                 schema=schema.calender_schema, sep=",",
                                 mode=config.readmode)


def get_inventory_data(spark):
    """
    :param spark: spark session
    :return: inventory DataFrame
    """
    return spark.read.csv(config.inventory_file_path,
                             # dateFormat=config.dateFormat,
                             schema=schema.inventory_schema, sep="\t",
                             mode=config.readmode)

def get_category_data(spark):
    """
    :param spark: spark session
    :return: Hierarchy/category DataFrame
    """
    return spark.read.csv(config.hierarchy_file_path,
                             # dateFormat=config.dateFormat,
                             schema=schema.hierarchy_schema, sep="\t",
                             mode=config.readmode)


def get_competitor_data(spark):
    """
    :param spark: spark session
    :return: competitor DataFrame
    """
    return spark.read.csv(config.competitor_file_path,
                             # dateFormat=config.dateFormat,
                             schema=schema.competitor_schema, sep="|",
                             mode=config.readmode)

def get_store_data(spark):
    """
    :param spark: spark session
    :return: store DataFrame
    """
    return spark.read.csv(config.store_file_path,
                             # dateFormat=config.dateFormat,
                             schema=schema.store_schema, sep="|",
                             mode=config.readmode)

def get_online_data(spark):
    """
    :param spark: spark session
    :return: online DataFrame
    """
    return spark.read.csv(config.onlinedata_file_path,
                             # dateFormat=config.dateFormat,
                             schema=schema.online_schema, sep="\t",
                             mode=config.readmode)

def get_transaction_data(spark):
    """
    :param spark: spark session
    :return: transaction DataFrame
    """
    return spark.read.csv(config.transaction_file_path,
                                    # timestampFormat=config.timestampFormat,
                                    schema=schema.transaction_schema, sep= ",",
                                    mode=config.readmode)

def get_constraint_data(spark):
    return spark.read.csv(config.constraint_file_path,
                          # timestampFormat=config.timestampFormat,
                          header=True, inferSchema=True).toPandas()
    # mode=config.read_mode)
    # constraint_df = pd.read_csv('Constraint.csv')
    # return constraint_df
