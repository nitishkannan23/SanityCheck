"""
This module contains all the function for the operations which all we need to perform
"""
import config
import loaddata
import pandas as pd

# Creating an empty data frame to write result into it
sanity_result = pd.DataFrame()

"""
Transaction data sanity function is to perform the sanity operation on transaction data.
In this function we are reading the respective data and then calling required constraint functions.

"""


def check_data(df, datatype, spark):
    print(df.show(2,False))
    key_list = get_constraint_key(datatype, spark=spark)
    for key in key_list:
        split_values = key.split('||')
        if split_values[2].strip() == 'positive':
            sign_check(df, split_values[1], split_values[0], split_values[4])
        elif split_values[2].strip() == 'non zero':
            non_zero_check(df, split_values[1], split_values[0], split_values[4])
        elif split_values[2].strip() == 'distinct values':
            distinct_count(df, split_values[1], split_values[0], split_values[3])
        elif split_values[2].strip() == 'is null':
            null_check(df, split_values[1], split_values[0], split_values[4])
        elif split_values[2].strip() == 'is greater':
            Greaterthan_check(df, split_values[0], split_values[1], split_values[3], split_values[4])
        else:
            pass


def transaction_data_sanity_checks(spark):
    # Reading the data from load data file.
    df_transaction = loaddata.get_transaction_data(spark)
    df_transaction.repartition(config.repartitions)
    df_transaction.cache()

    # calling the function to get the mapping of constraints and their respective columns.
    datatype = "Transaction data"
    print("df_txan",df_transaction.show(2,False))

    check_data(df_transaction, datatype, spark)
    # key_list = get_constraint_key(datatype, spark = spark)
    # for key in key_list:
    #     split_values = key.split('_')
    #     if split_values[2].strip() == 'positive':
    #         sign_check(df_transaction, split_values[1], split_values[0])
    #     elif split_values[2].strip() == 'non zero':
    #         non_zero_check(df_transaction, split_values[1], split_values[0])
    #     elif split_values[2].strip() == 'distinct values':
    #         distinct_count(df_transaction, split_values[1], split_values[0], split_values[3])
    #
    #     # elif
    #     else:
    #         continue


"""
calender data sanity function is to perform the sanity operation on calender data.
In this function we are reading the respective data and then calling required constraint functions.

"""


def calender_data_sanity_checks(spark):
    # Reading the data from load data file.
    df_calender = loaddata.get_calender_data(spark)
    df_calender.repartition(config.repartitions)
    df_calender.cache()
    # calling the function to get the mapping of constraints and their respective columns.
    datatype = "Calender Data"

    check_data(df_calender, datatype, spark)
    # key_list = get_constraint_key(datatype="Calender Data", spark=spark)
    # for key in key_list:
    #     split_values = key.split('_')
    #     if split_values[2].strip() == 'positive':
    #         sign_check(df_calender, split_values[1], split_values[0])
    #     elif split_values[2].strip() == 'non zero':
    #         non_zero_check(df_calender, split_values[1], split_values[0])
    #     elif split_values[2].strip() == 'distinct values':
    #         distinct_count(df_calender, split_values[1], split_values[0], split_values[3])
    #     else:
    #         continue


"""
inventory data sanity function is to perform the sanity operation on inventory data.
In this function we are reading the respective data and then calling required constraint functions.

"""


def inventory_data_sanity_checks(spark):
    # Reading the data from load data file.
    df_inventory = loaddata.get_inventory_data(spark)
    df_inventory.repartition(config.repartitions)
    df_inventory.cache()

    # calling the function to get the mapping of constraints and their respective columns.
    datatype = "Inventory data"

    check_data(df_inventory, datatype, spark)
    # key_list = get_constraint_key(datatype="Inventory data", spark=spark)
    #
    # for key in key_list:
    #     split_values = key.split('_')
    #     if (split_values[2].strip() == 'positive'):
    #         sign_check(df_inventory, split_values[1], split_values[0])
    #     elif (split_values[2].strip() == 'non zero'):
    #         non_zero_check(df_inventory, split_values[1], split_values[0])
    #     elif (split_values[2].strip() == 'distinct values'):
    #         distinct_count(df_inventory, split_values[1], split_values[0], split_values[3])
    #     else:
    #         continue


"""
category data sanity function is to perform the sanity operation on category data.
In this function we are reading the respective data and then calling required constraint functions.

"""


def category_data_sanity_checks(spark):
    # Reading the data from load data file.
    df_category = loaddata.get_category_data(spark)
    df_category.repartition(config.repartitions)
    df_category.cache()

    # calling the function to get the mapping of constraints and their respective columns.
    datatype = "Category Data"
    check_data(df_category, datatype, spark)

    # key_list = get_constraint_key(datatype="Category Data", spark=spark)
    # for key in key_list:
    #     split_values = key.split('_')
    #     if (split_values[2].strip() == 'positive'):
    #         sign_check(df_category, split_values[1], split_values[0])
    #     elif (split_values[2].strip() == 'non zero'):
    #         non_zero_check(df_category, split_values[1], split_values[0])
    #     elif (split_values[2].strip() == 'distinct values'):
    #         distinct_count(df_category, split_values[1], split_values[0], split_values[3])
    #     else:
    #         continue


"""
competior data sanity function is to perform the sanity operation on competior data.
In this function we are reading the respective data and then calling required constraint functions.

"""


def competior_data_sanity_checks(spark):
    # Reading the data from load data file.
    df_competior = loaddata.get_competitor_data(spark)
    df_competior.repartition(config.repartitions)
    df_competior.cache()

    # calling the function to get the mapping of constraints and their respective columns.
    datatype = "Competior Data"
    check_data(df_competior, datatype, spark)

    # key_list = get_constraint_key(datatype="Competior Data", spark=spark)
    #
    # for key in key_list:
    #     split_values = key.split('_')
    #     if (split_values[2].strip() == 'positive'):
    #         sign_check(df_competior, split_values[1], split_values[0])
    #     elif (split_values[2].strip() == 'non zero'):
    #         non_zero_check(df_competior, split_values[1], split_values[0])
    #     elif (split_values[2].strip() == 'distinct values'):
    #         distinct_count(df_competior, split_values[1], split_values[0], split_values[3])
    #     else:
    #         continue


"""
online data sanity function is to perform the sanity operation on online data.
In this function we are reading the respective data and then calling required constraint functions.

"""


def online_data_sanity_checks(spark):
    # Reading the data from load data file.
    df_online = loaddata.get_online_data(spark)
    df_online.repartition(config.repartitions)
    df_online.cache()
    datatype = "Online Data"

    check_data(df_online, datatype, spark)

    # calling the function to get the mapping of constraints and their respective columns.
    # key_list = get_constraint_key(datatype="Online Data", spark=spark)
    #
    # for key in key_list:
    #     split_values = key.split('_')
    #     if (split_values[2].strip() == 'positive'):
    #         sign_check(df_online, split_values[1], split_values[0])
    #     elif (split_values[2].strip() == 'non zero'):
    #         non_zero_check(df_online, split_values[1], split_values[0])
    #     elif (split_values[2].strip() == 'distinct values'):
    #         distinct_count(df_online, split_values[1], split_values[0], split_values[3])
    #     else:
    #         continue


"""
store data sanity function is to perform the sanity operation on store data.
In this function we are reading the respective data and then calling required constraint functions.

"""


def store_data_sanity_checks(spark):
    # Reading the data from load data file.
    df_store = loaddata.get_store_data(spark)
    df_store.repartition(config.repartitions)
    df_store.cache()
    datatype = "Store Data"

    check_data(df_store, datatype, spark)
    # calling the function to get the mapping of constraints and their respective columns.
    # key_list = get_constraint_key(datatype="Store Data", spark=spark)
    #
    # for key in key_list:
    #     split_values = key.split('_')
    #     if (split_values[2].strip() == 'positive'):
    #         sign_check(df_store, split_values[1], split_values[0])
    #     elif (split_values[2].strip() == 'non zero'):
    #         non_zero_check(df_store, split_values[1], split_values[0])
    #     elif (split_values[2].strip() == 'distinct values'):
    #         distinct_count(df_store, split_values[1], split_values[0], split_values[3])
    #     else:
    #         continue


"""
Below function is to create the key from the constraint file.
As in constraint file we have 4 columns i.e. data type, column name,constraint
and other values. We are creating a key of all four concatenated with _.
Below function creates the key for required data type and return it.

"""


def get_constraint_key(datatype, spark):
    datatype = datatype
    # Reading constraint data
    df_constraint = loaddata.get_constraint_data(spark=spark)

    # filtered constraint data for transaction data
    filtered_transaction_df = df_constraint[df_constraint['DataType'].str.strip() == datatype]

    keys = list(
        filtered_transaction_df[['DataType', 'Column',
                                 'CheckConstraints', 'OtherValues',
                                 'Threshold']].fillna(value={'Threshold': 0}).astype(str).apply('||'.join, 1))
    return keys


"""
Below Function is to check if a column has only +ve values or not.
So it takes 3 parameters as input i.e. data frame on which we need to check, column name and
last parameter we are passing as we need to append type of data column(ex. transaction data, calender data etx) in our result file.
Result is being append in empty data frame (sanityResult) which we have created top of this file

"""
def Greaterthan_check(dataframe_to_check, data,column1,column2,threshold=0):
    threshold = float(threshold)
    df = dataframe_to_check
    datatype = data
    column = column1
    col2 = column2
    # print(df.show(5,False))
    # print(column,col2)
    nrows = df.filter(df[column] > df[col2]).count()
    print(nrows)
    global sanity_result
    try:
        fraction_invalid = nrows / df.count()
        print(fraction_invalid)
    except ZeroDivisionError:
        fraction_invalid = 0

    if fraction_invalid <= threshold:
        sanity_result = sanity_result.append(pd.DataFrame([(datatype, column, 'is greater', 'Passed',fraction_invalid)],
                                                          columns=['DataType', 'Column', 'Constraint', 'Status',
                                                                   'Percentage Failed']), ignore_index=True)
    else:
        sanity_result = sanity_result.append(
            pd.DataFrame([(datatype, column, 'is greater', 'Failed', fraction_invalid)],
                         columns=['DataType', 'Column', 'Constraint', 'Status', 'Percentage Failed']),
            ignore_index=True)

def null_check(dataframe_to_check, columns_to_check, data, threshold=0):
    threshold = float(threshold)
    df = dataframe_to_check
    column = columns_to_check
    datatype = data
    nrows = df.filter(df[column].isNull()).count()
    global sanity_result
    # for column in Positive_column_check:
    try:
        fraction_invalid = nrows / df.count()
    except ZeroDivisionError:
        fraction_invalid = 0
    if fraction_invalid <= threshold:
        sanity_result = sanity_result.append(pd.DataFrame([(datatype, column, 'Not Null', 'Passed', fraction_invalid)],
                                                          columns=['DataType', 'Column', 'Constraint', 'Status',
                                                                   'Percentage Failed']), ignore_index=True)
    else:
        sanity_result = sanity_result.append(pd.DataFrame([(datatype, column, 'Not Null', 'Failed', fraction_invalid)],
                                                          columns=['DataType', 'Column', 'Constraint', 'Status',
                                                                   'Percentage Failed']), ignore_index=True)


def sign_check(dataframe_to_check, columns_to_check, data, threshold=0):
    threshold = float(threshold)
    df = dataframe_to_check
    column = columns_to_check
    datatype = data
    nrows = df.filter((df[column].isNotNull()) & (df[column] < 0)).count()
    global sanity_result
    # for column in Positive_column_check:
    try:
        fraction_invalid = nrows / df.count()
    except ZeroDivisionError:
        fraction_invalid = 0

    if fraction_invalid <= threshold:
        sanity_result = sanity_result.append(pd.DataFrame([(datatype, column, 'positive', 'Passed', 0)],
                                                          columns=['DataType', 'Column', 'Constraint', 'Status',
                                                                   'Percentage Failed']), ignore_index=True)
    else:
        sanity_result = sanity_result.append(
            pd.DataFrame([(datatype, column, 'positive', 'Failed', nrows / df.count())],
                         columns=['DataType', 'Column', 'Constraint', 'Status', 'Percentage Failed']),
            ignore_index=True)


"""
Below Function is to check if a column has any zero present .
So it takes 3 parameters as input i.e. data frame on which we need to check, column name and
last parameter we are passing as we need to append type of data column(ex. transaction data, calender data etx) in our result file.
Result is being append in empty data frame (sanityResult) which we have created top of this file

"""


def non_zero_check(dataframe_to_check, columns_to_check, data, threshold=0):
    threshold = float(threshold)
    df = dataframe_to_check
    column = columns_to_check
    datatype = data
    n_rows = df.filter(df[column] == 0).count()
    global sanity_result
    try:
        fraction_invalid = n_rows / df.count()
    except ZeroDivisionError:
        fraction_invalid = 0
    if fraction_invalid <= threshold:
        sanity_result = sanity_result.append(pd.DataFrame([(datatype, column, 'non zero', 'Passed', 0)],
                                                          columns=['DataType', 'Column', 'Constraint', 'Status',
                                                                   'Percentage Failed']), ignore_index=True)
    else:
        sanity_result = sanity_result.append(
            pd.DataFrame([(datatype, column, 'non zero', 'Failed', n_rows / df.count())],
                         columns=['DataType', 'Column', 'Constraint', 'Status', 'Percentage Failed']),
            ignore_index=True)


"""
Below Function is to check the count of distinct values in a column.
So it takes 4 parameters as input i.e. data frame on which we need to check, column name, data and value.
3rd parameter we are passing as we need to append type of data column(ex. transaction data, calender data etx) in our result file and last
parameter is to match the count of our function with our given value.
Result is being append in empty data frame (sanityResult) which we have created top of this file

"""


def distinct_count(dataframe_to_check, columns_to_check, data, count_value):
    # threshold = float(threshold)
    df = dataframe_to_check
    column = columns_to_check
    count_check = count_value
    datatype = data
    global sanity_result
    count_of_distinct = df.select(column).distinct().count()
    if count_of_distinct == float(count_check):
        sanity_result = sanity_result.append(pd.DataFrame([(datatype, column, 'distinct count', 'Passed', 0)],
                                                          columns=['DataType', 'Column', 'Constraint', 'Status',
                                                                   'Percentage Failed']), ignore_index=True)
    else:
        sanity_result = sanity_result.append(
            pd.DataFrame([(datatype, column, 'distinct count', 'Failed', count_of_distinct)],
                         columns=['DataType', 'Column', 'Constraint', 'Status', 'Percentage Failed']),
            ignore_index=True)

# def store_data_sanity_checks(spark):
#
#     #Reading the data from load data file.
#     df_store = loaddata.get_store_data(spark)
#     df_store.repartition(config.repartitions)
#     df_store.cache()
#
#     #calling the function to get the mapping of constraints and their respective columns.
#     key_list = get_constraint_key(datatype="Store Data", spark=spark)

# positive_column = mapping_dict['positive']
# non_zero_column = mapping_dict['non zero']
# distinct_count_column = mapping_dict['12 distinct count']
# sign_check(df_store,positive_column,data="Store Data")
# non_zero_check(df_calender,non_zero_column,data="Store Data")
# distinct_count(df_store,distinct_count_column,count_value=12,data="Store Data")
# sanity_result.to_csv("sanity_result.csv", encoding='utf-8')
