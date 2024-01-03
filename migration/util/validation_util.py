import decimal
import logging
import uuid
from datetime import datetime

import pandas
import sqlparse
import re

from migration.connector.source.enum import Column

from migration.connector.destination.base import Destination

from migration.connector.source import Source

logger = logging.getLogger(__name__)

NUMBER_TYPE = ['BIGINT', 'DECIMAL', 'DOUBLE', 'FLOAT', 'INT', 'SMALLINT', 'TINYINT']
LEFT_BRACKET = '('

STDDEV_SAMP_DBS = ['clickzetta', 'doris', 'mysql', 'postgresql', 'odps']


def is_number_type(column: Column, type_mapping: dict) -> bool:
    if LEFT_BRACKET in column.type:
        column_type = column.type.split(LEFT_BRACKET)[0]
        return type_mapping.get(column_type, column_type) in NUMBER_TYPE

    return type_mapping.get(column.type, column.type) in NUMBER_TYPE


def create_query_result_table(source: Source, destination: Destination, source_query: str,
                              destination_query: str):
    try:
        if source.name.lower() == 'odps':
            project = source.connection_params['project']
            source_temp_db = project
        else:
            source_temp_db = f"validation_query_result_db"
        temp_table = f"validation_query_result_table_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
        if source.name.lower() == 'clickzetta' or source.name.lower() == "postgresql":
            source.execute_sql(f"create schema if not exists {source_temp_db}")
            temp_table_ddl = f"create table {source_temp_db}.{temp_table} as {source_query}"
        elif source.name.lower() == 'doris':
            source.execute_sql(f"create database if not exists {source_temp_db}")
            temp_table_ddl = f"create table {source_temp_db}.{temp_table} PROPERTIES(\"replication_num\" = \"1\") as {source_query}"
        elif source.name.lower() == 'odps':
            temp_table_ddl = f"create table {temp_table} as {source_query}"
        else:
            source.execute_sql(f"create database if not exists {source_temp_db}")
            temp_table_ddl = f"create table {source_temp_db}.{temp_table} as {source_query}"

        if source.name.lower() == 'odps':
            source.execute_sql(f"drop table if exists {temp_table}")
        else:
            source.execute_sql(f"drop table if exists {source_temp_db}.{temp_table}")
        source.execute_sql(temp_table_ddl)

        if destination.name.lower() == 'odps':
            project = destination.get_connection_params()['project']
            dest_temp_db = project
        else:
            dest_temp_db = f"validation_query_result_db"
        if destination.name.lower() == 'clickzetta' or destination.name.lower() == "postgresql":
            destination.execute_sql(f"create schema if not exists {dest_temp_db}")
            temp_table_ddl = f"create table {dest_temp_db}.{temp_table} as {destination_query}"
        elif destination.name.lower() == 'doris':
            destination.execute_sql(f"create database if not exists {dest_temp_db}")
            temp_table_ddl = f"create table {dest_temp_db}.{temp_table} PROPERTIES(\"replication_num\" = \"1\") as {destination_query}"
        elif destination.name.lower() == 'odps':
            temp_table_ddl = f"create table {temp_table} as {destination_query}"
        else:
            destination.execute_sql(f"create database if not exists {dest_temp_db}")
            temp_table_ddl = f"create table {dest_temp_db}.{temp_table} as {destination_query}"

        if destination.name.lower() == 'odps':
            destination.execute_sql(f"drop table if exists {temp_table}")
        else:
            destination.execute_sql(f"drop table if exists {dest_temp_db}.{temp_table}")
        destination.execute_sql(temp_table_ddl)
        return f"{source_temp_db}.{temp_table}", f"{dest_temp_db}.{temp_table}"
    except Exception as e:
        raise Exception(e)
    except BaseException as e:
        raise Exception(e)

def is_element_numeric(data):
    if type(data) == int or type(data) == float or type(data) == decimal.Decimal:
        return True
    return False
def decimal_precision(data):
    if isinstance(data, int):
        return(0)
    x_str = str(data)
    x_split = x_str.split('.')
    n_decimals = len(x_split[1])
    return(n_decimals)

def get_max_precision(data: list):
    max_precision = 0
    for entry in data:
        prec = decimal_precision(entry)

        if prec > max_precision:
            max_precision = prec
    return max_precision

def alignment_decimal(source_data: list, dest_data: list):
    processed_source_data = []
    processed_dest_data = []
    for index, data in enumerate(source_data):
        if is_element_numeric(data) and is_element_numeric(dest_data[index]):
            temp_list = [source_data[index], dest_data[index]]
            max_precision = get_max_precision(temp_list)
            processed_source_data.append(f'{temp_list[0]:.{max_precision}f}')
            processed_dest_data.append(f'{temp_list[1]:.{max_precision}f}')
        else:
            processed_source_data.append(source_data[index])
            processed_dest_data.append(dest_data[index])
    return processed_source_data, processed_dest_data


def basic_validation(source: Source, destination: Destination, source_query: str, destination_query: str):
    try:
        source_tbl_name, dest_tbl_name = create_query_result_table(source, destination, source_query, destination_query)
        source_count_sql = f"select count(*) from {source_tbl_name}"
        dest_count_sql = f"select count(*) from {dest_tbl_name}"
        source_count_res = source.execute_sql(source_count_sql)[0]
        result = {'source_count': source_count_res[0]}
        destination_count_res = destination.execute_sql(dest_count_sql)[0]
        result['destination_count'] = destination_count_res[0]
        type_mapping = source.type_mapping()
        table_columns = source.get_table_columns(source_tbl_name.split('.')[0], source_tbl_name.split('.')[1])
        source_quote = source.quote_character()
        destination_quote = destination.quote_character()
        for column in table_columns:
            if is_number_type(column, type_mapping):
                source_sql = (
                    f"select min({source_quote}{column.name}{source_quote}), max({source_quote}{column.name}{source_quote}),"
                    f" avg({source_quote}{column.name}{source_quote}) from {source_tbl_name}")
                destination_sql = (
                    f"select min({destination_quote}{column.name}{destination_quote}), max({destination_quote}{column.name}{destination_quote}),"
                    f" avg({destination_quote}{column.name}{destination_quote}) from {dest_tbl_name}")
                source_result = source.execute_sql(source_sql)[0]
                destination_result = destination.execute_sql(destination_sql)[0]
                processed_source_data, processed_dest_data = alignment_decimal(source_result, destination_result)
                result[f'{column.name}_source_min'] = processed_source_data[0]
                result[f'{column.name}_source_max'] = processed_source_data[1]
                result[f'{column.name}_source_avg'] = processed_source_data[2]
                result[f'{column.name}_destination_min'] = processed_dest_data[0]
                result[f'{column.name}_destination_max'] = processed_dest_data[1]
                result[f'{column.name}_destination_avg'] = processed_dest_data[2]
        return result
    except Exception as e:
        raise Exception(e)
    except BaseException as e:
        raise Exception(e)


def multidimensional_validation(source_query: str, destination_query: str, source: Source, destination: Destination):
    try:
        source_tbl_name, dest_tbl_name = create_query_result_table(source, destination, source_query, destination_query)
        table_columns = source.get_table_columns(source_tbl_name.split('.')[0], source_tbl_name.split('.')[1])
        type_mapping = source.type_mapping()
        source_quote = source.quote_character()
        destination_quote = destination.quote_character()
        source_profile_sql = f"with source_data as (select * from {source_tbl_name}), \n" \
                             f"column_profiles  as ( \n"
        for index, column in enumerate(table_columns):
            source_profile_sql += f"select '{column.name}' as column_name, \n" \
                                  f"'{column.type}' as column_type, \n" \
                                  f"count(*) as row_count, \n" \
                                  f"sum(case when {source_quote}{column.name}{source_quote} is null then 0 else 1 end) / count(*) as not_null_proportion,\n" \
                                  f"count(distinct {source_quote}{column.name}{source_quote}) / count(*) as distinct_proportion, \n" \
                                  f"count(distinct {source_quote}{column.name}{source_quote}) as distinct_count, \n" \
                                  f"count(distinct {source_quote}{column.name}{source_quote}) = count(*) as is_unique, \n"
            if is_number_type(column, type_mapping):
                source_profile_sql += f"min({source_quote}{column.name}{source_quote}) as min_value, \n" \
                                      f"max({source_quote}{column.name}{source_quote}) as max_value, \n" \
                                      f"avg({source_quote}{column.name}{source_quote}) as avg_value, \n" \
                                      f"stddev_pop({source_quote}{column.name}{source_quote}) as stddev_pop_value, \n"
                if source.name.lower() in STDDEV_SAMP_DBS:
                    source_profile_sql += f"stddev_samp({source_quote}{column.name}{source_quote}) as stddev_sample_value \n"
                else:
                    source_profile_sql += f"stddev_sample({source_quote}{column.name}{source_quote}) as stddev_sample_value \n"
            else:
                source_profile_sql += f"null as min_value, \n" \
                                      f"null as max_value, \n" \
                                      f"null as avg_value, \n" \
                                      f"null as stddev_pop_value, \n" \
                                      f"null as stddev_sample_value \n"
            source_profile_sql += f"from source_data \n"
            if index != len(table_columns) - 1:
                source_profile_sql += f"union all \n"
        source_profile_sql += f") \n" \
                              f"select * from column_profiles;"
        source_result = source.execute_sql(source_profile_sql)
        list_source_result = []
        for row in source_result:
            list_source_result.append(list(row))
        logger.info(f"mutil source_result: {source_result}")
        des_profile_sql = f"with source_data as (select * from {dest_tbl_name}), \n" \
                          f"column_profiles  as ( \n"
        for index, column in enumerate(table_columns):
            des_profile_sql += f"select '{column.name}' as column_name, \n" \
                               f"'{column.type}' as column_type, \n" \
                               f"count(*) as row_count, \n" \
                               f"sum(case when {destination_quote}{column.name}{destination_quote} is null then 0 else 1 end) / count(*) as not_null_proportion,\n" \
                               f"count(distinct {destination_quote}{column.name}{destination_quote}) / count(*) as distinct_proportion, \n" \
                               f"count(distinct {destination_quote}{column.name}{destination_quote}) as distinct_count, \n" \
                               f"count(distinct {destination_quote}{column.name}{destination_quote}) = count(*) as is_unique, \n"
            if is_number_type(column, type_mapping):
                des_profile_sql += f"min({destination_quote}{column.name}{destination_quote}) as min_value, \n" \
                                   f"max({destination_quote}{column.name}{destination_quote}) as max_value, \n" \
                                   f"avg({destination_quote}{column.name}{destination_quote}) as avg_value, \n" \
                                   f"stddev_pop({destination_quote}{column.name}{destination_quote}) as stddev_pop_value, \n"
                if destination.name.lower() in STDDEV_SAMP_DBS:
                    des_profile_sql += f"stddev_samp({destination_quote}{column.name}{destination_quote}) as stddev_sample_value \n"
                else:
                    des_profile_sql += f"stddev_sample({destination_quote}{column.name}{destination_quote}) as stddev_sample_value \n"
            else:
                des_profile_sql += f"null as min_value, \n" \
                                   f"null as max_value, \n" \
                                   f"null as avg_value, \n" \
                                   f"null as stddev_pop_value, \n" \
                                   f"null as stddev_sample_value \n"
            des_profile_sql += f"from source_data \n"
            if index != len(table_columns) - 1:
                des_profile_sql += f"union all \n"
        des_profile_sql += f") \n" \
                           f"select * from column_profiles;"
        destination_result = destination.execute_sql(des_profile_sql)
        list_destination_result = []
        for row in destination_result:
            list_destination_result.append(list(row))
        logger.info(f"mutil destination_result: {destination_result}")
        if source.name.lower() == 'clickzetta':
            for row in list_source_result:
                if not row[6]:
                    row[6] = 0
                elif row[6]:
                    row[6] = 1
        if destination.name.lower() == 'clickzetta':
            for row in list_destination_result:
                if not row[6]:
                    row[6] = 0
                elif row[6]:
                    row[6] = 1

        for index, source_row in enumerate(list_source_result):
            if source_row[10] and list_destination_result[index][10]:
                if abs(source_row[10] - list_destination_result[index][10]) < 50:
                    source_row[10] = list_destination_result[index][10]
            if source_row[11] and list_destination_result[index][11]:
                if abs(source_row[11] - list_destination_result[index][11]) < 50:
                    source_row[11] = list_destination_result[index][11]
        processed_source_data = []
        processed_dest_data = []
        for index, source_row_data in enumerate(list_source_result):
            source_data, dest_data = alignment_decimal(source_row_data, list_destination_result[index])
            processed_source_data.append(source_data)
            processed_dest_data.append(dest_data)
        return processed_source_data, processed_dest_data
    except Exception as e:
        raise Exception(e)
    except BaseException as e:
        raise Exception(e)


def line_by_line_validation(source_query: str, destination_query: str, source: Source, destination: Destination):
    try:
        source_tbl_name, dest_tbl_name = create_query_result_table(source, destination, source_query, destination_query)
        source_result = source.execute_sql(source_query)
        list_source_result = []
        for row in source_result:
            list_source_result.append(list(row))

        destination_result = destination.execute_sql(destination_query)
        list_destination_result = []
        for row in destination_result:
            list_destination_result.append(list(row))

        columns = source.get_table_columns(source_tbl_name.split('.')[0], source_tbl_name.split('.')[1])
        if source.name.lower() == 'clickzetta' or source.name.lower() == 'doris' or source.name.lower() == 'mysql':
            for row in list_source_result:
                for index, col in enumerate(row):
                    if columns[index].type == 'DATETIME':
                        row[index] = str(pandas.to_datetime(row[index])).split('+')[0]
        if destination.name.lower() == 'clickzetta' or source.name.lower() == 'doris' or source.name.lower() == 'mysql':
            for row in list_destination_result:
                for index, col in enumerate(row):
                    if columns[index].type == 'DATETIME':
                        row[index] = str(pandas.to_datetime(row[index])).split('+')[0]

        processed_source_data = []
        processed_dest_data = []
        for index, source_row_data in enumerate(list_source_result):
            source_data, dest_data = alignment_decimal(source_row_data, list_destination_result[index])
            processed_source_data.append(source_data)
            processed_dest_data.append(dest_data)

        result = {'source_result': processed_source_data,
                  'destination_result': processed_dest_data,
                  'columns': [column.name for column in columns]}
        return result
    except Exception as e:
        raise Exception(e)
    except BaseException as e:
        raise Exception(e)

def line_by_line_without_ddl_validation(source_query: str, destination_query: str, source: Source, destination: Destination):
    try:
        source_result = source.execute_sql(source_query)
        list_source_result = []
        for row in source_result:
            list_source_result.append(list(row))

        destination_result = destination.execute_sql(destination_query)
        list_destination_result = []
        for row in destination_result:
            list_destination_result.append(list(row))

        result = {'source_result': list_source_result,
                  'destination_result': list_destination_result,
                  }
        return result
    except Exception as e:
        raise Exception(e)
    except BaseException as e:
        raise Exception(e)