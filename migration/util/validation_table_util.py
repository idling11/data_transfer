import decimal
import logging
import uuid
from datetime import datetime
from urllib.parse import quote, unquote

import data_diff
import pandas
import sqlparse
import re

from migration.connector.source.enum import Column

from migration.connector.destination.base import Destination

from migration.connector.source import Source

from data_diff import connect, TableSegment, connect_to_table, diff_tables, Algorithm

logger = logging.getLogger(__name__)

NUMBER_TYPE = ['BIGINT', 'DECIMAL', 'DOUBLE', 'FLOAT', 'INT', 'SMALLINT', 'TINYINT']
LEFT_BRACKET = '('

STDDEV_SAMP_DBS = ['clickzetta', 'doris', 'mysql', 'postgresql', 'odps']


def is_number_type(column: Column, type_mapping: dict) -> bool:
    if LEFT_BRACKET in column.type:
        column_type = column.type.split(LEFT_BRACKET)[0]
        return type_mapping.get(column_type, column_type) in NUMBER_TYPE

    return type_mapping.get(column.type, column.type) in NUMBER_TYPE


def is_element_numeric(data):
    if type(data) == int or type(data) == float or type(data) == decimal.Decimal:
        return True
    return False


def decimal_precision(data):
    if isinstance(data, int):
        return (0)
    x_str = str(data)
    x_split = x_str.split('.')
    if len(x_split) == 1:
        return (0)
    n_decimals = len(x_split[1])
    return (n_decimals)


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


def count_validation(source_table: str, destination_table: str, source: Source, destination: Destination):
    try:
        src_count_sql = f"select count(*) from {source_table}"
        dest_count_sql = f"select count(*) from {destination_table}"
        source_count_res = source.execute_sql(src_count_sql)[0]
        result = {'source_count': source_count_res[0]}
        destination_count_res = destination.execute_sql(dest_count_sql)[0]
        result['destination_count'] = destination_count_res[0]
        return result
    except Exception as e:
        raise Exception(e)
    except BaseException as e:
        raise Exception(e)


def basic_validation(source: Source, destination: Destination, source_table: str, destination_table: str):
    try:
        src_count_sql = f"select count(*) from {source_table}"
        dest_count_sql = f"select count(*) from {destination_table}"
        source_count_res = source.execute_sql(src_count_sql)[0]
        result = {'source_count': source_count_res[0]}
        destination_count_res = destination.execute_sql(dest_count_sql)[0]
        result['destination_count'] = destination_count_res[0]
        type_mapping = source.type_mapping()
        table_columns = source.get_table_columns(source_table.split('.')[0], source_table.split('.')[1])
        source_quote = source.quote_character()
        destination_quote = destination.quote_character()
        for column in table_columns:
            if is_number_type(column, type_mapping):
                source_sql = (
                    f"select min({source_quote}{column.name}{source_quote}), max({source_quote}{column.name}{source_quote}),"
                    f" avg({source_quote}{column.name}{source_quote}) from {source_table}")
                destination_sql = (
                    f"select min({destination_quote}{column.name}{destination_quote}), max({destination_quote}{column.name}{destination_quote}),"
                    f" avg({destination_quote}{column.name}{destination_quote}) from {destination_table}")
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


def multidimensional_validation(source_table: str, destination_table: str, source: Source, destination: Destination):
    try:
        table_columns = source.get_table_columns(source_table.split('.')[0], source_table.split('.')[1])
        type_mapping = source.type_mapping()
        source_quote = source.quote_character()
        destination_quote = destination.quote_character()
        source_profile_sql = f"with source_data as (select * from {source_table}), \n" \
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
        des_profile_sql = f"with source_data as (select * from {destination_table}), \n" \
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


def line_by_line_validation(source_table: str, destination_table: str, source: Source, destination: Destination):
    try:
        source_result = source.execute_sql(f"select * from {source_table}")
        list_source_result = []
        for row in source_result:
            list_source_result.append(list(row))
        destination_result = destination.execute_sql(f"select * from {destination_table}")
        list_destination_result = []
        for row in destination_result:
            list_destination_result.append(list(row))
        logger.info(f"line by line destination_result: {destination_result}")
        columns = source.get_table_columns(source_table.split('.')[0], source_table.split('.')[1])
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


def data_diff_validation(source_table: str, destination_table: str, source: Source, destination: Destination):
    try:
        source_db_type = source.name.lower()
        destination_db_type = destination.name.lower()

        if source_db_type == 'postgresql':
            config = source.get_connection_params()
            source_db_url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        elif source_db_type == 'mysql':
            config = source.get_connection_params()
            source_db_url = f"mysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{source_table.split('.')[0]}"
        elif source_db_type == 'clickzetta':
            config = source.get_connection_params()
            source_db_url = f"clickzetta://{config['username']}:{config['password']}@{config['instance']}.{config['service']}/{config['workspace']}?virtualcluster={config['vcluster']}&schema={source_table.split('.')[0]}"
        else:
            raise Exception(f"Unsupported database type {source_db_type} in data-diff-validation")

        if destination_db_type == 'postgresql':
            config = destination.get_connection_params()
            destination_db_url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        elif destination_db_type == 'mysql':
            config = destination.get_connection_params()
            destination_db_url = f"mysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{destination_table.split('.')[0]}"
        elif destination_db_type == 'clickzetta':
            config = destination.get_connection_params()
            destination_db_url = f"clickzetta://{config['username']}:{config['password']}@{config['instance']}.{config['service']}/{config['workspace']}?virtualcluster={config['vcluster']}&schema={destination_table.split('.')[0]}"
        else:
            raise Exception(f"Unsupported database type {destination_db_type} in data-diff-validation")

        pk_colmns = source.get_table_pk_columns(source_table.split('.')[0], source_table.split('.')[1])
        if len(pk_colmns) == 0:
            raise Exception(f"[data-diff] Table {source_table} has no primary key")
        source_diff_table = connect_to_table(source_db_url, source_table.split('.')[1], pk_colmns)
        destination_diff_table = connect_to_table(destination_db_url, destination_table.split('.')[1], pk_colmns)
        extra_columns = [column.name for column in
                         source.get_table_columns(source_table.split('.')[0], source_table.split('.')[1]) if
                         column.name not in pk_colmns]
        diff_result = diff_tables(source_diff_table, destination_diff_table, algorithm=Algorithm.HASHDIFF,
                                  key_columns=pk_colmns,
                                  threaded=True, max_threadpool_size=6, extra_columns=tuple(extra_columns))
        result = []
        for sign, columns in diff_result:
            result.append(f'{sign} {columns}')
        return result
    except Exception as e:
        raise Exception('data-diff-error:', e)


def data_diff_validation_with_pks(source_table: str, destination_table: str, source: Source, destination: Destination,
                                  pks_str: str):
    try:
        source_db_type = source.name.lower()
        destination_db_type = destination.name.lower()
        source_count = source.execute_sql(f"select count(*) from {source_table}")[0][0]
        destination_count = destination.execute_sql(f"select count(*) from {destination_table}")[0][0]

        if source_db_type == 'postgresql':
            config = source.get_connection_params()
            pwd = config['password'] if '@' not in config['password'] else config['password'].replace('@', '%40')
            logger.info(f"pwd: {pwd}")
            source_db_url = f"postgresql://{config['user']}:{pwd}@{config['host']}:{config['port']}/{config['database']}"
        elif source_db_type == 'mysql':
            config = source.get_connection_params()
            source_db_url = f"mysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{source_table.split('.')[0]}"
        elif source_db_type == 'clickzetta':
            config = source.get_connection_params()
            source_db_url = f"clickzetta://{config['username']}:{config['password']}@{config['instance']}.{config['service']}/{config['workspace']}?virtualcluster={config['vcluster']}&schema={source_table.split('.')[0]}"
        else:
            raise Exception(f"Unsupported database type {source_db_type} in data-diff-validation")

        if destination_db_type == 'postgresql':
            config = destination.get_connection_params()
            destination_db_url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        elif destination_db_type == 'mysql':
            config = destination.get_connection_params()
            destination_db_url = f"mysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{destination_table.split('.')[0]}"
        elif destination_db_type == 'clickzetta':
            config = destination.get_connection_params()
            destination_db_url = f"clickzetta://{config['username']}:{config['password']}@{config['instance']}.{config['service']}/{config['workspace']}?virtualcluster={config['vcluster']}&schema={destination_table.split('.')[0]}"
        else:
            raise Exception(f"Unsupported database type {destination_db_type} in data-diff-validation")
        pk_columns = tuple(pks_str.split(','))
        if len(pk_columns) == 0:
            raise Exception(f"[data-diff] Table {source_table} has no primary key")
        logger.info(f"source_table name is :{source_table}")
        if source_db_type == 'postgresql':
            source_diff_table = connect_to_table(source_db_url, source_table, pk_columns,
                                                 case_sensitive=False)
        else:
            source_diff_table = connect_to_table(source_db_url, source_table.split('.')[1], pk_columns,
                                                 case_sensitive=False)
        destination_diff_table = connect_to_table(destination_db_url, destination_table.split('.')[1], pk_columns,
                                                  case_sensitive=False)
        columns = [column.name.lower() for column in
                   source.get_table_columns(source_table.split('.')[0], source_table.split('.')[1])]
        pk_index = 0
        for index, column in enumerate(columns):
            if column == pk_columns[0]:
                pk_index = index
        extra_columns = [column for column in columns if column not in pk_columns]
        diff_result = diff_tables(source_diff_table, destination_diff_table, algorithm=Algorithm.HASHDIFF,
                                  key_columns=pk_columns,
                                  threaded=True, max_threadpool_size=6, extra_columns=tuple(extra_columns))
        only_in_source_result = []
        only_in_dest_result = []
        only_in_source_result_final = []
        only_in_dest_result_final = []
        only_in_source_map = {}
        only_in_dest_map = {}
        both_in_but_diff = []
        for sign, columns in diff_result:
            if sign == '-':
                only_in_source_result.append(columns)
            elif sign == '+':
                only_in_dest_result.append(columns)
        for entry in only_in_source_result:
            only_in_source_map[entry[pk_index]] = entry
        for entry in only_in_dest_result:
            only_in_dest_map[entry[pk_index]] = entry
        for key, value in only_in_source_map.items():
            if key in only_in_dest_map:
                if only_in_dest_map[key] != value:
                    both_in_but_diff.append([value, only_in_dest_map[key]])
                only_in_dest_map.pop(key)
            else:
                only_in_source_result_final.append(value)
        for key, value in only_in_dest_map.items():
            only_in_dest_result_final.append(value)


        return {'only_in_source': len(only_in_source_result_final), 'only_in_source_list': only_in_source_result_final,
                'only_in_dest': len(only_in_dest_result_final), 'only_in_dest_list': only_in_dest_result_final,
                'both_in_but_diff': both_in_but_diff, 'check_count': max(source_count, destination_count)}
    except Exception as e:
        raise Exception('data-diff-error:', e)


def schema_validation(source_table: str, destination_table: str, source: Source, destination: Destination):
    try:
        only_in_source_cols = []
        only_in_dest_cols = []
        source_table_columns = source.get_table_columns(source_table.split('.')[0], source_table.split('.')[1])
        destination_table_columns = destination.get_table_columns(destination_table.split('.')[0],
                                                                  destination_table.split('.')[1])
        source_table_columns = set([column.name.lower() for column in source_table_columns])
        destination_table_columns = set([column.name.lower() for column in destination_table_columns])
        for column in source_table_columns - destination_table_columns:
            only_in_source_cols.append(column)
        for column in destination_table_columns - source_table_columns:
            only_in_dest_cols.append(column)
        return {'only_in_source_cols': only_in_source_cols, 'only_in_dest_cols': only_in_dest_cols}
    except Exception as e:
        logger.error(f"schema validation failed, error: {e}")
        raise Exception('schema validation failed:', e)


def pk_id_column_validation(source_table: str, destination_table: str, source: Source, destination: Destination,
                            pk_id: str):
    try:
        source_max_min_id = source.execute_sql(f"select min({pk_id}), max({pk_id}) from {source_table}")[0]
        source_min_id = source_max_min_id[0]
        source_max_id = source_max_min_id[1]
        dest_max_min_id = destination.execute_sql(f"select min({pk_id}), max({pk_id}) from {destination_table}")[0]
        dest_min_id = dest_max_min_id[0]
        dest_max_id = dest_max_min_id[1]
        min_id = min(source_min_id, dest_min_id)
        max_id = max(source_max_id, dest_max_id)
        logger.info(f"{source_table}-min_id: {min_id}, max_id: {max_id}, prepare full table check.")
        cur_id = max_id
        cur_min_id = 0
        only_in_source = 0
        only_in_source_list = []
        only_in_dest = 0
        only_in_dest_list = []
        while cur_id >= min_id:
            logger.debug(f'{datetime.now()} cur_id={cur_id}, ')
            ids_source = set()
            source_result = source.execute_sql(
                f'select {pk_id} from {source_table} where {pk_id} <= {cur_id} order by {pk_id} desc limit 50000;')
            for r in source_result:
                ids_source.add(r[0])
                cur_min_id = r[0]
            logger.debug(f'cur_min_id={cur_min_id}, ')

            ids_dest = set()
            dest_result = destination.execute_sql(
                f'select {pk_id} from {destination_table} where {pk_id} >= {cur_min_id} and {pk_id} <={cur_id};')

            for r in dest_result:
                ids_dest.add(r[0])

            for i in ids_source - ids_dest:
                only_in_source += 1
                only_in_source_list.append(i)
            logger.info(f'{source_table}-only_in_source count={only_in_source} at {datetime.now()}')

            for i in ids_dest - ids_source:
                only_in_dest += 1
                only_in_dest_list.append(i)
            logger.info(f'{destination_table}-only_in_dest count={only_in_dest} as {datetime.now()}')
            cur_id = cur_min_id - 1
        return {'only_in_source': only_in_source, 'only_in_source_list': only_in_source_list,
                'only_in_dest': only_in_dest, 'only_in_dest_list': only_in_dest_list}
    except Exception as e:
        logger.error(f"pk-id validation failed, error: {e}")
        raise Exception('pk-id validation failed:', e)


def pk_all_column_validation(source_table: str, destination_table: str, source: Source, destination: Destination,
                             pk_id: str):
    try:
        source_count = source.execute_sql(f"select count(*) from {source_table}")[0][0]
        destination_count = destination.execute_sql(f"select count(*) from {destination_table}")[0][0]
        logger.info(
            f"{source_table}-source_count: {source_count}, destination_count: {destination_count}, prepare full table check.")
        source_max_min_id = source.execute_sql(f"select min({pk_id}), max({pk_id}) from {source_table}")[0]
        source_min_id = source_max_min_id[0]
        source_max_id = source_max_min_id[1]
        dest_max_min_id = destination.execute_sql(f"select min({pk_id}), max({pk_id}) from {destination_table}")[0]
        dest_min_id = dest_max_min_id[0]
        dest_max_id = dest_max_min_id[1]
        min_id = min(source_min_id, dest_min_id)
        max_id = max(source_max_id, dest_max_id)
        logger.info(f"{source_table}-min_id: {min_id}, max_id: {max_id}, prepare full table check.")
        cur_id = max_id
        cur_min_id = 0
        only_in_source = 0
        only_in_source_list = []
        only_in_dest = 0
        only_in_dest_list = []
        both_in_but_diff = []
        while cur_id >= min_id:
            logger.debug(f'{datetime.now()} cur_id={cur_id}, ')
            ids_source = {}
            source_result = source.execute_sql(
                f'select {pk_id} as temp_id,* from {source_table} where {pk_id} <= {cur_id} order by {pk_id} desc limit 10000;')
            for r in source_result:
                temp_r = [x if str(x) != 'None' and str(x) != 'nan' else 'Null' for x in r]
                ids_source[temp_r[0]] = temp_r[1:]
                cur_min_id = temp_r[0]
            logger.debug(f'cur_min_id={cur_min_id}, ')

            ids_dest = {}
            dest_result = destination.execute_sql(
                f'select {pk_id} as temp_id,* from {destination_table} where {pk_id} >= {cur_min_id} and {pk_id} <={cur_id} order by {pk_id} desc;')

            for r in dest_result:
                temp_r = [x if str(x) != 'None' and str(x) != 'nan' else 'Null' for x in r]
                ids_dest[temp_r[0]] = temp_r[1:]

            for key, value in ids_source.items():
                if key not in ids_dest:
                    only_in_source += 1
                    only_in_source_list.append(value)
                elif ids_dest[key] != value:
                    both_in_but_diff.append([value, ids_dest[key]])
                    ids_dest.pop(key)
                elif ids_dest[key] == value:
                    ids_dest.pop(key)
            for key, value in ids_dest.items():
                only_in_dest += 1
                only_in_dest_list.append(value)

            logger.info(f'{source_table}-only_in_source count={only_in_source} at {datetime.now()}')
            logger.info(f'{destination_table}-only_in_dest count={only_in_dest} as {datetime.now()}')
            logger.info(f'{source_table}-both_in_but_diff count={len(both_in_but_diff)} as {datetime.now()}')

            cur_id = cur_min_id - 1
        return {'only_in_source': only_in_source, 'only_in_source_list': only_in_source_list,
                'only_in_dest': only_in_dest, 'only_in_dest_list': only_in_dest_list,
                'both_in_but_diff': both_in_but_diff, 'check_count': max(source_count, destination_count)}
    except Exception as e:
        logger.error(f"pk-id validation failed, error: {e}")
        raise Exception('pk-id validation failed:', e)


def pk_id_column_validation_with_count(source_table: str, destination_table: str, source: Source,
                                       destination: Destination, pk_id: str, expect_count: int):
    try:
        source_max_min_id = source.execute_sql(f"select min({pk_id}), max({pk_id}) from {source_table}")[0]
        source_min_id = source_max_min_id[0]
        source_max_id = source_max_min_id[1]
        dest_max_min_id = destination.execute_sql(f"select min({pk_id}), max({pk_id}) from {destination_table}")[0]
        dest_min_id = dest_max_min_id[0]
        dest_max_id = dest_max_min_id[1]
        min_id = min(source_min_id, dest_min_id)
        max_id = max(source_max_id, dest_max_id)
        logger.info(f"{source_table}-min_id: {min_id}, max_id: {max_id}, expect_check_count: {expect_count}")
        cur_id = max_id
        cur_min_id = 0
        only_in_source = 0
        only_in_source_list = []
        only_in_dest = 0
        only_in_dest_list = []
        max_search_times = 15
        current_search_times = 0
        check_status = 0
        while cur_id >= min_id and current_search_times < max_search_times:
            logger.debug(f'{datetime.now()} cur_id={cur_id}, ')
            ids_source = set()
            source_result = source.execute_sql(
                f'select {pk_id} from {source_table} where {pk_id} <= {cur_id} order by {pk_id} desc limit 50000;')
            for r in source_result:
                ids_source.add(r[0])
                cur_min_id = r[0]
            logger.debug(f'cur_min_id={cur_min_id}, ')

            ids_dest = set()
            dest_result = destination.execute_sql(
                f'select {pk_id} from {destination_table} where {pk_id} >= {cur_min_id} and {pk_id} <={cur_id};')

            for r in dest_result:
                ids_dest.add(r[0])

            for i in ids_source - ids_dest:
                only_in_source += 1
                only_in_source_list.append(i)
            logger.info(f'{source_table}-only_in_source count={only_in_source} at {datetime.now()}')

            for i in ids_dest - ids_source:
                only_in_dest += 1
                only_in_dest_list.append(i)
            logger.info(f'{destination_table}-only_in_dest count={only_in_dest} as {datetime.now()}')
            if abs(only_in_source - only_in_dest) >= expect_count:
                check_status = 1
                break
            cur_id = cur_min_id - 1
            current_search_times += 1
            if current_search_times == max_search_times:
                logger.error(
                    f"max_search_times reached, {source_table}-only_in_source: {only_in_source}, {destination_table}-only_in_dest: {only_in_dest}")
        return {'only_in_source': only_in_source, 'only_in_source_list': only_in_source_list,
                'only_in_dest': only_in_dest, 'only_in_dest_list': only_in_dest_list, 'check_status': check_status}
    except Exception as e:
        logger.error(f"pk-id-with-count validation failed, error: {e}")
        raise Exception('pk-id validation failed:', e)
