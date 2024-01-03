"""
migration migration tool
usage:
    cz-migration test -p <profile_path> [test connection for source and destination]
    cz-migration meta -p <profile_path>  --out_path <meta_out_file_path>[generate meta from source]
    cz-migration schema -p <profile_path> [--table_list_file <external_table_list_file>][migrate schema from source to destination]
    cz-migration data -p <profile_path> --meta_conf <meta_conf> --storage_conf <storage_conf> [--table_list_file <external_table_list_file>][migrate data from source to destination]
    cz-migration validate -p <profile_path> [--table_list_file <external_table_list_file>][validate data from source and destination]
    cz-migration run -p <profile_path> --meta_conf <meta_conf> --storage_conf <storage_conf> [--table_list_file <external_table_list_file>][migrate schema and data from source to destination, including validation]
    cz-migration status -p <profile_path> [check migration status]
    cz-migration resume -p <profile_path> [resume migration]
    cz-migration report -p <profile_path> [generate migration report]
"""
import logging
import os.path
import time

import docopt
import yaml
from migration.connector.destination.base import Destination
from migration.connector.destination.clickzetta.destination import ClickZettaDestination
from migration.connector.destination.doris.destination import DorisDestination
from migration.connector.source import Source
from migration.connector.source.clickzetta.source import ClickzettaSource
from migration.connector.source.doris.source import DorisSource
from migration.scheduler.data_transformer.transformer import DataTransformer
from migration.scheduler.data_validation.validation import Validation
from migration.scheduler.schema_transformer.transformer import SchemaTransformer
from migration.scheduler.unify_transformer.transformer import UnifyTransformer

logger = logging.getLogger(__name__)


def test(source: Source, destination: Destination):
    if source.migration_test_connection() and destination.migration_test_connection():
        print("Test connection successfully!")
    else:
        print("Test connection failed!")


def meta(source: Source, out_path: str):
    logger.info(f"Generating meta from source {source} to {out_path}")
    print(f"Generating meta from source {source} to {out_path}")
    start_time = time.time()
    dbs = source.get_database_names()
    out_path_dir = os.path.dirname(out_path)
    if not os.path.exists(out_path_dir):
        os.makedirs(out_path_dir)
    with open(out_path, 'w') as f:
        for db in dbs:
            if db.startswith("__") or db.startswith("information_schema"):
                continue
            tables = source.get_table_names(db)
            for table in tables:
                f.write(f"{db}.{table}\n")

    logger.info(f"Generating meta from source {source} to {out_path} finished, time cost: {time.time() - start_time}")


def schema(source: Source, destination: Destination, db_list: list, config_table_list: list, external_table_list: list,
           project_name: str, scheduler_concurrency: int, quit_if_fail: bool, thread_concurrency: int):
    logger.info(f"Transforming schema from source {source} to destination {destination}")
    print(f"Transforming schema from source {source} to destination {destination}")
    start_time = time.time()
    SchemaTransformer(source, destination, project_name, db_list, config_table_list, external_table_list,
                      scheduler_concurrency,
                      quit_if_fail, thread_concurrency).transform()
    logger.info(
        f"Transforming schema from source {source} to destination {destination} finished, time cost: {time.time() - start_time}")


def data(source: Source, destination: Destination, db_list: list, config_table_list: list, external_table_list: list,
         project_name: str, scheduler_concurrency: int, quit_if_fail: bool, thread_concurrency: int,
         transform_partitions: dict, dest_table_list: list):
    logger.info(f"Transforming data from source {source} to destination {destination}")
    print(f"Transforming data from source {source} to destination {destination}")
    start_time = time.time()
    DataTransformer(source, destination, project_name, db_list, config_table_list, external_table_list,
                    scheduler_concurrency,
                    quit_if_fail, thread_concurrency, transform_partitions, dest_table_list).transform()
    logger.info(
        f"Transforming data from source {source} to destination {destination} finished, time cost: {time.time() - start_time}")


def validate(source: Source, destination: Destination, db_list: list, config_table_list: list,
             external_table_list: list, project_name: str, scheduler_concurrency: int, quit_if_fail: bool,
             thread_concurrency: int):
    logger.info(f"Validating data from source {source} and destination {destination}")
    print(f"Validating data from source {source} and destination {destination}")
    start_time = time.time()
    Validation(source, destination, project_name, db_list, config_table_list, external_table_list,
               scheduler_concurrency,
               quit_if_fail, thread_concurrency).transform()
    logger.info(
        f"Validating data from source {source} and destination {destination} finished, time cost: {time.time() - start_time}")


def run(source: Source, destination: Destination, db_list: list, config_table_list: list, external_table_list: list,
        project_name: str, scheduler_concurrency: int, quit_if_fail: bool, thread_concurrency: int,
        transform_partitions: dict):
    logger.info(f"Running migration from source {source} to destination {destination}")
    print(f"Running migration from source {source} to destination {destination}")
    start_time = time.time()
    UnifyTransformer(source, destination, project_name, db_list, config_table_list, external_table_list,
                     scheduler_concurrency,
                     quit_if_fail, thread_concurrency, transform_partitions).transform()
    logger.info(
        f"Running migration from source {source} to destination {destination} finished, time cost: {time.time() - start_time}")


def status(destination: Destination, project_name: str):
    import migration.util.migration_tasks_status as mts
    mts.get_last_migration_status(destination, project_name)


def resume(source: Source, destination: Destination):
    pass


def report(destination: Destination, project_name: str):
    import migration.util.migration_tasks_status as mts
    mts.get_last_migration_report(destination, project_name)


def main():
    logging.basicConfig(level=logging.INFO)
    args = docopt.docopt(__doc__)
    profile = None
    source = None
    destination = None
    concurrency = 1
    thread_concurrency = 1
    quit_if_fail = False
    meta_conf_path = None
    storage_conf_path = None
    if not args['-p']:
        print("Please specify profile path!")
        logger.error("Please specify profile path!")
    profile_path = args['<profile_path>']
    print(f"Using profile {profile_path}")
    with open(profile_path, 'r') as f:
        profile = yaml.safe_load(f)['config']
    project_name = profile['name']
    print(f"Using project name  {project_name}")
    project_version = profile['version']
    print(f"Using project version  {project_version}")
    project_desc = profile['description']
    print(f"Using project description  {project_desc}")
    source_config = profile['source']
    if args['--meta_conf']:
        meta_conf_path = args['<meta_conf>']
        print(f"Using meta conf path  {meta_conf_path}")
    if args['--storage_conf']:
        storage_conf_path = args['<storage_conf>']
        print(f"Using storage conf path  {storage_conf_path}")
    db_list = []
    config_table_list = []
    dest_table_list = None
    external_table_list = []
    transform_partitions = {}
    if source_config['type'].strip() == 'doris':
        config = {'fe_servers': source_config['fe_servers'], 'user': source_config['username'],
                  'password': source_config['password']}
        logger.info(f"migration source {config}")
        if 'transform_partitions' in source_config and source_config['transform_partitions']:
            tables = source_config['transform_partitions']['tables']
            values = source_config['transform_partitions']['values']
            if not len(values) == len(tables):
                raise Exception("transform_partitions columns, values and tables should have same length")
            for i in range(len(tables)):
                transform_partitions[tables[i]] = [values[i]]
        print(f"migration source {config}")
        source = DorisSource(config, meta_conf_path, storage_conf_path)
        if args['--table_list_file']:
            with open(args['<external_table_list_file>'], 'r') as f:
                lines = f.readlines()
                for line in lines:
                    external_table_list.append(line.strip())
        else:
            if 'migration_dbs' not in source_config:
                if 'migration_tables' not in source_config:
                    raise Exception("migration_dbs and migration_tables are both not provided in profile.yml")
                else:
                    config_table_list = source_config['migration_tables']
                    if 'dest_tables' in source_config:
                        dest_table_list = source_config['dest_tables']
                        if len(config_table_list) != len(dest_table_list):
                            raise Exception("migration_tables and dest_tables should have same length")
            else:
                db_list = source_config['migration_dbs']
    elif source_config['type'].strip() == 'clickzetta':
        config = {'service': source_config['service'], 'username': source_config['username'],
                  'workspace': source_config['workspace'], 'password': source_config['password'],
                  'instance': source_config['instance'], 'vcluster': source_config['vcluster'],
                  'instanceId': source_config['instanceId']}
        logger.info(f"migration source {config}")
        if 'transform_partitions' in source_config and source_config['transform_partitions']:
            tables = source_config['transform_partitions']['tables']
            columns = source_config['transform_partitions']['columns']
            values = source_config['transform_partitions']['values']
            if not len(columns) == len(values) == len(tables):
                raise Exception("transform_partitions columns, values and tables should have same length")
            for i in range(len(columns)):
                transform_partitions[tables[i]] = [columns[i], values[i]]
        print(f"migration source {config}")
        source = ClickzettaSource(config, meta_conf_path, storage_conf_path)
        if args['--table_list_file']:
            with open(args['<external_table_list_file>'], 'r') as f:
                lines = f.readlines()
                for line in lines:
                    external_table_list.append(line.strip())
        else:
            if 'migration_dbs' not in source_config:
                if 'migration_tables' not in source_config:
                    raise Exception("migration_dbs and migration_tables are both not provided in profile.yml")
                else:
                    config_table_list = source_config['migration_tables']
                    if 'dest_tables' in source_config:
                        dest_table_list = source_config['dest_tables']
                        if len(config_table_list) != len(dest_table_list):
                            raise Exception("migration_tables and dest_tables should have same length")
            else:
                db_list = source_config['migration_dbs']
    else:
        raise Exception(f"Unsupported source type: {source_config['type']}")
    destination_config = profile['destination']
    if destination_config['type'].strip() == 'clickzetta':
        config = {'service': destination_config['service'], 'username': destination_config['username'],
                  'workspace': destination_config['workspace'], 'password': destination_config['password'],
                  'instance': destination_config['instance'], 'vcluster': destination_config['vcluster'],
                  'instanceId': destination_config['instanceId']}
        logger.info(f"migration destination {config}")
        print(f"migration destination {config}")
        destination = ClickZettaDestination(config, meta_conf_path, storage_conf_path)
    elif destination_config['type'].strip() == 'doris':
        config = {'fe_servers': destination_config['fe_servers'], 'user': destination_config['username'],
                  'password': destination_config['password']}
        logger.info(f"migration destination {config}")
        print(f"migration destination {config}")
        destination = DorisDestination(config, meta_conf_path, storage_conf_path)
    else:
        raise Exception(f"Unsupported destination type: {destination_config['type']}")
    if 'concurrency' in profile:
        concurrency = profile['scheduler_concurrency']
    if 'quit_if_fail' in profile:
        quit_if_fail = profile['quit_if_fail']
    if 'thread_concurrency' in profile:
        thread_concurrency = profile['thread_concurrency']
    if args['test']:
        test(source, destination)
    elif args['meta']:
        meta(source, args['<meta_out_file_path>'])
    elif args['schema']:
        schema(source, destination, db_list, config_table_list, external_table_list, project_name, concurrency,
               quit_if_fail, thread_concurrency)
    elif args['data']:
        data(source, destination, db_list, config_table_list, external_table_list, project_name, concurrency,
             quit_if_fail, thread_concurrency, transform_partitions, dest_table_list)
    elif args['validate']:
        validate(source, destination, db_list, config_table_list, external_table_list, project_name, concurrency,
                 quit_if_fail, thread_concurrency)
    elif args['run']:
        run(source, destination, db_list, config_table_list, external_table_list, project_name, concurrency,
            quit_if_fail, thread_concurrency, transform_partitions)
    elif args['status']:
        status(destination, project_name)
    elif args['resume']:
        resume(source, destination)
    elif args['report']:
        report(destination, project_name)
    else:
        print("Unknown command")


if __name__ == '__main__':
    main()
