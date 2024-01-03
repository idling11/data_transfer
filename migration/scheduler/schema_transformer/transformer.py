import logging
from migration.base.exceptions import ProfileConfigError, GrammarRestrictionsError, SchedulerRuntimeError
from migration.scheduler.task.schema_task import SchemaMigrationTask
from migration.scheduler.task.base_task import TaskType
from migration.scheduler.transformer import Transformer
from migration.scheduler.scheduler import SchedulerConfig, Scheduler
import migration.util.migration_tasks_status as migration_tasks_status

logger = logging.getLogger(__name__)

DOT_SPLITTER = '.'
LEFT_BRACKET = '('


class SchemaTransformer(Transformer):
    def get_migration_tasks(self):
        logger.info("Start to get schema migration tasks")
        transform_table_list = self.get_transform_table_list()
        if not transform_table_list:
            logger.warning("No table to transform, please check your profile.yml")
            raise ProfileConfigError("No table to transform, please check your profile.yml")
        schema_migration_tables = []

        if DOT_SPLITTER not in transform_table_list[0]:
            for db in transform_table_list:
                tables = self.source.get_table_names(db)
                for table in tables:
                    schema_migration_tables.append(f"{db}.{table[0]}")
        else:
            for table in transform_table_list:
                if '*' in table:
                    db = table.split(DOT_SPLITTER)[0]
                    tables = self.source.get_table_names(db)
                    for result in tables:
                        schema_migration_tables.append(f"{db}.{result[0]}")
                else:
                    schema_migration_tables.append(table)
        logger.debug(f"schema migration tasks: {schema_migration_tables}")
        logger.info(f"schema migration tasks count: {len(schema_migration_tables)}")
        schema_migration_tasks = []
        for table in schema_migration_tables:
            schema_migration_tasks.append(self.construct_schema_task(table))

        return schema_migration_tasks

    def construct_schema_task(self, table_name):
        db, table = table_name.split(DOT_SPLITTER)
        type_mapping = self.source.type_mapping()
        for k, v in type_mapping.items():
            type_mapping.update({k.upper(): v.upper()})
        primary_keys = self.source.get_primary_key(db, table)
        cluster_info = self.source.get_table_cluster_info(db, table)
        partition_columns = self.source.get_table_partition_columns(db, table)
        columns = self.source.get_table_columns(db, table)
        for column in columns:
            if LEFT_BRACKET in column.type:
                column_type = column.type.split(LEFT_BRACKET)[0]
                if column_type in type_mapping:
                    column.type.replace(column_type, type_mapping.get(column_type))
            else:
                if column.type in type_mapping:
                    column.type = type_mapping.get(column.type)

        destination_ddl = self.destination.gen_destination_ddl(db, table, columns, primary_keys, cluster_info,
                                                               partition_columns)
        task = SchemaMigrationTask(name=table_name, ddl=destination_ddl, task_type=TaskType.SCHEMA_MIGRATION,
                                   project_id=self.project_id, destination=self.destination)
        migration_tasks_status.init_task_status(self.destination, task)
        return task

    def schedule_migration_tasks(self):
        logger.info("Start to schedule schema migration tasks")
        migration_tasks = self.get_migration_tasks()
        if migration_tasks:
            if len(migration_tasks) < self.transform_concurrency:
                schedule_config = SchedulerConfig(quit_if_failed=self.quit_if_fail, max_workers=self.thread_concurrency)
                scheduler = Scheduler(schedule_config)
                self.schedules_tasks_map[scheduler] = migration_tasks
            else:
                for i in range(self.transform_concurrency):
                    schedule_config = SchedulerConfig(quit_if_failed=self.quit_if_fail, max_workers=self.thread_concurrency)
                    scheduler = Scheduler(schedule_config)
                    self.schedules_tasks_map[scheduler] = migration_tasks[i::self.transform_concurrency]
