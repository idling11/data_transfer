import os.path
import random
import unittest
import sqlparse
import re

from migration.connector.destination.clickzetta.destination import ClickZettaDestination


class TestUtil(unittest.TestCase):
    def test_sqlparse(self):
        sql = '''
        CREATE TABLE table2
(
    event_day DATE,
    siteid INT DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
UNIQUE    KEY (event_day, siteid, citycode, username)
PARTITION BY RANGE(`event_day`)
(
    PARTITION p201706 VALUES LESS THAN ('2017-07-01'),
    PARTITION p201707 VALUES LESS THAN ('2017-08-01'),
    PARTITION p201708 VALUES LESS THAN ('2017-09-01')
)
DISTRIBUTED BY HASH(`siteid`) BUCKETS 10
PROPERTIES("replication_num" = "1");
        '''
        sql = sqlparse.format(sql, reindent=True, keyword_case='upper')
        # result = re.match(r'(.*)AGGREGATE(.*)KEY(.*?)\((.*?)\).*', sql, re.S)
        result = re.match(r'(.*)DISTRIBUTED(.*)BY(.*?)\((.*?)\)(.*)BUCKETS(.*?)(\d+).*', sql, re.S)
        # result = re.match(r'(.*?)PARTITION(.*?)BY(.*?)\((.*?)\).*', sql, re.S)
        # result = re.match(r'(.*?)UNIQUE(.*?)KEY(.*?)\((.*?)\).*', sql, re.S)
        for column in result.group(4).strip().split(','):
            print(column.replace('`', '').strip())

        print(result.group(7).strip())
        print('vachar(32)'.upper())

    def test_list_spilt(self):
        class Box:
            def __init__(self, name):
                self.name = name

        boxes_tasks_map = {}
        tasks = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        b = [tasks[i:i + 3] for i in range(0, len(tasks), 3)]

        print(b)

    def test_path(self):

        all_tasks = []
        a = ['a1', 'a2', 'a3']
        b = ['b1', 'b2', 'b3']
        c = ['c1', 'c2', 'c3']
        for i in range(3):
            all_tasks.extend((a[i], b[i], c[i]))
        print(all_tasks)

    def test_destination(self):
        PK_TABLE_DML_HINT = {'hints': {'cz.sql.allow.insert.table.with.pk': 'true'}}
        config = {'service': '', 'username': '',
                  'workspace': 'quickStart_WS',
                  'password': '', 'instance': '', 'vcluster': 'DEFAULT', "instanceId": 32}
        destination = ClickZettaDestination(config)
        print(destination.execute_sql('show schemas;'))
        print(os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))), 'scripts/linux/meta_cmd'))
        list_1 = (8, 8, 8.0)
        list_2 = (8.0, 8.0, 8.0)
        print(list_1 == list_2)
        for i in range(1):
            print(i)
        a = [1000]
        b = (1000,)
        print(a[0] == b[0])

    def test_text_file(self):
        with open('/tmp/doris_test_3.txt', 'w') as f:
            for i in range(2500000):
                line = f"{random.randint(0, 1000000)}\t{random.randint(0, 1000000)}\t{random.randint(0, 1000000)}\t{random.randint(0, 1000000)}\t{random.randint(0, 1000000)}\t{random.randint(0, 1000000)}\n"
                f.write(line)
