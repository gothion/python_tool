# coding=utf8
import logging
import pyhs2
import project_helper

logger = logging.getLogger(__name__)


class HiveClient(object):

    def __init__(self):
        self.conn = pyhs2.connect(host=project_helper.get_hive_host(),
                                  port=project_helper.get_hive_port(),
                                  authMechanism=project_helper.get_hive_auth_mechanism(),
                                  user=project_helper.get_hive_user(),
                                  password=project_helper.get_hive_password(),
                                  database=project_helper.get_hive_database(),
                                  )

    def close(self):
        if self.conn:
            self.conn.close()

    def execute(self, sql):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetch()
            return result

    def execute_no_fetch(self, sql):
        logger.info(sql)
        with self.conn.cursor() as cur:
            cur.execute(sql)

    def execute_arr(self, sql_arr):
        with self.conn.cursor() as cur:
            for sql in sql_arr:
                cur.execute(sql)
            return cur.fetch()

    def execute_no_fetch_arr(self, sql_arr):
        with self.conn.cursor() as cur:
            for sql in sql_arr:
                cur.execute(sql)

    def execute_and_export(self, sql, data_file):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetch()
            with open(data_file, 'w') as f:
                for row in result:
                    row_str = ','.join(row)
                    f.write(row_str)
                    f.write('\n')

    def execute_and_export_arr(self, sql_arr, data_file):
        with self.conn.cursor() as cur:
            with open(data_file, 'w') as f:
                for sql in sql_arr:
                    cur.execute(sql)
                    result = cur.fetch()

                    for row in result:
                        for column in row:
                            if column is None:
                                column = 'NULL'
                            f.write('%s,' % column)
                        f.write('\n')
