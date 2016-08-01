# coding=utf8
from direct_association.utils.hive_client import HiveClient


# B.user_id,A.photo_id,A.cate_type
def main(file_name):
    hive_client = HiveClient()
    table_name = "big_data.direct_association1"
    statistic_people_sql = " SELECT user_id, count(1) FROM {0} GROUP BY user_id ".format(table_name)
    statistic_photo_id_sql = 'SELECT photo_id, count(1) FROM {0} GROUP BY photo_id '.format(table_name)
    statistic_cate_type_sql = 'SELECT cate_type, count(1) FROM {0} GROUP BY cate_type '.format(table_name)
    statistic_people_num_sql = 'SELECT count(distinct user_id) FROM {0}  '.format(table_name)
    statistic_photo_num_sql = 'SELECT count(distinct photo_id) FROM {0} '.format(table_name)
    statistic_hql_arr = [statistic_people_sql, statistic_cate_type_sql,
                         statistic_photo_id_sql, statistic_people_num_sql,
                         statistic_photo_num_sql]
    hive_client.execute_and_export_arr(statistic_hql_arr, file_name)
    hive_client.close()

if __name__ == '__main__':
    main("direct_association_stat.txt")
