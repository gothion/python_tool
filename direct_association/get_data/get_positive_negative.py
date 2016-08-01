# coding=utf8
import logging
from direct_association.utils import time_helper
from direct_association.utils.hive_client import HiveClient

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

day = time_helper.get_yesterday()
month = time_helper.get_yesterday("%Y%m")

# 获取正样本,直接找有点击的图片就是正样本, 找到和有点击的图片在一屏的图片,但没有被点击的图片就是负样本
# 第一步: 找人, 找到那些15天之前的,和15天之后都有点击行为的人
# 第二步: 找到那些人,以及补充信息


def get_positive_and_negative_data(day_num=7):
    hive_client = HiveClient()
    create_user_id_token_map(hive_client)
    get_active_users(hive_client, day_num)
    filter_friends_action(hive_client,
                          'temp.click_positive_token_photo_id_sz', 'temp.positive_sample_sz')
    get_negative_sample(hive_client, day_num)
    filter_friends_action(hive_client,
                          'temp.negative_token_sz', 'temp.negative_sample_sz')
    hive_client.close()


def _create_table(hive_client, table_name, field_info, partition_info=None):
    create_hql = 'CREATE TABLE IF NOT EXISTS {0}({1})'.format(table_name, field_info)
    if partition_info:
        create_hql += ' PARTITIONED BY ({0})'.format(partition_info)
    store_info = ' STORED AS RCFILE'
    create_hql += store_info
    logger.info(create_hql)
    hive_client.execute_no_fetch(create_hql)


def get_active_users(hive_client, day_num):
    get_users_before_days(hive_client, day_num)
    get_users_positive_data(hive_client, day_num)


def get_users_before_days(hive_client, day_num):
    table_name = 'temp.click_users_sz'
    field_info = 'token string'
    _create_table(hive_client, table_name, field_info)
    yesterday = time_helper.get_yesterday()
    day_former = time_helper.get_day_before(yesterday, day_num * 2)
    day_later = time_helper.get_day_before(yesterday, day_num)
    hql = 'INSERT OVERWRITE TABLE {0} ' \
          '  SELECT a.token FROM (SELECT distinct token as token  ' \
          '  FROM web_data.stats_photo_detail where date>={1} and date<{2} ) a ' \
          '  JOIN (SELECT distinct token as token  FROM web_data.stats_photo_detail where date >={2}) b' \
          '  ON(a.token=b.token) '.format(table_name, day_former, day_later)
    hive_client.execute_no_fetch(hql)


def get_users_positive_data(hive_client, day_num):
    table_name = 'temp.click_positive_token_photo_id_sz'
    input_table_name = 'temp.click_users_sz'
    field_info = 'token string, photo_id string, date int '
    days_before = time_helper.get_day_before(time_helper.get_yesterday(), day_num)
    _create_table(hive_client, table_name, field_info)
    hql = 'INSERT OVERWRITE TABLE {0} ' \
          '  SELECT distinct b.token as token, b.photo_id as photo_id, date FROM {1} a ' \
          '  JOIN  web_data.stats_photo_detail b ON(a.token=b.token) WHERE b.date>={2} ' \
          ' '.format(table_name, input_table_name, days_before)
    hive_client.execute_no_fetch(hql)


def create_user_id_token_map(hive_client):
    table_name = 'temp.user_id_token_map'
    field_info = 'token string, user_id bigint'
    _create_table(hive_client, table_name, field_info)
    hql = 'INSERT OVERWRITE TABLE {0} ' \
          ' SELECT private_key as token, id from mysql_data.user ' \
          ' UNION ALL ' \
          ' SELECT private_key as token, id from mysql_data.user_token '
    hive_client.execute_no_fetch(hql)


def filter_friends_action(hive_client, input_table, table_name):
    # table_name = 'temp.positive_sample_sz'
    user_token_map = 'temp.user_id_token_map'
    # origin_table = 'temp.click_positive_token_photo_id_sz'
    field_info = 'user_id bigint, action_user_id bigint, photo_id, date int '
    _create_table(hive_client, table_name, field_info)

    hql = 'INSERT OVERWRITE TABLE {0} ' \
          ' SELECT c.user_id, b.user_id as action_user_id, a.photo_id, a.date '\
          ' FROM {1} a ' \
          ' JOIN {2} b ON(a.token=b.token) '\
          ' JOIN mysql_data.photo c ON(a.photo_id = c.id) ' \
          ' WHERE c.user_id<>b.user_id '.format(table_name, input_table, user_token_map)
    hive_client.execute_no_fetch(hql)


def get_negative_sample(hive_client, day_num):
    output_table = 'temp.negative_token_sz'
    input_table1 = 'temp.click_positive_token_photo_id_sz'
    create_hql = 'CREATE TABLE  IF NOT EXISTS {0} like {1} '.format(output_table, input_table1)
    hive_client.execute_no_fetch(create_hql)
    days_before = time_helper.get_day_before(time_helper.get_yesterday(), day_num)
    input_table2 = 'web_data.stats1_faxianbg'
    input_table3 = 'temp.click_users_sz'

    hql = ' INSERT OVERWRITE TABLE {0}  ' \
          '  SELECT distinct a.token , a.photo_id , a. date FROM ' \
          '    ( SELECT distinct a.token , a.photo_id , a. date  FROM {1} a ' \
          '      JOIN {2} b ON(a.token=b.token) where a.date >={4}) a ' \
          ' LEFT OUTER JOIN {3} c ON(a.token=c.token and a.photo_id = c.photo_id) ' \
          ' where  c.photo_id is NULL '.format(output_table, input_table2, input_table3, input_table1, days_before)
    hive_client.execute_no_fetch(hql)


if __name__ == '__main__':
    get_positive_and_negative_data()
