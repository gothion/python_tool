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


def get_user_and_item_info():
    hive_client = HiveClient()
    process_table(hive_client, 'temp.positive_sample_sz')
    process_table(hive_client, 'temp.negative_sample_sz')
    process_table(hive_client, 'temp.positive_sample_sz_history')
    process_table(hive_client, 'temp.negative_sample_sz_history')

    hive_client.close()


def process_table(hive_client, table_name):
    new_table_name, new_field_info = get_following_actions(hive_client,
                                                           7, table_name,
                                                           'user_id bigint, action_user_id bigint, '
                                                           'photo_id string,date int')
    new_table_name, new_field_info = get_comment_actions(hive_client, 7, new_table_name, new_field_info)
    new_table_name, new_field_info = get_user_author_infos(hive_client, new_table_name, new_field_info)
    new_table_name, new_field_info = get_picture_info(hive_client, new_table_name, new_field_info)
    logger.info("the new_table_name is {0} and filed info is ".format(new_table_name, new_field_info))


def _create_table(hive_client, table_name, field_info, partition_info=None):
    create_hql = 'CREATE TABLE IF NOT EXISTS {0}({1})'.format(table_name, field_info)
    if partition_info:
        create_hql += ' PARTITIONED BY ({0})'.format(partition_info)
    store_info = ' STORED AS RCFILE'
    create_hql += store_info
    logger.info(create_hql)
    hive_client.execute_no_fetch(create_hql)


def get_following_actions(hive_client, day_num, table_name, filed_info):
    logger.info("getting following infos")
    output_table = table_name + '_follow'
    real_field_info = filed_info + ', ' + 'action_is_follow int'
    _create_table(hive_client, output_table, real_field_info)
    days_before = time_helper.get_day_before(time_helper.get_yesterday(), day_num)
    month_before = time_helper.get_day_before(time_helper.get_yesterday('%Y%m'), day_num, '%Y%m')

    hql = ' INSERT OVERWRITE TABLE {0}  ' \
          ' SELECT a.*, if(b.action_user_id IS NOT NULL, 1, 0) from {1} a LEFT OUTER JOIN ' \
          '    ( SELECT  distinct action_user_id FROM mysql_data.user_watch ' \
          '    WHERE month={2} and cast(updated_at/1000000 as int)>{3}) b ' \
          ' ON a.action_user_id=b.action_user_id '.format(output_table, table_name,
                                                          month_before, days_before)
    hive_client.execute_no_fetch(hql)
    return output_table, real_field_info


def get_comment_actions(hive_client, day_num, table_name, field_info):
    logger.info("getting comment infos")
    output_table = table_name + '_comment'
    real_field_info = field_info + ', ' + 'action_is_comment int'
    _create_table(hive_client, output_table, real_field_info)

    days_before = time_helper.get_day_before(time_helper.get_yesterday(), day_num)
    month_before = time_helper.get_day_before(time_helper.get_yesterday('%Y%m'), day_num, '%Y%m')

    hql = ' INSERT OVERWRITE TABLE {0}  ' \
          ' SELECT a.*, if(b.action_user_id IS NOT NULL, 1, 0) from {1} a LEFT OUTER JOIN ' \
          '    ( SELECT  distinct user_id as action_user_id FROM mysql_data.photo_comment ' \
          '    WHERE month={2} and cast(updated_at/1000000 as int)>{3}) b ' \
          ' ON a.action_user_id=b.action_user_id '.format(output_table, table_name,
                                                          month_before, days_before)
    hive_client.execute_no_fetch(hql)
    return output_table, real_field_info


def get_user_author_infos(hive_client, table_name, field_info):
    logger.info("getting user_author infos")
    output_table = table_name + '_people_info'
    real_field_info = field_info + ', ' + 'action_user_gender String,' \
                                          ' user_gender String, in_same_city int'
    _create_table(hive_client, output_table, real_field_info)

    hql = ' INSERT OVERWRITE TABLE {0}  ' \
          ' SELECT a.*, b.gender, c.gender if(b.province=c.province and b.city=c.city, 1, 0) FROM {1} a ' \
          '  JOIN  mysql_data.user b  ON a.action_user_id =b.id ' \
          '  JOIN mysql_data.user c ON a.user_id=c.id '.format(output_table, table_name)
    hive_client.execute_no_fetch(hql)
    return output_table, real_field_info


def get_picture_info(hive_client, table_name, field_info):
    logger.info("getting picture info")
    output_table = table_name + '_picture'
    real_field_info = field_info + ', ' + 'score_b decimal(10,8), category string'
    _create_table(hive_client, output_table, real_field_info)
    hql = ' INSERT OVERWRITE TABLE {0}  ' \
          ' SELECT a.*, b.score_b, b.cate_a  FROM {1} a ' \
          '  JOIN  mysql_data.geekeye_photo b  ON a.photo_id =b.photo_id '.format(output_table, table_name)

    hive_client.execute_no_fetch(hql)
    return output_table, real_field_info


if __name__ == '__main__':
    get_user_and_item_info()
