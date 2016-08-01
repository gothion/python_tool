# coding=utf8

import logging

from direct_association import get_picture_data
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


def main():
    hive_client = HiveClient()
    # create_dimension_table(hive_client)
    # load_data_to_table(hive_client)
    get_recommend_picture()
    get_users(hive_client)
    get_associate_result(hive_client)
    filter_picture_of_friends(hive_client)
    fetch_top_n_by_type(hive_client, 20)
    hive_client.close()


# # create table for dimension
# def create_dimension_table(hive_client):
#     create_sql = ' create table if not exists big_data.geekeye_photo_dim ( ' \
#                  ' cate_a string, cate_type string)' \
#                  ' row format delimited fields terminated by \',\' stored as textfile;'
#     hive_client.execute_no_fetch(create_sql)
#
#
# def load_data_to_table(hive_client):
#     input_data_file = '/big_data/tmp1/interest_cate_filter.csv'
#     load_sql = ' LOAD DATA INPATH \'{0}\'' \
#                ' OVERWRITE INTO TABLE big_data.geekeye_photo_dim'.format(input_data_file)
#     hive_client.execute_no_fetch(load_sql)


def drop_table(table_name, hive_client):
    drop_hql = ' DROP TABLE IF EXISTS {0}'.format(table_name)
    hive_client.execute_no_fetch(drop_hql)


# # get recommended pictures
# def get_recommend_picture(hive_client):
#     output_table = 'big_data.geekeye_photo_map_dim1'
#     input_table = 'big_data.geekeye_photo_dim'
#     yesterday = time_helper.get_yesterday_timestamp()
#     month = time_helper.get_yesterday(time_format='%Y%m')
#     drop_table(output_table, hive_client)
#     recommend_hql = ' create table {0} as select A.cate_type, B.photo_id  ' \
#                     ' FROM {1} A join ' \
#                     ' ( select distinct cate_a, photo_id from mysql_data.geekeye_photo ' \
#                     '  WHERE month={2} AND created_at>{3}  AND score_a>0.0 AND score_b > 0.5) B ' \
#                     ' ON(A.cate_a=B.cate_a)'.format(output_table, input_table,
#                                                     month, yesterday)
#     logger.info("recommend_hql is : " + recommend_hql)
#     hive_client.execute_no_fetch(recommend_hql)

def get_recommend_picture():
    get_picture_data.get_picture_data()


# get sampling data
def get_users(hive_client):
    output_table = 'big_data.direct_association_user'
    yesterday = time_helper.get_yesterday()
    drop_table(output_table, hive_client)
    sample_sql = ' create table {0} as ' \
                 ' select distinct AB.user_id, B.level_1_interest' \
                 ' from ( select  user_id' \
                 '       from (   select user_id, count(date) as count from temp.xl_week_token_token' \
                 '       group by user_id ) A ' \
                 '       where A.count > 2  )AB ' \
                 ' JOIN ( select distinct user_id, level_1_interest from ' \
                 '            user_profile.user_interest_profile_level where import_date={1} and ' \
                 '            level_1_interest in ' \
                 '          ( "entertainment", "pet",  "draw", "photograph", "travel", "fashion", "food", "sport") ' \
                 '            and user_id % 10 = 7 ) B ' \
                 ' ON(AB.user_id = B.user_id) '.format(output_table, yesterday)
    logger.info(" sample_sql is: " + sample_sql)
    hive_client.execute_no_fetch(sample_sql)


def get_associate_result(hive_client):
    output_table = 'big_data.direct_association1'
    picture_table = 'big_data.photo_cate_score_no_blur_sz'
    drop_table(output_table, hive_client)
    associate_hql = ' create table big_data.direct_association1 as' \
                    ' select distinct B.user_id,A.photo_id,A.type, cast(100*A.score as int) as score' \
                    ' from {0}  A join big_data.direct_association_user B' \
                    ' on A.type = B.level_1_interest' \
                    ' WHERE A.date = {1}'.format(picture_table, day)
    logger.info(associate_hql)
    hive_client.execute_no_fetch(associate_hql)


def filter_picture_of_friends(hive_client):
    table_name = 'big_data.direct_association1'
    hql = 'INSERT OVERWRITE TABLE {0} ' \
          ' SELECT a.user_id, a.photo_id, a.type, a.score FROM ' \
          ' (  SELECT a.user_id, a.photo_id, a.type, a.score, b.user_id as author_id ' \
          '   FROM  {1} a JOIN mysql_data.photo b ON(a.photo_id=b.id) ) a ' \
          ' LEFT OUTER JOIN mysql_data.user_watch b ' \
          ' ON(a.user_id=b.user_id AND a.author_id=b.action_user_id) ' \
          ' WHERE(b.user_id is NULL OR b.action_user_id is NULL)'.format(table_name, table_name)
    hive_client.execute_no_fetch(hql)


def fetch_top_n_by_type(hive_client, top_num):
    table_name = 'big_data.direct_association1'
    hql = 'INSERT OVERWRITE TABLE {0} ' \
          ' SELECT t.user_id, t.photo_id, t.type, t.score ' \
          ' FROM(select user_id, photo_id, type, score, ' \
          ' row_number() over(distribute by user_id, type sort by score desc) AS row_num FROM' \
          '   {1}) t where t.row_num < {2} '.format(table_name, table_name, top_num)
    hive_client.execute_no_fetch(hql)

if __name__ == '__main__':
    main()
