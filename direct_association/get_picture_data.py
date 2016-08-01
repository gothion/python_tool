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
category_map = {
    '动漫': 'comic', '娱乐': 'entertainment', '宠物': 'pet', '幽默': 'humor', '手绘': 'draw', '摄影': 'photograph',
    '旅行': 'travel', '游戏': 'game', '穿搭': 'fashion', '美妆': 'fashion', '美食': 'food', '萌娃': 'kid', '运动': 'sport'
}


def get_picture_data():
    hive_client = HiveClient()
    get_image_category(hive_client)
    load_data_into_talent(hive_client)
    load_data_into_hot(hive_client)
    load_data_into_tag(hive_client)
    union_above_tables(hive_client)
    join(hive_client)
    mapping(hive_client)
    follow(hive_client)
    click(hive_client)
    get_result(hive_client)
    filter_blur_picture(hive_client)
    filter_unwanted_picture(hive_client)
    convert_category(hive_client)
    hive_client.close()


def _create_table(hive_client, table_name, field_info, partition_info=None):
    create_hql = 'CREATE TABLE IF NOT EXISTS {0}({1})'.format(table_name, field_info)
    if partition_info:
        create_hql += ' PARTITIONED BY ({0})'.format(partition_info)
    store_info = ' STORED AS RCFILE'
    create_hql += store_info
    logger.info(create_hql)
    hive_client.execute_no_fetch(create_hql)


def get_image_category(hive_client):
    table_name = 'big_data.yw_typephoto_geekeye'
    filed_info = ' type string, photo_id string, source string, weight int'
    part_info = 'date string'
    _create_table(hive_client, table_name, filed_info, part_info)
    hql = 'INSERT OVERWRITE TABLE big_data.yw_typephoto_geekeye PARTITION(date={0})'\
          ' SELECT CASE '\
          '        WHEN cate_a LIKE \'2%\' THEN \'宠物\' ' \
          '        WHEN cate_a LIKE \'4%\' THEN \'美食\' ' \
          '        WHEN cate_a = 3004001 THEN \'萌娃\' END AS type, photo_id,\'geekeye\', 1' \
          ' FROM mysql_data.geekeye_photo     WHERE MONTH>={1} AND ' \
          ' from_unixtime(created_at,\'yyyyMMdd\')>={2}' \
          ' AND (cate_a LIKE \'2%\' OR cate_a LIKE \'4%\' OR cate_a=3004001)'.format(day, month, day)
    hive_client.execute_no_fetch(hql)


def load_data_into_talent(hive_client):
    table_name = 'big_data.yw_typephoto_talent'
    filed_info = ' type string, photo_id string, source string, weight int'
    part_info = 'date string'
    _create_table(hive_client, table_name, filed_info, part_info)
    hql = 'INSERT OVERWRITE TABLE {0} PARTITION(date={1}) ' \
          'SELECT CASE          ' \
          ' WHEN a.cate_ids =6 THEN \'摄影\' ' \
          ' WHEN a.cate_ids =26 THEN \'娱乐\' ' \
          ' WHEN a.cate_ids =31 THEN \'幽默\' ' \
          ' WHEN a.cate_ids IN (1, 13, 27) THEN \'穿搭\' ' \
          ' WHEN a.cate_ids =38 THEN \'美妆\' ' \
          ' WHEN a.cate_ids =14 THEN \'旅行\' '\
          ' WHEN a.cate_ids =29 THEN \'动漫\' '\
          ' WHEN a.cate_ids =30 THEN \'运动\' '\
          ' WHEN a.cate_ids =11 THEN \'手绘\' '\
          ' WHEN a.cate_ids =7 THEN \'美食\' ' \
          ' END AS type, b.id ,\'talent_user\' ,0.5 ' \
              ' FROM mysql_data.talent_user a JOIN mysql_data.photo b ON(a.user_id=b.user_id) ' \
          ' WHERE  b.month>={2} AND from_unixtime(b.created_at,\'yyyyMMdd\')>={3} ' \
          ' AND a.cate_ids IN (6,26,31,11,1,13,38,27,14,29,30,7)'.format(table_name, day, month, day)
    hive_client.execute_no_fetch(hql)


def load_data_into_hot(hive_client):
    table_name = 'big_data.yw_typephoto_hot'
    filed_info = ' type string, photo_id string, source string, weight int'
    part_info = 'date string'
    _create_table(hive_client, table_name, filed_info, part_info)
    hql = 'INSERT OVERWRITE TABLE {0} PARTITION(date={1}) ' \
          'SELECT CASE          ' \
          ' WHEN a.cate_id =6 THEN \'摄影\' ' \
          ' WHEN a.cate_id =26 THEN \'娱乐\' ' \
          ' WHEN a.cate_id =31 THEN \'幽默\' ' \
          ' WHEN a.cate_id IN (1, 13, 27) THEN \'穿搭\' ' \
          ' WHEN a.cate_id =38 THEN \'美妆\' ' \
          ' WHEN a.cate_id =14 THEN \'旅行\' '\
          ' WHEN a.cate_id =29 THEN \'动漫\' '\
          ' WHEN a.cate_id =30 THEN \'运动\' '\
          ' WHEN a.cate_id =11 THEN \'手绘\' '\
          ' WHEN a.cate_id =7 THEN \'美食\' ' \
          ' END AS type,a.photo_id ,\'hot\' , 2 ' \
          ' FROM mysql_data.photo_hot a ' \
          ' WHERE  from_unixtime(a.created_at, \'yyyyMMdd\')>={2} ' \
          ' AND a.cate_id IN (6,26,31,11,1,13,38,27,14,29,30,7)'.format(table_name, day, day)
    hive_client.execute_no_fetch(hql)


def load_data_into_tag(hive_client):
    table_name = 'big_data.yw_typephoto_tag'
    filed_info = ' type string, photo_id string, source string, weight int'
    part_info = 'date string'
    _create_table(hive_client, table_name, filed_info, part_info)
    hql = 'INSERT OVERWRITE TABLE {0} PARTITION(date={1})'\
          ' SELECT DISTINCT b.type, a.photo_id, \'tag\', 1 '\
          ' FROM mysql_data.photo_tag_mapping a JOIN temp.yw_tag_type b ' \
          ' ON (a.tag_id=b.tag_id) ' \
          ' WHERE from_unixtime(a.created_at,\'yyyyMMdd\')>={2} '.format(table_name, day, day)
    hive_client.execute_no_fetch(hql)


def union_above_tables(hive_client):
    table_name = ' big_data.yw_typephoto '
    field_info = ' type string, photo_id bigint '
    part_info = 'date string'
    _create_table(hive_client, table_name, field_info, part_info)

    insert_hql = ' INSERT OVERWRITE TABLE {0} PARTITION(date={1}) '.format(table_name, day)
    table_arr = ['big_data.yw_typephoto_geekeye',
                 'big_data.yw_typephoto_talent',
                 'big_data.yw_typephoto_hot',
                 'big_data.yw_typephoto_hot']
    select_sqls = [_form_select_sql(a_table) for a_table in table_arr]
    union_part = ' UNION ALL '.join(select_sqls)
    hql = insert_hql + ' SELECT DISTINCT type, photo_id FROM( ' + union_part + ' ) a '
    hive_client.execute_no_fetch(hql)


def _form_select_sql(table_name):
    return 'SELECT type,photo_id FROM {0} WHERE date={1}'.format(table_name, day)


def join(hive_client):
    table_name = 'big_data.yw_typephoto_1'
    field_info = ' type string, photo_id bigint, geekeye int, talent int, hot int, tag int '
    part_info = 'date string'
    _create_table(hive_client, table_name, field_info, part_info)

    insert_part = 'INSERT OVERWRITE TABLE {0} PARTITION(date={1})'.format(table_name, day)
    case_info_arr = [('b', 'geekeye'), ('c', 'talent'), ('d', 'hot'), ('e', 'tag')]
    case_info_part = ', '.join([_convert_case_info(a_info) for a_info in case_info_arr])

    select_part = ' SELECT a.type, a.photo_id, {0} FROM big_data.yw_typephoto a  '.format(case_info_part)

    to_be_join_table_arr = [('b', 'big_data.yw_typephoto_geekeye'),
                            ('c', 'big_data.yw_typephoto_talent'),
                            ('d', 'big_data.yw_typephoto_hot'),
                            ('e', 'big_data.yw_typephoto_tag')]
    to_be_join_part = '  '.join([_convert_join_info(a_table_info) for a_table_info in to_be_join_table_arr])

    hql = insert_part + ' ' + select_part + ' ' + to_be_join_part + ' WHERE a.date={0} '.format(day)
    hive_client.execute_no_fetch(hql)


def _convert_case_info(case_info_pair):
    alias = case_info_pair[0]
    field_name = case_info_pair[1]
    return ' CASE WHEN {0}.photo_id  IS NOT NULL THEN 1 ELSE 0  END AS {1} '.format(alias, field_name)


def _convert_join_info(table_alias_pair):
    alias = table_alias_pair[0]
    table_name = table_alias_pair[1]
    return ' LEFT OUTER JOIN( SELECT * FROM {0} where date={1}) {2}' \
           ' ON(a.photo_id={3}.photo_id AND a.type={4}.type) '.format(table_name, day, alias, alias, alias)


def mapping(hive_client):
    table_name = 'big_data.yw_typephoto_2'
    field_info = 'type string, photo_id bigint, geekeye int, talent int, ' \
                 'hot int, tag int, action_user_id int, user_id int'
    part_info = 'date string'
    _create_table(hive_client, table_name, field_info, part_info)
    hql = 'INSERT OVERWRITE TABLE {0} PARTITION(date={1})' \
          ' SELECT DISTINCT a.type, a.photo_id, a.geekeye, a.talent, a.hot, a.tag, b.action_user_id, b.user_id ' \
          ' FROM( SELECT * FROM big_data.yw_typephoto_1 where date={2}) a ' \
          ' JOIN mysql_data.photo_zan_mapping b ON(a.photo_id=b.photo_id) '.format(table_name, day, day)
    hive_client.execute_no_fetch(hql)


def follow(hive_client):
    table_name = 'big_data.yw_typephoto_3'
    field_info = 'type string, photo_id bigint, geekeye int, talent int, ' \
                 'hot int, tag int, action_user_id int, user_id int, is_eachother tinyint'
    part_info = 'date string'
    _create_table(hive_client, table_name, field_info, part_info)
    hql = 'INSERT OVERWRITE TABLE {0} PARTITION(date={1})'\
          ' SELECT DISTINCT a.type, a.photo_id, a.geekeye, a.talent, a.hot, a.tag, a.action_user_id, a.user_id, ' \
          ' b.is_eachother ' \
          ' FROM( SELECT * FROM big_data.yw_typephoto_2 where date={2} ) a ' \
          ' LEFT OUTER JOIN mysql_data.user_watch b ON (a.action_user_id=b.action_user_id ' \
          ' AND a.user_id=b.user_id AND b.is_eachother=1) '.format(table_name, day, day)

    hive_client.execute_no_fetch(hql)


def click(hive_client):
    table_name = 'big_data.yw_typephoto_4'
    field_info = 'type string, photo_id bigint, geekeye int, talent int, ' \
                 'hot int, tag int, o_zan_count bigint'
    part_info = 'date string'
    _create_table(hive_client, table_name, field_info, part_info)
    hql = 'INSERT OVERWRITE TABLE {0} PARTITION(date={1})'\
          ' SELECT a.type, a.photo_id, a.geekeye, a.talent, ' \
          ' a.hot, a.tag, count(DISTINCT a.action_user_id) as o_zan_count ' \
          ' FROM (SELECT * FROM big_data.yw_typephoto_3 where date={2}) a ' \
          ' GROUP BY a.type,a.photo_id,a.geekeye,a.talent,a.hot,a.tag'.format(table_name, day, day)
    hive_client.execute_no_fetch(hql)


def get_result(hive_client):
    table_name = 'big_data.photo_cate_score'
    field_info = ' photo_id bigint, type string, score double '
    part_info = 'date string'
    _create_table(hive_client, table_name, field_info, part_info)
    hql = 'INSERT OVERWRITE TABLE {0} PARTITION(date={1})' \
          ' SELECT photo_id, type, sum(geekeye+(talent*0.5+hot*2+tag)*o_zan_count) as score ' \
          ' FROM big_data.yw_typephoto_4 ' \
          ' GROUP BY photo_id, type '.format(table_name, day)
    hive_client.execute_no_fetch(hql)


def filter_blur_picture(hive_client):
    table_name = 'big_data.photo_cate_score_no_blur_sz'
    field_info = ' photo_id bigint, type string, score double '
    part_info = 'date string'
    _create_table(hive_client, table_name, field_info, part_info)
    hql = 'INSERT OVERWRITE TABLE {0} PARTITION(date={1})' \
          ' select DISTINCT a.photo_id, a.type, a.score FROM big_data.photo_cate_score a' \
          ' JOIN mysql_data.geekeye_photo b ' \
          ' ON(a.photo_id=b.photo_id) where a.date={2} and  b.score_b > 0.5'.format(table_name, day, day)
    hive_client.execute_no_fetch(hql)


def filter_unwanted_picture(hive_client):
    table_name = 'big_data.photo_cate_score_no_blur_sz'
    input_table_name = 'big_data.photo_cate_score_no_blur_sz_new'
    create_hql = 'CREATE TABLE  IF NOT EXISTS {0} like {1} '.format(table_name, input_table_name)
    hive_client.execute_no_fetch(create_hql)

    hql = 'INSERT OVERWRITE TABLE {0} PARTITION(date={1})' \
          ' SELECT a.photo_id, a.type, a.score FROM {2} a ' \
          ' LEFT OUTER JOIN (select id from  mysql_data.photo ' \
          '   where is_delete<>\"no\"  or is_private<>0 or is_show<>1 or status=\"disable\") b ' \
          ' ON(a.photo_id = b.id) WHERE a.date={3} and b.id IS NULL'.format(table_name, day, input_table_name, day)
    hive_client.execute_no_fetch(hql)


def convert_category(hive_client):
    table_name = 'big_data.photo_cate_score_no_blur_sz_new'
    input_table_name = 'big_data.photo_cate_score_no_blur_sz'
    case_info = _convert_cate_case_info()
    hql = 'INSERT OVERWRITE TABLE {0} PARTITION(date={1})' \
          ' SELECT photo_id, {2}, score  FROM {3} WHERE date={4}'.format(table_name, day,
                                                                         case_info, input_table_name, day)
    hive_client.execute_no_fetch(hql)


def _convert_cate_case_info():
    case_info = 'CASE type '
    condition_info = ' '.join(['WHEN "{0}" THEN "{1}" '.format(k, v) for k, v in category_map.items()])
    case_info = case_info + " " + condition_info + " " + " END  AS type "
    return case_info


if __name__ == '__main__':
    get_picture_data()
