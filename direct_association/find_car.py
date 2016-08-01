# coding=utf8
import logging
from direct_association.utils.hive_client import HiveClient

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

car_brand = ['法拉利', '劳斯莱斯', '阿斯顿·马丁', '阿斯顿', '宾利', '本特利', '兰博基尼',
             '保时捷', '迈巴赫', '卡宴', '林肯', '奔驰', '宝马', '奥迪', '捷豹', '凯迪拉克',
             '雷克萨斯', '大众', '福特', '玛莎拉蒂','帕加尼','阿尔法·罗密欧','蓝旗亚', '布加迪',
             '菲亚特', 'ABARTH', '迈凯轮', 'MINI', 'Eterniti', '捷豹', '路虎' , '路特斯',
             '摩根', 'Gumpert']
base_file_name = "_car.txt"
print len(car_brand)


def find_car_by_name():
    hive_client = HiveClient()
    for car in car_brand:
        hql = 'select photo_id, tag_name from  big_data.mds_photo_tag_info' \
              ' where tag_name like \'%{0}%\''.format(car)
        file_name = car + base_file_name
        hive_client.execute_and_export(hql, file_name)
    hive_client.close()


if __name__ == '__main__':
    find_car_by_name()