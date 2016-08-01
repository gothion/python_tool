# coding=utf8


from direct_association.utils.hive_client import HiveClient
import sys
import os


def main(file_name):
    out_put_folder = os.path.dirname(file_name)
    relative_name = out_put_folder + os.path.basename(file_name)
    hive_client = HiveClient()
    table_name = "temp." + relative_name + "_xj"
    output_file_name = out_put_folder + "/" + "new_" + file_name
    create_temp_table(hive_client, table_name)
    load_data_to_table(hive_client, table_name, file_name)
    filter_blur_picture(hive_client, table_name, output_file_name)
    hive_client.close()


def create_temp_table(hive_client, table_name):
    create_hql = ' create table if not exists {0}( ' \
                 ' photo_id  int, score double) ' \
                 ' row format delimited fields terminated by \',\' stored as textfile'.format(table_name)
    print create_hql
    hive_client.execute_no_fetch(create_hql)


def load_data_to_table(hive_client, table_name, file_name):
    load_hql = ' LOAD DATA LOCAL INPATH \'{0}\'' \
               ' OVERWRITE INTO TABLE {1}'.format(file_name, table_name)
    hive_client.execute_no_fetch(load_hql)


def filter_blur_picture(hive_client, table_name, output_file_name):
    filter_hql = 'select a.photo_id, a.score, b.score_b from  {0} a ' \
                 'JOIN mysql_data.geekeye_photo b ON(a.photo_id=b.photo_id) where b.score_b > 0.5 '.format(table_name)
    hive_client.execute_and_export(filter_hql, output_file_name)

if __name__ == '__main__':
    print "file name is {0} and output folder is {1}".format(sys.argv[1], sys.argv[2])
    main(sys.argv[1])
