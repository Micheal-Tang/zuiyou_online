# !/usr/bin/python
# -*-coding: utf-8 -*-

import pandas as pd
from odps import ODPS

# 初始化类
AccessKeyID = 'x'
AccessKeySecret = 'x'
odps_db = ODPS(AccessKeyID, AccessKeySecret, 'x')
odps_settings = {'set odps.sql.python.version': 'cp37', "odps.sql.submit.mode": "script"}

def get_date(start_date):
    sql = """
        SELECT p_date,
               mid,
               round(sum(duration)/60/60,2) as dur
        FROM zy_bigdata.dws_user_bhv_did_mid_di_zy
        WHERE p_date = '%s'
        AND mid = '1820839'
        GROUP BY p_date, mid;
        """ % (str(start_date))
    print(sql)
    with odps_db.execute_sql(sql).open_reader() as reader:
        record_list = list()
        for record in reader:
            record_list.append(dict(record))
    content = pd.DataFrame(record_list)
    if content.empty:
        dur = 0
    else:
        dur = content['dur']
    return dur

def alter_table_odps(sql, db='x', debug=False):
    """
    在odps里修改表的数据
    """
    db = ODPS(AccessKeyID, AccessKeySecret, db)
    if debug:
        print(sql)
    print(odps_settings)
    with db.execute_sql(sql, hints=odps_settings).open_reader() as reader:
        print(reader.raw)
    return reader.raw

def query_data_size(sql, db='x', debug=False):
    """
    在odps里查询表的数据条数
    """
    db = ODPS(AccessKeyID, AccessKeySecret, db)

    if debug:
        print(sql)
    print(odps_settings)
    with db.execute_sql(sql, hints=odps_settings).open_reader() as reader:
        print(reader.count)

    return reader.count


def query_data(sql, db='x', debug=False):
    """
    在odps里查询表的数据,并返回
    """
    db = ODPS(AccessKeyID, AccessKeySecret, db)

    if debug:
        print(sql)
    print(odps_settings)
    with db.execute_sql(sql, hints=odps_settings).open_reader() as reader:
        record_list = list()
        for record in reader:
            record_list.append(dict(record))

    df = pd.DataFrame(record_list)
    print(df)
    return df


def check_partition(table_name, partition, db='x'):
    """
    检查分区是否存在:注意使用该函数得所有分区都检测
    @param table_name, str, 表名
    @param partition, str, 分区 如 "p_date=2019-09-01,app_name=zuiyou"
    @return bool
    """
    db = ODPS(AccessKeyID, AccessKeySecret, db)

    t = db.get_table(table_name)
    return t.exist_partition(partition)


def delete_partition(table_name, partition, db='x'):
    """
    删除分区
    :param table_name: 分区表名
    :param partition: 分区名， eg: "p_date=2019-09-01,app_name=zuiyou"
    :param db: 数据库名
    """
    if check_partition(table_name, partition, db):

        db = ODPS(AccessKeyID, AccessKeySecret, db)

        t = db.get_table(table_name)
        t.delete_partition(partition, if_exists=True)
        return True


def write_data_partition(table_name, data_records, partition, db='x'):
    """
    写入分区数据
    :param table_name: 分区表名
    :param partition: 分区名，eg: "p_date=2019-09-01,app_name=zuiyou"
    :param db: 数据库名
    """
    db = ODPS(AccessKeyID, AccessKeySecret, db)
    db.write_table(table_name, data_records, partition=partition, create_partition=True)


if __name__ == '__main__':
    pass
