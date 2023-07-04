# !/usr/bin/python
# -*-coding: utf-8 -*-

"""
Created on 2019/9/20

@author: LiLe
"""

from sqlalchemy import create_engine
import pymysql
import re
import pandas as pd
import os

# mysql
from configparser import ConfigParser
dirname, _ = os.path.split(os.path.abspath(__file__))
#初始化类
cf = ConfigParser()
cf.read(dirname + "/mysqldb.cfg")
# 得到所有的section，以列表的形式返回
section = cf.sections()[0]
host = cf.get(section, "host")
port = cf.getint(section, "port")
user = cf.get(section, "user")
db_name = cf.get(section, "db")
password = cf.get(section, "password")


AccessKeyID = cf.get('odps','AccessKeyID')
AccessKeySecret = cf.get('odps','AccessKeySecret')


def get_data_from_mysql(quary_sql,table_name=""):
    """
    在mysql里查询结果并返回
    """
    connection = pymysql.connect(host=host,
                                 user=user,
                                 password=password,
                                 db=db_name,
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection.cursor() as cursor:
        cursor.execute(quary_sql)
        quary_pd = cursor.fetchall()
    print('quary_sql',quary_sql)
    print('table_name',table_name)
    print('quary_pd',quary_pd)
    if table_name=='app_ad_novel_di_zy':
        quary_pd = pd.DataFrame(quary_pd,columns=['mid','zy_id','username','pid','title','click_unlock_uv','suss_unlock_uv','ad_income','earn_income','p_date'])
        print('通过')
    else:
        quary_pd = pd.DataFrame(quary_pd)
    connection.commit()
    connection.close()
    return quary_pd


def commit_stmt_in_mysql(sql):
    """
    提交并执行 sql 语句
    :param sql:
    :return: None
    """

    conn = pymysql.connect(host=host, port=port, user=user, passwd=password, db=db_name, charset='utf8')
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()
    print(sql)



def insert_mysql_overwrite_by_day(temp_pd, table_name, p_date,column_name="p_date"):
    """
     写入数据，覆盖当天数据
    :param temp_pd:需要写入的数据
    :param table_name: 需要写入的数据表
    :param p_date: 日期
    :return:
    """
    # print(user,password,host,db_name)


    conn = pymysql.connect(host=host, port=port, user=user, passwd=password, db=db_name, charset='utf8')
    cur = conn.cursor()
    # 检查该表是否存在
    if table_exists(cur,table_name):
        # 删除该天数据
        delete_sql = """DELETE from """ + table_name + """ where """+column_name+"""='""" + str(p_date) + """'"""
        print(delete_sql)
        cur.execute(delete_sql)
        conn.commit()
        conn.close()

    mysql_url = 'mysql+pymysql://' + user + ':' + password + '@' + host + '/' + db_name + '?charset=utf8'
    engine = create_engine(mysql_url, echo=True)
    temp_pd.to_sql(name=table_name, con=engine, index=False, if_exists='append')


# 这个函数用来判断表是否存在
def table_exists(con, table_name):
    sql = "show tables;"
    con.execute(sql)
    tables = [con.fetchall()]
    table_list = re.findall('(\'.*?\')', str(tables))
    table_list = [re.sub("'", '', each) for each in table_list]
    if table_name in table_list:
        return True
    else:
        return False




def insert_mysql_append(temp_pd, table_name):
    """
     写入数据，不覆盖当天数据，追加
    :param temp_pd:需要写入的数据
    :param table_name: 需要写入的数据表
    :return:
    """
    # print(user,password,host,db_name)
    mysql_url = 'mysql+pymysql://' + user + ':' + password + '@' + host + '/' + db_name + '?charset=utf8'
    engine = create_engine(mysql_url, echo=True)
    temp_pd.to_sql(name=table_name, con=engine, index=False, if_exists='append')




def insert_mysql_truncate_table(temp_pd, table_name):
    """
         写入数据，写入之前清空表
        :param temp_pd:需要写入的数据
        :param table_name: 需要写入的数据表
        :return:
        """
    # print(user,password,host,db_name)

    conn = pymysql.connect(host=host, port=port, user=user, passwd=password, db=db_name, charset='utf8')
    cur = conn.cursor()
    # 检查该表是否存在
    if table_exists(cur, table_name):
        # 删除该天数据
        delete_sql = """truncate table """+table_name
        print(delete_sql)
        cur.execute(delete_sql)
        conn.commit()
        conn.close()

    mysql_url = 'mysql+pymysql://' + user + ':' + password + '@' + host + '/' + db_name + '?charset=utf8'
    engine = create_engine(mysql_url, echo=True)
    temp_pd.to_sql(name=table_name, con=engine, index=False, if_exists='append')


def insert_mysql_transposition_table(data_pd,table_name):
    app_names = data_pd['app_name'].drop_duplicates()
    p_date = data_pd['p_date'].drop_duplicates().iloc[0]
    data_type = data_pd['type'].drop_duplicates().iloc[0]
    print(p_date)
    print(data_type)
    for app_name in app_names:
        data_t = data_pd[data_pd['app_name'] == app_name].T.drop(['app_name', 'p_date', 'type']).reset_index()
        data_t['p_date'] = p_date
        data_t['app_name'] = app_name
        data_t['type'] = data_type
        data_t.columns = ['index_name', 'index_value', 'p_date', 'app_name', 'type']

        conn = pymysql.connect(host=host,user=user, passwd=password, db=db_name, charset='utf8')
        cur = conn.cursor()

        if table_exists(cur, table_name):
            # 删除该天数据
            delete_sql = """DELETE from """ + table_name + """ where p_date='""" + str(p_date) + """'"""\
            +""" and app_name='""" + app_name + """'""" + """ and type= '""" + data_type + """'"""
            print(delete_sql)
            cur.execute(delete_sql)
            conn.commit()
            conn.close()

        mysql_url = 'mysql+pymysql://' + user + ':' + password + '@' + host + '/' + db_name + '?charset=utf8'
        engine = create_engine(mysql_url, echo=True)
        data_t.to_sql(name=table_name, con=engine, index=False, if_exists='append')


if __name__ == '__main__':
    pass