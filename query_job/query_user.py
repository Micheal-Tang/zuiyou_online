# !/usr/bin/python
# -*-coding: utf-8 -*-

from odps import ODPS
import pandas as pd
import datetime
import time
import configparser
import sys
import numpy as np
import json
import requests
from pymongo import MongoClient
import os
import oss2

reload(sys)
sys.setdefaultencoding('utf-8')


class GetPeople(object):
    """
    解析参数数据
    """

    def __init__(self, task_id, app_name):
        ## 应用名称
        self.app_name = str(app_name)

        ## mongodb
        if self.app_name == 'zuiyou_lite':
            self.client = MongoClient("xxx")
            self.db_name = 'xx'
            self.table_name = 'xx'
            self.db = self.client.get_database(self.db_name)
            self.table = self.db.get_collection(self.table_name)
            self.docs = self.table.find_one({"toolkit_type": "user", "_id": int(task_id)})
        elif self.app_name == 'zuiyou':
            self.client = MongoClient("xxxx")
            self.db_name = 'xx'
            self.table_name = 'xx'
            self.db = self.client.get_database(self.db_name)
            self.table = self.db.get_collection(self.table_name)
            self.docs = self.table.find_one({"toolkit_type": "user", "_id": int(task_id)})
        elif self.app_name == 'omg':
            self.client = MongoClient(
                "xx")
            self.db_name = 'xx'
            self.table_name = 'xx'
            self.db = self.client.get_database(self.db_name)
            self.table = self.db.get_collection(self.table_name)
            self.docs = self.table.find_one({"toolkit_type": "user", "_id": int(task_id)})
        elif self.app_name == 'maga':
            self.client = MongoClient("xxx")
            self.db_name = 'xx'
            self.table_name = 'xx'
            self.db = self.client.get_database(self.db_name)
            self.table = self.db.get_collection(self.table_name)
            self.docs = self.table.find_one({"toolkit_type": "user", "_id": int(task_id)})

        ## odps
        if self.app_name in ('zuiyou', 'zuiyou_lite'):
            AccessKeyID = 'xx'
            AccessKeySecret = 'xx'
            self.db_odps = ODPS(AccessKeyID, AccessKeySecret, 'xx')
        elif self.app_name == 'omg':
            AccessKeyID = 'xx'
            AccessKeySecret = 'xx'
            self.db_odps = ODPS(AccessKeyID, AccessKeySecret, 'xx','xxx')
        elif self.app_name == 'maga':
            AccessKeyID = 'xx'
            AccessKeySecret = 'xx'
            self.db_odps = ODPS(AccessKeyID, AccessKeySecret, 'xx','xxx')

        ## 日期处理
        self.start_date_pre = int(self.docs.get('start_date', 0))
        self.start_date = (datetime.datetime.fromtimestamp(self.start_date_pre)).strftime("%Y-%m-%d")
        self.end_date_pre = int(self.docs.get('end_date', 0))
        self.end_date = (datetime.datetime.fromtimestamp(self.end_date_pre)).strftime("%Y-%m-%d")
        self.p_date = " between '" + str(self.start_date) + "' and '" + str(self.end_date) + "'"

        ## 维度解析
        self.params_list = self.docs.get('field_value', None)
        self.user = self.params_list.get('user', None)
        self.post_consume = self.params_list.get('post_consume', None)
        self.actionlog_consume = self.params_list.get('actionlog_consume', None)
        self.topic_consume = self.params_list.get('topic_consume', None)
        self.send_post = self.params_list.get('send_post', None)
        self.send_comment = self.params_list.get('send_comment', None)
        self.attention = self.params_list.get('attention', None)

        ## 模块间关系
        self.relation = self.docs.get('relation', [])
        self.final_relation = ""
        if len(self.relation) > 0:
            for val in self.relation:
                if val in ['(', ')']:
                    self.final_relation += str(val) + ' '
                elif val == '&&':
                    self.final_relation += ' AND '
                elif val == '||':
                    self.final_relation += ' OR '
                elif val in ['user', 'post_consume', 'actionlog_consume', 'topic_consume', 'send_post', 'send_comment',
                             'attention']:
                    self.final_relation += ' ' + str(val) + '.mid IS NOT NULL '
        elif len(self.relation) == 0:
            for val in self.params_list.keys():
                if len(self.final_relation) == 0:
                    self.final_relation += ' ' + str(val) + '.mid IS NOT NULL '
                elif len(self.final_relation) > 0:
                    self.final_relation += ' AND ' + str(val) + '.mid IS NOT NULL '

        ## 表名同步
        if self.app_name == 'zuiyou':
            self.dws_table = 'zy_bigdata.dws_user_bhv_did_mid_di_zy'
            self.ups_table = 'zuiyou_recommendation.user_post_score_v2'
            self.post_table = 'zy_bigdata.postmetadata'
            self.part_table = 'zuiyou_recommendation.topic_partition_map'
            self.tid_table = 'zy_bigdata.topicmetadata'
            self.actionlog_table = 'zy_bigdata.actionlog_zuiyou'
            self.topic_table = 'zy_bigdata.dws_topic_did_mid_di_zy'
            self.comment_table = 'zy_bigdata.dim_comment_h_zy'
            self.user_table = 'zy_bigdata.usermetadata'
            self.attention_tid = 'zy_bigdata.zy_user_topics'
            self.attention_mid = 'zy_bigdata.zy_user_atts'
        elif self.app_name == 'zuiyou_lite':
            self.dws_table = 'pipi_bigdata.dws_user_bhv_did_mid_di_pp'
            self.ups_table = 'pipi_bigdata.pp_user_post_score'
            self.post_table = 'pipi_bigdata.pp_postmetadata'
            self.part_table = 'pipi_bigdata.pipi_topic_partition_map'
            self.tid_table = 'pipi_bigdata.pp_topicmetadata'
            self.actionlog_table = 'pipi_bigdata.ppactionlog_new'
            self.topic_table = 'pipi_bigdata.dws_topic_did_mid_di_pp'
            self.comment_table = 'pipi_bigdata.dim_comment_h_pp'
            self.user_table = 'pipi_bigdata.pp_usermetadata'
            self.attention_tid = 'zy_bigdata.user_topics'
            self.attention_mid = 'zy_bigdata.pp_user_atts'
        elif self.app_name == 'maga':
            self.dws_table = 'maga_bigdata.dws_user_bhv_did_mid_di_maga'
            self.ups_table = 'maga_bigdata.user_post_score'
            self.post_table = 'maga_bigdata.postmetadata_maga'
            self.part_table = 'maga_bigdata.topic_partition_map_maga'
            self.tid_table = 'maga_bigdata.topicmetadata_maga'
            self.actionlog_table = 'maga_bigdata.maga_actionlog'
            self.user_table = 'maga_bigdata.usermetadata_maga'
        elif self.app_name == 'omg':
            self.dws_table = 'omg_data.dws_user_bhv_did_mid_di_omg'
            self.ups_table = 'omg_data.omg_user_post_score'
            self.post_table = 'omg_data.omg_postmetadata'
            self.part_table = 'omg_data.omg_tid_partition_metadata'
            self.tid_table = 'omg_data.omg_topicmetadata'
            self.actionlog_table = 'omg_data.omg_actionlog'
            self.topic_table = 'omg_data.dws_topic_did_mid_di_omg'
            self.comment_table = 'omg_data.dim_comment_h_omg'
            self.user_table = 'omg_data.omg_usermetadata'

    """
    组装SQL
    """

    def get_sql(self):
        if self.app_name == 'zuiyou':
            sql_head = """
            SELECT userall.app_name,
                   userall.device_id,
                   userall.mid,
                   CASE WHEN userall.app_name = 'zuiyou' AND userdata.age = 1 THEN '16-'
                        WHEN userall.app_name = 'zuiyou' AND userdata.age = 2 THEN '16~18'
                        WHEN userall.app_name = 'zuiyou' AND userdata.age = 3 THEN '19~22'
                        WHEN userall.app_name = 'zuiyou' AND userdata.age = 4 THEN '22+'
                        ELSE '未知' end AS 年龄范围,
                   CASE WHEN userdata.gender = 1 then '男性'
                        WHEN userdata.gender = 2 then '女性'
                        ELSE '未知' end AS gender,
                   nvl(userdata.name,'未知') AS 昵称,
                   nvl(userdata.reg_date,'未知') AS 注册日期,
                   nvl(userdata.c_date,'未知') AS 激活日期,
                   nvl(userdata.province,'未知') AS 省份,
                   nvl(userdata.city,'未知') AS 城市
            FROM 
            (SELECT base.device_id,
                    base.mid,
                    base.app_name
            FROM
            -- 区间内全部用户
            (
                SELECT device_id,
                       mid,
                       app_name
                FROM {}
                WHERE p_date {}
                AND app_name = '{}'
                GROUP BY device_id, mid, app_name
            ) base
            """.format(str(self.dws_table), str(self.p_date), str(self.app_name))
        elif self.app_name == 'zuiyou_lite':
            sql_head = """
            SELECT userall.app_name,
                   userall.device_id,
                   userall.mid,
                   CASE WHEN userall.app_name = 'zuiyou_lite' AND userdata.age = 1 THEN '17-'
                        WHEN userall.app_name = 'zuiyou_lite' AND userdata.age = 2 THEN '18~22'
                        WHEN userall.app_name = 'zuiyou_lite' AND userdata.age = 3 THEN '23~28'
                        WHEN userall.app_name = 'zuiyou_lite' AND userdata.age = 4 THEN '28+'
                        ELSE '未知' end AS 年龄范围,
                   CASE WHEN userdata.gender = 1 then '男性'
                        WHEN userdata.gender = 2 then '女性'
                        ELSE '未知' end AS gender,
                   nvl(userdata.name,'未知') AS 昵称,
                   nvl(userdata.reg_date,'未知') AS 注册日期,
                   nvl(userdata.c_date,'未知') AS 激活日期
            FROM 
            (SELECT base.device_id,
                    base.mid,
                    base.app_name
            FROM
            -- 区间内全部用户
            (
                SELECT device_id,
                       mid,
                       app_name
                FROM {}
                WHERE p_date {}
                AND app_name = '{}'
                GROUP BY device_id, mid, app_name
            ) base
            """.format(str(self.dws_table), str(self.p_date), str(self.app_name))
        elif self.app_name == 'maga':
            sql_head = """
            SELECT userall.app_name,
                   userall.device_id,
                   userall.mid,
                   CASE WHEN userall.app_name = 'maga' AND userdata.age = 1 THEN '18-'
                        WHEN userall.app_name = 'maga' AND userdata.age = 2 THEN '19~34'
                        WHEN userall.app_name = 'maga' AND userdata.age = 3 THEN '35~60'
                        WHEN userall.app_name = 'maga' AND userdata.age = 4 THEN '60+'
                        WHEN userall.app_name = 'maga' AND userdata.age = 5 THEN '19~24'
                        WHEN userall.app_name = 'maga' AND userdata.age = 6 THEN '25~34'
                        WHEN userall.app_name = 'maga' AND userdata.age = 7 THEN '35~44'
                        WHEN userall.app_name = 'maga' AND userdata.age = 8 THEN '45~60'
                        ELSE '未知' end AS 年龄范围,
                   CASE WHEN userdata.gender = 1 then '男性'
                        WHEN userdata.gender = 2 then '女性'
                        ELSE '未知' end AS gender,
                   nvl(userdata.name,'未知') AS 昵称,
                   nvl(userdata.reg_date,'未知') AS 注册日期,
                   nvl(userdata.c_date,'未知') AS 激活日期
            FROM 
            (SELECT base.device_id,
                    base.mid,
                    base.app_name
            FROM
            -- 区间内全部用户
            (
                SELECT device_id,
                       mid,
                       app_name
                FROM {}
                WHERE p_date {}
                AND app_name = '{}'
                GROUP BY device_id, mid, app_name
            ) base
            """.format(str(self.dws_table), str(self.p_date), str(self.app_name))
        elif self.app_name == 'omg':
            sql_head = """
            SELECT userall.app_name,
                   userall.device_id,
                   userall.mid,
                   CASE WHEN userall.app_name = 'omg' AND userdata.age = 1 THEN '18-'
                        WHEN userall.app_name = 'omg' AND userdata.age = 2 THEN '18~24'
                        WHEN userall.app_name = 'omg' AND userdata.age = 3 THEN '25~30'
                        WHEN userall.app_name = 'omg' AND userdata.age = 4 THEN '30+'
                        ELSE '未知' end AS 年龄范围,
                   CASE WHEN userdata.gender = 1 then '男性'
                        WHEN userdata.gender = 2 then '女性'
                        ELSE '未知' end AS gender,
                   nvl(userdata.reg_date,'未知') AS 注册日期,
                   nvl(userdata.c_date,'未知') AS 激活日期
            FROM 
            (SELECT base.device_id,
                    base.mid,
                    base.app_name
            FROM
            -- 区间内全部用户
            (
                SELECT device_id,
                       mid,
                       app_name
                FROM {}
                WHERE p_date {}
                AND app_name = '{}'
                GROUP BY device_id, mid, app_name
            ) base
            """.format(str(self.dws_table), str(self.p_date), str(self.app_name))

        if self.app_name == 'zuiyou':
            sql_tail = """
            GROUP BY base.device_id, base.mid, base.app_name) userall
            INNER JOIN
            (
                SELECT mid,
                       MIN(to_date(substr(from_unixtime(ctime) ,1,10))) AS c_date,
                       min(to_date(substr(from_unixtime(CASE WHEN mid<=0 then 0 
                                                             when regtime>1325347200 THEN regtime 
                                                             WHEN isreg = 1 THEN ctime END),1,10))) AS reg_date,
                       MAX(CASE WHEN gender IN (1,2) THEN gender ELSE 0 END) AS gender,
                       MAX(CASE WHEN age IN (1,2,3,4,5,6,7,8) THEN age ELSE 0 END) AS age,
                       MAX(name) AS name,
                       MAX(province) AS province,
                       MAX(city) AS city
                FROM {}
                GROUP BY mid
            ) userdata
            ON userall.mid = userdata.mid
            WHERE userall.app_name = '{}';
            """.format(str(self.user_table), str(self.app_name))
        elif self.app_name == 'zuiyou_lite':
            sql_tail = """
            GROUP BY base.device_id, base.mid, base.app_name) userall
            INNER JOIN
            (
                SELECT mid,
                       MIN(to_date(substr(from_unixtime(ctime) ,1,10))) AS c_date,
                       min(to_date(substr(from_unixtime(CASE WHEN mid<=0 then 0 
                                                             when regtime>1325347200 THEN regtime 
                                                             WHEN isreg = 1 THEN ctime END),1,10))) AS reg_date,
                       MAX(CASE WHEN gender IN (1,2) THEN gender ELSE 0 END) AS gender,
                       MAX(CASE WHEN age IN (1,2,3,4) THEN age ELSE 0 END) AS age,
                       MAX(name) AS name
                FROM {}
                GROUP BY mid
            ) userdata
            ON userall.mid = userdata.mid
            WHERE userall.app_name = '{}';
            """.format(str(self.user_table), str(self.app_name))
        elif self.app_name == 'maga':
            sql_tail = """
            GROUP BY base.device_id, base.mid, base.app_name) userall
            INNER JOIN
            (
                SELECT mid,
                       MIN(to_date(substr(from_unixtime(cast (ctime-12*3600 as bigint)) ,1,10))) AS c_date,
                       min(to_date(substr(from_unixtime(CASE WHEN isreg = 1 THEN cast(regtime-12*3600 as bigint) END),1,10))) AS reg_date,
                       MAX(CASE WHEN gender IN (1,2) THEN gender ELSE 0 END) AS gender,
                       MAX(CASE WHEN age IN (1,2,3,4,5,6,7,8) THEN age ELSE 0 END) AS age,
                       MAX(name) AS name
                FROM {}
                GROUP BY mid
            ) userdata
            ON userall.mid = userdata.mid
            WHERE userall.app_name = '{}';
            """.format(str(self.user_table), str(self.app_name))
        elif self.app_name == 'omg':
            sql_tail = """
            GROUP BY base.device_id, base.mid, base.app_name) userall
            INNER JOIN
            (
                SELECT mid,
                       MIN(to_date(substr(from_unixtime(cast (ct as bigint)) ,1,10))) AS c_date,
                       min(to_date(substr(from_unixtime(CASE WHEN isreg = 1 THEN cast(rt as bigint) END),1,10))) AS reg_date,
                       MAX(CASE WHEN gender IN (1,2) THEN gender ELSE 0 END) AS gender,
                       MAX(CASE WHEN age IN (1,2,3,4,5,6,7,8) THEN age ELSE 0 END) AS age
                FROM {}
                GROUP BY mid
            ) userdata
            ON userall.mid = userdata.mid
            WHERE userall.app_name = '{}';
            """.format(str(self.user_table), str(self.app_name))

        ## 用户维度
        if self.user:
            sql_user_tmp = ''
            for v in self.user:
                user_type = []
                user_type_condition = ''
                user_type_condition_new = ''
                user_condition_sel = ""
                user_date_type = ""
                exp_id = ""
                exp_group = ""
                user_start_date = str(self.start_date)
                user_end_date = str(self.end_date)

                for key, value in v.items():
                    if key == "show_keys":
                        continue
                    else:
                        if key in (
                                'live_days_mid', 'live_days_did', 'last_days_mid', 'last_days_did', 'first_launch_type',
                                'reg_date',
                                'nt', 'province', 'channel_click', 'app_version', 'os_type', 'gender',
                                'age', 'lt7_did', 'lt30_did') and key not in user_type:
                            user_type.append(key)
                        if key == 'user_start_date':
                            user_start_date = (datetime.datetime.fromtimestamp(int(value))).strftime("%Y-%m-%d")
                        elif key == 'user_end_date':
                            user_end_date = (datetime.datetime.fromtimestamp(int(value))).strftime("%Y-%m-%d")
                        elif key == 'user_date_type':
                            user_date_type = str(value).strip()
                        elif key == 'exp_id':
                            exp_id = 'INT(CONV(SUBSTR(MD5(CONCAT("' + str(
                                value).strip() + '",device_id)),25,8),16,10 )%100)'
                        elif key == 'exp_group':
                            if '~' in value:
                                exp_group = " BETWEEN " + str(value.split('~')[0]).strip() + " AND " + str(
                                    value.split('~')[1]).strip()
                            elif ',' in value:
                                exp_len = len(value.split(','))
                                exp_tmp = ""
                                for i in range(exp_len):
                                    if i == 0:
                                        exp_tmp += " IN (" + str(value.split(',')[i]).strip()
                                    else:
                                        exp_tmp += "," + str(value.split(',')[i]).strip()
                                exp_group = exp_tmp + ")"
                            else:
                                exp_group = " IN (" + str(value) + ")"
                        else:
                            if type(value) == unicode:
                                if '~' in value:
                                    if len(user_condition_sel) > 0:
                                        user_condition_sel += " AND " + str(key) + " BETWEEN " + str(value.split('~')[
                                                                                                         0]).strip() + " AND " + \
                                                              str(value.split('~')[1]).strip()
                                    else:
                                        user_condition_sel += "WHERE " + str(key) + " BETWEEN " + str(
                                            value.split('~')[0]).strip() + " AND " + \
                                                              str(value.split('~')[
                                                                      1]).strip()
                                elif ',' in value:
                                    condition_len = len(value.split(','))
                                    condition_tmp = ""
                                    for i in range(condition_len):
                                        if i == 0:
                                            condition_tmp += str(key) + " IN ( " + "'" + str(
                                                value.split(',')[i]).strip() + "'"
                                        else:
                                            condition_tmp += ",'" + str(value.split(',')[i]).strip() + "'"
                                    if len(user_condition_sel) > 0:
                                        user_condition_sel += " AND " + condition_tmp.strip() + ") "
                                    else:
                                        user_condition_sel += "WHERE " + condition_tmp.strip() + ") "
                                else:
                                    if len(user_condition_sel) > 0:
                                        user_condition_sel += " AND " + str(key) + " IN ('" + str(value).strip() + "')"
                                    else:
                                        user_condition_sel += "WHERE " + str(key) + " IN ('" + str(value).strip() + "')"
                            elif type(value) == list:
                                con_tmp = ''
                                for con in value:
                                    con_tmp += "'" + str(con) + "',"
                                con_tmp_final = con_tmp.strip(',')
                                if len(user_condition_sel) > 0:
                                    user_condition_sel += " AND " + str(key) + " IN (" + str(
                                        con_tmp_final).strip() + ")"
                                else:
                                    user_condition_sel += "WHERE " + str(key) + " IN (" + str(con_tmp_final).strip() + ")"

                if len(exp_id) > 0 and len(exp_group) > 0:
                    if len(user_condition_sel) > 0:
                        exp_sel = " AND " + str(exp_id) + str(exp_group)
                    else:
                        exp_sel = "WHERE " + str(exp_id) + str(exp_group)
                else:
                    exp_sel = ""

                ## 用户维度
                for m in user_type:
                    if m == 'channel_click':
                        if self.app_name in ('zuiyou', 'zuiyou_lite'):
                            user_type_condition += """CASE WHEN channel_click like '%huawei%' THEN 'huawei'
                                                           WHEN channel_click like '%oppo%' THEN 'oppo'
                                                           WHEN channel_click LIKE '%vivo%' THEN 'vivo'
                                                           WHEN channel_click = 'xiaomi' THEN 'xiaomi'
                                                           WHEN channel_click = 'appstore' THEN 'appstore'
                                                           ELSE 'other' END AS channel_click,""" + """\n"""
                            user_type_condition_new += """CASE WHEN channel_click like '%huawei%' THEN 'huawei'
                                                           WHEN channel_click like '%oppo%' THEN 'oppo'
                                                           WHEN channel_click LIKE '%vivo%' THEN 'vivo'
                                                           WHEN channel_click = 'xiaomi' THEN 'xiaomi'
                                                           WHEN channel_click = 'appstore' THEN 'appstore'
                                                           ELSE 'other' END,""" + """\n"""
                        elif self.app_name == 'maga':
                            user_type_condition = """channel_click,""" + """\n"""
                            user_type_condition_new = """channel_click,""" + """\n"""
                        elif self.app_name == 'omg':
                            user_type_condition = """channel_click,""" + """\n"""
                            user_type_condition_new = """channel_click,""" + """\n"""
                    elif m == 'age':
                        user_type_condition += """REPLACE(age,'~','-') AS age,""" + """\n"""
                        user_type_condition_new += """REPLACE(age,'~','-'),""" + """\n"""
                    else:
                        user_type_condition += str(m).strip() + "," + """\n"""
                        user_type_condition_new += str(m).strip() + "," + """\n"""

                ## 时间类型
                if user_date_type == 'day':
                    user_date_range = 'p_date,'
                elif user_date_type == 'period':
                    user_date_range = ''
                else:
                    user_date_range = 'p_date,'

                if len(sql_user_tmp) > 0:
                    sql_user_tmp += """
                                UNION

                                SELECT device_id,
                                       mid
                                FROM 
                                (SELECT {}
                                       device_id,
                                       mid,
                                       -- 可以添加需要的用户属性维度
                                       {}
                                       -- 可以添加需要的app行为数据
                                       SUM(duration)/60 AS duration,
                                       SUM(session_launch) AS session_launch,
                                       SUM(expose) AS expose,
                                       SUM(real_expose) AS real_expose,
                                       SUM(score) AS score,
                                       SUM(img_expose) AS img_expose,
                                       SUM(img_score) AS img_score,
                                       SUM(video_expose) AS video_expose,
                                       SUM(video_score) AS video_score,
                                       SUM(detail_post) AS detail_post,
                                       SUM(view_post_dur)/60 AS view_post_dur,
                                       SUM(create_post) AS create_post,
                                       SUM(like_post) AS like_post,
                                       SUM(dislike_post) AS dislike_post,
                                       SUM(share_post) AS share_post,
                                       SUM(favor_post) AS favor_post,
                                       SUM(create_review) AS create_review,
                                       SUM(reply_review) AS reply_review,
                                       SUM(create_review+reply_review) AS total_review,
                                       SUM(like_create_review) AS like_create_review,
                                       SUM(like_reply_review) AS like_reply_review,
                                       SUM(like_create_review+like_reply_review) AS like_review,
                                       SUM(dislike_create_review) AS dislike_create_review,
                                       SUM(dislike_reply_review) AS dislike_reply_review,
                                       SUM(dislike_create_review+dislike_reply_review) AS dislike_review,
                                       SUM(share_review) AS share_review,
                                       SUM(create_danmaku) AS create_danmaku,
                                       SUM(like_danmaku) AS like_danmaku,
                                       SUM(dislike_danmaku) AS dislike_danmaku,
                                       SUM(total_view_img) AS total_view_img,
                                       SUM(total_view_img_dur)/60 AS total_view_img_dur,
                                       SUM(view_img) AS view_img,
                                       SUM(view_img_dur)/60 AS view_img_dur,
                                       SUM(total_play_video) AS total_play_video,
                                       SUM(total_play_video_dur)/60 AS total_play_video_dur,
                                       SUM(play_video) AS play_video,
                                       SUM(play_video_dur)/60 AS play_video_dur
                                FROM {}
                                WHERE p_date BETWEEN '{}' AND '{}'
                                GROUP BY {}
                                       -- 可以添加需要的用户属性维度
                                       {}
                                       device_id,
                                       mid
                                       )
                                {} {}
                                GROUP BY device_id, mid
                            """.format(str(user_date_range), str(user_type_condition), str(self.dws_table),
                                       str(user_start_date), str(user_end_date), str(user_date_range),
                                       str(user_type_condition_new),
                                       str(user_condition_sel), str(exp_sel))
                else:
                    sql_user_tmp += """
                                                    SELECT device_id,
                                                           mid
                                                    FROM 
                                                    (SELECT {}
                                                           device_id,
                                                           mid,
                                                           -- 可以添加需要的用户属性维度
                                                           {}
                                                           -- 可以添加需要的app行为数据
                                                           SUM(duration)/60 AS duration,
                                                           SUM(session_launch) AS session_launch,
                                                           SUM(expose) AS expose,
                                                           SUM(real_expose) AS real_expose,
                                                           SUM(score) AS score,
                                                           SUM(img_expose) AS img_expose,
                                                           SUM(img_score) AS img_score,
                                                           SUM(video_expose) AS video_expose,
                                                           SUM(video_score) AS video_score,
                                                           SUM(detail_post) AS detail_post,
                                                           SUM(view_post_dur)/60 AS view_post_dur,
                                                           SUM(create_post) AS create_post,
                                                           SUM(like_post) AS like_post,
                                                           SUM(dislike_post) AS dislike_post,
                                                           SUM(share_post) AS share_post,
                                                           SUM(favor_post) AS favor_post,
                                                           SUM(create_review) AS create_review,
                                                           SUM(reply_review) AS reply_review,
                                                           SUM(create_review+reply_review) AS total_review,
                                                           SUM(like_create_review) AS like_create_review,
                                                           SUM(like_reply_review) AS like_reply_review,
                                                           SUM(like_create_review+like_reply_review) AS like_review,
                                                           SUM(dislike_create_review) AS dislike_create_review,
                                                           SUM(dislike_reply_review) AS dislike_reply_review,
                                                           SUM(dislike_create_review+dislike_reply_review) AS dislike_review,
                                                           SUM(share_review) AS share_review,
                                                           SUM(create_danmaku) AS create_danmaku,
                                                           SUM(like_danmaku) AS like_danmaku,
                                                           SUM(dislike_danmaku) AS dislike_danmaku,
                                                           SUM(total_view_img) AS total_view_img,
                                                           SUM(total_view_img_dur)/60 AS total_view_img_dur,
                                                           SUM(view_img) AS view_img,
                                                           SUM(view_img_dur)/60 AS view_img_dur,
                                                           SUM(total_play_video) AS total_play_video,
                                                           SUM(total_play_video_dur)/60 AS total_play_video_dur,
                                                           SUM(play_video) AS play_video,
                                                           SUM(play_video_dur)/60 AS play_video_dur
                                                    FROM {}
                                                    WHERE p_date BETWEEN '{}' AND '{}'
                                                    GROUP BY {}
                                                           -- 可以添加需要的用户属性维度
                                                           {}
                                                           device_id,
                                                           mid
                                                           )
                                                    {} {}
                                                    GROUP BY device_id, mid
                                                """.format(str(user_date_range), str(user_type_condition),
                                                           str(self.dws_table), str(user_start_date),
                                                           str(user_end_date), str(user_date_range),
                                                           str(user_type_condition_new),
                                                           str(user_condition_sel), str(exp_sel))

            sql_user = """
                -- 用户消费数据
                FULL OUTER JOIN
                (
                """ + sql_user_tmp + """\n""" \
                       + """) user
                on base.device_id = user.device_id and base.mid = user.mid
                """
        else:
            sql_user = ""

        ## 帖子消费维度
        if self.post_consume:
            sql_post_consume_tmp = ""

            if self.app_name == 'zuiyou':
                ct_time = 'ct'
                media_num = """,nvl(MAX(size(split(imgids,', '))),0) AS media_num"""
                part_name = 'MAX(l1part_name) AS l1part_name, \
                             MAX(l2part_name) AS l2part_name, \
                             MAX(l1part_id) AS l1part_id, \
                             MAX(l2part_id) AS l2part_id'
            elif self.app_name == 'zuiyou_lite':
                ct_time = 'ct'
                media_num = """,nvl(MAX(size(split(imgids,', '))),0) AS media_num"""
                part_name = 'MAX(l1part_name) AS l1part_name, \
                             MAX(l2part_name) AS l2part_name, \
                             MAX(l1part_id) AS l1part_id, \
                             MAX(l2part_id) AS l2part_id'
            elif self.app_name == 'maga':
                ct_time = 'cast(ct-12*3600 as bigint)'
                media_num = """,nvl(MAX(size(split(imgids,', '))),0) AS media_num"""
                part_name = 'MAX(l1part_name) AS l1part_name, \
                             MAX(l2part_name) AS l2part_name, \
                             MAX(l1part_id) AS l1part_id, \
                             MAX(l2part_id) AS l2part_id'
            elif self.app_name == 'omg':
                ct_time = 'ct'
                media_num = ',NVL(MAX(img_cnt),0) AS media_num'
                part_name = 'MAX(part1_name) AS l1part_name, \
                             MAX(part2_name) AS l2part_name, \
                             MAX(part1_id) AS l1part_id, \
                             MAX(part2_id) AS l2part_id'

            if self.app_name == 'zuiyou':
                ups_consume = """
                               CONCAT(SUBSTR(ymd,1,4),'-',SUBSTR(ymd,5,2),'-',SUBSTR(ymd,7,2)) AS p_date,
                               did,
                               mid,
                               pid,
                               expose_t,
                               -- 帖子消费数据
                               COUNT(1) AS expose
                               ,COUNT(CASE WHEN real_expose>1 THEN 1 END) AS real_expose
                               ,COUNT(CASE WHEN score>0 THEN 1 END) AS score
                               ,COUNT(CASE WHEN score>0.8 THEN 1 END) AS score_satisfied
                               ,SUM(CASE WHEN stay_time>0 THEN stay_time ELSE 0 END) AS stay_time
                               ,SUM(CASE WHEN total_play_video>0 THEN total_play_video ELSE 0 END) AS total_play_video
                                ,SUM(
                                    CASE    WHEN total_play_video_dur>0 THEN total_play_video_dur 
                                            ELSE 0 
                                    END
                                ) AS total_play_video_dur
                                ,SUM(CASE WHEN play_video>0 THEN play_video ELSE 0 END) AS play_video
                                ,SUM(CASE WHEN play_video_dur>0 THEN play_video_dur ELSE 0 END) AS play_video_dur
                                ,SUM(CASE WHEN play_video>0 THEN 1 END) AS video_click_times
                                ,SUM(
                                    CASE    WHEN play_review_video>0 THEN play_review_video 
                                            ELSE 0 
                                    END
                                ) AS play_review_video
                                ,SUM(
                                    CASE    WHEN play_review_video_dur>0 THEN play_review_video_dur 
                                            ELSE 0 
                                    END
                                ) AS play_review_video_dur
                                ,SUM(CASE WHEN play_recommend_video > 0 THEN play_recommend_video ELSE 0 END) AS play_recommend_video
                                ,SUM(
                                    CASE    WHEN play_recommend_video_dur>0 THEN play_recommend_video_dur
                                            ELSE 0 
                                    END
                                ) AS play_recommend_video_dur
                                ,SUM(CASE WHEN finish_video>0 THEN finish_video ELSE 0 END) AS finish_video
                                ,SUM(CASE WHEN ones_video>0 THEN ones_video ELSE 0 END) AS ones_video
                                ,SUM(CASE WHEN total_view_img>0 THEN total_view_img ELSE 0 END) AS total_view_img
                                ,SUM(
                                    CASE    WHEN total_view_img_dur>0 THEN total_view_img_dur 
                                            ELSE 0 
                                    END
                                ) AS total_view_img_dur
                                ,SUM(CASE WHEN view_img>0 THEN view_img ELSE 0 END) AS view_img
                                ,SUM(CASE WHEN view_img_dur>0 THEN view_img_dur ELSE 0 END) AS view_img_dur
                                ,SUM(CASE WHEN view_img>0 THEN 1 END) AS img_click_times
                                ,SUM(CASE WHEN detail_post>0 THEN detail_post ELSE 0 END) AS detail_post
                                ,SUM(
                                    CASE    WHEN detail_post_dur>0 THEN detail_post_dur 
                                            ELSE 0 
                                    END
                                ) AS detail_post_dur
                                ,SUM(CASE WHEN view_post_dur>0 THEN view_post_dur ELSE 0 END) AS view_post_dur
                                ,COUNT(CASE WHEN LIKE>0 THEN 1 END) AS LIKE
                                ,COUNT(CASE WHEN like_attitude>0 THEN 1 END) AS like_attitude
                                ,COUNT(CASE WHEN like_attitude=2 THEN 1 END) AS like_funny
                                ,COUNT(CASE WHEN like_attitude=3 THEN 1 END) AS like_warm
                                ,COUNT(CASE WHEN like_attitude=7 THEN 1 
                                    END
                                ) AS like_silly
                                ,COUNT(CASE WHEN like_attitude=4 THEN 1 END) AS like_good
                                ,COUNT(CASE WHEN dislike>0 THEN 1 END) AS dislike
                                ,SUM(CASE WHEN share > 0 THEN share ELSE 0 END) AS share
                                ,SUM(CASE WHEN favor>0 THEN favor ELSE 0 END) AS favor
                                ,COUNT(CASE WHEN tedium>0 THEN 1 END) AS tedium
                                ,SUM(CASE WHEN create_review > 0 THEN create_review ELSE 0 END) AS create_review
                                ,SUM(CASE WHEN reply_review > 0 THEN reply_review ELSE 0 END) AS reply_review
                                ,SUM(CASE WHEN like_review>0 THEN like_review ELSE 0 END) AS like_review
                                ,SUM(CASE WHEN like_review_retained > 0 THEN like_review_retained ELSE 0 END) AS like_review_retained
                                ,SUM(CASE WHEN dislike_review>0 THEN dislike_review ELSE 0 END) AS dislike_review
                                ,SUM(CASE WHEN share_review>0 THEN share_review ELSE 0 END) AS share_review
                                ,SUM(CASE WHEN hots_review>0 THEN hots_review ELSE 0 END) AS hots_review
                                ,SUM(CASE WHEN news_review>0 THEN news_review ELSE 0 END) AS news_review
                                ,SUM(CASE WHEN subreviews_review > 0 THEN subreviews_review ELSE 0 END) AS subreviews_review
                                ,SUM(CASE WHEN review_expose>0 THEN review_expose ELSE 0 END) AS review_expose
                                ,SUM(CASE WHEN create_danmaku>0 THEN create_danmaku ELSE 0 END) AS create_danmaku
                                ,COUNT(CASE WHEN like_danmaku>0 THEN 1 END) AS like_danmaku
                                ,COUNT(CASE WHEN dislike_danmaku > 0 THEN 1 END) AS dislike_danmaku
                                ,COUNT(CASE WHEN report_review>0 THEN 1 END) AS report_review
                                ,COUNT(CASE WHEN report_post>0 THEN 1  END) AS report_post
                                ,COUNT(CASE WHEN report_danmaku>0 THEN 1  END) AS report_danmaku"""
            elif self.app_name == 'maga':
                ups_consume = """
                               CONCAT(SUBSTR(ymd,1,4),'-',SUBSTR(ymd,5,2),'-',SUBSTR(ymd,7,2)) AS p_date,
                               did,
                               mid,
                               pid,
                               expose_t,
                               -- 帖子消费数据
                               COUNT(1) AS expose
                               ,COUNT(CASE WHEN real_expose>1 THEN 1 END) AS real_expose
                               ,COUNT(CASE WHEN score>0 THEN 1 END) AS score
                               ,COUNT(CASE WHEN score>0.8 THEN 1 END) AS score_satisfied
                               ,SUM(CASE WHEN stay_time>0 THEN stay_time ELSE 0 END) AS stay_time
                               ,SUM(CASE WHEN total_play_video>0 THEN total_play_video ELSE 0 END) AS total_play_video
                                ,SUM(
                                    CASE    WHEN total_play_video_dur>0 THEN total_play_video_dur 
                                            ELSE 0 
                                    END
                                ) AS total_play_video_dur
                                ,SUM(CASE WHEN play_video>0 THEN play_video ELSE 0 END) AS play_video
                                ,SUM(CASE WHEN play_video_dur>0 THEN play_video_dur ELSE 0 END) AS play_video_dur
                                ,SUM(CASE WHEN play_video>0 THEN 1 END) AS video_click_times
                                ,SUM(
                                    CASE    WHEN play_review_video>0 THEN play_review_video 
                                            ELSE 0 
                                    END
                                ) AS play_review_video
                                ,SUM(
                                    CASE    WHEN play_review_video_dur>0 THEN play_review_video_dur 
                                            ELSE 0 
                                    END
                                ) AS play_review_video_dur
                                ,0 AS play_recommend_video
                                ,0 AS play_recommend_video_dur
                                ,SUM(CASE WHEN finish_video>0 THEN finish_video ELSE 0 END) AS finish_video
                                ,SUM(CASE WHEN ones_video>0 THEN ones_video ELSE 0 END) AS ones_video
                                ,SUM(CASE WHEN total_view_img>0 THEN total_view_img ELSE 0 END) AS total_view_img
                                ,SUM(
                                    CASE    WHEN total_view_img_dur>0 THEN total_view_img_dur 
                                            ELSE 0 
                                    END
                                ) AS total_view_img_dur
                                ,SUM(CASE WHEN view_img>0 THEN view_img ELSE 0 END) AS view_img
                                ,SUM(CASE WHEN view_img_dur>0 THEN view_img_dur ELSE 0 END) AS view_img_dur
                                ,SUM(CASE WHEN view_img>0 THEN 1 END) AS img_click_times
                                ,SUM(CASE WHEN detail_post>0 THEN detail_post ELSE 0 END) AS detail_post
                                ,SUM(
                                    CASE    WHEN detail_post_dur>0 THEN detail_post_dur 
                                            ELSE 0 
                                    END
                                ) AS detail_post_dur
                                ,SUM(CASE WHEN view_post_dur>0 THEN view_post_dur ELSE 0 END) AS view_post_dur
                                ,COUNT(CASE WHEN LIKE>0 THEN 1 END) AS LIKE
                                ,COUNT(CASE WHEN like_attitude>0 THEN 1 END) AS like_attitude
                                ,COUNT(CASE WHEN like_attitude=2 THEN 1 END) AS like_funny
                                ,COUNT(CASE WHEN like_attitude=3 THEN 1 END) AS like_warm
                                ,0 AS like_silly
                                ,COUNT(CASE WHEN like_attitude=4 THEN 1 END) AS like_good
                                ,COUNT(CASE WHEN dislike>0 THEN 1 END) AS dislike
                                ,SUM(CASE WHEN share > 0 THEN share ELSE 0 END) AS share
                                ,SUM(CASE WHEN favor>0 THEN favor ELSE 0 END) AS favor
                                ,0 AS tedium
                                ,SUM(CASE WHEN create_review > 0 THEN create_review ELSE 0 END) AS create_review
                                ,SUM(CASE WHEN reply_review > 0 THEN reply_review ELSE 0 END) AS reply_review
                                ,SUM(CASE WHEN like_review>0 THEN like_review ELSE 0 END) AS like_review
                                ,0 AS like_review_retained
                                ,SUM(CASE WHEN dislike_review>0 THEN dislike_review ELSE 0 END) AS dislike_review
                                ,SUM(CASE WHEN share_review>0 THEN share_review ELSE 0 END) AS share_review
                                ,0 AS hots_review
                                ,0 AS news_review
                                ,0 AS subreviews_review
                                ,SUM(CASE WHEN review_expose>0 THEN review_expose ELSE 0 END) AS review_expose
                                ,SUM(CASE WHEN create_danmaku>0 THEN create_danmaku ELSE 0 END) AS create_danmaku
                                ,COUNT(CASE WHEN like_danmaku>0 THEN 1 END) AS like_danmaku
                                ,0 AS dislike_danmaku
                                ,0 AS report_review
                                ,0 AS report_post
                                ,0 AS report_danmaku"""
            elif self.app_name == 'zuiyou_lite':
                ups_consume = """
                               CONCAT(SUBSTR(ymd,1,4),'-',SUBSTR(ymd,5,2),'-',SUBSTR(ymd,7,2)) AS p_date,
                               did,
                               mid,
                               pid,
                               expose_t,
                               -- 帖子消费数据
                               COUNT(1) AS expose
                               ,COUNT(CASE WHEN real_expose>1 THEN 1 END) AS real_expose
                               ,COUNT(CASE WHEN score>0 THEN 1 END) AS score
                               ,COUNT(CASE WHEN score>0.8 THEN 1 END) AS score_satisfied
                               ,SUM(CASE WHEN stay_time>0 THEN stay_time ELSE 0 END) AS stay_time
                               ,SUM(CASE WHEN total_play_video>0 THEN total_play_video ELSE 0 END) AS total_play_video
                                ,SUM(
                                    CASE    WHEN total_play_video_dur>0 THEN total_play_video_dur 
                                            ELSE 0 
                                    END
                                ) AS total_play_video_dur
                                ,SUM(CASE WHEN play_video>0 THEN play_video ELSE 0 END) AS play_video
                                ,SUM(CASE WHEN play_video_dur>0 THEN play_video_dur ELSE 0 END) AS play_video_dur
                                ,SUM(CASE WHEN play_video>0 THEN 1 END) AS video_click_times
                                ,SUM(
                                    CASE    WHEN play_review_video>0 THEN play_review_video 
                                            ELSE 0 
                                    END
                                ) AS play_review_video
                                ,SUM(
                                    CASE    WHEN play_review_video_dur>0 THEN play_review_video_dur 
                                            ELSE 0 
                                    END
                                ) AS play_review_video_dur
                                ,0 AS play_recommend_video
                                ,0 AS play_recommend_video_dur
                                ,SUM(CASE WHEN finish_video>0 THEN finish_video ELSE 0 END) AS finish_video
                                ,SUM(CASE WHEN ones_video>0 THEN ones_video ELSE 0 END) AS ones_video
                                ,SUM(CASE WHEN total_view_img>0 THEN total_view_img ELSE 0 END) AS total_view_img
                                ,SUM(
                                    CASE    WHEN total_view_img_dur>0 THEN total_view_img_dur 
                                            ELSE 0 
                                    END
                                ) AS total_view_img_dur
                                ,SUM(CASE WHEN view_img>0 THEN view_img ELSE 0 END) AS view_img
                                ,SUM(CASE WHEN view_img_dur>0 THEN view_img_dur ELSE 0 END) AS view_img_dur
                                ,SUM(CASE WHEN view_img>0 THEN 1 END) AS img_click_times
                                ,SUM(CASE WHEN detail_post>0 THEN detail_post ELSE 0 END) AS detail_post
                                ,SUM(
                                    CASE    WHEN detail_post_dur>0 THEN detail_post_dur 
                                            ELSE 0 
                                    END
                                ) AS detail_post_dur
                                ,SUM(CASE WHEN view_post_dur>0 THEN view_post_dur ELSE 0 END) AS view_post_dur
                                ,COUNT(CASE WHEN LIKE>0 THEN 1 END) AS LIKE
                                ,COUNT(CASE WHEN like_attitude>0 THEN 1 END) AS like_attitude
                                ,COUNT(CASE WHEN like_attitude=2 THEN 1 END) AS like_funny
                                ,COUNT(CASE WHEN like_attitude=3 THEN 1 END) AS like_warm
                                ,COUNT(
                                    CASE WHEN like_attitude=5 THEN 1 
                                    END
                                ) AS like_silly
                                ,COUNT(CASE WHEN like_attitude=4 THEN 1 END) AS like_good
                                ,COUNT(CASE WHEN dislike>0 THEN 1 END) AS dislike
                                ,SUM(CASE WHEN share > 0 THEN share ELSE 0 END) AS share
                                ,SUM(CASE WHEN favor>0 THEN favor ELSE 0 END) AS favor
                                ,0 AS tedium
                                ,SUM(CASE WHEN create_review > 0 THEN create_review ELSE 0 END) AS create_review
                                ,SUM(CASE WHEN reply_review > 0 THEN reply_review ELSE 0 END) AS reply_review
                                ,SUM(CASE WHEN like_review>0 THEN like_review ELSE 0 END) AS like_review
                                ,0 AS like_review_retained
                                ,SUM(CASE WHEN dislike_review>0 THEN dislike_review ELSE 0 END) AS dislike_review
                                ,SUM(CASE WHEN share_review>0 THEN share_review ELSE 0 END) AS share_review
                                ,0 AS hots_review
                                ,0 AS news_review
                                ,0 AS subreviews_review
                                ,SUM(CASE WHEN review_expose>0 THEN review_expose ELSE 0 END) AS review_expose
                                ,SUM(CASE WHEN create_danmaku>0 THEN create_danmaku ELSE 0 END) AS create_danmaku
                                ,COUNT(CASE WHEN like_danmaku>0 THEN 1 END) AS like_danmaku
                                ,COUNT(CASE WHEN dislike_danmaku>0 THEN 1 END) AS dislike_danmaku
                                ,0 AS report_review
                                ,0 AS report_post
                                ,0 AS report_danmaku"""
            elif self.app_name == 'omg':
                ups_consume = """
                               CONCAT(SUBSTR(ymd,1,4),'-',SUBSTR(ymd,5,2),'-',SUBSTR(ymd,7,2)) AS p_date,
                               did,
                               mid,
                               pid,
                               expose_t,
                               -- 帖子消费数据
                               COUNT(1) AS expose
                               ,COUNT(CASE WHEN real_expose>1 THEN 1 END) AS real_expose
                               ,COUNT(CASE WHEN score>0 THEN 1 END) AS score
                               ,COUNT(CASE WHEN score>0.8 THEN 1 END) AS score_satisfied
                               ,SUM(CASE WHEN stay_time>0 THEN stay_time ELSE 0 END) AS stay_time
                               ,SUM(CASE WHEN total_play_video>0 THEN total_play_video ELSE 0 END) AS total_play_video
                                ,SUM(
                                    CASE    WHEN total_play_video_dur>0 THEN total_play_video_dur 
                                            ELSE 0 
                                    END
                                ) AS total_play_video_dur
                                ,SUM(CASE WHEN play_video>0 THEN play_video ELSE 0 END) AS play_video
                                ,SUM(CASE WHEN play_video_dur>0 THEN play_video_dur ELSE 0 END) AS play_video_dur
                                ,SUM(CASE WHEN play_video>0 THEN 1 END) AS video_click_times
                                ,SUM(
                                    CASE    WHEN play_review_video>0 THEN play_review_video 
                                            ELSE 0 
                                    END
                                ) AS play_review_video
                                ,SUM(
                                    CASE    WHEN play_review_video_dur>0 THEN play_review_video_dur 
                                            ELSE 0 
                                    END
                                ) AS play_review_video_dur
                                ,SUM(case when play_recommend_video > 0 then play_recommend_video else 0 end) AS play_recommend_video
                                ,SUM(case when play_recommend_video_dur > 0 then play_recommend_video_dur else 0 end) AS play_recommend_video_dur
                                ,SUM(CASE WHEN finish_video>0 THEN finish_video ELSE 0 END) AS finish_video
                                ,SUM(CASE WHEN ones_video>0 THEN ones_video ELSE 0 END) AS ones_video
                                ,SUM(CASE WHEN total_view_img>0 THEN total_view_img ELSE 0 END) AS total_view_img
                                ,SUM(
                                    CASE    WHEN total_view_img_dur>0 THEN total_view_img_dur 
                                            ELSE 0 
                                    END
                                ) AS total_view_img_dur
                                ,SUM(CASE WHEN view_img>0 THEN view_img ELSE 0 END) AS view_img
                                ,SUM(CASE WHEN view_img_dur>0 THEN view_img_dur ELSE 0 END) AS view_img_dur
                                ,SUM(CASE WHEN view_img>0 THEN 1 END) AS img_click_times
                                ,SUM(CASE WHEN detail_post>0 THEN detail_post ELSE 0 END) AS detail_post
                                ,SUM(
                                    CASE    WHEN detail_post_dur>0 THEN detail_post_dur 
                                            ELSE 0 
                                    END
                                ) AS detail_post_dur
                                ,SUM(CASE WHEN view_post_dur>0 THEN view_post_dur ELSE 0 END) AS view_post_dur
                                ,COUNT(CASE WHEN like>0 THEN 1 END) AS LIKE
                                ,COUNT(CASE WHEN like_attitude>0 THEN 1 END) AS like_attitude
                                ,COUNT(CASE WHEN like_attitude=2 THEN 1 END) AS like_funny
                                ,COUNT(CASE WHEN like_attitude=3 THEN 1 END) AS like_warm
                                ,0 AS like_silly
                                ,COUNT(CASE WHEN like_attitude=4 THEN 1 END) AS like_good
                                ,COUNT(CASE WHEN dislike>0 THEN 1 END) AS dislike
                                ,SUM(CASE WHEN share > 0 THEN share ELSE 0 END) AS share
                                ,SUM(CASE WHEN favor>0 THEN favor ELSE 0 END) AS favor
                                ,SUM(CASE WHEN tedium > 0 then tedium else 0 end) AS tedium
                                ,SUM(CASE WHEN create_review > 0 THEN create_review ELSE 0 END) AS create_review
                                ,SUM(CASE WHEN reply_review > 0 THEN reply_review ELSE 0 END) AS reply_review
                                ,SUM(CASE WHEN like_review>0 THEN like_review ELSE 0 END) AS like_review
                                ,SUM(CASE WHEN like_review_retained>0 THEN like_review_retained ELSE 0 END) AS like_review_retained
                                ,SUM(CASE WHEN dislike_review>0 THEN dislike_review ELSE 0 END) AS dislike_review
                                ,SUM(CASE WHEN share_review>0 THEN share_review ELSE 0 END) AS share_review
                                ,SUM(CASE WHEN hots_review>0 THEN hots_review ELSE 0 END) AS hots_review
                                ,SUM(CASE WHEN news_review>0 THEN news_review ELSE 0 END) AS news_review
                                ,SUM(CASE WHEN subreviews_review>0 THEN subreviews_review ELSE 0 END) AS subreviews_review
                                ,SUM(CASE WHEN review_expose>0 THEN review_expose ELSE 0 END) AS review_expose
                                ,SUM(CASE WHEN create_danmaku>0 THEN create_danmaku ELSE 0 END) AS create_danmaku
                                ,COUNT(CASE WHEN like_danmaku>0 THEN 1 END) AS like_danmaku
                                ,COUNT(CASE WHEN dislike_danmaku>0 THEN 1 END) AS dislike_danmaku
                                ,SUM(CASE WHEN report_review>0 THEN report_review ELSE 0 END) AS report_review
                                ,COUNT(CASE WHEN report_post>0 THEN 1 END) AS report_post
                                ,COUNT(CASE WHEN report_danmaku>0 THEN 1 END) AS report_danmaku"""

            for v in self.post_consume:
                post_consume_type = []
                post_consume_condition_sel = ""
                post_consume_condition = ""
                post_consume_date_type = ""
                post_consume_start_date = str(self.start_date)
                post_consume_end_date = str(self.end_date)

                ## 读取填写的参数条件
                for key, value in v.items():
                    if key == "show_keys":
                        continue
                    else:
                        if key in ('total_dur', 'media_num', 'tid', 'tname', 'ptype', 'post_age','l1part_name',
                                'l2part_name', 'l1part_id', 'l2part_id') and key not in post_consume_type:
                            post_consume_type.append(key)
                        if key == "post_consume_start_date":
                            post_consume_start_date = (datetime.datetime.fromtimestamp(int(value))).strftime("%Y-%m-%d")
                        elif key == "post_consume_end_date":
                            post_consume_end_date = (datetime.datetime.fromtimestamp(int(value))).strftime("%Y-%m-%d")
                        elif key == "post_consume_date_type":
                            post_consume_date_type = str(value)
                        else:
                            if type(value) == unicode:
                                if '~' in value:
                                    if len(post_consume_condition_sel) > 0:
                                        post_consume_condition_sel += " AND " + str(key) + " BETWEEN " + \
                                                                      value.split('~')[
                                                                          0].strip() + " AND " + value.split('~')[
                                                                          1].strip()
                                    else:
                                        post_consume_condition_sel += "WHERE " + str(key) + " BETWEEN " + value.split('~')[
                                            0].strip() + " AND " + \
                                                                      value.split('~')[1].strip()
                                elif ',' in value:
                                    condition_len = len(value.split(','))
                                    condition_tmp = ""
                                    for i in range(condition_len):
                                        if i == 0:
                                            condition_tmp += str(key) + " IN ( " + "'" + str(
                                                value.split(',')[i]).strip() + "'"
                                        else:
                                            condition_tmp += ",'" + str(value.split(',')[i]).strip() + "'"
                                    if len(post_consume_condition_sel) > 0:
                                        post_consume_condition_sel += " AND " + condition_tmp.strip() + ") "
                                    else:
                                        post_consume_condition_sel += "WHERE " + condition_tmp.strip() + ") "
                                else:
                                    if len(post_consume_condition_sel) > 0:
                                        post_consume_condition_sel += " AND " + str(key) + " IN ('" + str(
                                            value).strip() + "')"
                                    else:
                                        post_consume_condition_sel += "WHERE " + str(key) + " IN ('" + str(
                                            value).strip() + "')"
                            elif type(value) == list:
                                con_tmp = ''
                                for con in value:
                                    con_tmp += "'" + str(con) + "',"
                                con_tmp_final = con_tmp.strip(',')
                                if len(post_consume_condition_sel) > 0:
                                    post_consume_condition_sel += " AND " + str(key) + " IN (" + str(
                                        con_tmp_final).strip() + ")"
                                else:
                                    post_consume_condition_sel += "WHERE " + str(key) + " IN (" + str(
                                        con_tmp_final).strip() + ")"

                ## 选择的帖子类型
                for m in post_consume_type:
                    post_consume_condition += str(m).strip() + "," + """\n"""

                ## 时间范围
                if post_consume_date_type == "period":
                    post_consume_date_range = ""
                elif post_consume_date_type == "day":
                    post_consume_date_range = "p_date,"
                else:
                    post_consume_date_range = "p_date,"

                ## 根据参数拼接SQL
                if len(sql_post_consume_tmp) > 0:
                    sql_post_consume_tmp += """
                    UNION 

                    SELECT did AS device_id,
                           mid
                    FROM 
                    (SELECT {}
                           did,
                           mid,
                           {}
                           nvl(SUM(t1.expose),0) AS expose
                            ,nvl(SUM(t1.real_expose),0) AS real_expose
                            ,nvl(SUM(t1.score),0) AS score
                            ,nvl(SUM(t1.score_satisfied),0) AS score_satisfied
                            ,nvl(SUM(t1.stay_time),0) AS stay_time
                            ,nvl(SUM(t1.total_play_video),0) AS total_play_video
                            ,nvl(SUM(t1.total_play_video_dur),0)/60 AS total_play_video_dur
                            ,nvl(SUM(t1.play_video),0) AS play_video
                            ,nvl(SUM(play_video_dur),0)/60 AS play_video_dur
                            ,nvl(SUM(t1.play_review_video),0) AS play_review_video
                            ,nvl(SUM(t1.play_review_video_dur),0)/60 AS play_review_video_dur
                            ,nvl(SUM(t1.play_recommend_video),0) AS play_recommend_video
                            ,nvl(SUM(t1.play_recommend_video_dur),0)/60 AS play_recommend_video_dur
                            ,nvl(SUM(t1.finish_video),0) AS finish_video
                            ,nvl(SUM(t1.ones_video),0) AS ones_video
                            ,nvl(SUM(t1.total_view_img),0) AS total_view_img
                            ,nvl(SUM(t1.total_view_img_dur),0)/60 AS total_view_img_dur
                            ,nvl(SUM(t1.view_img),0) AS view_img
                            ,nvl(SUM(t1.view_img_dur),0)/60 AS view_img_dur
                            ,nvl(SUM(t1.detail_post),0) AS detail_post
                            ,nvl(SUM(t1.detail_post_dur),0)/60 AS detail_post_dur
                            ,nvl(SUM(t1.view_post_dur),0)/60 AS view_post_dur
                            ,nvl(SUM(t1.LIKE),0) AS LIKE
                            ,nvl(SUM(t1.like_attitude),0) AS like_attitude
                            ,nvl(SUM(t1.like_funny),0) AS like_funny
                            ,nvl(SUM(t1.like_warm),0) AS like_warm
                            ,nvl(SUM(t1.like_silly),0) AS like_silly
                            ,nvl(SUM(t1.like_good),0) AS like_good
                            ,nvl(SUM(t1.dislike),0) AS dislike
                            ,nvl(SUM(t1.share),0) AS share
                            ,nvl(SUM(t1.favor),0) AS favor
                            ,nvl(SUM(t1.tedium),0) AS tedium
                            ,nvl(SUM(t1.create_review),0) AS create_review
                            ,nvl(SUM(t1.reply_review),0) AS reply_review
                            ,nvl(SUM(t1.create_review+t1.reply_review),0) AS total_review
                            ,nvl(SUM(t1.like_review),0) AS like_review
                            ,nvl(SUM(t1.like_review_retained),0) AS like_review_retained
                            ,nvl(SUM(t1.dislike_review),0) AS dislike_review
                            ,nvl(SUM(t1.share_review),0) AS share_review
                            ,nvl(SUM(t1.hots_review),0) AS hots_review
                            ,nvl(SUM(t1.news_review),0) AS news_review
                            ,nvl(SUM(t1.subreviews_review),0) AS subreviews_review
                            ,nvl(SUM(t1.review_expose),0) AS review_expose
                            ,nvl(SUM(t1.create_danmaku),0) AS create_danmaku
                            ,nvl(SUM(t1.like_danmaku),0) AS like_danmaku
                            ,nvl(SUM(t1.dislike_danmaku),0) AS dislike_danmaku
                            ,nvl(SUM(t1.video_click_times),0) AS video_click_times
                            ,nvl(SUM(t1.img_click_times),0) AS img_click_times
                            ,nvl(SUM(t1.report_review),0) AS report_review
                            ,nvl(SUM(t1.report_post),0) AS report_post
                            ,nvl(SUM(t1.report_danmaku),0) AS report_danmaku
                    FROM 
                    (SELECT t1.p_date,
                           t1.did,
                           t1.mid,
                           CASE  WHEN total_dur<=0 OR total_dur IS NULL THEN 'other'
                                 WHEN total_dur <=5 THEN '0-5'
                                 WHEN total_dur <=15 THEN '6-15'
                                 WHEN total_dur <=30 THEN '16-30'
                                 WHEN total_dur <=60 THEN '31-60'
                                 WHEN total_dur <=90 THEN '61-90'
                                 WHEN total_dur <=120 THEN '91-120'
                                 WHEN total_dur <=300 THEN '121-300'
                                 WHEN total_dur >300 THEN '300+' 
                            END AS total_dur,
                            CASE WHEN t2.media_num <= 9 THEN media_num ELSE 'other' END AS media_num,
                            nvl(t2.tid,0) AS tid,
                            tname,
                        CASE    WHEN t2.ptype = 1 THEN '文字'
                                 WHEN t2.ptype = 2 THEN '静图'
                                 WHEN t2.ptype = 3 THEN '长静图'
                                 WHEN t2.ptype = 4 THEN '动图'
                                 WHEN t2.ptype = 5 THEN '视频'
                                 WHEN t2.ptype = 6 THEN '长文'
                                 WHEN t2.ptype = 7 THEN '音频' 
                                 ELSE 'other' 
                         END AS ptype,
                        CASE    WHEN expose_t - ct BETWEEN 0 AND 86400-1 THEN '0'
                                 WHEN expose_t - ct BETWEEN 86400 AND 86400*2-1 THEN '1'
                                 WHEN expose_t - ct BETWEEN 86400*2 AND 86400*3-1 THEN '2'
                                 WHEN expose_t - ct BETWEEN 86400*3 AND 86400*4-1 THEN '3'
                                 WHEN expose_t - ct BETWEEN 86400*4 AND 86400*8-1 THEN '4-7'
                                 WHEN expose_t - ct BETWEEN 86400*8 AND 86400*15-1 THEN '8-14'
                                 WHEN expose_t - ct BETWEEN 86400*15 AND 86400*31-1 THEN '15-30'
                                 WHEN expose_t - ct >= 86400*31 THEN '30+' 
                                 ELSE 'other' END AS post_age,
                            l1part_name,
                            l2part_name,
                            l1part_id,
                            l2part_id,
                            nvl(SUM(t1.expose),0) AS expose
                            ,nvl(SUM(t1.real_expose),0) AS real_expose
                            ,nvl(SUM(t1.score),0) AS score
                            ,nvl(SUM(t1.score_satisfied),0) AS score_satisfied
                            ,nvl(SUM(t1.stay_time),0) AS stay_time
                            ,nvl(SUM(t1.total_play_video),0) AS total_play_video
                            ,nvl(SUM(t1.total_play_video_dur),0) AS total_play_video_dur
                            ,nvl(SUM(t1.play_video),0) AS play_video
                            ,nvl(
                            SUM(
                                CASE    WHEN t1.play_video_dur<=total_dur THEN play_video_dur 
                                        ELSE total_dur 
                                END
                            )
                            ,0
                            ) AS play_video_dur
                            ,nvl(SUM(t1.play_review_video),0) AS play_review_video
                            ,nvl(SUM(t1.play_review_video_dur),0) AS play_review_video_dur
                            ,nvl(SUM(t1.play_recommend_video),0) AS play_recommend_video
                            ,nvl(SUM(t1.play_recommend_video_dur),0) AS play_recommend_video_dur
                            ,nvl(SUM(t1.finish_video),0) AS finish_video
                            ,nvl(SUM(t1.ones_video),0) AS ones_video
                            ,nvl(SUM(t1.total_view_img),0) AS total_view_img
                            ,nvl(SUM(t1.total_view_img_dur),0) AS total_view_img_dur
                            ,nvl(SUM(t1.view_img),0) AS view_img
                            ,nvl(SUM(t1.view_img_dur),0) AS view_img_dur
                            ,nvl(SUM(t1.detail_post),0) AS detail_post
                            ,nvl(SUM(t1.detail_post_dur),0) AS detail_post_dur
                            ,nvl(SUM(t1.view_post_dur),0) AS view_post_dur
                            ,nvl(SUM(t1.LIKE),0) AS LIKE
                            ,nvl(SUM(t1.like_attitude),0) AS like_attitude
                            ,nvl(SUM(t1.like_funny),0) AS like_funny
                            ,nvl(SUM(t1.like_warm),0) AS like_warm
                            ,nvl(SUM(t1.like_silly),0) AS like_silly
                            ,nvl(SUM(t1.like_good),0) AS like_good
                            ,nvl(SUM(t1.dislike),0) AS dislike
                            ,nvl(SUM(t1.share),0) AS share
                            ,nvl(SUM(t1.favor),0) AS favor
                            ,nvl(SUM(t1.tedium),0) AS tedium
                            ,nvl(SUM(t1.create_review),0) AS create_review
                            ,nvl(SUM(t1.reply_review),0) AS reply_review
                            ,nvl(SUM(t1.like_review),0) AS like_review
                            ,nvl(SUM(t1.like_review_retained),0) AS like_review_retained
                            ,nvl(SUM(t1.dislike_review),0) AS dislike_review
                            ,nvl(SUM(t1.share_review),0) AS share_review
                            ,nvl(SUM(t1.hots_review),0) AS hots_review
                            ,nvl(SUM(t1.news_review),0) AS news_review
                            ,nvl(SUM(t1.subreviews_review),0) AS subreviews_review
                            ,nvl(SUM(t1.review_expose),0) AS review_expose
                            ,nvl(SUM(t1.create_danmaku),0) AS create_danmaku
                            ,nvl(SUM(t1.like_danmaku),0) AS like_danmaku
                            ,nvl(SUM(t1.dislike_danmaku),0) AS dislike_danmaku
                            ,nvl(SUM(t1.video_click_times),0) AS video_click_times
                            ,nvl(SUM(t1.img_click_times),0) AS img_click_times
                            ,nvl(SUM(t1.report_review),0) AS report_review
                            ,nvl(SUM(t1.report_post),0) AS report_post
                            ,nvl(SUM(t1.report_danmaku),0) AS report_danmaku
                    FROM 
                    (
                        SELECT {}
                        FROM {}
                        WHERE CONCAT(SUBSTR(ymd,1,4),'-',SUBSTR(ymd,5,2),'-',SUBSTR(ymd,7,2)) BETWEEN '{}' AND '{}'
                        AND expose > 0
                        AND expose_t > 0
                        GROUP BY CONCAT(SUBSTR(ymd,1,4),'-',SUBSTR(ymd,5,2),'-',SUBSTR(ymd,7,2)), did, mid, pid, expose_t
                    ) t1 
                    LEFT JOIN 
                    -- 帖子属性维度
                    (
                        SELECT  pid
                                ,min(ct) AS ct
                                ,max(ptype) AS ptype
                                ,max(tid) AS tid
                                ,min(SUBSTR(FROM_UNIXTIME({}),1,10)) AS c_date_pid
                                {}
                                ,max(nvl(vdur,0)) AS total_dur
                        FROM    {}
                        GROUP BY pid
                    ) t2 
                    ON t1.pid = t2.pid
                    LEFT JOIN 
                    -- 分区维度
                    (
                        SELECT tid,
                               {}
                        FROM {}
                        GROUP BY tid 
                    ) t3 
                    ON t2.tid = t3.tid
                    LEFT JOIN 
                    -- 话题维度
                    (
                        SELECT tid,
                               MAX(tname) AS tname 
                        FROM {}
                        GROUP BY tid
                    ) t4
                    ON t2.tid = t4.tid
                    GROUP BY t1.p_date,
                           CASE  WHEN total_dur<=0 OR total_dur IS NULL THEN 'other'
                                 WHEN total_dur <=5 THEN '0-5'
                                 WHEN total_dur <=15 THEN '6-15'
                                 WHEN total_dur <=30 THEN '16-30'
                                 WHEN total_dur <=60 THEN '31-60'
                                 WHEN total_dur <=90 THEN '61-90'
                                 WHEN total_dur <=120 THEN '91-120'
                                 WHEN total_dur <=300 THEN '121-300'
                                 WHEN total_dur >300 THEN '300+' 
                            END,
                            CASE WHEN t2.media_num <= 9 THEN t2.media_num ELSE 'other' END,
                            nvl(t2.tid,0),
                            tname,
                        CASE    WHEN t2.ptype = 1 THEN '文字'
                                 WHEN t2.ptype = 2 THEN '静图'
                                 WHEN t2.ptype = 3 THEN '长静图'
                                 WHEN t2.ptype = 4 THEN '动图'
                                 WHEN t2.ptype = 5 THEN '视频'
                                 WHEN t2.ptype = 6 THEN '长文'
                                 WHEN t2.ptype = 7 THEN '音频' 
                                 ELSE 'other' 
                         END,
                        CASE    WHEN expose_t - ct BETWEEN 0 AND 86400-1 THEN '0'
                                 WHEN expose_t - ct BETWEEN 86400 AND 86400*2-1 THEN '1'
                                 WHEN expose_t - ct BETWEEN 86400*2 AND 86400*3-1 THEN '2'
                                 WHEN expose_t - ct BETWEEN 86400*3 AND 86400*4-1 THEN '3'
                                 WHEN expose_t - ct BETWEEN 86400*4 AND 86400*8-1 THEN '4-7'
                                 WHEN expose_t - ct BETWEEN 86400*8 AND 86400*15-1 THEN '8-14'
                                 WHEN expose_t - ct BETWEEN 86400*15 AND 86400*31-1 THEN '15-30'
                                 WHEN expose_t - ct >= 86400*31 THEN '30+' 
                                 ELSE 'other' END,
                            l1part_name,
                            l2part_name,
                            l1part_id,
                            l2part_id,
                            t1.did,
                           t1.mid) t1
                    GROUP BY {}
                           {}
                           did,
                           mid
                           )
                    {}
                    GROUP BY did, mid
                    """.format(str(post_consume_date_range), str(post_consume_condition), str(ups_consume),str(self.ups_table),
                               str(post_consume_start_date), str(post_consume_end_date), str(ct_time),str(media_num),
                               str(self.post_table), str(part_name),str(self.part_table), str(self.tid_table),str(post_consume_date_range),
                               str(post_consume_condition), str(post_consume_condition_sel))
                else:
                    sql_post_consume_tmp += """
                                        SELECT did AS device_id,
                                               mid
                                        FROM 
                                        (SELECT {}
                                               did,
                                               mid,
                                               {}
                                               nvl(SUM(t1.expose),0) AS expose
                                                ,nvl(SUM(t1.real_expose),0) AS real_expose
                                                ,nvl(SUM(t1.score),0) AS score
                                                ,nvl(SUM(t1.score_satisfied),0) AS score_satisfied
                                                ,nvl(SUM(t1.stay_time),0) AS stay_time
                                                ,nvl(SUM(t1.total_play_video),0) AS total_play_video
                                                ,nvl(SUM(t1.total_play_video_dur),0)/60 AS total_play_video_dur
                                                ,nvl(SUM(t1.play_video),0) AS play_video
                                                ,nvl(SUM(play_video_dur),0)/60 AS play_video_dur
                                                ,nvl(SUM(t1.play_review_video),0) AS play_review_video
                                                ,nvl(SUM(t1.play_review_video_dur),0)/60 AS play_review_video_dur
                                                ,nvl(SUM(t1.play_recommend_video),0) AS play_recommend_video
                                                ,nvl(SUM(t1.play_recommend_video_dur),0)/60 AS play_recommend_video_dur
                                                ,nvl(SUM(t1.finish_video),0) AS finish_video
                                                ,nvl(SUM(t1.ones_video),0) AS ones_video
                                                ,nvl(SUM(t1.total_view_img),0) AS total_view_img
                                                ,nvl(SUM(t1.total_view_img_dur),0)/60 AS total_view_img_dur
                                                ,nvl(SUM(t1.view_img),0) AS view_img
                                                ,nvl(SUM(t1.view_img_dur),0)/60 AS view_img_dur
                                                ,nvl(SUM(t1.detail_post),0) AS detail_post
                                                ,nvl(SUM(t1.detail_post_dur),0)/60 AS detail_post_dur
                                                ,nvl(SUM(t1.view_post_dur),0)/60 AS view_post_dur
                                                ,nvl(SUM(t1.LIKE),0) AS LIKE
                                                ,nvl(SUM(t1.like_attitude),0) AS like_attitude
                                                ,nvl(SUM(t1.like_funny),0) AS like_funny
                                                ,nvl(SUM(t1.like_warm),0) AS like_warm
                                                ,nvl(SUM(t1.like_silly),0) AS like_silly
                                                ,nvl(SUM(t1.like_good),0) AS like_good
                                                ,nvl(SUM(t1.dislike),0) AS dislike
                                                ,nvl(SUM(t1.share),0) AS share
                                                ,nvl(SUM(t1.favor),0) AS favor
                                                ,nvl(SUM(t1.tedium),0) AS tedium
                                                ,nvl(SUM(t1.create_review),0) AS create_review
                                                ,nvl(SUM(t1.reply_review),0) AS reply_review
                                                ,nvl(SUM(t1.create_review+t1.reply_review),0) AS total_review
                                                ,nvl(SUM(t1.like_review),0) AS like_review
                                                ,nvl(SUM(t1.like_review_retained),0) AS like_review_retained
                                                ,nvl(SUM(t1.dislike_review),0) AS dislike_review
                                                ,nvl(SUM(t1.share_review),0) AS share_review
                                                ,nvl(SUM(t1.hots_review),0) AS hots_review
                                                ,nvl(SUM(t1.news_review),0) AS news_review
                                                ,nvl(SUM(t1.subreviews_review),0) AS subreviews_review
                                                ,nvl(SUM(t1.review_expose),0) AS review_expose
                                                ,nvl(SUM(t1.create_danmaku),0) AS create_danmaku
                                                ,nvl(SUM(t1.like_danmaku),0) AS like_danmaku
                                                ,nvl(SUM(t1.dislike_danmaku),0) AS dislike_danmaku
                                                ,nvl(SUM(t1.video_click_times),0) AS video_click_times
                                                ,nvl(SUM(t1.img_click_times),0) AS img_click_times
                                                ,nvl(SUM(t1.report_review),0) AS report_review
                                                ,nvl(SUM(t1.report_post),0) AS report_post
                                                ,nvl(SUM(t1.report_danmaku),0) AS report_danmaku
                                        FROM 
                                        (SELECT t1.p_date,
                                               t1.did,
                                               t1.mid,
                                               CASE  WHEN total_dur<=0 OR total_dur IS NULL THEN 'other'
                                                     WHEN total_dur <=5 THEN '0-5'
                                                     WHEN total_dur <=15 THEN '6-15'
                                                     WHEN total_dur <=30 THEN '16-30'
                                                     WHEN total_dur <=60 THEN '31-60'
                                                     WHEN total_dur <=90 THEN '61-90'
                                                     WHEN total_dur <=120 THEN '91-120'
                                                     WHEN total_dur <=300 THEN '121-300'
                                                     WHEN total_dur >300 THEN '300+' 
                                                END AS total_dur,
                                                CASE WHEN t2.media_num <= 9 THEN media_num ELSE 'other' END AS media_num,
                                                nvl(t2.tid,0) AS tid,
                                                tname,
                                            CASE    WHEN t2.ptype = 1 THEN '文字'
                                                     WHEN t2.ptype = 2 THEN '静图'
                                                     WHEN t2.ptype = 3 THEN '长静图'
                                                     WHEN t2.ptype = 4 THEN '动图'
                                                     WHEN t2.ptype = 5 THEN '视频'
                                                     WHEN t2.ptype = 6 THEN '长文'
                                                     WHEN t2.ptype = 7 THEN '音频' 
                                                     ELSE 'other' 
                                             END AS ptype,
                                            CASE    WHEN expose_t - ct BETWEEN 0 AND 86400-1 THEN '0'
                                                     WHEN expose_t - ct BETWEEN 86400 AND 86400*2-1 THEN '1'
                                                     WHEN expose_t - ct BETWEEN 86400*2 AND 86400*3-1 THEN '2'
                                                     WHEN expose_t - ct BETWEEN 86400*3 AND 86400*4-1 THEN '3'
                                                     WHEN expose_t - ct BETWEEN 86400*4 AND 86400*8-1 THEN '4-7'
                                                     WHEN expose_t - ct BETWEEN 86400*8 AND 86400*15-1 THEN '8-14'
                                                     WHEN expose_t - ct BETWEEN 86400*15 AND 86400*31-1 THEN '15-30'
                                                     WHEN expose_t - ct >= 86400*31 THEN '30+' 
                                                     ELSE 'other' END AS post_age,
                                                l1part_name,
                                                l2part_name,
                                                l1part_id,
                                                l2part_id,
                                                nvl(SUM(t1.expose),0) AS expose
                                                ,nvl(SUM(t1.real_expose),0) AS real_expose
                                                ,nvl(SUM(t1.score),0) AS score
                                                ,nvl(SUM(t1.score_satisfied),0) AS score_satisfied
                                                ,nvl(SUM(t1.stay_time),0) AS stay_time
                                                ,nvl(SUM(t1.total_play_video),0) AS total_play_video
                                                ,nvl(SUM(t1.total_play_video_dur),0) AS total_play_video_dur
                                                ,nvl(SUM(t1.play_video),0) AS play_video
                                                ,nvl(
                                                SUM(
                                                    CASE    WHEN t1.play_video_dur<=total_dur THEN play_video_dur 
                                                            ELSE total_dur 
                                                    END
                                                )
                                                ,0
                                                ) AS play_video_dur
                                                ,nvl(SUM(t1.play_review_video),0) AS play_review_video
                                                ,nvl(SUM(t1.play_review_video_dur),0) AS play_review_video_dur
                                                ,nvl(SUM(t1.play_recommend_video),0) AS play_recommend_video
                                                ,nvl(SUM(t1.play_recommend_video_dur),0) AS play_recommend_video_dur
                                                ,nvl(SUM(t1.finish_video),0) AS finish_video
                                                ,nvl(SUM(t1.ones_video),0) AS ones_video
                                                ,nvl(SUM(t1.total_view_img),0) AS total_view_img
                                                ,nvl(SUM(t1.total_view_img_dur),0) AS total_view_img_dur
                                                ,nvl(SUM(t1.view_img),0) AS view_img
                                                ,nvl(SUM(t1.view_img_dur),0) AS view_img_dur
                                                ,nvl(SUM(t1.detail_post),0) AS detail_post
                                                ,nvl(SUM(t1.detail_post_dur),0) AS detail_post_dur
                                                ,nvl(SUM(t1.view_post_dur),0) AS view_post_dur
                                                ,nvl(SUM(t1.LIKE),0) AS LIKE
                                                ,nvl(SUM(t1.like_attitude),0) AS like_attitude
                                                ,nvl(SUM(t1.like_funny),0) AS like_funny
                                                ,nvl(SUM(t1.like_warm),0) AS like_warm
                                                ,nvl(SUM(t1.like_silly),0) AS like_silly
                                                ,nvl(SUM(t1.like_good),0) AS like_good
                                                ,nvl(SUM(t1.dislike),0) AS dislike
                                                ,nvl(SUM(t1.share),0) AS share
                                                ,nvl(SUM(t1.favor),0) AS favor
                                                ,nvl(SUM(t1.tedium),0) AS tedium
                                                ,nvl(SUM(t1.create_review),0) AS create_review
                                                ,nvl(SUM(t1.reply_review),0) AS reply_review
                                                ,nvl(SUM(t1.like_review),0) AS like_review
                                                ,nvl(SUM(t1.like_review_retained),0) AS like_review_retained
                                                ,nvl(SUM(t1.dislike_review),0) AS dislike_review
                                                ,nvl(SUM(t1.share_review),0) AS share_review
                                                ,nvl(SUM(t1.hots_review),0) AS hots_review
                                                ,nvl(SUM(t1.news_review),0) AS news_review
                                                ,nvl(SUM(t1.subreviews_review),0) AS subreviews_review
                                                ,nvl(SUM(t1.review_expose),0) AS review_expose
                                                ,nvl(SUM(t1.create_danmaku),0) AS create_danmaku
                                                ,nvl(SUM(t1.like_danmaku),0) AS like_danmaku
                                                ,nvl(SUM(t1.dislike_danmaku),0) AS dislike_danmaku
                                                ,nvl(SUM(t1.video_click_times),0) AS video_click_times
                                                ,nvl(SUM(t1.img_click_times),0) AS img_click_times
                                                ,nvl(SUM(t1.report_review),0) AS report_review
                                                ,nvl(SUM(t1.report_post),0) AS report_post
                                                ,nvl(SUM(t1.report_danmaku),0) AS report_danmaku
                                        FROM 
                                        (
                                            SELECT {}
                                            FROM {}
                                            WHERE CONCAT(SUBSTR(ymd,1,4),'-',SUBSTR(ymd,5,2),'-',SUBSTR(ymd,7,2)) BETWEEN '{}' AND '{}'
                                            AND expose > 0
                                            AND expose_t > 0
                                            GROUP BY CONCAT(SUBSTR(ymd,1,4),'-',SUBSTR(ymd,5,2),'-',SUBSTR(ymd,7,2)), did, mid, pid, expose_t
                                        ) t1 
                                        LEFT JOIN 
                                        -- 帖子属性维度
                                        (
                                            SELECT  pid
                                                    ,min(ct) AS ct
                                                    ,max(ptype) AS ptype
                                                    ,max(tid) AS tid
                                                    ,min(SUBSTR(FROM_UNIXTIME({}),1,10)) AS c_date_pid
                                                    {}
                                                    ,max(nvl(vdur,0)) AS total_dur
                                            FROM    {}
                                            GROUP BY pid
                                        ) t2 
                                        ON t1.pid = t2.pid
                                        LEFT JOIN 
                                        -- 分区维度
                                        (
                                            SELECT tid,
                                                   {}
                                            FROM {}
                                            GROUP BY tid 
                                        ) t3 
                                        ON t2.tid = t3.tid
                                        LEFT JOIN 
                                        -- 话题维度
                                        (
                                            SELECT tid,
                                                   MAX(tname) AS tname 
                                            FROM {}
                                            GROUP BY tid
                                        ) t4
                                        ON t2.tid = t4.tid
                                        GROUP BY t1.p_date,
                                               CASE  WHEN total_dur<=0 OR total_dur IS NULL THEN 'other'
                                                     WHEN total_dur <=5 THEN '0-5'
                                                     WHEN total_dur <=15 THEN '6-15'
                                                     WHEN total_dur <=30 THEN '16-30'
                                                     WHEN total_dur <=60 THEN '31-60'
                                                     WHEN total_dur <=90 THEN '61-90'
                                                     WHEN total_dur <=120 THEN '91-120'
                                                     WHEN total_dur <=300 THEN '121-300'
                                                     WHEN total_dur >300 THEN '300+' 
                                                END,
                                                CASE WHEN t2.media_num <= 9 THEN media_num ELSE 'other' END,
                                                nvl(t2.tid,0),
                                                tname,
                                            CASE    WHEN t2.ptype = 1 THEN '文字'
                                                     WHEN t2.ptype = 2 THEN '静图'
                                                     WHEN t2.ptype = 3 THEN '长静图'
                                                     WHEN t2.ptype = 4 THEN '动图'
                                                     WHEN t2.ptype = 5 THEN '视频'
                                                     WHEN t2.ptype = 6 THEN '长文'
                                                     WHEN t2.ptype = 7 THEN '音频' 
                                                     ELSE 'other' 
                                             END,
                                            CASE    WHEN expose_t - ct BETWEEN 0 AND 86400-1 THEN '0'
                                                     WHEN expose_t - ct BETWEEN 86400 AND 86400*2-1 THEN '1'
                                                     WHEN expose_t - ct BETWEEN 86400*2 AND 86400*3-1 THEN '2'
                                                     WHEN expose_t - ct BETWEEN 86400*3 AND 86400*4-1 THEN '3'
                                                     WHEN expose_t - ct BETWEEN 86400*4 AND 86400*8-1 THEN '4-7'
                                                     WHEN expose_t - ct BETWEEN 86400*8 AND 86400*15-1 THEN '8-14'
                                                     WHEN expose_t - ct BETWEEN 86400*15 AND 86400*31-1 THEN '15-30'
                                                     WHEN expose_t - ct >= 86400*31 THEN '30+' 
                                                     ELSE 'other' END,
                                                l1part_name,
                                                l2part_name,
                                                l1part_id,
                                                l2part_id,
                                                t1.did,
                                               t1.mid) t1 
                                        GROUP BY {}
                                               {}
                                               did,
                                               mid
                                               )
                                        {}
                                        GROUP BY did, mid
                                        """.format(str(post_consume_date_range), str(post_consume_condition),str(ups_consume),
                                                   str(self.ups_table), str(post_consume_start_date),str(post_consume_end_date), str(ct_time),str(media_num),
                                                   str(self.post_table), str(part_name), str(self.part_table), str(self.tid_table),str(post_consume_date_range),
                                                   str(post_consume_condition), str(post_consume_condition_sel))
            sql_post_consume = """
                -- 分区或者话题消费数据
                FULL OUTER JOIN 
                (
                """ \
                               + sql_post_consume_tmp + """\n""" \
                               + """) post_consume
                on base.device_id = post_consume.device_id and base.mid = post_consume.mid
                """
        else:
            sql_post_consume = ""

        ## 话题内消费维度
        if self.topic_consume:
            sql_topic_tmp = ""
            for v in self.topic_consume:
                topic_type = []
                topic_condition_sel = ""
                topic_condition = ""
                topic_date_type = ""
                topic_start_date = str(self.start_date)
                topic_end_date = str(self.end_date)

                ## 参数读取
                for key, value in v.items():
                    if key == "show_keys":
                        continue
                    else:
                        if key in ('tid', 'tname') and key not in topic_type:
                            topic_type.append(key)
                        if key == 'topic_consume_start_date':
                            topic_start_date = (datetime.datetime.fromtimestamp(int(value))).strftime("%Y-%m-%d")
                        elif key == 'topic_consume_end_date':
                            topic_end_date = (datetime.datetime.fromtimestamp(int(value))).strftime("%Y-%m-%d")
                        elif key == 'topic_consume_date_type':
                            topic_date_type = str(value)
                        else:
                            if type(value) == unicode:
                                if '~' in value:
                                    if len(topic_condition_sel) > 0:
                                        topic_condition_sel += " AND " + str(key) + " BETWEEN " + value.split('~')[
                                            0].strip() + " AND " + value.split('~')[1].strip()
                                    else:
                                        topic_condition_sel += "WHERE " + str(key) + " BETWEEN " + value.split('~')[
                                            0].strip() + " AND " + value.split('~')[1].strip()
                                elif ',' in value:
                                    condition_len = len(value.split(','))
                                    condition_tmp = ""
                                    for i in range(condition_len):
                                        if i == 0:
                                            condition_tmp += str(key) + " IN ( " + "'" + str(
                                                value.split(',')[i]).strip() + "'"
                                        else:
                                            condition_tmp += ",'" + str(value.split(',')[i]).strip() + "'"
                                    if len(topic_condition_sel) > 0:
                                        topic_condition_sel += " AND " + condition_tmp.strip() + ") "
                                    else:
                                        topic_condition_sel += "WHERE " + condition_tmp.strip() + ") "
                                else:
                                    if len(topic_condition_sel) > 0:
                                        topic_condition_sel += " AND " + str(key) + " IN ('" + str(value).strip() + "')"
                                    else:
                                        topic_condition_sel += "WHERE " + str(key) + " IN ('" + str(value).strip() + "')"
                            elif type(value) == list:
                                con_tmp = ''
                                for con in value:
                                    con_tmp += "'" + str(con) + "',"
                                con_tmp_final = con_tmp.strip(',')
                                if len(topic_condition_sel) > 0:
                                    topic_condition_sel += " AND " + str(key) + " IN (" + str(
                                        con_tmp_final).strip() + ")"
                                else:
                                    topic_condition_sel += "WHERE " + str(key) + " IN (" + str(con_tmp_final).strip() + ")"

                ## 维度
                for m in topic_type:
                    topic_condition += str(m) + "," + """\n"""

                ## 时间维度
                if topic_date_type == 'period':
                    topic_date_range = ''
                elif topic_date_type == 'day':
                    topic_date_range = 'p_date,'
                else:
                    topic_date_range = 'p_date,'

                if len(sql_topic_tmp) > 0:
                    sql_topic_tmp += """
                        UNION

                        SELECT device_id,
                               mid
                        FROM 
                        (SELECT {}
                               device_id,
                               mid,
                               tid,
                               tname,
                               --话题内消费行为
                               SUM(total_detail_topic) total_detail_topic,
                               SUM(total_detail_topic_dur)/60 AS total_detail_topic_dur,
                               SUM(hot_detail_topic) AS hot_detail_topic,
                               SUM(hot_detail_topic_dur)/60 AS hot_detail_topic_dur,
                               SUM(new_detail_topic) AS new_detail_topic,
                               SUM(new_detail_topic_dur)/60 AS new_detail_topic_dur,
                               SUM(expose_topic) AS expose_topic,
                               SUM(real_expose_topic) AS real_expose_topic,
                               SUM(score_topic) AS score_topic,
                               SUM(detail_post_topic) AS detail_post_topic,
                               SUM(create_post_topic) AS create_post_topic,
                               SUM(like_post_topic) AS like_post_topic,
                               SUM(share_post_topic) AS share_post_topic,
                               SUM(favor_post_topic) AS favor_post_topic,
                               SUM(create_review_topic) AS create_review_topic,
                               SUM(reply_review_topic) AS reply_review_topic,
                               SUM(create_review_topic+reply_review_topic) AS total_review_topic,
                               SUM(like_review_topic) AS like_review_topic,
                               SUM(like_create_review_topic) AS like_create_review_topic,
                               SUM(like_reply_review_topic) AS like_reply_review_topic
                        FROM {}
                        WHERE p_date BETWEEN '{}' AND '{}'
                        GROUP BY {}
                               device_id,
                               mid,
                               tid,
                               tname)
                        {}
                        GROUP BY device_id, mid
                    """.format(str(topic_date_range), str(self.topic_table), str(topic_start_date), str(topic_end_date),
                               str(topic_date_range), str(topic_condition_sel))
                else:
                    sql_topic_tmp += """
                                            SELECT device_id,
                                                   mid
                                            FROM 
                                            (SELECT {}
                                                   device_id,
                                                   mid,
                                                   tid,
                                                   tname,
                                                   --话题内消费行为
                                                   SUM(total_detail_topic) total_detail_topic,
                                                   SUM(total_detail_topic_dur)/60 AS total_detail_topic_dur,
                                                   SUM(hot_detail_topic) AS hot_detail_topic,
                                                   SUM(hot_detail_topic_dur)/60 AS hot_detail_topic_dur,
                                                   SUM(new_detail_topic) AS new_detail_topic,
                                                   SUM(new_detail_topic_dur)/60 AS new_detail_topic_dur,
                                                   SUM(expose_topic) AS expose_topic,
                                                   SUM(real_expose_topic) AS real_expose_topic,
                                                   SUM(score_topic) AS score_topic,
                                                   SUM(detail_post_topic) AS detail_post_topic,
                                                   SUM(create_post_topic) AS create_post_topic,
                                                   SUM(like_post_topic) AS like_post_topic,
                                                   SUM(share_post_topic) AS share_post_topic,
                                                   SUM(favor_post_topic) AS favor_post_topic,
                                                   SUM(create_review_topic) AS create_review_topic,
                                                   SUM(reply_review_topic) AS reply_review_topic,
                                                   SUM(create_review_topic+reply_review_topic) AS total_review_topic,
                                                   SUM(like_review_topic) AS like_review_topic,
                                                   SUM(like_create_review_topic) AS like_create_review_topic,
                                                   SUM(like_reply_review_topic) AS like_reply_review_topic
                                            FROM {}
                                            WHERE p_date BETWEEN '{}' AND '{}'
                                            GROUP BY {}
                                                   device_id,
                                                   mid,
                                                   tid,
                                                   tname)
                                            {}
                                            GROUP BY device_id, mid
                                        """.format(str(topic_date_range), str(self.topic_table), str(topic_start_date),
                                                   str(topic_end_date), str(topic_date_range), str(topic_condition_sel))
            sql_topic = """
            -- 话题消费数据
            FULL OUTER JOIN
            (
            """ + sql_topic_tmp + """\n""" \
                        + """) topic_consume
            on base.device_id = topic_consume.device_id and base.mid = topic_consume.mid
            """
        else:
            sql_topic = ""

        ## 埋点消费维度
        if self.actionlog_consume:
            sql_actionlog_consume_tmp = ""
            for v in self.actionlog_consume:
                actionlog_condition = ""
                pv_condition = ""
                dur_condition = ""
                actionlog_consume_date_type = ""
                actionlog_consume_start_date = str(self.start_date)
                actionlog_consume_end_date = str(self.end_date)

                ## 读取填写参数拼接SQL
                for key, value in v.items():
                    if key == "show_keys":
                        continue
                    else:
                        if key == 'type' and '%' in value.replace('$old$', '%'):
                            if len(actionlog_condition) > 0:
                                actionlog_condition += " AND type like '" + str(
                                    value.replace('$old$', '%')).strip() + "'"
                            else:
                                actionlog_condition += " type like '" + str(value.replace('$old$', '%')).strip() + "'"
                        elif key == 'type' and '%' not in value.replace('$old$', '%'):
                            if len(actionlog_condition) > 0:
                                actionlog_condition += " AND type = '" + str(value.replace('$old$', '%')).strip() + "'"
                            else:
                                actionlog_condition += " type = '" + str(value.replace('$old$', '%')).strip() + "'"
                        elif key == 'stype' and '%' in value.replace('$old$', '%'):
                            if len(actionlog_condition) > 0:
                                actionlog_condition += " AND stype like '" + str(
                                    value.replace('$old$', '%')).strip() + "'"
                            else:
                                actionlog_condition += " stype like '" + str(value.replace('$old$', '%')).strip() + "'"
                        elif key == 'stype' and '%' not in value.replace('$old$', '%'):
                            if len(actionlog_condition) > 0:
                                actionlog_condition += " AND stype = '" + str(value.replace('$old$', '%')).strip() + "'"
                            else:
                                actionlog_condition += " stype = '" + str(value.replace('$old$', '%')).strip() + "'"
                        elif key == 'frominfo' and '%' in value.replace('$old$', '%') and len(
                                value.replace('$old$', '%').strip()) > 0:
                            if len(actionlog_condition) > 0:
                                actionlog_condition += " AND frominfo like '" + str(
                                    value.replace('$old$', '%')).strip() + "'"
                            else:
                                actionlog_condition += " frominfo like '" + str(value.replace('$old$', '%')) + "'"
                        elif key == 'frominfo' and '%' not in value.replace('$old$', '%') and len(
                                value.replace('$old$', '%').strip()) > 0:
                            if len(actionlog_condition) > 0:
                                actionlog_condition += " AND frominfo = '" + str(
                                    value.replace('$old$', '%')).strip() + "'"
                            else:
                                actionlog_condition += " frominfo = '" + str(value.replace('$old$', '%')).strip() + "'"
                        elif key == 'pv' and '~' in value:
                            pv_condition += " BETWEEN " + str(value.split('~')[0]).strip() + " AND " + str(
                                value.split('~')[1]).strip()
                        elif key == 'pv' and ',' in value:
                            pv_len = len(value.split(','))
                            for i in range(pv_len):
                                if i == 0:
                                    pv_condition += " IN (" + str(value.split(',')[i]).strip()
                                else:
                                    pv_condition += " ," + str(value.split(',')[i]).strip()
                            pv_condition += ")"
                        elif key == 'pv' and '~' not in value and ',' not in value:
                            pv_condition += " = " + str(value).strip()
                        elif key == 'dur' and '~' in value:
                            dur_condition += " BETWEEN " + str(value.split('~')[0]).strip() + " AND " + str(
                                value.split('~')[1]).strip()
                        elif key == 'dur' and ',' in value:
                            dur_len = len(value.split(','))
                            for i in range(dur_len):
                                if i == 0:
                                    dur_condition += " IN (" + str(value.split(',')[i]).strip()
                                else:
                                    dur_condition += " ," + str(value.split(',')[i]).strip()
                            dur_condition += ")"
                        elif key == 'dur' and '~' not in value and ',' not in value:
                            dur_condition += " = " + str(value)
                        elif key == 'actionlog_consume_start_date':
                            actionlog_consume_start_date = (datetime.datetime.fromtimestamp(int(value))).strftime(
                                "%Y-%m-%d")
                        elif key == 'actionlog_consume_end_date':
                            actionlog_consume_end_date = (datetime.datetime.fromtimestamp(int(value))).strftime(
                                "%Y-%m-%d")
                        elif key == 'actionlog_consume_date_type':
                            actionlog_consume_date_type = str(value)

                ## 时间范围
                if actionlog_consume_date_type == "period":
                    actionlog_consume_date_range_head = ""
                    actionlog_consume_date_range_tail = ""
                elif actionlog_consume_date_type == "day":
                    actionlog_consume_date_range_head = "CONCAT(SUBSTR(ym,1,4),'-',SUBSTR(ym,5,2),'-',day) AS p_date,"
                    actionlog_consume_date_range_tail = "CONCAT(SUBSTR(ym,1,4),'-',SUBSTR(ym,5,2),'-',day),"
                else:
                    actionlog_consume_date_range_head = "CONCAT(SUBSTR(ym,1,4),'-',SUBSTR(ym,5,2),'-',day) AS p_date,"
                    actionlog_consume_date_range_tail = "CONCAT(SUBSTR(ym,1,4),'-',SUBSTR(ym,5,2),'-',day),"

                if len(sql_actionlog_consume_tmp) > 0 and len(pv_condition) > 0:
                    sql_actionlog_consume_tmp += """
                    UNION

                    SELECT device_id,
                           mid
                   FROM 
                   (SELECT {}
                           GET_JSON_OBJECT(data,'$.did') AS device_id,
                           mid,
                           COUNT(1) AS pv
                    FROM {}
                    WHERE CONCAT(SUBSTR(ym,1,4),'-',SUBSTR(ym,5,2),'-',day) BETWEEN '{}' AND '{}'
                    AND {}
                    GROUP BY {} GET_JSON_OBJECT(data,'$.did'), mid)
                    WHERE pv {}
                    GROUP BY device_id, mid
                    """.format(str(actionlog_consume_date_range_head), str(self.actionlog_table),
                               str(actionlog_consume_start_date), str(actionlog_consume_end_date),
                               str(actionlog_condition), str(actionlog_consume_date_range_tail), str(pv_condition))
                elif len(pv_condition) > 0 and len(sql_actionlog_consume_tmp) == 0:
                    sql_actionlog_consume_tmp += """
                    SELECT device_id,
                           mid
                   FROM 
                   (SELECT {}
                           GET_JSON_OBJECT(data,'$.did') AS device_id,
                           mid,
                           COUNT(1) AS pv
                    FROM {}
                    WHERE CONCAT(SUBSTR(ym,1,4),'-',SUBSTR(ym,5,2),'-',day) BETWEEN '{}' AND '{}'
                    AND {}
                    GROUP BY {} GET_JSON_OBJECT(data,'$.did'), mid)
                    WHERE pv {}
                    GROUP BY device_id, mid
                    """.format(str(actionlog_consume_date_range_head), str(self.actionlog_table),
                               str(actionlog_consume_start_date), str(actionlog_consume_end_date),
                               str(actionlog_condition), str(actionlog_consume_date_range_tail), str(pv_condition))
            sql_actionlog_consume = """
                -- 埋点行为pv数据
                FULL OUTER JOIN 
                (
                """ \
                                    + sql_actionlog_consume_tmp + """\n""" \
                                    + """) actionlog_consume
                on base.device_id = actionlog_consume.device_id and base.mid = actionlog_consume.mid
                """
        else:
            sql_actionlog_consume = ""

        ## 发帖行为维度
        if self.send_post:
            sql_send_post_tmp = ""

            if self.app_name == 'zuiyou':
                ct_time = 'ct'
                media_num = """,nvl(MAX(size(split(imgids,', '))),0) AS media_num"""
                part_name = 'MAX(l1part_name) AS l1part_name, \
                             MAX(l2part_name) AS l2part_name, \
                             MAX(l1part_id) AS l1part_id, \
                             MAX(l2part_id) AS l2part_id'
            elif self.app_name == 'zuiyou_lite':
                ct_time = 'ct'
                media_num = """,nvl(MAX(size(split(imgids,', '))),0) AS media_num"""
                part_name = 'MAX(l1part_name) AS l1part_name, \
                             MAX(l2part_name) AS l2part_name, \
                             MAX(l1part_id) AS l1part_id, \
                             MAX(l2part_id) AS l2part_id'
            elif self.app_name == 'maga':
                ct_time = 'cast(ct-12*3600 as bigint)'
                media_num = """,nvl(MAX(size(split(imgids,', '))),0) AS media_num"""
                part_name = 'MAX(l1part_name) AS l1part_name, \
                             MAX(l2part_name) AS l2part_name, \
                             MAX(l1part_id) AS l1part_id, \
                             MAX(l2part_id) AS l2part_id'
            elif self.app_name == 'omg':
                ct_time = 'ct'
                media_num = ',NVL(MAX(img_cnt),0) AS media_num'
                part_name = 'MAX(part1_name) AS l1part_name, \
                             MAX(part2_name) AS l2part_name, \
                             MAX(part1_id) AS l1part_id, \
                             MAX(part2_id) AS l2part_id'

            for v in self.send_post:
                send_post_type = []
                send_post_condition_sel = ""
                send_post_condition = ""
                send_post_date_type = ""
                send_post_start_date = str(self.start_date)
                send_post_end_date = str(self.end_date)

                ## 参数读取
                for key, value in v.items():
                    if key == "show_keys":
                        continue
                    else:
                        if key in (
                                'total_dur', 'media_num', 'tid', 'tname', 'ptype', 'l1part_name',
                                'l2part_name',
                                'l1part_id', 'l2part_id') and key not in send_post_type:
                            send_post_type.append(key)
                        if key == 'send_post_start_date':
                            send_post_start_date = (datetime.datetime.fromtimestamp(int(value))).strftime("%Y-%m-%d")
                        elif key == 'send_post_end_date':
                            send_post_end_date = (datetime.datetime.fromtimestamp(int(value))).strftime("%Y-%m-%d")
                        elif key == 'send_post_date_type':
                            send_post_date_type = str(value)
                        else:
                            if type(value) == unicode:
                                if '~' in value:
                                    if len(send_post_condition_sel) > 0:
                                        send_post_condition_sel += " AND " + str(key) + " BETWEEN " + value.split('~')[
                                            0].strip() + " AND " + value.split('~')[1].strip()
                                    else:
                                        send_post_condition_sel += "WHERE " + str(key) + " BETWEEN " + value.split('~')[
                                            0].strip() + " AND " + \
                                                                   value.split('~')[1].strip()
                                elif ',' in value:
                                    condition_len = len(value.split(','))
                                    condition_tmp = ""
                                    for i in range(condition_len):
                                        if i == 0:
                                            condition_tmp += str(key) + " IN ( " + "'" + str(
                                                value.split(',')[i]).strip() + "'"
                                        else:
                                            condition_tmp += ",'" + str(value.split(',')[i]).strip() + "'"
                                    if len(send_post_condition_sel) > 0:
                                        send_post_condition_sel += " AND " + condition_tmp.strip() + ") "
                                    else:
                                        send_post_condition_sel += "WHERE " + condition_tmp.strip() + ") "
                                else:
                                    if len(send_post_condition_sel) > 0:
                                        send_post_condition_sel += " AND " + str(key) + " IN ('" + str(
                                            value).strip() + "')"
                                    else:
                                        send_post_condition_sel += "WHERE " + str(key) + " IN ('" + str(value).strip() + "')"
                            elif type(value) == list:
                                con_tmp = ''
                                for con in value:
                                    con_tmp += "'" + str(con).strip() + "',"
                                con_tmp_final = con_tmp.strip(',')
                                if len(send_post_condition_sel) > 0:
                                    send_post_condition_sel += " AND " + str(key) + " IN (" + str(
                                        con_tmp_final).strip() + ")"
                                else:
                                    send_post_condition_sel += "WHERE " + str(key) + " IN (" + str(
                                        con_tmp_final).strip() + ")"

                ## 发帖维度
                for m in send_post_type:
                    send_post_condition += str(m).strip() + "," + """\n"""

                ## 时间类型
                if send_post_date_type == 'day':
                    send_post_date_range = 'p_date,'
                elif send_post_date_type == 'period':
                    send_post_date_range = ''
                else:
                    send_post_date_range = 'p_date,'

                if len(sql_send_post_tmp) > 0:
                    sql_send_post_tmp += """
                    UNION

                    SELECT mid
                    FROM 
                    (SELECT {}
                            mid,
                            {}
                           SUM(post) AS post
                    FROM 
                    (SELECT t1.p_date,
                           t1.mid,
                           -- 帖子维度
                           CASE  WHEN t1.total_dur <=0 OR t1.total_dur IS NULL THEN 'other'
                                 WHEN t1.total_dur <=5 THEN '0-5'
                                 WHEN t1.total_dur <=15 THEN '6-15'
                                 WHEN t1.total_dur <=30 THEN '16-30'
                                 WHEN t1.total_dur <=60 THEN '31-60'
                                 WHEN t1.total_dur <=90 THEN '61-90'
                                 WHEN t1.total_dur <=120 THEN '91-120'
                                 WHEN t1.total_dur <=300 THEN '121-300'
                                 WHEN t1.total_dur >300 THEN '300+' 
                            END AS total_dur,
                            CASE WHEN media_num <= 9 THEN media_num ELSE 'other' END AS media_num,
                            nvl(t1.tid,0) AS tid,
                            tname,
                        CASE    WHEN ptype = 1 THEN '文字'
                                 WHEN ptype = 2 THEN '静图'
                                 WHEN ptype = 3 THEN '长静图'
                                 WHEN ptype = 4 THEN '动图'
                                 WHEN ptype = 5 THEN '视频'
                                 WHEN ptype = 6 THEN '长文'
                                 WHEN ptype = 7 THEN '音频' 
                                 ELSE 'other' END AS ptype,
                         l1part_name,
                         l2part_name,
                         l1part_id,
                         l2part_id,
                         COUNT(DISTINCT t1.pid) AS post
                    FROM 
                    (
                        SELECT  pid
                                ,min(ct) AS ct
                                ,max(ptype) AS ptype
                                ,max(tid) AS tid
                                ,min(SUBSTR(FROM_UNIXTIME({}),1,10)) AS p_date
                                {}
                                ,max(nvl(vdur,0)) AS total_dur
                                ,max(mid) as mid
                        FROM    {}
                        WHERE SUBSTR(FROM_UNIXTIME({}),1,10) BETWEEN '{}' AND '{}'
                        GROUP BY pid
                    ) t1 
                    -- 分区维度
                    LEFT JOIN 
                    (
                        SELECT tid,
                               {}
                        FROM {}
                        GROUP BY tid 
                    ) t2 
                    ON t1.tid = t2.tid
                    -- 话题维度
                    LEFT JOIN 
                    (
                        SELECT tid,
                               MAX(tname) AS tname 
                        FROM {}
                        GROUP BY tid
                    ) t3 
                    ON t1.tid = t3.tid
                    GROUP BY t1.p_date,
                           t1.mid,
                           -- 帖子维度
                           CASE  WHEN t1.total_dur <=0 OR t1.total_dur IS NULL THEN 'other'
                                 WHEN t1.total_dur <=5 THEN '0-5'
                                 WHEN t1.total_dur <=15 THEN '6-15'
                                 WHEN t1.total_dur <=30 THEN '16-30'
                                 WHEN t1.total_dur <=60 THEN '31-60'
                                 WHEN t1.total_dur <=90 THEN '61-90'
                                 WHEN t1.total_dur <=120 THEN '91-120'
                                 WHEN t1.total_dur <=300 THEN '121-300'
                                 WHEN t1.total_dur >300 THEN '300+' 
                            END,
                            CASE WHEN media_num <= 9 THEN media_num ELSE 'other' END,
                            nvl(t1.tid,0),
                            tname,
                        CASE    WHEN ptype = 1 THEN '文字'
                                 WHEN ptype = 2 THEN '静图'
                                 WHEN ptype = 3 THEN '长静图'
                                 WHEN ptype = 4 THEN '动图'
                                 WHEN ptype = 5 THEN '视频'
                                 WHEN ptype = 6 THEN '长文'
                                 WHEN ptype = 7 THEN '音频' 
                                 ELSE 'other' END,
                         l1part_name,
                         l2part_name,
                         l1part_id,
                         l2part_id)
                         GROUP BY {}
                                  {}
                                  mid
                                     )
                         {}
                         GROUP BY mid
                    """.format(str(send_post_date_range), str(send_post_condition), str(ct_time), str(media_num),(self.post_table),
                               str(ct_time),str(send_post_start_date), str(send_post_end_date), str(part_name),str(self.part_table),
                               str(self.tid_table), str(send_post_date_range), str(send_post_condition),str(send_post_condition_sel))
                else:
                    sql_send_post_tmp += """
                                        SELECT mid
                                        FROM 
                                        (SELECT {}
                                                mid,
                                                {}
                                               SUM(post) AS post
                                        FROM 
                                        (SELECT t1.p_date,
                                               t1.mid,
                                               -- 帖子维度
                                               CASE  WHEN t1.total_dur <=0 OR t1.total_dur IS NULL THEN 'other'
                                                     WHEN t1.total_dur <=5 THEN '0-5'
                                                     WHEN t1.total_dur <=15 THEN '6-15'
                                                     WHEN t1.total_dur <=30 THEN '16-30'
                                                     WHEN t1.total_dur <=60 THEN '31-60'
                                                     WHEN t1.total_dur <=90 THEN '61-90'
                                                     WHEN t1.total_dur <=120 THEN '91-120'
                                                     WHEN t1.total_dur <=300 THEN '121-300'
                                                     WHEN t1.total_dur >300 THEN '300+' 
                                                END AS total_dur,
                                                CASE WHEN media_num <= 9 THEN media_num ELSE 'other' END AS media_num,
                                                nvl(t1.tid,0) AS tid,
                                                tname,
                                            CASE    WHEN ptype = 1 THEN '文字'
                                                     WHEN ptype = 2 THEN '静图'
                                                     WHEN ptype = 3 THEN '长静图'
                                                     WHEN ptype = 4 THEN '动图'
                                                     WHEN ptype = 5 THEN '视频'
                                                     WHEN ptype = 6 THEN '长文'
                                                     WHEN ptype = 7 THEN '音频' 
                                                     ELSE 'other' END AS ptype,
                                             l1part_name,
                                             l2part_name,
                                             l1part_id,
                                             l2part_id,
                                             COUNT(DISTINCT t1.pid) AS post
                                        FROM 
                                        (
                                            SELECT  pid
                                                    ,min(ct) AS ct
                                                    ,max(ptype) AS ptype
                                                    ,max(tid) AS tid
                                                    ,min(SUBSTR(FROM_UNIXTIME({}),1,10)) AS p_date
                                                    {}
                                                    ,max(nvl(vdur,0)) AS total_dur
                                                    ,max(mid) as mid
                                            FROM    {}
                                            WHERE SUBSTR(FROM_UNIXTIME({}),1,10) BETWEEN '{}' AND '{}'
                                            GROUP BY pid
                                        ) t1 
                                        -- 分区维度
                                        LEFT JOIN 
                                        (
                                            SELECT tid,
                                                   {}
                                            FROM {}
                                            GROUP BY tid 
                                        ) t2 
                                        ON t1.tid = t2.tid
                                        -- 话题维度
                                        LEFT JOIN 
                                        (
                                            SELECT tid,
                                                   MAX(tname) AS tname 
                                            FROM {}
                                            GROUP BY tid
                                        ) t3 
                                        ON t1.tid = t3.tid
                                        GROUP BY t1.p_date,
                                                   t1.mid,
                                                   -- 帖子维度
                                                   CASE  WHEN t1.total_dur <=0 OR t1.total_dur IS NULL THEN 'other'
                                                         WHEN t1.total_dur <=5 THEN '0-5'
                                                         WHEN t1.total_dur <=15 THEN '6-15'
                                                         WHEN t1.total_dur <=30 THEN '16-30'
                                                         WHEN t1.total_dur <=60 THEN '31-60'
                                                         WHEN t1.total_dur <=90 THEN '61-90'
                                                         WHEN t1.total_dur <=120 THEN '91-120'
                                                         WHEN t1.total_dur <=300 THEN '121-300'
                                                         WHEN t1.total_dur >300 THEN '300+' 
                                                    END,
                                                    CASE WHEN media_num <= 9 THEN media_num ELSE 'other' END,
                                                    nvl(t1.tid,0),
                                                    tname,
                                                CASE    WHEN ptype = 1 THEN '文字'
                                                         WHEN ptype = 2 THEN '静图'
                                                         WHEN ptype = 3 THEN '长静图'
                                                         WHEN ptype = 4 THEN '动图'
                                                         WHEN ptype = 5 THEN '视频'
                                                         WHEN ptype = 6 THEN '长文'
                                                         WHEN ptype = 7 THEN '音频' 
                                                         ELSE 'other' END,
                                                 l1part_name,
                                                 l2part_name,
                                                 l1part_id,
                                                 l2part_id)
                                             GROUP BY {}
                                                      {}
                                                      mid
                                                      )
                                             {}
                                             GROUP BY mid
                                        """.format(str(send_post_date_range), str(send_post_condition),str(ct_time), str(media_num), (self.post_table), str(ct_time),
                                                   str(send_post_start_date),str(send_post_end_date),str(part_name),str(self.part_table),str(self.tid_table), str(send_post_date_range),
                                                   str(send_post_condition), str(send_post_condition_sel))
            sql_send_post = """
                -- 发帖行为数据
                FULL OUTER JOIN 
                (
                """ \
                            + sql_send_post_tmp + """\n""" \
                            + """) send_post
                on base.mid = send_post.mid
                """
        else:
            sql_send_post = ""

        ## 发评论行为维度
        if self.send_comment:
            sql_send_comment_tmp = ""

            if self.app_name == 'zuiyou':
                ct_time = 'ct'
            elif self.app_name == 'zuiyou_lite':
                ct_time = 'ct'
            elif self.app_name == 'maga':
                ct_time = 'cast(ct-12*3600 as bigint)'
            elif self.app_name == 'omg':
                ct_time = 'ct'

            for v in self.send_comment:
                send_comment_type = []
                send_comment_condition_sel = ""
                send_comment_condition = ""
                send_comment_date_type = ""
                send_comment_start_date = str(self.start_date)
                send_comment_end_date = str(self.end_date)

                ## 参数读取
                for key, value in v.items():
                    if key == "show_keys":
                        continue
                    else:
                        if key in ('rtype', 'ctype') and key not in send_comment_type:
                            send_comment_type.append(key)
                        if key == 'send_comment_start_date':
                            send_comment_start_date = (datetime.datetime.fromtimestamp(int(value))).strftime("%Y-%m-%d")
                        elif key == 'send_comment_end_date':
                            send_comment_end_date = (datetime.datetime.fromtimestamp(int(value))).strftime("%Y-%m-%d")
                        elif key == 'send_comment_date_type':
                            send_comment_date_type = str(value)
                        else:
                            if type(value) == unicode:
                                if '~' in value:
                                    if len(send_comment_condition_sel) > 0:
                                        send_comment_condition_sel += " AND " + str(key) + " BETWEEN " + \
                                                                      value.split('~')[
                                                                          0].strip() + " AND " + value.split('~')[
                                                                          1].strip()
                                    else:
                                        send_comment_condition_sel += "WHERE " + str(key) + " BETWEEN " + value.split('~')[
                                            0].strip() + " AND " + \
                                                                      value.split('~')[1].strip()
                                elif ',' in value:
                                    condition_len = len(value.split(','))
                                    condition_tmp = ""
                                    for i in range(condition_len):
                                        if i == 0:
                                            condition_tmp += str(key) + " IN ( " + "'" + str(
                                                value.split(',')[i]).strip() + "'"
                                        else:
                                            condition_tmp += ",'" + str(value.split(',')[i]).strip() + "'"
                                    if len(send_comment_condition_sel) > 0:
                                        send_comment_condition_sel += " AND " + condition_tmp + ") "
                                    else:
                                        send_comment_condition_sel += "WHERE " + condition_tmp + ") "
                                else:
                                    if len(send_comment_condition_sel) > 0:
                                        send_comment_condition_sel += " AND " + str(key) + " IN ('" + str(
                                            value).strip() + "')"
                                    else:
                                        send_comment_condition_sel += "WHERE " + str(key) + " IN ('" + str(
                                            value).strip() + "')"
                            elif type(value) == list:
                                con_tmp = ''
                                for con in value:
                                    con_tmp += "'" + str(con).strip() + "',"
                                con_tmp_final = con_tmp.strip(',')
                                if len(send_comment_condition_sel) > 0:
                                    send_comment_condition_sel += " AND " + str(key) + " IN (" + str(
                                        con_tmp_final).strip() + ")"
                                else:
                                    send_comment_condition_sel += "WHERE " + str(key) + " IN (" + str(
                                        con_tmp_final).strip() + ")"
                ## 维度
                for m in send_comment_type:
                    send_comment_condition += str(m).strip() + "," + """\n"""

                ## 时间范围
                if send_comment_date_type == 'day':
                    send_comment_date_range = 'p_date,'
                elif send_comment_date_type == 'period':
                    send_comment_date_range = ''
                else:
                    send_comment_date_range = 'p_date,'

                if len(sql_send_comment_tmp) > 0:
                    sql_send_comment_tmp += """
                    UNION

                    SELECT mid
                    FROM 
                    (SELECT {}
                            mid,
                            {}
                           SUM(review) AS review
                    FROM 
                    (
                        SELECT SUBSTR(FROM_UNIXTIME({}),1,10) AS p_date,
                               CASE WHEN rtype = 1 THEN '文字'
                                    WHEN rtype = 2 THEN '语音'
                                    WHEN rtype = 3 THEN '图片'
                                    WHEN rtype = 4 THEN '视频'
                                    ELSE '未知' END AS rtype,
                                CASE WHEN ctype = 1 THEN '一级评论'
                                     WHEN ctype = 2 THEN '二级评论'
                                     ELSE '未知' END AS ctype,
                                mid,
                                COUNT(DISTINCT rid) AS review
                        FROM {}
                        WHERE SUBSTR(FROM_UNIXTIME({}),1,10) BETWEEN '{}' AND '{}'
                        GROUP BY SUBSTR(FROM_UNIXTIME({}),1,10),
                                 CASE WHEN rtype = 1 THEN '文字'
                                      WHEN rtype = 2 THEN '语音'
                                      WHEN rtype = 3 THEN '图片'
                                      WHEN rtype = 4 THEN '视频'
                                      ELSE '未知' END,
                                 CASE WHEN ctype = 1 THEN '一级评论'
                                      WHEN ctype = 2 THEN '二级评论'
                                      ELSE '未知' END,
                                 mid
                    )
                    GROUP BY {}
                             {}
                             mid)
                    {}
                    GROUP BY mid
                    """.format(str(send_comment_date_range), str(send_comment_condition), str(ct_time), str(self.comment_table),str(ct_time),
                               str(send_comment_start_date), str(send_comment_end_date),str(ct_time),str(send_comment_date_range), str(send_comment_condition),str(send_comment_condition_sel))
                else:
                    sql_send_comment_tmp += """
                                        SELECT mid
                                        FROM 
                                        (SELECT {}
                                                mid,
                                                {}
                                               SUM(review) AS review
                                        FROM 
                                        (
                                            SELECT SUBSTR(FROM_UNIXTIME({}),1,10) AS p_date,
                                                   CASE WHEN rtype = 1 THEN '文字'
                                                        WHEN rtype = 2 THEN '语音'
                                                        WHEN rtype = 3 THEN '图片'
                                                        WHEN rtype = 4 THEN '视频'
                                                        ELSE '未知' END AS rtype,
                                                    CASE WHEN ctype = 1 THEN '一级评论'
                                                         WHEN ctype = 2 THEN '二级评论'
                                                         ELSE '未知' END AS ctype,
                                                    mid,
                                                    COUNT(DISTINCT rid) AS review
                                            FROM {}
                                            WHERE SUBSTR(FROM_UNIXTIME({}),1,10) BETWEEN '{}' AND '{}'
                                            GROUP BY SUBSTR(FROM_UNIXTIME({}),1,10),
                                                     CASE WHEN rtype = 1 THEN '文字'
                                                          WHEN rtype = 2 THEN '语音'
                                                          WHEN rtype = 3 THEN '图片'
                                                          WHEN rtype = 4 THEN '视频'
                                                          ELSE '未知' END,
                                                     CASE WHEN ctype = 1 THEN '一级评论'
                                                          WHEN ctype = 2 THEN '二级评论'
                                                          ELSE '未知' END,
                                                     mid
                                        )
                                        GROUP BY {}
                                                 {}
                                                 mid)
                                        {}
                                        GROUP BY mid
                                        """.format(str(send_comment_date_range), str(send_comment_condition),str(ct_time), str(self.comment_table),str(ct_time), str(send_comment_start_date),
                                                   str(send_comment_end_date),str(ct_time),str(send_comment_date_range), str(send_comment_condition),str(send_comment_condition_sel))
            sql_send_comment = """
                            -- 发评论行为数据
                            FULL OUTER JOIN 
                            (
                            """ \
                               + sql_send_comment_tmp + """\n""" \
                               + """) send_comment
                            on base.mid = send_comment.mid
                            """
        else:
            sql_send_comment = ""

        ## 关注维度
        if self.attention:
            sql_attention_tmp = ""
            for v in self.attention:
                attention_condition_sel_tid = ""
                attention_condition_sel_mid = ""
                attention_tid_start_date = ""
                attention_tid_end_date = ""
                attention_mid_start_date = ""
                attention_mid_end_date = ""
                if int(datetime.datetime.now().strftime('%H')) >= 0 and int(
                        datetime.datetime.now().strftime('%H')) <= 7:
                    date = (datetime.datetime.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
                else:
                    date = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

                ## 参数读取
                for key, value in v.items():
                    if key == "show_keys":
                        continue
                    else:
                        if key == 'attention_tid_start_date':
                            attention_tid_start_date = (datetime.datetime.fromtimestamp(int(value))).strftime(
                                "%Y-%m-%d")
                        elif key == 'attention_tid_end_date':
                            attention_tid_end_date = (datetime.datetime.fromtimestamp(int(value))).strftime("%Y-%m-%d")
                        elif key == 'attention_mid_start_date':
                            attention_mid_start_date = (datetime.datetime.fromtimestamp(int(value))).strftime(
                                "%Y-%m-%d")
                        elif key == 'attention_mid_end_date':
                            attention_mid_end_date = (datetime.datetime.fromtimestamp(int(value))).strftime("%Y-%m-%d")
                        elif key == 'attention_tid':
                            if ',' in str(value):
                                condition_len = len(str(value).split(','))
                                condition_tmp = ""
                                for i in range(condition_len):
                                    if i == 0:
                                        condition_tmp += " IN ( " + "'" + str(value.split(',')[i]).strip() + "'"
                                    else:
                                        condition_tmp += ",'" + str(value.split(',')[i]).strip() + "'"
                                if self.app_name == 'zuiyou':
                                    attention_condition_sel_tid += " AND tid " + condition_tmp + " ) "
                                elif self.app_name == 'zuiyou_lite':
                                    attention_condition_sel_tid += " AND topic_id " + condition_tmp + " ) "
                            else:
                                if self.app_name == 'zuiyou':
                                    attention_condition_sel_tid += " AND tid IN ('" + str(value).strip() + "')"
                                elif self.app_name == 'zuiyou_lite':
                                    attention_condition_sel_tid += " AND topic_id IN ('" + str(value).strip() + "')"
                        elif key == 'attention_mid':
                            if ',' in str(value):
                                condition_len = len(str(value).split(','))
                                condition_tmp = ""
                                for i in range(condition_len):
                                    if i == 0:
                                        condition_tmp += " IN ( " + "'" + str(value.split(',')[i]).strip() + "'"
                                    else:
                                        condition_tmp += ",'" + str(value.split(',')[i]).strip() + "'"
                                if self.app_name == 'zuiyou':
                                    attention_condition_sel_mid += " AND aid " + condition_tmp + " ) "
                                elif self.app_name == 'zuiyou_lite':
                                    attention_condition_sel_mid += " AND mid " + condition_tmp + " ) "
                            else:
                                if self.app_name == 'zuiyou':
                                    attention_condition_sel_mid += " AND aid IN ('" + str(value).strip() + "')"
                                elif self.app_name == 'zuiyou_lite':
                                    attention_condition_sel_mid += " AND mid IN ('" + str(value).strip() + "')"

                if len(attention_tid_start_date) > 0 and len(attention_tid_end_date) > 0:
                    attention_tid_date_range = " AND SUBSTR(FROM_UNIXTIME(ct),1,10) BETWEEN '" + str(
                        attention_tid_start_date) + "' AND '" + str(attention_tid_end_date) + "'"
                elif len(attention_tid_start_date) > 0 and len(attention_tid_end_date) == 0:
                    attention_tid_date_range = " AND SUBSTR(FROM_UNIXTIME(ct),1,10) = '" + str(
                        attention_tid_start_date) + "'"
                else:
                    attention_tid_date_range = ""

                if self.app_name == 'zuiyou_lite':
                    if len(attention_mid_start_date) > 0 and len(attention_mid_end_date) > 0:
                        attention_mid_date_range = " AND SUBSTR(FROM_UNIXTIME(ct),1,10) BETWEEN '" + str(
                            attention_mid_start_date) + "' AND '" + str(attention_mid_end_date) + "'"
                    elif len(attention_mid_start_date) > 0 and len(attention_mid_end_date) == 0:
                        attention_mid_date_range = " AND SUBSTR(FROM_UNIXTIME(ct),1,10) = '" + str(
                            attention_mid_start_date) + "'"
                    else:
                        attention_mid_date_range = ""
                elif self.app_name == 'zuiyou':
                    if len(attention_mid_start_date) > 0 and len(attention_mid_end_date) > 0:
                        attention_mid_date_range = " AND SUBSTR(FROM_UNIXTIME(aut),1,10) BETWEEN '" + str(
                            attention_mid_start_date) + "' AND '" + str(attention_mid_end_date) + "'"
                    elif len(attention_mid_start_date) > 0 and len(attention_mid_end_date) == 0:
                        attention_mid_date_range = " AND SUBSTR(FROM_UNIXTIME(aut),1,10) = '" + str(
                            attention_mid_start_date) + "'"
                    else:
                        attention_mid_date_range = ""
                else:
                    attention_mid_date_range = ""

                if len(attention_condition_sel_tid) > 0 or len(attention_tid_date_range) > 0:
                    sql_tid = """
                    SELECT mid
                    FROM {}
                    WHERE CONCAT(SUBSTR(ymd,1,4),'-',SUBSTR(ymd,5,2),'-',SUBSTR(ymd,7,2)) = '{}'
                    {}
                    {}
                    GROUP BY mid
                    """.format(str(self.attention_tid), str(date), str(attention_tid_date_range),
                               str(attention_condition_sel_tid))
                else:
                    sql_tid = ""

                if (len(attention_condition_sel_mid) > 0 or len(
                        attention_mid_date_range) > 0) and self.app_name == 'zuiyou':
                    sql_mid = """
                    SELECT mid
                    FROM {}
                    WHERE CONCAT(SUBSTR(ymd,1,4),'-',SUBSTR(ymd,5,2),'-',SUBSTR(ymd,7,2)) = '{}'
                    {}
                    {}
                    GROUP BY mid
                    """.format(str(self.attention_mid), str(date), str(attention_mid_date_range),
                               str(attention_condition_sel_mid))
                elif (len(attention_condition_sel_mid) > 0 or len(
                        attention_mid_date_range) > 0) and self.app_name == 'zuiyou_lite':
                    sql_mid = """
                    SELECT fid AS mid
                    FROM {}
                    WHERE CONCAT(SUBSTR(ymd,1,4),'-',SUBSTR(ymd,5,2),'-',SUBSTR(ymd,7,2)) = '{}'
                    {}
                    {}
                    GROUP BY fid
                    """.format(str(self.attention_mid), str(date), str(attention_mid_date_range),
                               str(attention_condition_sel_mid))
                else:
                    sql_mid = ""

                if len(sql_attention_tmp) > 0:
                    if len(sql_mid) > 0 and len(sql_tid) > 0:
                        sql_attention_tmp += """
                        UNION

                        {}

                        UNION

                        {}

                        """.format(sql_tid, sql_mid)
                    elif len(sql_tid) > 0 and len(sql_mid) == 0:
                        sql_attention_tmp += """
                        UNION

                        {}
                        """.format(sql_tid)
                    elif len(sql_mid) > 0 and len(sql_tid) == 0:
                        sql_attention_tmp += """
                        UNION

                        {}

                        """.format(sql_mid)
                    else:
                        continue
                else:
                    if len(sql_mid) > 0 and len(sql_tid) > 0:
                        sql_attention_tmp += """

                        {}

                        UNION

                        {}

                        """.format(sql_tid, sql_mid)
                    elif len(sql_tid) > 0 and len(sql_mid) == 0:
                        sql_attention_tmp += """

                        {}

                        """.format(sql_tid)
                    elif len(sql_mid) > 0 and len(sql_tid) == 0:
                        sql_attention_tmp += """

                        {}

                        """.format(sql_mid)
                    else:
                        continue
            sql_attention = """
            -- 关注行为数据
            FULL OUTER JOIN
            (
            """ + sql_attention_tmp + """\n""" \
                            + """) attention
            ON base.mid = attention.mid
            """
        else:
            sql_attention = ""

        ## 整体
        sql_final = sql_head + """\n""" \
                    + sql_user + """\n""" \
                    + sql_post_consume + """\n""" \
                    + sql_actionlog_consume + """\n""" \
                    + sql_topic + """\n""" \
                    + sql_send_post + """\n""" \
                    + sql_send_comment + """\n""" \
                    + sql_attention + """\n""" \
                    + """where """ + str(self.final_relation) + """\n""" \
                    + sql_tail + """\n"""
        return sql_final

    def get_data(self, sql):
        with self.db_odps.execute_sql(sql).open_reader() as reader:
            print(reader.count)
            record_list = list()
            for record in reader:
                record_list.append(dict(record))
        record_pd = pd.DataFrame(record_list)
        if record_pd.empty:
            record_id = record_pd
        else:
            record_id = pd.DataFrame(record_pd['mid'])
        id_num = len(record_pd)
        return record_pd, record_id, id_num

    def fail_update(self, task_id):
        filter = {"toolkit_type": "user", "_id": int(task_id)}
        newvalues = {"$set": {"status": -1, "ut": int(time.time())}}
        self.table.update_one(filter, newvalues)

    def update_mongo(self, task_id, size, upload_result):
        if upload_result:
            filter = {"toolkit_type": "user", "_id": int(task_id)}
            newvalues = {"$set": {"status": 2, "ut": int(time.time()), "size": int(size)}}
            self.table.update_one(filter, newvalues)
        else:
            filter = {"toolkit_type": "user", "_id": int(task_id)}
            newvalues = {"$set": {"status": -1, "ut": int(time.time())}}
            self.table.update_one(filter, newvalues)

    def upload_oss(self, result_detail, result_id):
        if self.app_name == 'zuiyou_lite':
            try:
                auth = oss2.Auth('xx', 'xx')
                bucket = oss2.Bucket(auth, 'xx', 'xx')
                oss_filename_detail = "rec-op/" + str(task_id) + "_detail.csv"
                oss_filename_id = "rec-op/" + str(task_id) + ".csv"
                bucket.put_object_from_file(oss_filename_detail, result_detail)
                bucket.put_object_from_file(oss_filename_id, result_id)
                headers = dict()
                headers['content-disposition'] = 'attachment'
                return True
            except Exception as e:
                return False
        elif self.app_name == 'zuiyou':
            try:
                auth = oss2.Auth('xx', 'xx')
                bucket = oss2.Bucket(auth, 'xx', 'xx')
                oss_filename_detail = "crowd/" + str(task_id) + "_detail.csv"
                oss_filename_id = "crowd/" + str(task_id) + ".csv"
                bucket.put_object_from_file(oss_filename_detail, result_detail)
                bucket.put_object_from_file(oss_filename_id, result_id)
                headers = dict()
                headers['content-disposition'] = 'attachment'
                return True
            except Exception as e:
                return False
        elif self.app_name == 'maga':
            try:
                auth = oss2.Auth('xx', 'xx')
                bucket = oss2.Bucket(auth, 'xx', 'xx')
                oss_filename_detail = "rec-debug-op/" + str(task_id) + "_detail.csv"
                oss_filename_id = "rec-debug-op/" + str(task_id) + ".csv"
                bucket.put_object_from_file(oss_filename_detail, result_detail)
                bucket.put_object_from_file(oss_filename_id, result_id)
                headers = dict()
                headers['content-disposition'] = 'attachment'
                return True
            except Exception as e:
                return False
        elif self.app_name == 'omg':
            try:
                auth = oss2.Auth('xx', 'xxx')
                bucket = oss2.Bucket(auth, 'xx', 'xxx')
                oss_filename_detail = "rec-op/" + str(task_id) + "_detail.csv"
                oss_filename_id = "rec-op/" + str(task_id) + ".csv"
                bucket.put_object_from_file(oss_filename_detail, result_detail)
                bucket.put_object_from_file(oss_filename_id, result_id)
                headers = dict()
                headers['content-disposition'] = 'attachment'
                return True
            except Exception as e:
                return False


if __name__ == '__main__':
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[%s]: start!" % (now))
    task_id = sys.argv[1]
    app_name = sys.argv[2]
    task = GetPeople(task_id,app_name)
    sql = task.get_sql()
    print(sql)
    try:
        data_detail, data_id, id_num = task.get_data(sql)
        result_detail = "/home/devs/tangyongjun/user/result/" + str(app_name) + "/final_result/" + str(task_id) + "_detail.csv"
        result_id = "/home/devs/tangyongjun/user/result/" + str(app_name) + "/final_result/" + str(task_id) + ".csv"
        data_detail.to_csv(result_detail, encoding='utf_8_sig', index=False)
        data_id.to_csv(result_id, encoding='utf_8_sig', index=False)
        print("业务名称:" + str(app_name) + " 任务id: " + str(task_id) + "," + " 结果地址: " + str(result_detail) + ", " + str(result_id))
        upload_result = task.upload_oss(result_detail, result_id)
        task.update_mongo(task_id, id_num, upload_result)
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("[%s]: finish!" % (now))
    except Exception as e:
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        task.fail_update(task_id)
        print("[%s]: fail!" % (now))
