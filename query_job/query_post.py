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


class GetPost(object):
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
            self.docs = self.table.find_one({"toolkit_type":"post","_id": int(task_id)})
        elif self.app_name == 'zuiyou':
            self.client = MongoClient("xxx")
            self.db_name = 'xx'
            self.table_name = 'xx'
            self.db = self.client.get_database(self.db_name)
            self.table = self.db.get_collection(self.table_name)
            self.docs = self.table.find_one({"toolkit_type":"post","_id": int(task_id)})
        elif self.app_name == 'omg':
            self.client = MongoClient("xxx")
            self.db_name = 'xx'
            self.table_name = 'xx'
            self.db = self.client.get_database(self.db_name)
            self.table = self.db.get_collection(self.table_name)
            self.docs = self.table.find_one({"toolkit_type":"post","_id": int(task_id)})
        elif self.app_name == 'maga':
            self.client = MongoClient("xxx")
            self.db_name = 'xx'
            self.table_name = 'xx'
            self.db = self.client.get_database(self.db_name)
            self.table = self.db.get_collection(self.table_name)
            self.docs = self.table.find_one({"toolkit_type":"post","_id": int(task_id)})

        ## odps
        if self.app_name in ('zuiyou', 'zuiyou_lite'):
            AccessKeyID = 'xx'
            AccessKeySecret = 'xx'
            self.db_odps = ODPS(AccessKeyID, AccessKeySecret, 'xx')
        elif self.app_name == 'omg':
            AccessKeyID = 'xx'
            AccessKeySecret = 'xx'
            self.db_odps = ODPS(AccessKeyID, AccessKeySecret, 'xx','xx')
        elif self.app_name == 'maga':
            AccessKeyID = 'xx'
            AccessKeySecret = 'xx'
            self.db_odps = ODPS(AccessKeyID, AccessKeySecret, 'xx','xxx')
        ## 参数提取
        self.params_list = self.docs.get('field_value', None)
        self.post_type = self.params_list.get('post_type', None)
        self.post_consume_type = self.params_list.get('post_consume_type', None)
        self.post_consume = self.params_list.get('post_consume', None)
        self.order_func = self.params_list.get('order_func', None)

        ## 数据表
        if self.app_name == 'zuiyou':
            self.dws_table = 'zy_bigdata.dws_post_pid_multi_di_zy'
            self.post_table = 'zy_bigdata.postmetadata'
            self.part_table = 'zuiyou_recommendation.topic_partition_map'
            self.tid_table = 'zy_bigdata.topicmetadata'
            self.user_table = 'zy_bigdata.usermetadata'
        elif self.app_name == 'zuiyou_lite':
            self.dws_table = 'pipi_bigdata.dws_post_pid_multi_di_pp'
            self.post_table = 'pipi_bigdata.pp_postmetadata'
            self.part_table = 'pipi_bigdata.pipi_topic_partition_map'
            self.tid_table = 'pipi_bigdata.pp_topicmetadata'
            self.user_table = 'pipi_bigdata.pp_usermetadata'
        elif self.app_name == 'omg':
            self.dws_table = 'omg_data.dws_post_pid_multi_di_omg'
            self.post_table = 'omg_data.omg_postmetadata'
            self.part_table = 'omg_data.omg_tid_partition_metadata'
            self.tid_table = 'omg_data.omg_topicmetadata'
            self.user_table = 'omg_data.omg_usermetadata'
        elif self.app_name == 'maga':
            self.dws_table = 'maga_bigdata.dws_post_pid_multi_di_maga'
            self.post_table = 'maga_bigdata.postmetadata_maga'
            self.part_table = 'maga_bigdata.topic_partition_map_maga'
            self.tid_table = 'maga_bigdata.topicmetadata_maga'
            self.user_table = 'maga_bigdata.usermetadata_maga'

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
            record_id = pd.DataFrame(record_pd['pid'])
        id_num = len(record_pd)
        return record_pd, record_id, id_num

    def get_sql(self):

        if self.app_name == 'zuiyou':
            age_group = """
            CASE WHEN age = 1 THEN '16-'
                 WHEN age = 2 THEN '16~18'
                 WHEN age = 3 THEN '19~22'
                 WHEN age = 4 THEN '22+'
                 ELSE '未知' END
            """
            tag_replace = ''
            tag_post = ''
            tag_final = ''
            media_num = """
            ,CASE WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 1 THEN '1'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 2 THEN '2'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 3 THEN '3'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 4 THEN '4'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 5 THEN '5'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 6 THEN '6'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 7 THEN '7'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 8 THEN '8'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 9 THEN '9'
                                      ELSE 'other' END AS media_num
            """
            ct = 'ct'
            part_name = 'MAX(l1part_name) AS l1part_name, \
                         MAX(l2part_name) AS l2part_name, \
                         MAX(l1part_id) AS l1part_id, \
                         MAX(l2part_id) AS l2part_id'
        elif self.app_name == 'zuiyou_lite':
            age_group = """
            CASE WHEN age = 1 THEN '17-'
                 WHEN age = 2 THEN '18~22'
                 WHEN age = 3 THEN '23~28'
                 WHEN age = 4 THEN '28+'
                 ELSE '未知' END
            """
            tag_replace = ",MAX(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(tag_ids,'[',''),']',''),' ',','),'100','美女'),'101','恐怖'),'103','普通ugc'),'104','优质ugc'),'502','恐怖提示'),'500','内涵'),'301','重点话题贴'),'20221018','运营热帖')) as tag"
            tag_post = "post.tag,"
            tag_final = "posttype.tag AS 帖子标签,"
            media_num = """
            ,CASE WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 1 THEN '1'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 2 THEN '2'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 3 THEN '3'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 4 THEN '4'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 5 THEN '5'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 6 THEN '6'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 7 THEN '7'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 8 THEN '8'
                                      WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 9 THEN '9'
                                      ELSE 'other' END AS media_num
            """
            ct = 'ct'
            part_name = 'MAX(l1part_name) AS l1part_name, \
                         MAX(l2part_name) AS l2part_name, \
                         MAX(l1part_id) AS l1part_id, \
                         MAX(l2part_id) AS l2part_id'
        elif self.app_name == 'omg':
            age_group = """
                        CASE WHEN age = 1 THEN '18-'
                             WHEN age = 2 THEN '18~24'
                             WHEN age = 3 THEN '25~30'
                             WHEN age = 4 THEN '30+'
                             ELSE '未知' END
                        """
            tag_replace = ''
            tag_post = ''
            tag_final = ''
            media_num = """
            ,CASE WHEN NVL(MAX(img_cnt),0) = 1 THEN '1'
                                      WHEN NVL(MAX(img_cnt),0) = 2 THEN '2'
                                      WHEN NVL(MAX(img_cnt),0) = 3 THEN '3'
                                      WHEN NVL(MAX(img_cnt),0) = 4 THEN '4'
                                      WHEN NVL(MAX(img_cnt),0) = 5 THEN '5'
                                      WHEN NVL(MAX(img_cnt),0) = 6 THEN '6'
                                      WHEN NVL(MAX(img_cnt),0) = 7 THEN '7'
                                      WHEN NVL(MAX(img_cnt),0) = 8 THEN '8'
                                      WHEN NVL(MAX(img_cnt),0) = 9 THEN '9'
                                      ELSE 'other' END AS media_num
            """
            ct = 'ct'
            part_name = 'MAX(part1_name) AS l1part_name, \
                         MAX(part2_name) AS l2part_name, \
                         MAX(part1_id) AS l1part_id, \
                         MAX(part2_id) AS l2part_id'
        elif self.app_name == 'maga':
            age_group = """
                        CASE WHEN age = 1 THEN '18-'
                             WHEN age = 2 THEN '19~34'
                             WHEN age = 3 THEN '35~60'
                             WHEN age = 4 THEN '60+'
                             WHEN age = 5 THEN '19~24'
                             WHEN age = 6 THEN '25~34'
                             WHEN age = 7 THEN '35~44'
                             WHEN age = 8 THEN '45~60'
                             ELSE '未知' END
                        """
            tag_replace = ''
            tag_post = ''
            tag_final = ''
            media_num = """
                        ,CASE WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 1 THEN '1'
                                                  WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 2 THEN '2'
                                                  WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 3 THEN '3'
                                                  WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 4 THEN '4'
                                                  WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 5 THEN '5'
                                                  WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 6 THEN '6'
                                                  WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 7 THEN '7'
                                                  WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 8 THEN '8'
                                                  WHEN NVL(MAX(SIZE(SPLIT(imgids,','))),0) = 9 THEN '9'
                                                  ELSE 'other' END AS media_num
                        """
            ct = 'ct - 12*3600'
            part_name = 'MAX(l1part_name) AS l1part_name, \
                         MAX(l2part_name) AS l2part_name, \
                         MAX(l1part_id) AS l1part_id, \
                         MAX(l2part_id) AS l2part_id'

        ## 帖子基础维度
        if self.post_type:
            ## 帖子基础特征
            post_type_condition = ''
            for v in self.post_type:
                post_type_condition_tmp = ''
                post_type_condition_dic = {}
                relation = v.get('relation', None)
                date_range = " BETWEEN '" + str(
                    (datetime.datetime.fromtimestamp(int(v.get('create_start_date', 0)))).strftime(
                        "%Y-%m-%d")) + "' AND '" + str(
                    (datetime.datetime.fromtimestamp(int(v.get('create_end_date', 0)))).strftime("%Y-%m-%d")) + "'"
                for key, value in v.items():
                    if key not in ('create_end_date', 'create_start_date', 'relation', 'show_keys','tag'):
                        if type(value) == unicode:
                            if ',' in value:
                                condition_len = len(value.split(','))
                                condition_tmp = ""
                                for i in range(condition_len):
                                    if i == 0:
                                        condition_tmp += str(key) + " IN ( " + "'" + str(value.split(',')[i]) + "'"
                                    else:
                                        condition_tmp += ",'" + str(value.split(',')[i]) + "'"
                                post_type_condition_dic[key] = str(condition_tmp) + ")"
                            elif '~' in value:
                                post_type_condition_dic[key] = str(key) + " BETWEEN " + str(value.split('~')[0]) + " AND " + str(value.split('~')[1])
                            else:
                                post_type_condition_dic[key] = str(key) + " IN ('" + str(value) + "')"
                        elif type(value) == list:
                            con_tmp = ''
                            for con in value:
                                con_tmp += "'" + str(con) + "',"
                            con_tmp_final = con_tmp.strip(',')
                            post_type_condition_dic[key] = str(key) + " IN (" + str(con_tmp_final).strip() + ")"
                    elif key == 'tag':
                        tag_len = len(value.split(','))
                        tag_tmp = ""
                        for i in range(tag_len):
                            if i == 0:
                                tag_tmp += "tag like " + "'%" + str(value.split(',')[i]) + "%' "
                            else:
                                tag_tmp += " OR tag like " + "'%" + str(value.split(',')[i]) + "%' "
                        post_type_condition_dic[key] = "(" + str(tag_tmp) + ")"
                    else:
                        continue

                if relation:
                    for i in relation:
                        if i in ['(', ')']:
                            post_type_condition_tmp += str(i)
                        elif i == '||':
                            post_type_condition_tmp += ' OR '
                        elif i == '&&':
                            post_type_condition_tmp += ' AND '
                        else:
                            post_type_condition_tmp += str(post_type_condition_dic[str(i)])
                else:
                    for i in post_type_condition_dic.keys():
                        if len(post_type_condition_tmp) > 0:
                            post_type_condition_tmp += ' AND ' + str(post_type_condition_dic[str(i)])
                        else:
                            post_type_condition_tmp += str(post_type_condition_dic[str(i)])

                if len(post_type_condition_tmp) > 0:
                    post_type_condition_final = " WHERE " + str(post_type_condition_tmp)
                else:
                    post_type_condition_final = ""

                if len(post_type_condition) > 0:
                    post_type_condition += """
                    UNION ALL 

                    SELECT post.pid,
                           post.create_date,
                           post.omid,
                           post.ptype,
                           post.content,
                           {}
                           nvl(topic.tname,'无') AS tname,
                           nvl(part.l1part_name,'无') AS l1part_name,
                           nvl(part.l2part_name,'无') AS l2part_name
                    FROM 
                    -- 帖子基础维度
                    (
                        SELECT  pid
                                ,min(ct) AS ct
                                ,CASE WHEN max(ptype) = 1 THEN '文字'
                                      WHEN max(ptype) = 2 THEN '静图'
                                      WHEN max(ptype) = 3 THEN '长静图'
                                      WHEN max(ptype) = 4 THEN '动图'
                                      WHEN max(ptype) = 5 THEN '视频'
                                      WHEN max(ptype) = 6 THEN '长文字'
                                      WHEN max(ptype) = 7 THEN '音频'
                                      ELSE '未知' END AS ptype
                                ,max(tid) AS tid
                                ,min(SUBSTR(FROM_UNIXTIME({}),1,10)) AS create_date
                                {}
                                ,CASE WHEN MAX(NVL(vdur,0)) BETWEEN 0 AND 5 THEN '0~5'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 6 AND 15 THEN '6~15'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 16 AND 30 THEN '16~30'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 31 AND 60 THEN '31~60'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 61 AND 90 THEN '61~90'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 91 AND 120 THEN '91~120'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 121 AND 300 THEN '121~300'
                                      WHEN MAX(NVL(vdur,0)) > 300 THEN '300+'
                                      ELSE 'other' END AS total_dur
                                ,max(content) AS content
                                ,max(nvl(LENGTH(content),0)) AS content_length
                                ,CASE WHEN max(status) = -4 THEN '自己删除' 
                                      WHEN max(status) = -3 THEN '不可见'
                                      WHEN max(status) = -2 THEN '自己可见'
                                      WHEN max(status) = -1 THEN '话题自见'
                                      WHEN max(status) = 1 THEN '可见'
                                      WHEN max(status) = 2 THEN '推荐'
                                      WHEN max(status) = 3 THEN '话题推荐'
                                      ELSE '未知' END AS status
                                ,max(mid) AS omid
                                {}
                        FROM    {}
                        WHERE SUBSTR(FROM_UNIXTIME({}),1,10) {}
                        GROUP BY pid
                    ) post
                    -- 发帖用户维度
                    LEFT JOIN 
                    (
                        SELECT mid,
                               MAX(CASE WHEN gender = 1 THEN '男' WHEN gender = 2 THEN '女' ELSE '未知' END) AS gender,
                               MAX({}) AS age
                        FROM {}
                        GROUP BY mid
                    ) user
                    on post.omid = user.mid
                    LEFT JOIN 
                    -- 话题维度
                    (
                        SELECT tid,
                               MAX(tname) AS tname
                        FROM {}
                        GROUP BY tid
                    ) topic 
                    ON post.tid = topic.tid
                    -- 分区维度
                    LEFT JOIN 
                    (
                        SELECT tid,
                               {}
                        FROM {}
                        GROUP BY tid  
                    ) part
                    ON post.tid = part.tid
                    {}
                    """.format(str(tag_post),str(ct), str(media_num),str(tag_replace),str(self.post_table), str(ct),str(date_range), str(age_group), str(self.user_table), str(self.tid_table),
                               str(part_name),str(self.part_table), str(post_type_condition_final).replace('tid','post.tid'))
                else:
                    post_type_condition += """
                    SELECT post.pid,
                           post.create_date,
                           post.omid,
                           post.ptype,
                           post.content,
                           {}
                           nvl(topic.tname,'无') AS tname,
                           nvl(part.l1part_name,'无') AS l1part_name,
                           nvl(part.l2part_name,'无') AS l2part_name
                    FROM 
                    -- 帖子基础维度
                    (
                        SELECT  pid
                                ,min(ct) AS ct
                                ,CASE WHEN max(ptype) = 1 THEN '文字'
                                      WHEN max(ptype) = 2 THEN '静图'
                                      WHEN max(ptype) = 3 THEN '长静图'
                                      WHEN max(ptype) = 4 THEN '动图'
                                      WHEN max(ptype) = 5 THEN '视频'
                                      WHEN max(ptype) = 6 THEN '长文字'
                                      WHEN max(ptype) = 7 THEN '音频'
                                      ELSE '未知' END AS ptype
                                ,max(tid) AS tid
                                ,min(SUBSTR(FROM_UNIXTIME({}),1,10)) AS create_date
                                {}
                                ,CASE WHEN MAX(NVL(vdur,0)) BETWEEN 0 AND 5 THEN '0~5'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 6 AND 15 THEN '6~15'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 16 AND 30 THEN '16~30'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 31 AND 60 THEN '31~60'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 61 AND 90 THEN '61~90'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 91 AND 120 THEN '91~120'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 121 AND 300 THEN '121~300'
                                      WHEN MAX(NVL(vdur,0)) > 300 THEN '300+'
                                      ELSE 'other' END AS total_dur
                                ,max(content) AS content
                                ,max(nvl(LENGTH(content),0)) AS content_length
                                ,CASE WHEN max(status) = -4 THEN '自己删除' 
                                      WHEN max(status) = -3 THEN '不可见'
                                      WHEN max(status) = -2 THEN '自己可见'
                                      WHEN max(status) = -1 THEN '话题自见'
                                      WHEN max(status) = 1 THEN '可见'
                                      WHEN max(status) = 2 THEN '推荐'
                                      WHEN max(status) = 3 THEN '话题推荐'
                                      ELSE '未知' END AS status
                                ,max(mid) AS omid
                                {}
                        FROM    {}
                        WHERE SUBSTR(FROM_UNIXTIME({}),1,10) {}
                        GROUP BY pid
                    ) post
                    -- 发帖用户维度
                    LEFT JOIN 
                    (
                        SELECT mid,
                               MAX(CASE WHEN gender = 1 THEN '男' WHEN gender = 2 THEN '女' ELSE '未知' END) AS gender,
                               MAX({}) AS age
                        FROM {}
                        GROUP BY mid
                    ) user
                    on post.omid = user.mid
                    LEFT JOIN 
                    -- 话题维度
                    (
                        SELECT tid,
                               MAX(tname) AS tname
                        FROM {}
                        GROUP BY tid
                    ) topic 
                    ON post.tid = topic.tid
                    -- 分区维度
                    LEFT JOIN 
                    (
                        SELECT tid,
                               {}
                        FROM {}
                        GROUP BY tid  
                    ) part
                    ON post.tid = part.tid
                    {}
                    """.format(str(tag_post),str(ct),str(media_num),str(tag_replace),str(self.post_table), str(ct),str(date_range), str(age_group), str(self.user_table), str(self.tid_table),
                               str(part_name),str(self.part_table), str(post_type_condition_final).replace('tid','post.tid'))

            post_type_sql = """
            -- 帖子基础特征数据
            (
            """ + post_type_condition + """\n""" \
                            + """) posttype
            """
        else:
            post_type_condition = """
                    SELECT post.pid,
                           post.create_date,
                           post.omid,
                           post.ptype,
                           post.content,
                           {}
                           nvl(topic.tname,'无') AS tname,
                           nvl(part.l1part_name,'无') AS l1part_name,
                           nvl(part.l2part_name,'无') AS l2part_name
                    FROM 
                    -- 帖子基础维度
                    (
                        SELECT  pid
                                ,min(ct) AS ct
                                ,CASE WHEN max(ptype) = 1 THEN '文字'
                                      WHEN max(ptype) = 2 THEN '静图'
                                      WHEN max(ptype) = 3 THEN '长静图'
                                      WHEN max(ptype) = 4 THEN '动图'
                                      WHEN max(ptype) = 5 THEN '视频'
                                      WHEN max(ptype) = 6 THEN '长文字'
                                      WHEN max(ptype) = 7 THEN '音频'
                                      ELSE '未知' END AS ptype
                                ,max(tid) AS tid
                                ,min(SUBSTR(FROM_UNIXTIME({}),1,10)) AS create_date
                                {}
                                ,CASE WHEN MAX(NVL(vdur,0)) BETWEEN 0 AND 5 THEN '0~5'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 6 AND 15 THEN '6~15'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 16 AND 30 THEN '16~30'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 31 AND 60 THEN '31~60'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 61 AND 90 THEN '61~90'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 91 AND 120 THEN '91~120'
                                      WHEN MAX(NVL(vdur,0)) BETWEEN 121 AND 300 THEN '121~300'
                                      WHEN MAX(NVL(vdur,0)) > 300 THEN '300+'
                                      ELSE 'other' END AS total_dur
                                ,max(content) AS content
                                ,max(nvl(LENGTH(content),0)) AS content_length
                                ,CASE WHEN max(status) = -4 THEN '自己删除' 
                                      WHEN max(status) = -3 THEN '不可见'
                                      WHEN max(status) = -2 THEN '自己可见'
                                      WHEN max(status) = -1 THEN '话题自见'
                                      WHEN max(status) = 1 THEN '可见'
                                      WHEN max(status) = 2 THEN '推荐'
                                      WHEN max(status) = 3 THEN '话题推荐'
                                      ELSE '未知' END AS status
                                ,max(mid) AS omid
                                {}
                        FROM    {}
                        GROUP BY pid
                    ) post
                    -- 发帖用户维度
                    LEFT JOIN 
                    (
                        SELECT mid,
                               MAX(CASE WHEN gender = 1 THEN '男' WHEN gender = 2 THEN '女' ELSE '未知' END) AS gender,
                               MAX({}) AS age
                        FROM {}
                        GROUP BY mid
                    ) user
                    on post.omid = user.mid
                    LEFT JOIN 
                    -- 话题维度
                    (
                        SELECT tid,
                               MAX(tname) AS tname
                        FROM {}
                        GROUP BY tid
                    ) topic 
                    ON post.tid = topic.tid
                    -- 分区维度
                    LEFT JOIN 
                    (
                        SELECT tid,
                               {}
                        FROM {}
                        GROUP BY tid  
                    ) part
                    ON post.tid = part.tid
                    """.format(str(tag_post),str(ct),str(media_num),str(tag_replace),str(self.post_table), str(age_group), str(self.user_table), str(self.tid_table),str(part_name),str(self.part_table))
            post_type_sql = """
                        -- 帖子基础特征数据
                        (
                        """ + post_type_condition + """\n""" \
                            + """) posttype
                        """

        ## 帖子消费维度
        if self.post_consume_type:
            post_consume_type_final = ''
            for v in self.post_consume_type:
                post_consume_type_dic = {}
                post_consume_type_tmp = ''
                relation = v.get('relation', None)
                for key, value in v.items():
                    if key in ('show_keys', 'relation'):
                        continue
                    else:
                        if type(value) == unicode:
                            if ',' in value:
                                condition_len = len(value.split(','))
                                condition_tmp = ""
                                for i in range(condition_len):
                                    if i == 0:
                                        condition_tmp += str(key) + " IN ( " + "'" + str(value.split(',')[i]) + "'"
                                    else:
                                        condition_tmp += ",'" + str(value.split(',')[i]) + "'"
                                post_consume_type_dic[key] = str(condition_tmp) + ")"
                            elif '~' in value:
                                post_consume_type_dic[key] = str(key) + " BETWEEN " + str(value.split('~')[0]) + " AND " + str(value.split('~')[1])
                            else:
                                post_consume_type_dic[key] = str(key) + " IN ('" + str(value) + "')"
                        elif type(value) == list:
                            con_tmp = ''
                            for con in value:
                                con_tmp += "'" + str(con) + "',"
                            con_tmp_final = con_tmp.strip(',')
                            post_consume_type_dic[key] = str(key) + " IN (" + str(con_tmp_final).strip() + ")"

                if relation:
                    for i in relation:
                        if i in ['(', ')']:
                            post_consume_type_tmp += str(i)
                        elif i == '||':
                            post_consume_type_tmp += ' OR '
                        elif i == '&&':
                            post_consume_type_tmp += ' AND '
                        else:
                            post_consume_type_tmp += str(post_consume_type_dic[str(i)])
                else:
                    for i in post_consume_type_dic.keys():
                        if len(post_consume_type_tmp) > 0:
                            post_consume_type_tmp += ' AND ' + str(post_consume_type_dic[str(i)])
                        else:
                            post_consume_type_tmp += str(post_consume_type_dic[str(i)])

                post_consume_type_final += " AND (" + str(post_consume_type_tmp) + ") "

        else:
            post_consume_type_final = ""

        ## 排序
        if self.order_func:
            order_type = ''
            rank_num = ''
            for v in self.order_func:
                for key, value in v.items():
                    if key not in ('show_keys', 'rank'):
                        order_type += str(key) + " " + str(value) + ","
                    elif key == 'rank':
                        rank_num += " WHERE rank BETWEEN " + str(value.split('~')[0]) + " AND " + str(value.split('~')[1])
                    else:
                        continue
            order_final = "ROW_NUMBER() OVER(PARTITION BY p_date ORDER BY " + order_type.strip('\n').strip().strip(',') + " ) AS rank"
        else:
            order_final = ""
            rank_num = ""

        ## 帖子消费指标
        if self.post_consume:
            post_consume_final = ''
            for v in self.post_consume:
                post_consume_dic = {}
                post_consume_tmp = ''
                post_consume_date_range = " BETWEEN '" + str(
                    (datetime.datetime.fromtimestamp(int(v.get('consume_start_date', 0)))).strftime(
                        "%Y-%m-%d")) + "' AND '" + str(
                    (datetime.datetime.fromtimestamp(int(v.get('consume_end_date', 0)))).strftime("%Y-%m-%d")) + "'"
                post_consume_date_type = v.get('date_type', '')
                if post_consume_date_type == 'day':
                    date_group_up = 'p_date,'
                    date_group_down = 'p_date,'
                elif post_consume_date_type == 'period':
                    date_group_up = "'" + str(
                        (datetime.datetime.fromtimestamp(int(v.get('consume_start_date', 0)))).strftime(
                            "%Y-%m-%d")) + "~" + str(
                        (datetime.datetime.fromtimestamp(int(v.get('consume_end_date', 0)))).strftime(
                            "%Y-%m-%d")) + "' AS p_date,"
                    date_group_down = ''
                else:
                    date_group_up = 'p_date,'
                    date_group_down = 'p_date,'
                relation = v.get('relation', None)
                for key, value in v.items():
                    if key in ('show_keys', 'relation', 'date_type', 'consume_start_date', 'consume_end_date'):
                        continue
                    else:
                        if type(value) == unicode:
                            if ',' in value:
                                condition_len = len(value.split(','))
                                condition_tmp = ""
                                for i in range(condition_len):
                                    if i == 0:
                                        condition_tmp += str(key) + " IN ( " + "'" + str(value.split(',')[i]) + "'"
                                    else:
                                        condition_tmp += ",'" + str(value.split(',')[i]) + "'"
                                post_consume_dic[key] = str(condition_tmp) + ")"
                            elif '~' in value:
                                post_consume_dic[key] = str(key) + " BETWEEN " + str(value.split('~')[0]) + " AND " + str(value.split('~')[1])
                            else:
                                post_consume_dic[key] = str(key) + " IN ('" + str(value) + "')"
                        elif type(value) == list:
                            con_tmp = ''
                            for con in value:
                                con_tmp += "'" + str(con) + "',"
                            con_tmp_final = con_tmp.strip(',')
                            post_consume_dic[key] = str(key) + " IN (" + str(con_tmp_final).strip() + ")"

                if relation:
                    for i in relation:
                        if i in ['(', ')']:
                            post_consume_tmp += str(i)
                        elif i == '||':
                            post_consume_tmp += ' OR '
                        elif i == '&&':
                            post_consume_tmp += ' AND '
                        else:
                            post_consume_tmp += str(post_consume_dic[str(i)])
                else:
                    for i in post_consume_dic.keys():
                        if len(post_consume_tmp) > 0:
                            post_consume_tmp += ' AND ' + str(post_consume_dic[str(i)])
                        else:
                            post_consume_tmp += str(post_consume_dic[str(i)])

                if len(post_consume_final) > 0:
                    post_consume_final += " AND (" + str(post_consume_tmp) + ") "
                else:
                    post_consume_final += " WHERE (" + str(post_consume_tmp) + ") "

            if len(order_final) > 0:
                post_consume_sql = """
                INNER JOIN
                ( SELECT *
                FROM 
                (
                    SELECT *,
                           {}
                    FROM 
                    (
                        SELECT  {}
                                pid,
                                nvl(SUM(t1.expose),0) AS expose,
                                nvl(SUM(t1.real_expose),0) AS real_expose,
                                nvl(SUM(t1.score),0) AS score,
                                ROUND(nvl(SUM(t1.score),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS ctr,
                                nvl(SUM(t1.score_satisfied),0) AS score_satisfied,
                                ROUND(nvl(SUM(t1.score_satisfied),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS ctr_satisfied,
                                nvl(SUM(t1.stay_time),0) AS stay_time,
                                ROUND(nvl(SUM(t1.stay_time),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS stay_time_avg,
                                nvl(SUM(t1.total_play_video),0) AS total_play_video,
                                nvl(SUM(t1.total_play_video_dur),0) AS total_play_video_dur,
                                nvl(SUM(t1.play_video),0) AS play_video,
                                nvl(SUM(t1.play_video_dur),0) AS play_video_dur,
                                nvl(SUM(t1.play_review_video),0) AS play_review_video,
                                nvl(SUM(t1.play_review_video_dur),0) AS play_review_video_dur,
                                nvl(SUM(t1.play_recommend_video),0) AS play_recommend_video,
                                nvl(SUM(t1.play_recommend_video_dur),0) AS play_recommend_video_dur,
                                nvl(SUM(t1.finish_video),0) AS finish_video,
                                nvl(SUM(t1.ones_video),0) AS ones_video,
                                nvl(SUM(t1.total_view_img),0) AS total_view_img,
                                nvl(SUM(t1.total_view_img_dur),0) AS total_view_img_dur,
                                nvl(SUM(t1.view_img),0) AS view_img,
                                nvl(SUM(t1.view_img_dur),0) AS view_img_dur,
                                nvl(SUM(t1.detail_post),0) AS detail_post,
                                ROUND(nvl(SUM(t1.detail_post),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS detail_rate,
                                nvl(SUM(t1.detail_post_dur),0) AS detail_post_dur,
                                ROUND(nvl(SUM(t1.detail_post_dur),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS detail_post_dur_avg,
                                nvl(SUM(t1.view_post_dur),0) AS view_post_dur,
                                nvl(SUM(t1.like),0) AS like,
                                ROUND(nvl(SUM(t1.like),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS like_rate,
                                nvl(SUM(t1.like_attitude),0) AS like_attitude,
                                nvl(SUM(t1.like_funny),0) AS like_funny,
                                nvl(SUM(t1.like_warm),0) AS like_warm,
                                nvl(SUM(t1.like_silly),0) AS like_silly,
                                nvl(SUM(t1.like_good),0) AS like_good,
                                nvl(SUM(t1.dislike),0) AS dislike,
                                ROUND(nvl(SUM(t1.dislike),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS dislike_rate,
                                nvl(SUM(t1.share),0) AS share,
                                ROUND(nvl(SUM(t1.share),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS share_rate,
                                nvl(SUM(t1.favor),0) AS favor,
                                ROUND(nvl(SUM(t1.favor),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS favor_rate,
                                nvl(SUM(t1.tedium),0) AS tedium,
                                nvl(SUM(t1.create_review),0) AS create_review,
                                ROUND(nvl(SUM(t1.create_review),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS create_review_rate,
                                nvl(SUM(t1.reply_review),0) AS reply_review,
                                ROUND(nvl(SUM(t1.reply_review),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS reply_review_rate,
                                nvl(SUM(t1.create_review+t1.reply_review),0) AS total_review,
                                ROUND(nvl(SUM(t1.create_review+t1.reply_review),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS total_review_rate,
                                nvl(SUM(t1.like_review),0) AS like_review,
                                nvl(SUM(t1.like_review_retained),0) AS like_review_retained,
                                nvl(SUM(t1.dislike_review),0) AS dislike_review,
                                nvl(SUM(t1.share_review),0) AS share_review,
                                nvl(SUM(t1.hots_review),0) AS hots_review,
                                nvl(SUM(t1.news_review),0) AS news_review,
                                nvl(SUM(t1.subreviews_review),0) AS subreviews_review,
                                nvl(SUM(t1.review_expose),0) AS review_expose,
                                nvl(SUM(t1.create_danmaku),0) AS create_danmaku,
                                nvl(SUM(t1.like_danmaku),0) AS like_danmaku,
                                nvl(SUM(t1.dislike_danmaku),0) AS dislike_danmaku,
                                nvl(SUM(t1.video_click_times),0) AS video_click_times,
                                nvl(SUM(t1.img_click_times),0) AS img_click_times,
                                nvl(SUM(t1.video_click_dur),0) AS video_click_dur
                        FROM {} AS t1
                        WHERE p_date {}
                        {}
                        GROUP BY {} pid
                    )
                    {}
                )
                {}
                ) postconsume
                ON posttype.pid = postconsume.pid
                """.format(str(order_final), str(date_group_up), str(self.dws_table), str(post_consume_date_range),
                           str(post_consume_type_final), str(date_group_down), str(post_consume_final), str(rank_num))
            else:
                post_consume_sql = """
                                INNER JOIN
                                ( 
                                    SELECT *
                                    FROM 
                                    (
                                        SELECT  {}
                                                pid,
                                                nvl(SUM(t1.expose),0) AS expose,
                                                nvl(SUM(t1.real_expose),0) AS real_expose,
                                                nvl(SUM(t1.score),0) AS score,
                                                ROUND(nvl(SUM(t1.score),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS ctr,
                                                nvl(SUM(t1.score_satisfied),0) AS score_satisfied,
                                                ROUND(nvl(SUM(t1.score_satisfied),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS ctr_satisfied,
                                                nvl(SUM(t1.stay_time),0) AS stay_time,
                                                ROUND(nvl(SUM(t1.stay_time),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS stay_time_avg,
                                                nvl(SUM(t1.total_play_video),0) AS total_play_video,
                                                nvl(SUM(t1.total_play_video_dur),0) AS total_play_video_dur,
                                                nvl(SUM(t1.play_video),0) AS play_video,
                                                nvl(SUM(t1.play_video_dur),0) AS play_video_dur,
                                                nvl(SUM(t1.play_review_video),0) AS play_review_video,
                                                nvl(SUM(t1.play_review_video_dur),0) AS play_review_video_dur,
                                                nvl(SUM(t1.play_recommend_video),0) AS play_recommend_video,
                                                nvl(SUM(t1.play_recommend_video_dur),0) AS play_recommend_video_dur,
                                                nvl(SUM(t1.finish_video),0) AS finish_video,
                                                nvl(SUM(t1.ones_video),0) AS ones_video,
                                                nvl(SUM(t1.total_view_img),0) AS total_view_img,
                                                nvl(SUM(t1.total_view_img_dur),0) AS total_view_img_dur,
                                                nvl(SUM(t1.view_img),0) AS view_img,
                                                nvl(SUM(t1.view_img_dur),0) AS view_img_dur,
                                                nvl(SUM(t1.detail_post),0) AS detail_post,
                                                ROUND(nvl(SUM(t1.detail_post),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS detail_rate,
                                                nvl(SUM(t1.detail_post_dur),0) AS detail_post_dur,
                                                ROUND(nvl(SUM(t1.detail_post_dur),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS detail_post_dur_avg,
                                                nvl(SUM(t1.view_post_dur),0) AS view_post_dur,
                                                nvl(SUM(t1.like),0) AS like,
                                                ROUND(nvl(SUM(t1.like),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS like_rate,
                                                nvl(SUM(t1.like_attitude),0) AS like_attitude,
                                                nvl(SUM(t1.like_funny),0) AS like_funny,
                                                nvl(SUM(t1.like_warm),0) AS like_warm,
                                                nvl(SUM(t1.like_silly),0) AS like_silly,
                                                nvl(SUM(t1.like_good),0) AS like_good,
                                                nvl(SUM(t1.dislike),0) AS dislike,
                                                ROUND(nvl(SUM(t1.dislike),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS dislike_rate,
                                                nvl(SUM(t1.share),0) AS share,
                                                ROUND(nvl(SUM(t1.share),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS share_rate,
                                                nvl(SUM(t1.favor),0) AS favor,
                                                ROUND(nvl(SUM(t1.favor),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS favor_rate,
                                                nvl(SUM(t1.tedium),0) AS tedium,
                                                nvl(SUM(t1.create_review),0) AS create_review,
                                                ROUND(nvl(SUM(t1.create_review),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS create_review_rate,
                                                nvl(SUM(t1.reply_review),0) AS reply_review,
                                                ROUND(nvl(SUM(t1.reply_review),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS reply_review_rate,
                                                nvl(SUM(t1.create_review+t1.reply_review),0) AS total_review,
                                                ROUND(nvl(SUM(t1.create_review+t1.reply_review),0)/(nvl(SUM(t1.real_expose),0)+0.01),4) AS total_review_rate,
                                                nvl(SUM(t1.like_review),0) AS like_review,
                                                nvl(SUM(t1.like_review_retained),0) AS like_review_retained,
                                                nvl(SUM(t1.dislike_review),0) AS dislike_review,
                                                nvl(SUM(t1.share_review),0) AS share_review,
                                                nvl(SUM(t1.hots_review),0) AS hots_review,
                                                nvl(SUM(t1.news_review),0) AS news_review,
                                                nvl(SUM(t1.subreviews_review),0) AS subreviews_review,
                                                nvl(SUM(t1.review_expose),0) AS review_expose,
                                                nvl(SUM(t1.create_danmaku),0) AS create_danmaku,
                                                nvl(SUM(t1.like_danmaku),0) AS like_danmaku,
                                                nvl(SUM(t1.dislike_danmaku),0) AS dislike_danmaku,
                                                nvl(SUM(t1.video_click_times),0) AS video_click_times,
                                                nvl(SUM(t1.img_click_times),0) AS img_click_times,
                                                nvl(SUM(t1.video_click_dur),0) AS video_click_dur
                                        FROM {} AS t1
                                        WHERE p_date {}
                                        {}
                                        GROUP BY {} pid
                                    )
                                    {}
                                ) postconsume
                                ON posttype.pid = postconsume.pid
                                """.format(str(date_group_up), str(self.dws_table), str(post_consume_date_range),
                                           str(post_consume_type_final), str(date_group_down), str(post_consume_final))

        else:
            post_consume_sql = ""

        if len(post_consume_sql) > 0 and len(order_final) > 0:
            sql_final = """
            SELECT postconsume.p_date AS 日期,
                   postconsume.pid AS pid,
                   postconsume.rank AS 排序,
                   postconsume.expose AS 拉取曝光,
                   postconsume.real_expose AS 真实曝光,
                   postconsume.ctr AS 点击率,
                   postconsume.stay_time AS 总时长,
                   postconsume.stay_time_avg AS 单次曝光时长,
                   postconsume.detail_rate AS 详情率,
                   postconsume.detail_post AS 详情次数,
                   postconsume.like AS 帖子点赞数,
                   postconsume.like_rate AS 帖子点赞率,
                   postconsume.dislike AS 帖子点踩数,
                   postconsume.dislike_rate AS 帖子点踩率,
                   postconsume.create_review AS 一级评论数,
                   postconsume.reply_review AS 二级评论数,
                   posttype.create_date AS 发帖日期,
                   posttype.omid AS 发帖人,
                   posttype.ptype AS 帖子体裁,
                   posttype.content AS 帖子内容,
                   {}
                   posttype.tname AS 话题名称,
                   posttype.l1part_name AS 一级分区名称,
                   posttype.l2part_name AS 二级分区名称
            FROM 
            {}
            {}
            ;
            """.format(str(tag_final),str(post_type_sql), str(post_consume_sql))
        elif len(post_consume_sql) > 0 and len(order_final) == 0:
            sql_final = """
                        SELECT postconsume.p_date AS 日期,
                               postconsume.pid AS pid,
                               postconsume.expose AS 拉取曝光,
                               postconsume.real_expose AS 真实曝光,
                               postconsume.ctr AS 点击率,
                               postconsume.stay_time AS 总时长,
                               postconsume.stay_time_avg AS 单次曝光时长,
                               postconsume.detail_rate AS 详情率,
                               postconsume.detail_post AS 详情次数,
                               postconsume.like AS 帖子点赞数,
                               postconsume.like_rate AS 帖子点赞率,
                               postconsume.dislike AS 帖子点踩数,
                               postconsume.dislike_rate AS 帖子点踩率,
                               postconsume.create_review AS 一级评论数,
                               postconsume.reply_review AS 二级评论数,
                               posttype.create_date AS 发帖日期,
                               posttype.omid AS 发帖人,
                               posttype.ptype AS 帖子体裁,
                               posttype.content AS 帖子内容,
                               {}
                               posttype.tname AS 话题名称,
                               posttype.l1part_name AS 一级分区名称,
                               posttype.l2part_name AS 二级分区名称
                        FROM 
                        {}
                        {}
                        ;
                        """.format(str(tag_final), str(post_type_sql), str(post_consume_sql))
        else:
            sql_final = """
            SELECT posttype.pid AS pid,
                   posttype.create_date AS 发帖日期,
                   posttype.omid AS 发帖人,
                   posttype.ptype AS 帖子体裁,
                   posttype.content AS 帖子内容,
                   {}
                   posttype.tname AS 话题名称,
                   posttype.l1part_name AS 一级分区名称,
                   posttype.l2part_name AS 二级分区名称
            FROM 
            {}
            ;
            """.format(str(tag_final),str(post_type_sql))

        return sql_final

    def fail_update(self, task_id):
        filter = {"toolkit_type":"post","_id": int(task_id)}
        newvalues = {"$set": {"status": -1, "ut": int(time.time())}}
        self.table.update_one(filter, newvalues)

    def update_mongo(self,task_id,size,upload_result):
        if upload_result:
            filter = {"toolkit_type":"post","_id": int(task_id)}
            newvalues = {"$set": {"status": 2, "ut": int(time.time()), "size": int(size)}}
            self.table.update_one(filter, newvalues)
        else:
            filter = {"toolkit_type": "post", "_id": int(task_id)}
            newvalues = {"$set": {"status": -1, "ut": int(time.time())}}
            self.table.update_one(filter, newvalues)

    def upload_oss(self,result_detail,result_id):
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

if __name__ == '__main__':
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[%s]: start!" % (now))
    task_id = sys.argv[1]
    app_name = sys.argv[2]
    task = GetPost(task_id,app_name)
    sql = task.get_sql()
    print(sql)
    try:
        data_detail,data_id,id_num = task.get_data(sql)
        result_detail = "/home/devs/tangyongjun/post/result/" + str(app_name) + "/final_result/" + str(task_id) + "_detail.csv"
        result_id = "/home/devs/tangyongjun/post/result/" + str(app_name) + "/final_result/" + str(task_id) + ".csv"
        data_detail.to_csv(result_detail, encoding='utf_8_sig', index=False)
        data_id.to_csv(result_id, encoding='utf_8_sig', index=False)
        print("业务名称:" + str(app_name) + " 任务id: " + str(task_id) + "," + " 结果地址: " + str(result_detail) + ", " + str(result_id))
        upload_result = task.upload_oss(result_detail,result_id)
        task.update_mongo(task_id,id_num,upload_result)
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("[%s]: finish!" % (now))
    except Exception as e:
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        task.fail_update(task_id)
        print("[%s]: fail!" % (now))