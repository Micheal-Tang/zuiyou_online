# !/usr/bin/python
# -*-coding: utf-8 -*-

from odps import ODPS
import pandas as pd
import datetime
import configparser
import sys
import numpy as np
import json
import requests
from pymongo import MongoClient
import os

reload(sys)
sys.setdefaultencoding('utf-8')


class GetActionlog(object):

    def __init__(self, task_id):

        ## odps
        AccessKeyID = 'xxx'
        AccessKeySecret = 'xxx'
        self.db_odps = ODPS(AccessKeyID, AccessKeySecret, 'xx')

        ## mongodb
        self.client = MongoClient("xxx")
        self.db_name = "xx"
        self.table_name = "xx"
        self.db = self.client.get_database(self.db_name)
        self.table = self.db.get_collection(self.table_name)
        self.docs = self.table.find_one({"id": int(task_id)})
        self.params = self.docs.get("params", None)
        self.task_owner = self.docs.get("owner", "").split("@")[0].strip().lower()

        ## 应用名称
        self.app_name = self.params.get('app_name', '').strip().lower()

        ## 数据表
        if self.app_name == "zuiyou":
            self.actionlog_table = "zy_bigdata.actionlog_zuiyou"
        elif self.app_name == "zuiyou_lite":
            self.actionlog_table = "pipi_bigdata.ppactionlog_new"
        else:
            self.actionlog_table = "zy_bigdata.actionlog"

        ## 埋点名称解析
        self.type = self.params.get('type', '').replace('$old$', '%')
        self.stype = self.params.get('stype', '').replace('$old$', '%')
        self.frominfo = self.params.get('frominfo', '').replace('$old$', '%')
        if '%' in self.type:
            self.type_sel = " LIKE '" + str(self.type).strip() + "'"
        else:
            self.type_sel = " = '" + str(self.type).strip() + "'"
        if '%' in self.stype:
            self.stype_sel = " LIKE '" + str(self.stype).strip() + "'"
        else:
            self.stype_sel = " = '" + str(self.stype).strip() + "'"
        if '%' in self.frominfo:
            self.frominfo_sel = " LIKE '" + str(self.frominfo).strip() + "'"
        else:
            self.frominfo_sel = " = '" + str(self.frominfo).strip() + "'"

        ## 日期解析
        self.start_date_pre = int(self.params.get('start_date', 0))
        self.start_date = (datetime.datetime.fromtimestamp(self.start_date_pre)).strftime("%Y-%m-%d")
        self.end_date_pre = int(self.params.get('end_date', 0))
        self.end_date = (datetime.datetime.fromtimestamp(self.end_date_pre)).strftime("%Y-%m-%d")
        self.p_date = " BETWEEN '" + str(self.start_date) + "' AND '" + str(self.end_date) + "'"
        self.ct = " BETWEEN " + str(self.start_date_pre) + " AND " + str(self.end_date_pre)

        ## 日期范围筛选
        self.date_type = self.params.get('date_type', '').strip()
        if self.date_type == 'period':
            self.date_sel = ""
            self.date_sel_down = ""
        elif self.date_type == 'day':
            self.date_sel = "CONCAT(SUBSTR(ym,1,4),'-',SUBSTR(ym,5,2),'-',day) AS 日期,"
            self.date_sel_down = ",CONCAT(SUBSTR(ym,1,4),'-',SUBSTR(ym,5,2),'-',day)"
        else:
            self.date_sel = ""
            self.date_sel_down = ""

        ## 维度解析
        self.group_type = self.params.get('group', None)
        self.group = ""
        self.group_down = ""
        if self.group_type:
            for v in self.group_type:
                if v.get("key", "").strip() not in (
                'mid', 'id', 'oid', 'atype', 'frominfo', 'ct', 'day', 'stype', 'type', 'ym') and len(
                        v.get("key", "").strip()) > 0:
                    self.group += "GET_JSON_OBJECT(data,'$." + str(v.get("key", "")).strip() + "') AS " + str(
                        v.get("key", "")).strip().replace('from', 'frm') + "," + """\n"""
                    self.group_down += "GET_JSON_OBJECT(data,'$." + str(v.get("key", "")).strip() + "')," + """\n"""
                elif len(v.get("key", "").strip()) > 0 and v.get("key", "").strip() in (
                'mid', 'id', 'oid', 'atype', 'frominfo', 'ct', 'day', 'stype', 'type', 'ym'):
                    self.group += str(v.get("key", "")).strip() + "," + """\n"""
                    self.group_down += str(v.get("key", "")).strip() + "," + """\n"""

        ## 筛选条件解析
        self.relation = self.params.get('relation', None)
        self.select_group = self.params.get('condition', None)
        self.select_filter = ""
        self.select_dict = {}
        if self.select_group:
            for module in self.select_group:
                key = module.get('id', None)
                if key and key not in self.select_dict.keys():
                    if '~' in module.get('op', '').strip():
                        if module.get('field', '').strip() not in (
                        'mid', 'id', 'oid', 'atype', 'frominfo', 'ct', 'day', 'stype', 'type', 'ym') and len(
                                module.get('field', '').strip()) > 0:
                            self.select_dict[key] = "GET_JSON_OBJECT(data,'$." + str(
                                module.get('field', '')).strip() + "') " + " BETWEEN " + \
                                                    module.get('op', '').split('~')[0].strip() + " AND " + \
                                                    module.get('op', '').split('~')[1].strip()
                            self.select_filter += " AND " + "GET_JSON_OBJECT(data,'$." + str(
                                module.get('field', '')).strip() + "') <> '' AND " + "GET_JSON_OBJECT(data,'$." + str(
                                module.get('field', '')).strip() + "') IS NOT NULL"
                        elif module.get('field', '').strip() in (
                        'mid', 'id', 'oid', 'atype', 'frominfo', 'ct', 'day', 'stype', 'type', 'ym') and len(
                                module.get('field', '').strip()) > 0:
                            self.select_dict[key] = str(module.get('field', '')) + " BETWEEN " + \
                                                    module.get('op', '').split('~')[0].strip() + " AND " + \
                                                    module.get('op', '').split('~')[1].strip()
                            self.select_filter += " AND " + str(module.get('field', '')).strip() + " <> '' AND " + str(
                                module.get('field', '')).strip() + " IS NOT NULL"
                    elif ',' in module.get('op', ''):
                        condition_len = len(module.get('op', '').split(','))
                        condition_tmp = ""
                        for i in range(condition_len):
                            if i == 0:
                                if module.get('field', '').strip() not in (
                                'mid', 'id', 'oid', 'atype', 'frominfo', 'ct', 'day', 'stype', 'type', 'ym') and len(
                                        module.get('field', '').strip()) > 0:
                                    condition_tmp += "GET_JSON_OBJECT(data,'$." + str(
                                        module.get('field', '')).strip() + "') " + " IN ( " + "'" + str(
                                        module.get('op', '').split(',')[i]).strip() + "'"
                                elif module.get('field', '').strip() in (
                                'mid', 'id', 'oid', 'atype', 'frominfo', 'ct', 'day', 'stype', 'type', 'ym') and len(
                                        module.get('field', '').strip()) > 0:
                                    condition_tmp += str(module.get('field', '')).strip() + " IN ( " + "'" + str(
                                        module.get('op', '').split(',')[i]).strip() + "'"
                            else:
                                condition_tmp += ",'" + str(module.get('op', '').split(',')[i]).strip() + "'"
                        self.select_dict[key] = condition_tmp + ")"

                        if module.get('field', '').strip() not in (
                        'mid', 'id', 'oid', 'atype', 'frominfo', 'ct', 'day', 'stype', 'type', 'ym') and len(
                                module.get('field', '').strip()) > 0:
                            self.select_filter += " AND " + "GET_JSON_OBJECT(data,'$." + str(
                                module.get('field', '')).strip() + "') <> '' AND " + "GET_JSON_OBJECT(data,'$." + str(
                                module.get('field', '')).strip() + "') IS NOT NULL"
                        elif module.get('field', '').strip() in (
                        'mid', 'id', 'oid', 'atype', 'frominfo', 'ct', 'day', 'stype', 'type', 'ym') and len(
                                module.get('field', '').strip()) > 0:
                            self.select_filter += " AND " + str(module.get('field', '')).strip() + " <> '' AND " + str(
                                module.get('field', '')).strip() + " IS NOT NULL"
                    else:
                        if module.get('field', '').strip() not in (
                        'mid', 'id', 'oid', 'atype', 'frominfo', 'ct', 'day', 'stype', 'type', 'ym') and len(
                                module.get('field', '').strip()) > 0:
                            self.select_dict[key] = "GET_JSON_OBJECT(data,'$." + str(
                                module.get('field', '')).strip() + "') " + " IN ('" + str(
                                module.get('op', '')).strip() + "')"
                            self.select_filter += " AND " + "GET_JSON_OBJECT(data,'$." + str(
                                module.get('field', '')).strip() + "') <> '' AND " + "GET_JSON_OBJECT(data,'$." + str(
                                module.get('field', '')).strip() + "') IS NOT NULL"
                        elif module.get('field', '').strip() in (
                        'mid', 'id', 'oid', 'atype', 'frominfo', 'ct', 'day', 'stype', 'type', 'ym') and len(
                                module.get('field', '').strip()) > 0:
                            self.select_dict[key] = str(module.get('field', '')).strip() + " IN ('" + str(
                                module.get('op', '')).strip() + "')"
                            self.select_filter += " AND " + str(module.get('field', '')).strip() + " <> '' AND " + str(
                                module.get('field', '')).strip() + " IS NOT NULL"
                else:
                    continue

        if self.relation:
            self.relation_tmp = ""
            for rel in self.relation:
                if rel in ('(', ')'):
                    self.relation_tmp += " " + str(rel)
                elif rel == '&&':
                    self.relation_tmp += " AND "
                elif rel == '||':
                    self.relation_tmp += " OR "
                elif rel not in ('(', ')', '&&', '||') and rel in self.select_dict.keys():
                    self.relation_tmp += " " + str(self.select_dict[rel])
            self.final_relation = " AND (" + self.relation_tmp + ")" + """\n"""
        else:
            self.final_relation = ""

        ## 度量解析
        self.calculation_group = self.params.get('calculation', None)
        self.calculation = ""
        for cal in self.calculation_group:
            if cal.get('field', '').strip() not in ('mid', 'id', 'oid', 'atype', 'frominfo', 'ct') and len(
                    cal.get('field', '').strip()) > 0:
                val = "GET_JSON_OBJECT(data,'$." + str(cal.get('field', '')) + "')"
            elif len(cal.get('field', '').strip()) > 0 and cal.get('field', '').strip() in (
            'mid', 'id', 'oid', 'atype', 'frominfo', 'ct'):
                val = cal.get('field', '').strip()
            else:
                val = 1
            if cal.get('op', '') == 'countdis':
                self.calculation += "COUNT(DISTINCT " + str(val).strip() + ") AS " + str(
                    cal.get('name', '')).strip() + "," + """\n"""
            elif cal.get('op', '') in ('sum', 'max', 'min', 'avg', 'count'):
                self.calculation += str(cal.get('op', '')).strip() + "(" + str(val).strip() + ") AS " + str(
                    cal.get('name', '')).strip() + "," + """\n"""
        self.calculation_final = self.calculation.strip('\n').strip(',')

    def get_sql(self):
        if len(str(self.group_down)) == 0 and len(str(self.date_sel_down)) != 0:
            sql_final = """
            SELECT {}
                {}
            FROM {}
            WHERE CONCAT(SUBSTR(ym,1,4),'-',SUBSTR(ym,5,2),'-',day) {}
            AND GET_JSON_OBJECT(data,'$.app_name') = '{}'
            {}
            AND ct <> ''
            AND ct IS NOT NULL
            AND ct {}
            AND type {}
            AND stype {}
            AND frominfo {}
            {}
            GROUP BY 
                    {}
            ;
            """.format(str(self.date_sel), str(self.calculation_final), str(self.actionlog_table), str(self.p_date),
                       str(self.app_name), str(self.select_filter), (self.ct), str(self.type_sel), str(self.stype_sel),
                       str(self.frominfo_sel), str(self.final_relation), str(self.date_sel_down).strip(','))
        elif len(str(self.group_down)) == 0 and len(str(self.date_sel_down)) == 0:
            sql_final = """
            SELECT 
                {}
            FROM {}
            WHERE CONCAT(SUBSTR(ym,1,4),'-',SUBSTR(ym,5,2),'-',day) {}
            AND GET_JSON_OBJECT(data,'$.app_name') = '{}'
            {}
            AND ct <> ''
            AND ct IS NOT NULL
            AND ct {}
            AND type {}
            AND stype {}
            AND frominfo {}
            {}
            ;
            """.format(str(self.calculation_final), str(self.actionlog_table), str(self.p_date), str(self.app_name),
                       str(self.select_filter), (self.ct), str(self.type_sel), str(self.stype_sel),
                       str(self.frominfo_sel), str(self.final_relation))
        else:
            sql_final = """
            SELECT {}
                {}
                {}
            FROM {}
            WHERE CONCAT(SUBSTR(ym,1,4),'-',SUBSTR(ym,5,2),'-',day) {}
            AND GET_JSON_OBJECT(data,'$.app_name') = '{}'
            {}
            AND ct <> ''
            AND ct IS NOT NULL
            AND ct {}
            AND type {}
            AND stype {}
            AND frominfo {}
            {}
            GROUP BY {}
                    {}
            ;
            """.format(str(self.date_sel), str(self.group), str(self.calculation_final), str(self.actionlog_table),
                       str(self.p_date), str(self.app_name), str(self.select_filter), (self.ct), str(self.type_sel),
                       str(self.stype_sel), str(self.frominfo_sel), str(self.final_relation),
                       str(self.group_down).strip('\n').strip(','), str(self.date_sel_down))

        return sql_final

    def get_data(self, sql):
        with self.db_odps.execute_sql(sql).open_reader() as reader:
            print(reader.count)
            record_list = list()
            for record in reader:
                record_list.append(dict(record))
        record_pd = pd.DataFrame(record_list)
        print(len(record_pd))
        return record_pd

    def fail_update(self, task_id):
        filter = {"id": int(task_id)}
        newvalues = {"$set": {"status": 3}}
        self.table.update_one(filter, newvalues)
        return True


if __name__ == '__main__':
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[%s]: start!" % (now))
    update_script_path = "/home/work/tangyongjun/query_job_actionlog/script/update_task_actionlog.py"
    task_id = sys.argv[1]
    task = GetActionlog(task_id)
    task_owner = task.task_owner
    sql = task.get_sql()
    print(sql)
    try:
        data = task.get_data(sql)
        result_path = "/home/work/tangyongjun/query_job_actionlog/result/query_actionlog_result/" + str(
            task_owner) + "_" + str(task_id) + "_data.csv"
        print("任务id: " + str(task_id) + "," + " 结果地址: " + str(result_path))
        data.to_csv(result_path, encoding='utf_8_sig')
        code = os.system(
            "nohup python %s %s %s %s > /home/work/tangyongjun/query_job_actionlog/update_result/%s_%s_update.log 2>&1 &" % (
            str(update_script_path), str(task_id), str(task_owner), str(result_path), str(task_owner), str(task_id)))
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("[%s]: finish!" % (now))
    except Exception as e:
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        result = task.fail_update(task_id)
        print("[%s]: fail!" % (now))
