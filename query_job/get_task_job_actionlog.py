# !/usr/bin/python
# -*-coding: utf-8 -*-

from pymongo import MongoClient
import os
import datetime
import threading

class Gettask(object):

    def __init__(self):
        self.client = MongoClient("xxxx")
        self.db_name = "xxx"
        self.table_name = "xxx"
        self.db = self.client.get_database(self.db_name)
        self.table = self.db.get_collection(self.table_name)
        self.script_path = "/home/work/tangyongjun/query_job_actionlog/script/query_actionlog.py"
        self.output_path = "/home/work/tangyongjun/query_job_actionlog/result/query_actionlog_log"

    def get_task_id(self):
        docs = self.table.find({'status':0})
        return docs

    def do_query_job(self,task_id,task_owner):
        code = os.system("nohup python %s %s > %s/%s_%s.log 2>&1 &" % (self.script_path, str(task_id), self.output_path, str(task_owner), str(task_id)))

    def task_run(self,docs,do_query_job):
        task_num = 0
        task_id_list = ""
        for doc in docs:
            task_id = doc.get('id',-99)
            task_owner = doc.get('owner','').split('@')[0].strip()
            task_status = int(doc.get('status',-99))
            if task_status == 0:
                thread = threading.Thread(target=do_query_job,args=(task_id, task_owner))
                thread.start()
                # code = os.system("nohup python %s %s > %s/%s_%s.log 2>&1 &" % (self.script_path, str(task_id), self.output_path, str(task_owner), str(task_id)))
                filter = {"id": int(task_id)}
                newvalues = {"$set": {"status": 1}}
                self.table.update_one(filter,newvalues)
                task_num += 1
                task_id_list += str(task_id) + ","
            else:
                continue
        if len(task_id_list) > 0:
            task_id_list = task_id_list.strip(",")
        else:
            task_id_list = task_id_list
        return task_num, task_id_list

if __name__ == '__main__':
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[%s]: start!" % (now))
    task = Gettask()
    docs = task.get_task_id()
    task_num, task_id_list = task.task_run(docs,task.do_query_job)
    if task_num == 0:
        print("本次无投递任务!")
    elif task_num > 0:
        print("本次投递任务数字为: %s, 任务id分别为: %s" % (str(task_num), str(task_id_list)))
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[%s]: finish!" % (now))
