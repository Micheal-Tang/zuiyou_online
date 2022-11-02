# !/usr/bin/python
# -*-coding: utf-8 -*-

from pymongo import MongoClient
import os
import datetime
import time
import threading

class Gettask(object):

    def __init__(self):

        ## 皮皮
        self.client_pp = MongoClient("xxxx")
        self.db_name_pp = "xx"
        self.table_name_pp = "xx"
        self.db_pp = self.client_pp.get_database(self.db_name_pp)
        self.table_pp = self.db_pp.get_collection(self.table_name_pp)

        ## 最右
        self.client_zy = MongoClient("xxx")
        self.db_name_zy = "xx"
        self.table_name_zy = "xx"
        self.db_zy = self.client_zy.get_database(self.db_name_zy)
        self.table_zy = self.db_zy.get_collection(self.table_name_zy)

        ## omg
        self.client_omg = MongoClient("xxxx")
        self.db_name_omg = "xx"
        self.table_name_omg = "xx"
        self.db_omg = self.client_omg.get_database(self.db_name_omg)
        self.table_omg = self.db_omg.get_collection(self.table_name_omg)

        ## maga
        self.client_maga = MongoClient("xxxx")
        self.db_name_maga = "xx"
        self.table_name_maga = "xx"
        self.db_maga = self.client_maga.get_database(self.db_name_maga)
        self.table_maga = self.db_maga.get_collection(self.table_name_maga)

        self.script_path = "/home/devs/tangyongjun/user/script/query_user.py"
        self.output_path = "/home/devs/tangyongjun/user/result"

    def get_task_id(self):
        docs_zy = self.table_zy.find({"toolkit_type":"user","type": 1,"status": {"$in": [1,2]}})
        docs_pp = self.table_pp.find({"toolkit_type": "user", "type": 1, "status": {"$in": [1, 2]}})
        docs_omg = self.table_omg.find({"toolkit_type": "user", "type": 1, "status": {"$in": [1, 2]}})
        docs_maga = self.table_maga.find({"toolkit_type": "user", "type": 1, "status": {"$in": [1, 2]}})
        return docs_zy, docs_pp, docs_omg, docs_maga

    def do_query_job(self,task_id,app_name):
        code = os.system("nohup /home/devs/anaconda3/envs/py27/bin/python %s %s %s >> %s/%s/log/%s_%s.log 2>&1 &" % (self.script_path, str(task_id), str(app_name), self.output_path, str(app_name), str(app_name), str(task_id)))

    def update_mongo(self,table,task_id):
        filter = {"toolkit_type": "user", "_id": int(task_id)}
        newvalues = {"$set": {"status": 3}}
        table.update_one(filter, newvalues)

    def task_run(self,docs_zy,docs_pp,docs_omg,docs_maga,do_query_job):

        ## 最右任务投递
        task_num_zy = 0
        task_id_list_zy = ""
        for doc in docs_zy:
            task_id = doc.get('_id', -99)
            task_status = int(doc.get('status', -99))
            task_update_frequency = int(doc.get('update_frequency', -99))
            task_ut = int(doc.get('ut', 0))
            if task_status == 1:
                thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou'))
                thread.start()
                task_num_zy += 1
                task_id_list_zy += str(task_id) + ","
                self.update_mongo(table=self.table_zy, task_id=task_id)
            elif task_status == 2 and task_update_frequency == 2:
                if (int(time.time()) - task_ut >= 300):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou'))
                    thread.start()
                    task_num_zy += 1
                    task_id_list_zy += str(task_id) + ","
                    self.update_mongo(table=self.table_zy, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 3:
                if (int(time.time()) - task_ut >= 600):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou'))
                    thread.start()
                    task_num_zy += 1
                    task_id_list_zy += str(task_id) + ","
                    self.update_mongo(table=self.table_zy, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 4:
                if (int(time.time()) - task_ut >= 1800):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou'))
                    thread.start()
                    task_num_zy += 1
                    task_id_list_zy += str(task_id) + ","
                    self.update_mongo(table=self.table_zy, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 5:
                if (int(time.time()) - task_ut >= 3600):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou'))
                    thread.start()
                    task_num_zy += 1
                    task_id_list_zy += str(task_id) + ","
                    self.update_mongo(table=self.table_zy, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 6:
                if (int(time.time()) - task_ut >= 21600):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou'))
                    thread.start()
                    task_num_zy += 1
                    task_id_list_zy += str(task_id) + ","
                    self.update_mongo(table=self.table_zy, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 7:
                if (int(time.time()) - task_ut >= 43200):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou'))
                    thread.start()
                    task_num_zy += 1
                    task_id_list_zy += str(task_id) + ","
                    self.update_mongo(table=self.table_zy, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 8:
                if (int(time.time()) - task_ut >= 86400):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou'))
                    thread.start()
                    task_num_zy += 1
                    task_id_list_zy += str(task_id) + ","
                    self.update_mongo(table=self.table_zy, task_id=task_id)
                else:
                    continue
            else:
                continue

        ## 皮皮任务投递
        task_num_pp = 0
        task_id_list_pp = ""
        for doc in docs_pp:
            task_id = doc.get('_id', -99)
            task_status = int(doc.get('status', -99))
            task_update_frequency = int(doc.get('update_frequency', -99))
            task_ut = int(doc.get('ut', 0))
            if task_status == 1:
                thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou_lite'))
                thread.start()
                task_num_pp += 1
                task_id_list_pp += str(task_id) + ","
                self.update_mongo(table=self.table_pp, task_id=task_id)
            elif task_status == 2 and task_update_frequency == 2:
                if (int(time.time()) - task_ut >= 300):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou_lite'))
                    thread.start()
                    task_num_pp += 1
                    task_id_list_pp += str(task_id) + ","
                    self.update_mongo(table=self.table_pp, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 3:
                if (int(time.time()) - task_ut >= 600):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou_lite'))
                    thread.start()
                    task_num_pp += 1
                    task_id_list_pp += str(task_id) + ","
                    self.update_mongo(table=self.table_pp, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 4:
                if (int(time.time()) - task_ut >= 1800):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou_lite'))
                    thread.start()
                    task_num_pp += 1
                    task_id_list_pp += str(task_id) + ","
                    self.update_mongo(table=self.table_pp, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 5:
                if (int(time.time()) - task_ut >= 3600):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou_lite'))
                    thread.start()
                    task_num_pp += 1
                    task_id_list_pp += str(task_id) + ","
                    self.update_mongo(table=self.table_pp, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 6:
                if (int(time.time()) - task_ut >= 21600):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou_lite'))
                    thread.start()
                    task_num_pp += 1
                    task_id_list_pp += str(task_id) + ","
                    self.update_mongo(table=self.table_pp, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 7:
                if (int(time.time()) - task_ut >= 43200):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou_lite'))
                    thread.start()
                    task_num_pp += 1
                    task_id_list_pp += str(task_id) + ","
                    self.update_mongo(table=self.table_pp, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 8:
                if (int(time.time()) - task_ut >= 86400):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'zuiyou_lite'))
                    thread.start()
                    task_num_pp += 1
                    task_id_list_pp += str(task_id) + ","
                    self.update_mongo(table=self.table_pp, task_id=task_id)
                else:
                    continue
            else:
                continue

        ## omg任务投递
        task_num_omg = 0
        task_id_list_omg = ""
        for doc in docs_omg:
            task_id = doc.get('_id', -99)
            task_status = int(doc.get('status', -99))
            task_update_frequency = int(doc.get('update_frequency', -99))
            task_ut = int(doc.get('ut', 0))
            if task_status == 1:
                thread = threading.Thread(target=do_query_job, args=(task_id, 'omg'))
                thread.start()
                task_num_omg += 1
                task_id_list_omg += str(task_id) + ","
                self.update_mongo(table=self.table_omg, task_id=task_id)
            elif task_status == 2 and task_update_frequency == 2:
                if (int(time.time()) - task_ut >= 300):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'omg'))
                    thread.start()
                    task_num_omg += 1
                    task_id_list_omg += str(task_id) + ","
                    self.update_mongo(table=self.table_omg, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 3:
                if (int(time.time()) - task_ut >= 600):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'omg'))
                    thread.start()
                    task_num_omg += 1
                    task_id_list_omg += str(task_id) + ","
                    self.update_mongo(table=self.table_omg, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 4:
                if (int(time.time()) - task_ut >= 1800):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'omg'))
                    thread.start()
                    task_num_omg += 1
                    task_id_list_omg += str(task_id) + ","
                    self.update_mongo(table=self.table_omg, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 5:
                if (int(time.time()) - task_ut >= 3600):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'omg'))
                    thread.start()
                    task_num_omg += 1
                    task_id_list_omg += str(task_id) + ","
                    self.update_mongo(table=self.table_omg, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 6:
                if (int(time.time()) - task_ut >= 21600):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'omg'))
                    thread.start()
                    task_num_omg += 1
                    task_id_list_omg += str(task_id) + ","
                    self.update_mongo(table=self.table_omg, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 7:
                if (int(time.time()) - task_ut >= 43200):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'omg'))
                    thread.start()
                    task_num_omg += 1
                    task_id_list_omg += str(task_id) + ","
                    self.update_mongo(table=self.table_omg, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 8:
                if (int(time.time()) - task_ut >= 86400):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'omg'))
                    thread.start()
                    task_num_omg += 1
                    task_id_list_omg += str(task_id) + ","
                    self.update_mongo(table=self.table_omg, task_id=task_id)
                else:
                    continue
            else:
                continue

        ## maga任务投递
        task_num_maga = 0
        task_id_list_maga = ""
        for doc in docs_maga:
            task_id = doc.get('_id', -99)
            task_status = int(doc.get('status', -99))
            task_update_frequency = int(doc.get('update_frequency', -99))
            task_ut = int(doc.get('ut', 0))
            if task_status == 1:
                thread = threading.Thread(target=do_query_job, args=(task_id, 'maga'))
                thread.start()
                task_num_maga += 1
                task_id_list_maga += str(task_id) + ","
                self.update_mongo(table=self.table_maga, task_id=task_id)
            elif task_status == 2 and task_update_frequency == 2:
                if (int(time.time()) - task_ut >= 300):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'maga'))
                    thread.start()
                    task_num_maga += 1
                    task_id_list_maga += str(task_id) + ","
                    self.update_mongo(table=self.table_maga, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 3:
                if (int(time.time()) - task_ut >= 600):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'maga'))
                    thread.start()
                    task_num_maga += 1
                    task_id_list_maga += str(task_id) + ","
                    self.update_mongo(table=self.table_maga, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 4:
                if (int(time.time()) - task_ut >= 1800):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'maga'))
                    thread.start()
                    task_num_maga += 1
                    task_id_list_maga += str(task_id) + ","
                    self.update_mongo(table=self.table_maga, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 5:
                if (int(time.time()) - task_ut >= 3600):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'maga'))
                    thread.start()
                    task_num_maga += 1
                    task_id_list_maga += str(task_id) + ","
                    self.update_mongo(table=self.table_maga, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 6:
                if (int(time.time()) - task_ut >= 21600):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'maga'))
                    thread.start()
                    task_num_maga += 1
                    task_id_list_maga += str(task_id) + ","
                    self.update_mongo(table=self.table_maga, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 7:
                if (int(time.time()) - task_ut >= 43200):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'maga'))
                    thread.start()
                    task_num_maga += 1
                    task_id_list_maga += str(task_id) + ","
                    self.update_mongo(table=self.table_maga, task_id=task_id)
                else:
                    continue
            elif task_status == 2 and task_update_frequency == 8:
                if (int(time.time()) - task_ut >= 86400):
                    thread = threading.Thread(target=do_query_job, args=(task_id, 'maga'))
                    thread.start()
                    task_num_maga += 1
                    task_id_list_maga += str(task_id) + ","
                    self.update_mongo(table=self.table_maga, task_id=task_id)
                else:
                    continue
            else:
                continue

        task_final = "最右投递任务数: " + str(task_num_zy) + ", 任务id分别为: " + str(task_id_list_zy).strip(',') + ";" + "\n" \
                     + "皮皮投递任务数: " + str(task_num_pp) + ", 任务id分别为: " + str(task_id_list_pp).strip(',') + ";" + "\n" \
                     + "omg投递任务数: " + str(task_num_omg) + ", 任务id分别为: " + str(task_id_list_omg).strip(',') + ";" + "\n" \
                     + "maga投递任务数: " + str(task_num_maga) + ", 任务id分别为: " + str(task_id_list_maga).strip(',') + "."

        return task_final

if __name__ == '__main__':
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[%s]: start!" % (now))
    task = Gettask()
    docs_zy,docs_pp,docs_omg,docs_maga = task.get_task_id()
    task_final = task.task_run(docs_zy,docs_pp,docs_omg,docs_maga,task.do_query_job)
    print(task_final)
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[%s]: finish!" % (now))