# !/usr/bin/python
# -*-coding: utf-8 -*-

from pymongo import MongoClient
import os
import datetime
import oss2
import requests
import sys
import logging

logging.basicConfig()


class Update(object):

    def __init__(self):
        self.client = MongoClient("xxx")
        self.db_name = "xxx"
        self.table_name = "xxx"
        self.db = self.client.get_database(self.db_name)
        self.table = self.db.get_collection(self.table_name)

    def update_mongo(self, task_id, download_url):
        filter = {"id": int(task_id)}
        if len(download_url) > 0:
            newvalues = {"$set": {"status": 2, "download_url": str(download_url).replace("http","https")}}
            self.table.update_one(filter, newvalues)
            print ("filter: %s, values: %s" % (filter,newvalues))
            return True
        elif len(download_url) == 0:
            newvalues = {"$set": {"status": 3}}
            self.table.update_one(filter, newvalues)
            print ("filter: %s, values: %s" % (filter,newvalues))
            return False

    def upload_oss(self, task_id, task_owner, result_path):
        auth = oss2.Auth('xxx', 'xxx')
        bucket = oss2.Bucket(auth, 'xxxm', 'xx')
        oss_filename = "actionlog_result/" + str(task_owner) + "_" + str(task_id) + ".csv"
        bucket.put_object_from_file(oss_filename, result_path)
        headers = dict()
        headers['content-disposition'] = 'attachment'
        url = bucket.sign_url('GET', oss_filename, 15552000)
        return url

if __name__ == '__main__':
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[%s]: start!" % (now))
    if len(sys.argv) == 4:
        task_id = sys.argv[1]
        task_owner = sys.argv[2]
        result_path = sys.argv[3]
        upload = Update()
        download_url = upload.upload_oss(task_id, task_owner, result_path)
        result = upload.update_mongo(task_id, download_url)
        if result:
            print("task succeeded!")
        else:
            print("task failed!")
    else:
        print("invalid arguments!")
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[%s]: finish!" % (now))
