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
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.metrics import roc_auc_score
AccessKeyID = 'x'
AccessKeySecret = 'x'
odps_db = ODPS(AccessKeyID, AccessKeySecret, 'x')

def get_data():
    sql = """
            SELECT member_type,
                   age,
                   gender,
                   live_days_mid,
                   real_expose,
                   ROUND(nvl(detail_post,0),6) AS detail_post,
                   ROUND(NVL(detail_post_dur,0),6) AS detail_post_dur,
                   active_days,
                   NVL(detail_post_good,0) AS detail_post_good,
                   result
            FROM bigdata_tmp.tangyongjun_model
            ;
          """
    print(sql)
    with odps_db.execute_sql(sql).open_reader() as reader:
        record_list = list()
        for record in reader:
            record_list.append(dict(record))
    content = pd.DataFrame(record_list)
    return content

x = get_data()
x_new = x.drop(columns='result')
y = x['result']

x_new_train,x_new_test,y_train,y_test = train_test_split(x_new,y,test_size=0.2,random_state=1)
model = LogisticRegression()
model.fit(x_new_train,y_train)

y_pred = model.predict(x_new_test)
y_pred[:20]
y_pred_proba = model.predict_proba(x_new_test)
y_pred_proba[:5]

accuracy = accuracy_score(y_pred,y_test)
model.coef_
model.intercept_

roc_auc_score(y_test,y_pred_proba[:,1])
