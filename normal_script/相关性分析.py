# !/usr/bin/python
# -*-coding: utf-8 -*-

from odps import ODPS
import pandas as pd
import numpy as np
from scipy.stats import pearsonr
from scipy.spatial.distance import euclidean
from scipy.spatial.distance import cosine
from sklearn.preprocessing import StandardScaler

def get_data():

    AccessKeyID = 'x'
    AccessKeySecret = 'x'
    odps_db = ODPS(AccessKeyID, AccessKeySecret, 'x')

    sql = """
    SELECT p_date,
           ROUND(SUM(stay1_user)/SUM(active_user),4) AS 次留,
           ROUND(SUM(duration)/SUM(active_user),2) AS 人均时长,
           ROUND(SUM(session_launch)/SUM(active_user),2) AS 人均启动次数,
           ROUND(SUM(expose)/SUM(active_user),2) AS 人均刷贴,
           ROUND(SUM(detail_post)/SUM(active_user),2) AS 人均详情次数,
           ROUND(SUM(view_post_dur)/SUM(active_user),2) AS 人均详情时长,
           ROUND(SUM(like_post)/SUM(active_user)*1000,2) AS 千人帖子赞,
           ROUND(SUM(dislike_post)/SUM(active_user)*1000,2) AS 千人帖子踩,
           ROUND(SUM(create_post)/SUM(active_user)*1000,2) AS 千人发帖,
           ROUND((SUM(create_review)+SUM(reply_review))/SUM(active_user)*1000,2) AS 千人评论
    FROM pipi_bigdata.app_user_bhv_di_pp
    WHERE p_date BETWEEN '2021-01-03' AND '2022-03-27'
    AND channel IN ('xiaomi','other')
    AND user_type = 'dau'
    GROUP BY p_date;
    """
    with odps_db.execute_sql(sql).open_reader() as reader:
        record_list = list()
        for record in reader:
            record_list.append(dict(record))
    content = pd.DataFrame(record_list)
    return content

def ZscoreNormalization(x):
    x_new = []
    x_mean = np.mean(x)
    x_std = np.std(x)
    for i in range(len(x)):
        i_new = (x[i] - x_mean) / x_std
        x_new.append(i_new)
    return x_new

def get_correlation_similarity(data):
    result = "消费指标与次留相关性系数: " + "\n"
    columns_list = data.columns.values.tolist()
    for i in columns_list:
        if i != 'p_date' and i != '次留':
            x1 = data['次留'].values.tolist()
            y1 = data[i].values.tolist()
            x1_n = ZscoreNormalization(x1)
            y1_n = ZscoreNormalization(y1)
            corr,p = pearsonr(x1_n, y1_n)
            result += str(i) + " 相关系数： " + str("%.2f" % corr) + " P值： " + str("%.2f" % p) + "\n"
        else:
            continue
    return result

def main():
    data = get_data()
    result = get_correlation_similarity(data)
    print(result)

if __name__ == '__main__':
    main()