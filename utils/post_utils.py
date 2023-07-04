# -*- coding: utf-8 -*-
from odps import ODPS
import pandas as pd
from datetime import *
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import os
import xlsxwriter
import numpy as np

def send_email(header,file,file_name,receiver,date):

    sender=''
    to=receiver.split(',')
    mail_host='smtp.exmail.qq.com'
    mail_user=''
    mail_pas=''

    m= MIMEMultipart()
    text="Hi:\n\n    附件中是%s的各渠道订单与激活数据,有问题联系tangyongjun2014@xiaochuankeji.cn\n\n顺颂\n时祺\n\n"%(str(date))
    text=MIMEText(text)
    m.attach(text)
    m["from"]=sender
    m["To"]=receiver
    m['subject']=Header(header,"utf-8")

    fujian = MIMEText(open(file,'rb').read(),'base64','utf-8')
    fujian["Content-Type"] = 'application/octet-stream'
    fujian.add_header("Content-Disposition","attachment",filename=('utf-8',"",file_name))
    m.attach(fujian)
    smtp = smtplib.SMTP(mail_host)
    print(smtp)
    smtp.login(mail_user, mail_pas)
    smtp.sendmail(sender,to,m.as_string())
    print('OK')

if __name__ == '__main__':
    start_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    xiaochuan_list = 'zhangyao2014@xiaochuankeji.cn,sunhexin2014@xiaochuankeji.cn,zhoujunhou2014@xiaochuankeji.cn,chenhui2014@xiaochuankeji.cn,tangyongjun2014@xiaochuankeji.cn'
    kol_list = 'kol@adxmax.com'
    columns = ['日期', 'cid', '总订单量', '新用户订单量', '老用户订单量', '新用户激活量']
    record_list = list()
    data = pd.DataFrame(record_list)
    file = '/home/work/tangyongjun/%s的kol会员数据.xlsx' % (start_date)
    file_name = '%s订单与激活数据.xlsx' % (str(start_date))
    header = '%s订单与激活数据' % (str(start_date))
    with pd.ExcelWriter(file) as writer:
        data.to_excel(writer, sheet_name='订单与激活数据', index=False, encoding='unicode', engine='xlsxwriter',
                            columns=columns, header=columns)
    send_email(header, file, file_name, xiaochuan_list+kol_list, start_date)