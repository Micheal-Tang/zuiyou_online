# !/usr/bin/python
# -*-coding: utf-8 -*-

from dingtalkchatbot.chatbot import DingtalkChatbot
import requests
import json

def ding_talk(message, mobiles=None):
    url_real = 'xx'
    dingrobot = DingtalkChatbot(url_real)
    dingrobot.send_text(message, at_mobiles=mobiles)

def dingding_monitor_zhouhui(monitor_content,robot_url):
    print('dingding monitor')
    payload = {'msgtype': 'markdown',
               'markdown':
                   {
                       'title': '今天又是周一么?',
                       'text': monitor_content
                   }
               }
    print(type(payload))
    print(payload)
    payload = json.dumps(payload)
    print(type(payload))
    headers = {'Content-type': 'application/json'}
    requests.post(robot_url, data=payload, headers=headers)