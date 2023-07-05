#!/usr/bin/env python
# coding: utf-8

# In[1]:


import matplotlib.pylab as plt
import pandas as pd
import numpy as np
from causalimpact import CausalImpact


# In[2]:


data=pd.read_csv('data_test.csv')


# In[3]:


data['launch_rate']=data['session_launch']/data['uv']
data['post_rate']=data['detail_post']/data['uv']
data['expose_rate']=data['real_expose']/data['uv']
data['score_rate']=data['score']/data['uv']



# In[4]:


data


# In[8]:


#duration_time
x_all_train0=data[data['live_days_cate']=='右龄0～30'].pivot(index='p_date',columns='exp_group',values='duration_time')
x_all_train0.columns=[str(s1) for s1 in  x_all_train0.columns.tolist() ]
x_all_train0.reset_index(inplace=True)
pre_period=[0,29]
post_period=[30,43]

ci=CausalImpact(x_all_train0[['双列实验组','单列实验组']].loc[0:43,:],pre_period,post_period)
ci.run()
print(ci.summary())
ci.plot()


# In[10]:


#实际实验组对照组时间趋势

plt.plot(x_all_train0['p_date'],x_all_train0['双列实验组'],color='black')
plt.plot(x_all_train0['p_date'],x_all_train0['单列实验组'],color='red')



# In[ ]:





# In[12]:


#duration_time
#数据准备
x_all_train0=data[data['live_days_cate']=='右龄30+'].pivot(index='p_date',columns='exp_group',values='duration_time')
x_all_train0.columns=[str(s1) for s1 in  x_all_train0.columns.tolist() ]
x_all_train0.reset_index(inplace=True)
#时间周期说明pre_period 实验前  post_period：实验后
pre_period=[0,29]
post_period=[30,43]
#CausalImpact package 说明:https://storage.googleapis.com/pub-tools-public-publication-data/pdf/41854.pdf
ci=CausalImpact(x_all_train0[['双列实验组','单列实验组']].loc[0:43,:],pre_period,post_period)
ci.run()
print(ci.summary())
ci.plot()


# In[13]:


plt.plot(x_all_train0['p_date'],x_all_train0['双列实验组'],color='black')
plt.plot(x_all_train0['p_date'],x_all_train0['单列实验组'],color='red')

