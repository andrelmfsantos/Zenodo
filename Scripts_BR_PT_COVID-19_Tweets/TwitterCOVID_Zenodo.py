#!/usr/bin/env python
# coding: utf-8

# # [Zenodo](https://zenodo.org/deposit/5073680) dataset:: Brazilian Portuguese COVID-19 Tweets.

# __Author__:  André L.M.F.Santos - andrelmfsantos@gmail.com

# ## (A) 1$^{°}$ Dataframe::split list of symptoms on Twitter into multiple lines.

# In[1]:


# Required packages ****************************************************************************************************
import pandas as pd
import numpy as np
import os
import time
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
#***********************************************************************************************************************


# In[2]:


# Load csv in dask format **********************************************************************************************
os.chdir("C:/Users/andre/OneDrive/Pesquisas/ANDRE_TESE_COVID/TeseDados/TwitterCOVID_Zenodo")
start = time.time()
#---------------------------------------------------
ddf = dd.read_csv('Brazil_Portuguese_COVID19_Tweets2019.csv')
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 1 second
ddf.head()
#***********************************************************************************************************************


# In[3]:


# Convert dask dataframe to pandas dataframe ***************************************************************************
start = time.time()
#---------------------------------------------------
df = ddf.compute()
df.sort_values('nsymptoms', ascending=False)
print(len(df))
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 243 seconds
df.head()
#***********************************************************************************************************************


# In[4]:


# Split the symptom list into multiple lines with 1 single symptom per line and use the date as the ID *****************
start = time.time()
#---------------------------------------------------
ref_rows = pd.DataFrame(df.symptoms.str.split(',').tolist(),index=df.date).stack()
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 137 seconds
ref_rows.head()
#************************************************************************************************************************


# In[5]:


# Subset with "date", "symptoms", and "n" columns ***********************************************************************
start = time.time()
#---------------------------------------------------
ref = pd.DataFrame(ref_rows)
ref = ref.reset_index([0,'date'])
ref = ref.rename(columns={'date':'date', 0:'symptoms'})
ref['symptoms'] = ref['symptoms'].str.lstrip() # remove blanck spaces
ref['n'] = 1
#---------------------------------------------------
# End count time
end = time.time()
print('rows:',len(ref))
print("Loading Time = {}".format(end-start)) # time: 17 seconds
ref.head()
#************************************************************************************************************************


# In[6]:


# Subset with new columns: "year", "month", "day ************************************************************************
start = time.time()
#---------------------------------------------------
ref['year'] = pd.DatetimeIndex(ref['date']).year
ref['month'] = pd.DatetimeIndex(ref['date']).month
ref['day'] = pd.DatetimeIndex(ref['date']).day
ref = ref.drop(columns=['date'])
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 109 seconds
ref
#************************************************************************************************************************


# In[7]:


# Group subset by ['symptoms','year','month','day'] colums **************************************************************
start = time.time()
#---------------------------------------------------
gk = ref.groupby(['symptoms','year','month','day'])['n'].sum().reset_index(name='n')
#gk = ref.groupby(['date','symptoms']).count().reset_index(name='n')
gk = gk.sort_values(by='n', ascending = False)
print('rows:',len(gk))
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 6 seconds
gk.head(10)
#************************************************************************************************************************


# ## (B) 2$^{°}$ Dataframe::initialize an empty datatime dataframe, then filling it.

# In[8]:


# Symptoms list *********************************************************************************************************
start = time.time()
#---------------------------------------------------
gklist = gk.symptoms.unique().tolist()
gklist.sort()
gk_count_unique = gk['symptoms'].nunique()
print(gklist)
print(type(gklist))
print('total sym:',gk_count_unique)
#---------------------------------------------------
# End count time
end = time.time()
print('rows:',len(gk))
print("Loading Time = {}".format(end-start)) # time: 1 second
#************************************************************************************************************************


# In[9]:


# Fill dataframe with symptoms list ************************************************************************************
start = time.time()
#---------------------------------------------------
date1 = '2019-01-01'
date2 = '2021-06-30'
mydate = pd.date_range(date1, date2).tolist()
md = pd.DataFrame({'date':mydate})
md['symptoms'] = ",".join(gklist)
#---------------------------------------------------
# End count time
end = time.time()
print('rows:',len(md))
print("Loading Time = {}".format(end-start)) # time: 1 second
md
#************************************************************************************************************************


# In[10]:


# Split the symptom list into multiple lines with 1 single symptom per line and use the date as the ID *****************
#import time
start = time.time()
#---------------------------------------------------
mdsym = pd.DataFrame(md.symptoms.str.split(',').tolist(),index=md.date).stack()
mdsym = mdsym.reset_index([0,'date'])
mdsym = mdsym.rename(columns={'date':'date',0:'symptoms'})
print(len(mdsym))
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 1 second
mdsym
#************************************************************************************************************************


# In[11]:


# Subset with new columns: "year", "month", "day ************************************************************************
start = time.time()
#---------------------------------------------------
mdsym['year'] = pd.DatetimeIndex(mdsym['date']).year
mdsym['month'] = pd.DatetimeIndex(mdsym['date']).month
mdsym['day'] = pd.DatetimeIndex(mdsym['date']).day
#mdsym = mdsym.drop(columns = ['date'])
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 1 second
mdsym
#************************************************************************************************************************


# ## (C) 3$^{°}$ Dataframe::merge dataframes & write csv file

# In[12]:


# Merge dataframes *************************************************************************************************
start = time.time()
#---------------------------------------------------
result = pd.merge(mdsym, gk, how="outer", on=["year","month", "day", "symptoms"])
print('mdsm:',len(mdsym))
print('gk:',len(gk))
print('result:',len(result))
print('NANs:',result["n"].isna().sum())
result['n'] = result['n'].fillna(0)
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 1 second
result
#************************************************************************************************************************


# In[13]:


# Write csv file *******************************************************************************************************
os.chdir("C:/Users/andre/OneDrive/Pesquisas/ANDRE_TESE_COVID/TeseDados/Dask_CSVs")
#import time
start = time.time()
#---------------------------------------------------
result.to_csv('TwitterTimeSeries2019.csv', index=False, header = True)
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 4 seconds
#**********************************************************************************************************************


# ## (D) 4$^{°}$ Statistics & Plots

# In[14]:


# Required packages ****************************************************************************************************
import itertools
#import numpy as np
import matplotlib.pyplot as plt
#warnings.filterwarnings("ignore")
plt.style.use('fivethirtyeight')
#import pandas as pd
import statsmodels.api as sm
import matplotlib
#**********************************************************************************************************************


# In[15]:


# Time series range  ***************************************************************************************************
start = time.time()
#---------------------------------------------------
print('Start:',result['date'].min())
print('End:  ',result['date'].max())
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 1 second
#**********************************************************************************************************************


# In[16]:


# Subset time series ***************************************************************************************************
start = time.time()
#---------------------------------------------------
ds = result.copy()
ds = ds.loc[ds['symptoms'] == 's21']
#---------------------------------------------------
print('Rows before:', len(ds))
ds['date'] = pd.to_datetime(ds['date'])
ds = ds[(ds['date'] >= '2019-01-01') & (ds['date'] <= '2019-12-31')]
print('Rows after:', len(ds))
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 1 second
ds
#**********************************************************************************************************************


# In[17]:


# Data preprocessing ****************************************************************************************************
#import time
start = time.time()
#---------------------------------------------------
ts= ds[['date','n']]
ts = ts.sort_values('date')
ts = ts.groupby(['date'])['n'].sum().reset_index(name='n')
ts = ts.set_index('date')
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 1 second
ts
#**********************************************************************************************************************


# In[18]:


# Summary *******************************************************************************************************
start = time.time()
#---------------------------------------------------
stats = result.groupby(['symptoms'])['n'].sum().reset_index(name='n')
stats = stats.sort_values(by=['n'], ascending = False)
stats.rename(columns = {'n':'frequence'}, inplace = True)
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 1 second
stats.head(10)
#**********************************************************************************************************************


# In[19]:


# Plot timeseries ******************************************************************************************************
#import time
start = time.time()
#---------------------------------------------------
y = ts['n'].resample('MS').mean()
#y = ts
#---------------------------------------------------
y.plot(figsize=(15, 6))
plt.show()
#---------------------------------------------------
# End count time
end = time.time()
print("Loading Time = {}".format(end-start)) # time: 5 seconds
#**********************************************************************************************************************


# __References:__
# 
# * [Zenodo Dataset](https://zenodo.org/record/5073680#.YQkoXY5KiM8)
# * [Merge df](https://pandas.pydata.org/pandas-docs/stable/user_guide/merging.html)
# * [Groupby](https://www.geeksforgeeks.org/python-pandas-dataframe-groupby/)
# * [Range](https://stackoverflow.com/questions/5868130/generating-all-dates-within-a-given-range-in-python/26583750)
# * [Repeat rows](https://www.datasciencemadesimple.com/repeat-or-replicate-the-dataframe-in-pandas-python/)
# * [Removing blank spaces](https://stackoverflow.com/questions/41476150/removing-space-from-columns-in-pandas/41476181#41476181)
# * [Timeseries](https://towardsdatascience.com/an-end-to-end-project-on-time-series-analysis-and-forecasting-with-python-4835e6bf050b)
# * [Visualization Pandas](https://pandas.pydata.org/pandas-docs/stable/user_guide/visualization.html)
