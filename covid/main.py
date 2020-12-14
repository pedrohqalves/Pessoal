#!/usr/bin/env python
# coding: utf-8

# In[1]:

def main(event,context):


	import pandas as pd
	import gcsfs
	gcsfs.GCSFileSystem(project='teste-covid',token='cloud')


	# In[2]:


	# Getting the source file from github

	table_confirmed = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')
	table_deaths = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
	table_recovered = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv')


	# In[3]:


	#Stacking the dataframe so we dont increase the amount of columns everyday, instead increase rows and maintain schema

	table_confirmed_final = table_confirmed.set_index(['Country/Region', 'Province/State','Lat','Long']).stack().reset_index()
	table_deaths_final = table_deaths.set_index(['Country/Region', 'Province/State','Lat','Long']).stack().reset_index()
	table_recovered_final = table_recovered.set_index(['Country/Region', 'Province/State','Lat','Long']).stack().reset_index()


	# In[4]:


	#Renaming the columns

	table_confirmed_final = table_confirmed_final.rename(columns={'level_4':'date',0:'confirmed'})
	table_deaths_final = table_deaths_final.rename(columns={'level_4':'date',0:'deaths'})
	table_recovered_final = table_recovered_final.rename(columns={'level_4':'date',0:'recovered'})


	# In[5]:


	# Merging everything into one dataframe

	table_final = pd.merge(table_confirmed_final,table_deaths_final, how = 'left', on = ['Country/Region','Province/State','Lat','Long','date'])
	table_final = pd.merge(table_final,table_recovered_final, how = 'left', on = ['Country/Region','Province/State','Lat','Long','date'])


	# In[6]:


	#Treating NaN values, which will only occur in deaths and recovered

	table_final.recovered = table_final.recovered.fillna(0)
	table_final.recovered = table_final.recovered.apply(lambda x: int(x))
	table_final.deaths = table_final.deaths.fillna(0)
	table_final.deaths = table_final.deaths.apply(lambda x: int(x))
	table_final = table_final.rename(columns={"Country/Region":"country", "Province/State":"state", "Lat":"lat","Long":"long"})


	# In[ ]:


	from datetime import datetime
	table_final.date = table_final.date.apply(lambda x: pd.to_datetime(x).date())


	# In[14]:


	# Exporting the result to Cloud Storage
	table_final = table_final.set_index('country')
	table_final.to_csv('gs://teste-pedroalves/teste-covid/covid_dataset.csv')

