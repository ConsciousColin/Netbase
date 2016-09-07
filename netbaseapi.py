import requests
import json
import pandas as pd
import time
from datetime import datetime, timedelta
import re

"""

This is a series of functions I've written to pull data from Netbase in python, currently the primary focus has been on timeseries data for multiple topics/themes. It relies heavily on the pandas DataFrame object to store and manipulate data. 

NB = NetBase
"""

def HelloWorld (User, Password):
	request_string = "https://api.netbase.com:443/cb/insight-api/2/helloWorld?language=English"
	response = requests.get(request_string, auth=(User, Password))
	print response.text.get("message")

def checkcreds(User, Password):
	"""
	Returns boolean True/False depending on succesfully make a hello world call 
	"""
	request_string = "https://api.netbase.com:443/cb/insight-api/2/helloWorld?language=English"
	response = requests.get(request_string, auth=(User, Password))
	return response.__bool__()
	
def CurrentRate(User, Password):
	request_string = "https://api.netbase.com:443/cb/insight-api/2/profile"
	response = requests.get(request_string, auth=(User, Password))
	respJSON = json.loads(response.text)
	return pd.DataFrame(respJSON.get("rateLimits"))
	
def getThemes (themes, User, Password):
	request_string = "https://api.netbase.com:443/cb/insight-api/2/themes?scope=USER"
	response = requests.get(request_string, auth=(User, Password))
	jData = json.loads(response.text)
	IDlist = [j.get('themeId') for j in jData if j.get('name') in themes]
	IDdict = dict(zip(themes,IDlist))
	if len(IDdict)>0:	
		return IDdict
	else:
		raise ValueError("themes don't exist")
			

def requestassembly (topic, themeid, rtimeperiod="30d",rtimeunit = "Day", rtype = "TotalBuzz"):
	"""
	Designed to be called within the Volumeovertime function, builds the NB Requests
	"""
	if themeid != "Total": #check if pulling just data for a theme or total
		request_string = "https://api.netbase.com:443/cb/insight-api/2/metricValues?topics=" + topic +"&metricSeries=" + type +"&datetimeISO=true&pretty=true&timeUnits=" + timeunit + "&smoothing=false&themeIds="+themeid + "&precision=LOW&realTime=false&timePeriod="+ timeperiod+ "&timePeriodOffset=0m"
	if themeid == "Total":
		request_string = "https://api.netbase.com:443/cb/insight-api/2/metricValues?topics=" + topic +"&metricSeries=" + type +"&datetimeISO=true&pretty=true&timeUnits=" + timeunit + "&smoothing=false&precision=LOW&realTime=false&timePeriod="+ timeperiod+ "&timePeriodOffset=0m"
	return request_string
	
def Volumeovertime(topic, User, Password,vtimeperiod="30d", vtimeunit = "Day", themeid="Total", vtype = "TotalBuzz"):
	"""
	Processes a single timeseries query to NB, desiegned to be used in conjunction with timeprocessing
	"""
	the_request = requestassembly(topic, themeid, rtimeperiod=vtimeperiod, rtimeunit = vtimeunit, rtype = vtype)
	response = requests.get(the_request, auth=(User,Password))
	jData = json.loads(response.text).get("metrics")[0]
	return jData


def timeprocessing(jsonData, dataframe,colname):
	"""
	Takes the result of a Volumeovertime function, adds a date column to the dataframe
	"""
	if "Date" in dataframe.columns:
		datethere = True
	else:
		datethere = False
	for i in range(len(jsonData.get("columns"))):
		if datethere == False:
			dataframe.loc[i,"Date"] = jsonData.get("columns")[i]
		dataframe.loc[i,colname] = jsonData.get("dataset")[0].get("set")[i]

def multivolume(topics, themes, days, User, Password, mv_type="TotalBuzz", mv_timeunit="Day"):
	"""
	- Iterates over multiple topics/themes and returns a pandas dataframe of results
	- Both topics & themes must be lists, if you don't want any themes then initialize with an empty list
	- days is the number of days from present
	- due to the rate limits we can only make one request a minute
	- mv_type is the metric you want to pull defaults to pulling mentions
	- mv_timeunit is the time period to pull by i.e. Day, Week, Month
	"""
	
	#print "the data will be pulled by ~" + '{:%H:%M}'.format(datetime.now() + timedelta(minutes = (len(topics)*(len(themes)+1.15))))
	dataframe = pd.DataFrame()
	if len(themes)>0:
		themeIDs = getThemes (themes, User, Password)
	else: themeIDs = "Total"
	for topic in topics:
		timeprocessing(
			Volumeovertime(topic, User, Password, vtimeperiod = str(days)+"d", vtype= mv_type),
			dataframe,
			topic+"-Total"
			)
		time.sleep(60) #this is to stay under the NB rate limits 
		if type(themeIDs)==dict:
			for theme in themeIDs:
					timeprocessing(
							Volumeovertime(topic,User, Password, vtimeperiod = str(days)+"d", themeid = themeIDs[theme],vtimeunit =mv_timeunit, vtype= mv_type),
							dataframe,
							topic+"-"+theme
							)
					time.sleep(60)
			time.sleep(60)
	dataframe['Date']=pd.to_datetime(dataframe['Date']) #Format into readable dates 
	return dataframe


def calcperc(dataframe):
	"""
	Takes the result of a multi-volume query and calculates the % of total for all themes for a given topic 
	"""
	cols = dataframe.columns
	tcols = [column for column in cols if "Total" in column]
	for tcol in tcols:
		topic = re.search(".*?(?=-Total)",tcol).group(0)
		for col in cols:
			if topic in col and "Total" not in col:
				dataframe[col+"_%"] = dataframe[col]/dataframe[tcol]


"""
"https://api.netbase.com:443/cb/insight-api/2/metricValues?topics=" + topic + "&metricSeries=" type + "&timeUnits="+ Unit + "&smoothing=false&publishedDate=" from_date + "&publishedDate=" + to_date +"&precision=LOW&realTime=false&timePeriodOffset=0m&timePeriodRounding=1m"				

"""