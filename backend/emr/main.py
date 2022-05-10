#!/usr/bin/env python3
import math
import random
import yfinance as yf
import pandas as pd
from datetime import date, timedelta
from pandas_datareader import data as pdr
from pyspark.sql import SparkSession




def get_trading_signals(data):
	buy_flag = 0
	sell_flag = 0
       
        
	# Hammer
	realbody=math.fabs(data.Open-data.Close)
	bodyprojection=0.3*math.fabs(data.Close-data.Open)

	if data.High >= data.Close and data.High-bodyprojection <= data.Close and data.Close > data.Open and data.Open > data.Low and data.Open - data.Low > realbody:
		buy_flag = 1
	   
	    #print("H", data.Open[i], data.High[i], data.Low[i], data.Close[i])
	    
	# Inverted Hammer
	if data.High > data.Close and data.High -data.Close > realbody and data.Close > data.Open and data.Open >= data.Low and data.Open <= data.Low +bodyprojection:
		buy_flag = 1
	   

	    #print("I", data.Open[i], data.High[i], data.Low[i], data.Close[i])


	# Hanging Man
	if data.High >= data.Open and data.High -bodyprojection <= data.Open and data.Open > data.Close and data.Close > data.Low and data.Close -data.Low > realbody:
		sell_flag = 1

	    #print("M", data.Open[i], data.High[i], data.Low[i], data.Close[i])


	# Shooting Star
	if data.High > data.Open and data.High - data.Open > realbody and data.Open > data.Close and data.Close >= data.Low and data.Close <= data.Low + bodyprojection:
		sell_flag = 1

	    #print("S", data.Open[i], data.High[i], data.Low[i], data.Close[i]) 
            
	return [data["Date"], data["Close"], buy_flag, sell_flag]
            
            
            
            
            
            
            
            
            
            
            

# override yfinance with pandas – seems to be a common step
yf.pdr_override()
# Get stock data from Yahoo Finance – here, asking for about 10 years of Gamestop
# which had an interesting time in 2021: https://en.wikipedia.org/wiki/GameStop_short_squeeze
today = date.today()
decadeAgo = today - timedelta(days=3652)
data = pdr.get_data_yahoo('GME', start=decadeAgo, end=today)
# Other symbols: TSLA – Tesla, AMZN – Amazon, NFLX – Netflix, BP.L – BP
# Add two columns to this to allow for Buy and Sell signals
# fill with zero.
data['Buy']=0
data['Sell']=0
data['Date'] = data.index
minhistory = 101
shots = 80000 

#Create PySpark SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()


#Create PySpark DataFrame from Pandas
sparkDF= spark.createDataFrame(data).rdd


# Find the 4 diffe  rent types of signals – uncomment print statements
# if you want to look at the data these pick out in some another way
newSparkDF = sparkDF.map(get_trading_signals)


output_df = pd.DataFrame(columns=["Date", "Var95", "Var99"])

trading_df = newSparkDF.toDF(['Date', 'Close', 'Buy', 'Sell']).toPandas()

for i in range(minhistory, len(trading_df)):
    if trading_df.Buy[i]==1: # if we were only interested in Buy signals
	    mean=trading_df.Close[i-minhistory:i].pct_change(1).mean()
	    std=trading_df.Close[i-minhistory:i].pct_change(1).std()
	    # generate rather larger (simulated) series with same broad characteristics
	    simulated = [random.gauss(mean,std) for x in range(shots)]
	    # sort, and pick 95% and 99% losses (not distinguishing any trading position)
	    simulated.sort(reverse=True)
	    var95 = simulated[int(len(simulated)*0.95)]
	    var99 = simulated[int(len(simulated)*0.99)]
	    print(var95, var99) # so you can see what is being produced
	    output_df = output_df.append([trading_df.Date[i], var95, var99])
	    
	
output_df.to_csv("/home/comm034/Desktop/cc/spark-3.2.1-bin-hadoop3.2/output.csv")
