#!/usr/bin/env python3
import math
import random
import yfinance as yf
import pandas as pd
from datetime import date, timedelta
from pandas_datareader import data as pdr
# override yfinance with pandas – seems to be a common step
yf.pdr_override()
# Get stock data from Yahoo Finance – here, asking for about 10 years of Gamestop
# which had an interesting time in 2021: 
#https://en.wikipedia.org/wiki/GameStop_short_squeeze
today = date.today()
decadeAgo = today - timedelta(days=3652)
data = pdr.get_data_yahoo('GME', start=decadeAgo, end=today)
# Other symbols: TSLA – Tesla, AMZN – Amazon, NFLX – Netflix, BP.L – BP
# Add two columns to this to allow for Buy and Sell signals
# fill with zero
data['Buy']=0
data['Sell']=0

# Find the 4 different types of signals – uncomment print statements
# if you want to look at the data these pick out in some another way
for i in range(len(data)):
    # Hammer
    realbody=math.fabs(data.Open[i]-data.Close[i])
    bodyprojection=0.3*math.fabs(data.Close[i]-data.Open[i])
    if data.High[i] >= data.Close[i] and data.High[i]-bodyprojection <= data.Close[i] and data.Close[i] > data.Open[i] and data.Open[i] > data.Low[i] and data.Open[i]-data.Low[i] > realbody:
        data.at[data.index[i], 'Buy'] = 1
        #print("H", data.Open[i], data.High[i], data.Low[i], data.Close[i])
    # Inverted Hammer
    if data.High[i] > data.Close[i] and data.High[i]-data.Close[i] > realbody and data.Close[i] > data.Open[i] and data.Open[i] >= data.Low[i] and data.Open[i] <= data.Low[i]+bodyprojection:
        data.at[data.index[i], 'Buy'] = 1
        #print("I", data.Open[i], data.High[i], data.Low[i], data.Close[i])
    # Hanging Man
    if data.High[i] >= data.Open[i] and data.High[i]-bodyprojection <= data.Open[i] and data.Open[i] > data.Close[i] and data.Close[i] > data.Low[i] and data.Close[i]-data.Low[i] > realbody:
        data.at[data.index[i], 'Sell'] = 1
        #print("M", data.Open[i], data.High[i], data.Low[i], data.Close[i])
    # Shooting Star
    if data.High[i] > data.Open[i] and data.High[i]-data.Open[i] > realbody and data.Open[i] > data.Close[i] and data.Close[i] >= data.Low[i] and data.Close[i] <= data.Low[i]+bodyprojection:
        data.at[data.index[i], 'Sell'] = 1
        #print("S", data.Open[i], data.High[i], data.Low[i], data.Close[i])


# Now have signals, so if they have the minimum amount of historic data can generate
# the number of simulated values (shots) needed in line with the mean and standard
# deviation of the that recent history
minhistory = 101
shots = 80000


for i in range(minhistory, len(data)):
    # if we were only interested in Buy signals
    if data.Buy[i]==1: 
    
               mean=data.Close[i-minhistory:i].pct_change(1).mean()
               std=data.Close[i-minhistory:i].pct_change(1).std()
               
               # generate rather larger (simulated) series with same broad characteristics
               simulated = [random.gauss(mean,std) for x in range(shots)]
               
               # sort, and pick 95% and 99% losses (not distinguishing any trading position)
               simulated.sort(reverse=True)
               
               var95 = simulated[int(len(simulated)*0.95)]
               var99 = simulated[int(len(simulated)*0.99)]
               
               print(var95, var99) # so you can see what is being produce
