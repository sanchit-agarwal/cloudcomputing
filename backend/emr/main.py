#!/usr/bin/env python3
import random
from datetime import date, timedelta
from pyspark.sql import SparkSession
import sys




price_history = int(sys.argv[1])
shots = int(sys.argv[2])


def get_risk_values(iterator):

    for i in range(price_history, len(iterator)):

        if int(iterator.IsSignal[i]) == 1: 

            mean= iterator.Close[i-price_history:i].pct_change(1).mean()
            std= iterator.Close[i-price_history:i].pct_change(1).std()

            simulated = [random.gauss(mean,std) for x in range(shots)]

            simulated.sort(reverse=True)
            var95 = simulated[int(len(simulated)*0.95)]
            var99 = simulated[int(len(simulated)*0.99)]
            yield [iterator.Date[i], var95, var99]
            
            
#Create PySpark SparkSession
with SparkSession.builder.appName("risk_simulator").getOrCreate() as spark:
    sparkDF = spark.read.csv("s3://cloudcomputingcw/input/trading_signal.csv")

    #TODO: Not parallelized
    pandasDF = sparkDF.toPandas()
    outputDF = pd.DataFrame(columns=["Date", "var95", "var99"])
    
    index = 0
    
    for i in range(price_history, len(pandasDF)):

        if int(pandasDF.IsSignal[i]) == 1: 

            mean= pandasDF.Close[i-price_history:i].pct_change(1).mean()
            std= pandasDF.Close[i-price_history:i].pct_change(1).std()

            simulated = [random.gauss(mean,std) for x in range(shots)]

            simulated.sort(reverse=True)
            var95 = simulated[int(len(simulated)*0.95)]
            var99 = simulated[int(len(simulated)*0.99)]
            
            outputDF.loc[index] = [pandasDF.Date[i], var95, var99]
            index += 1
            
    
    print(outputDF)
    outputDF.to_csv("s3://cloudcomputingcw/output/output.csv")
    
    
    
    
    
    #outputDF = sparkDF.mapInPandas(get_risk_values, sparkDF.schema)
    #outputDF.show()
    
    #output_df.to_csv("s3://cloudcomputingcw/output/output.csv")







