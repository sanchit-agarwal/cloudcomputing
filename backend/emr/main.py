#!/usr/bin/env python3
import random
from pyspark.sql import SparkSession
import sys
import math


def get_risk_values(row):

    std = float(row[1])
    mean = float(row[2])

    simulated = [random.gauss(mean,std) for x in range(shots)]
    simulated.sort(reverse=True)
    
    var95 = simulated[int(len(simulated)*0.95)]
    var99 = simulated[int(len(simulated)*0.99)]
    
    return [row[0], var95, var99]
            
            
            
if __name__ == "__main__":
    
    shots = int(sys.argv[1])
    chunks = int(sys.argv[2])
    
    #Create PySpark SparkSession
    with SparkSession.builder.appName("risk_simulator").getOrCreate() as spark:
        sparkDF = spark.read.option("header", True).csv("s3://cloudcomputingcw/input/trading_signal.csv")
        
        sparkDF = sparkDF.repartition(chunks)
        
        outputRDD = sparkDF.rdd.map(get_risk_values)
        outputDF = outputRDD.toDF(["Date", "Var95", "Var99"])
        
        outputDF.write.mode("overwrite").option("header", "true").csv("s3://cloudcomputingcw/output")
    
