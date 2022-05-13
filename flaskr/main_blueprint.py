from flask import Blueprint, render_template, request, current_app
import boto3
import json
import botocore
from flask_executor import Executor
import math
import random
import yfinance as yf
import pandas as pd
from datetime import date, timedelta
from pandas_datareader import data as pdr
from io import BytesIO
from threading import Thread
import time


bp = Blueprint('main', __name__)
var95s = [] 
var99s = []
lambda_function_name = "lambdacore"
no_of_resources = None
resource_type = None
emr_clusterid = None

cfg = botocore.config.Config(retries={'max_attempts': 0}, read_timeout=900, connect_timeout=900, region_name="us-east-1" )

lambda_client = boto3.client("lambda", config=cfg)


@bp.route('/')
def initialize():
	return render_template("/initialize.html")
	
	
@bp.route('/terminate', methods=["POST"])
def terminate():
	
	lambda_client = boto3.client("lambda")
	forbiddens = ["LightsailMonitoringFunction", "MainMonitoringFunction"]
	
	#TODO: Check for functions > 50
	lambda_functions = lambda_client.list_functions()
		
	for lambda_function in lambda_functions["Functions"]:
		name = lambda_function["FunctionName"]

		if name in forbiddens:
			continue

		lambda_client.delete_function(FunctionName=name)

	return render_template("/initialize.html")
	
	
@bp.route("/simulate", methods=["POST"])
def simulate():
	price_history = request.form["price_history"]
	shots = request.form["shots"]
	trading_signal = request.form["trading_signal"]

	start_simulation(price_history, shots, trading_signal)
	#create_emr()
	
	
	return render_template("/parameters.html")
	


def write_to_s3(data):
	s3 = boto3.client('s3')
	
	s3buffer = BytesIO()
	
	data.to_csv(s3buffer)
	
	response = s3.put_object(Bucket="cloudcomputingcw", Key="input/trading_signal.csv", Body=s3buffer.getvalue())
	
	print(response)

def get_trading_signals(trading_signal):
	
	yf.pdr_override()
	today = date.today()
	decadeAgo = today - timedelta(days=3652)
	data = pdr.get_data_yahoo('TSLA', start=decadeAgo, end=today)
	data['IsSignal']=0
	data['Date'] = data.index
	
	for i in range(len(data)):
	
		 # Hammer
		 realbody=math.fabs(data.Open[i]-data.Close[i])
		 bodyprojection=0.3*math.fabs(data.Close[i]-data.Open[i])
		 
		 if data.High[i] >= data.Close[i] and data.High[i]-bodyprojection <= data.Close[i] and data.Close[i] > data.Open[i] and data.Open[i] > data.Low[i] and data.Open[i]-data.Low[i] > realbody and trading_signal == "buy":
			 data.at[data.index[i], 'IsSignal'] = 1
			 #print("H", data.Open[i], data.High[i], data.Low[i], data.Close[i])
		 
		 # Inverted Hammer
		 if data.High[i] > data.Close[i] and data.High[i]-data.Close[i] > realbody and data.Close[i] > data.Open[i] and data.Open[i] >= data.Low[i] and data.Open[i] <= data.Low[i]+bodyprojection and trading_signal == "buy":
		 	data.at[data.index[i], 'IsSignal'] = 1
			#print("I", data.Open[i], data.High[i], data.Low[i], data.Close[i])
			
		 # Hanging Man
		 if data.High[i] >= data.Open[i] and data.High[i]-bodyprojection <= data.Open[i] and data.Open[i] > data.Close[i] and data.Close[i] > data.Low[i] and data.Close[i]-data.Low[i] > realbody and trading_signal == "sell":
			 data.at[data.index[i], 'IsSignal'] = 1
			 #print("M", data.Open[i], data.High[i], data.Low[i], data.Close[i])
			 
		 
		 # Shooting Star
		 if data.High[i] > data.Open[i] and data.High[i]-data.Open[i] > realbody and data.Open[i] > data.Close[i] and data.Close[i] >= data.Low[i] and data.Close[i] <= data.Low[i]+bodyprojection and trading_signal == "sell":
			 data.at[data.index[i], 'IsSignal'] = 1
			 #print("S", data.Open[i], data.High[i], data.Low[i], data.Close[i])
		    
	return data


def get_trading_signals_emr(trading_signal, price_history):
	
	yf.pdr_override()
	today = date.today()
	decadeAgo = today - timedelta(days=3652)
	data = pdr.get_data_yahoo('TSLA', start=decadeAgo, end=today)
	data['IsSignal']=0
	data['Date'] = data.index
	
	output_df = pd.DataFrame(columns=["Date", "Std", "Mean"])
	index = 0
	
	for i in range(len(data)):
	
		
		realbody=math.fabs(data.Open[i]-data.Close[i])
		bodyprojection=0.3*math.fabs(data.Close[i]-data.Open[i])

		if ((data.High[i] >= data.Close[i] and data.High[i]-bodyprojection <= data.Close[i] and data.Close[i] > data.Open[i] and data.Open[i] > data.Low[i] and data.Open[i]-data.Low[i] > realbody and trading_signal == "buy") or \
	
		   (data.High[i] > data.Close[i] and data.High[i]-data.Close[i] > realbody and data.Close[i] > data.Open[i] and data.Open[i] >= data.Low[i] and data.Open[i] <= data.Low[i]+bodyprojection and trading_signal == "buy") or \
			
                  (data.High[i] >= data.Open[i] and data.High[i]-bodyprojection <= data.Open[i] and data.Open[i] > data.Close[i] and data.Close[i] > data.Low[i] and data.Close[i]-data.Low[i] > realbody and trading_signal == "sell") or \
		
		  (data.High[i] > data.Open[i] and data.High[i]-data.Open[i] > realbody and data.Open[i] > data.Close[i] and data.Close[i] >= data.Low[i] and data.Close[i] <= data.Low[i]+bodyprojection and trading_signal == "sell")):
		  
			std = data.Close[i-price_history:i].pct_change(1).std()
			mean = data.Close[i-price_history:i].pct_change(1).mean()
			output_df.loc[index] = [data.at[data.index[i], "Date"], std if not math.isnan(std) else 0.0 , mean if not math.isnan(mean) else 0.0 ]
			index += 1
			
		
	return output_df


def listen_output_data():
	s3_client = boto3.client("s3")
	output_key = None
	payload = None
	while True:
		response = s3_client.list_objects_v2(Bucket="cloudcomputingcw", Prefix="output/")
		#print(response)
		
		if "Contents" in response:
		
			contents = response["Contents"]
			
			for content in contents:
			
				key = content["Key"].lower()
				
				if "success" in key:
					continue
					
				output_key = content["Key"] 

				response = s3_client.get_object(Bucket="cloudcomputingcw", Key=key)
				payload = response["Body"].read()
				
				payload = pd.read_csv(BytesIO(payload))
				
				print(payload)
				print("Data captured")
				break
			 
		else:
			print("Not yet")
			time.sleep(5)
	
			

	
def start_simulation(price_history, shots, trading_signal):
	global resource_type
	
	emr_clusterid = get_emr_status()
	
	
	if ((resource_type == "lambda" or resource_type==None) and emr_clusterid == None):
		#Lambda
		
		trading_signals = get_trading_signals(trading_signal)
		trading_signals = trading_signals[["Date", "Close", "IsSignal", "Std", "Mean"]]
		trading_signals.reset_index(drop=True, inplace=True)

		executor = Executor(current_app)
		
		global no_of_resources	
		
		no_of_resources = no_of_resources if no_of_resources != None else 3
		
		futures = executor.map(send_lambda_request, [price_history] * no_of_resources, [shots] * no_of_resources, [trading_signals] * no_of_resources)
			
		for response in futures:
			print(response["Payload"].read())
	
	else:
		#EMR 
		
		trading_signals = get_trading_signals_emr(trading_signal, int(price_history))
		
		emr_client = boto3.client('emr')
		
		write_to_s3(trading_signals)
		
		Steps=[
			{
			    'Name': 'main',
			    'ActionOnFailure': 'CONTINUE',
			    'HadoopJarStep': {
				'Jar': 'command-runner.jar',
				'Args': ["spark-submit","--deploy-mode","cluster", "s3://cloudcomputingcw/emr/main.py", shots]
			    }
			},
		]
		
		
		
		response = emr_client.add_job_flow_steps(
   			 JobFlowId= emr_clusterid,
   			 Steps=Steps)
		
		print(response)
	
	listener_thread = Thread(target=listen_output_data)
	listener_thread.start()
	
	print("Its done")
	
def send_lambda_request(price_history, shots, trading_signals):
	
	
	global lambda_function_name
	
	payload = {
	   "price_history": price_history,
	   "shots": shots,
	   "data": trading_signals.to_json()
	}
	
	response = lambda_client.invoke(FunctionName=lambda_function_name,InvocationType='RequestResponse', Payload=json.dumps(payload).encode('utf-8'))
	return response
	
	
@bp.route("/initialize", methods=["POST"])
def initialize_form():
	global resource_type
	global no_of_resources
	resource_type = request.form["resource-type"]
	
	no_of_resources = int(request.form["no_of_resources"])
	
	print(resource_type)
	print(no_of_resources)
	
	if resource_type == "lambda":
		create_lambda()
	elif resource_type == "emr":
		create_emr(no_of_resources)
	
	return render_template("/initialize.html")
		

def create_lambda():
	lambda_client = boto3.client("lambda")
	
	response = lambda_client.create_function(
	  FunctionName = lambda_function_name,
	  Role = "arn:aws:iam::580126642516:role/LabRole",
	  Code = dict(ImageUri="580126642516.dkr.ecr.us-east-1.amazonaws.com/lambdacore:1.0"),
	  PackageType = "Image",
	  Timeout = 360)
		
	print(response)


def get_emr_status():
	emr_client = boto3.client('emr')
	
	cluster = emr_client.list_clusters(ClusterStates=["WAITING","RUNNING"])
	
	#Assert only one cluster
	return cluster["Clusters"][0]["Id"] if len(cluster["Clusters"]) != 0 else None


def create_emr(no_of_resources):

	emr_client = boto3.client('emr')
	instance_groups = []
	instance_groups.append({
                'Name': 'master_node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm4.large',
                'InstanceCount': 1,
                
        })
		
	instance_groups.append({
                'Name': 'worker_node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm4.large',
                'InstanceCount': no_of_resources - 1,
                
         })
		
	 
	global emr_clusterid
	
	
	emr_clusterid = emr_client.run_job_flow(
	      Name = "trading_risk_simulator_emrcluster",
	      Instances = {
	         "InstanceGroups": instance_groups,
	         "KeepJobFlowAliveWhenNoSteps": True
	      },
	      LogUri = "s3://cloudcomputingcw/emr/logs",
	      ServiceRole="EMR_DefaultRole",
	      JobFlowRole="EMR_EC2_DefaultRole",
	      ReleaseLabel="emr-6.6.0",
	      Applications= [{
	         Name:"Spark",
	         Version: "3.2.0"
	      
	      
	      }])
	     
	      
	
	print("EMR Startup Done!!")
	print(emr_clusterid)
	
	emr_clusterid = emr_clusterid["JobFlowId"]
	
	
	
	
	
@bp.route('/parameters')
def parameters():
	return render_template("/parameters.html")
	
@bp.route('/output')
def output_dashboard():
	return render_template("/output.html", var95s=var95s, var99s=var99s)
	
@bp.route('/audit')
def audit_dashboard():
	return render_template("/initialize.html")
