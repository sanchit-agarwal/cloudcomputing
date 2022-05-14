from flask import Blueprint, render_template, request, current_app, flash
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
from statistics import mean


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
	
	emr_clusterid = get_emr_status()
	
	if emr_clusterid != None:
		emr_client = boto3.client("emr")
		emr_client.terminate_job_flows(JobFlowIds=[emr_clusterid])
		print("EMR Cluster with JobFlow ID {0} tasked for termination".format(emr_clusterid))
		
	else:
		flash("No EMR Cluster found")

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
			
			#No use of such trading signal 
			if math.isnan(std) or math.isnan(mean):
				continue
			
			output_df.loc[index] = [data.at[data.index[i], "Date"], std, mean ]
			index += 1
			
		
	return output_df
	
	
@bp.route("/reset", methods=["POST"])
def reset():
	reset_analysis()
	return render_template("/parameters.html")

def reset_analysis():
	s3_client = boto3.client("s3")
	
	object_keys = []
	
	try:

		response = s3_client.list_objects_v2(Bucket="cloudcomputingcw", Prefix="output/")
		
		#print(response)
			
		if "Contents" in response:
			contents = response["Contents"]
			for content in contents:
				key = content["Key"].lower()				
				output_key = content["Key"] 
				object_keys.append(output_key)
		
		
		delete = [{"Key": x} for x in object_keys]
		
		response = s3_client.delete_objects(Bucket="cloudcomputingcw", Delete={"Objects": delete})
		#print(response)
		print("{0} objects cleaned".format(len(object_keys)))
		
		
	except botocore.exceptions.ClientError:
		print("No Objects found in S3 bucket to delete")


def listen_output_data():
	s3_client = boto3.client("s3")
	output_key = None
	csv_files = []
	
	#Taking into account that each resource might write CSV files at different times
	unique_csv_files = set()
	max_tries = 10
	
	
	while max_tries != 0:
		response = s3_client.list_objects_v2(Bucket="cloudcomputingcw", Prefix="output/")
		
		if "Contents" in response:
		
			contents = response["Contents"]
			
			for content in contents:
			
				output_key = content["Key"] 				
				
				if "success" in output_key.lower():
					continue

				
				if output_key in unique_csv_files:
					break_loop = True			
					continue
					
				else:
					response = s3_client.get_object(Bucket="cloudcomputingcw", Key=output_key)
					payload = response["Body"].read()
					
					payload = pd.read_csv(BytesIO(payload))
					csv_files.append(payload)
					
					unique_csv_files.add(output_key)
					print("{0} found in S3 Bucket. Adding....".format(output_key))
					break_loop = False
			
			if break_loop:
				max_tries -= 1
				 
		else:
			print("No objects found in S3 Bucket yet")
			time.sleep(5)
	
	print("Read {0} objects".format(len(csv_files)))

	
def start_simulation(price_history, shots, trading_signal):
	global resource_type
	
	emr_clusterid = get_emr_status()
	
	global no_of_resources	
	
	if no_of_resources == None:
			flash("No of Resources not defined. Please submit the Initalize form again")
			print("No of resources not defined")
			return
			
	
	# Remove existing data
	cleanup_thread = Thread(target=reset_analysis)
	cleanup_thread.start()
	
	
	if resource_type == "emr" and emr_clusterid == None:
		flash("EMR Cluster still Initializing. Wait for some time and then try again")
		return 
	
	# If EMR Cluster is up and running, then that will be preferred over Lambda
	if ((resource_type == "lambda" or resource_type==None) and emr_clusterid == None):
		#Lambda
		
		trading_signals = get_trading_signals(trading_signal)
		trading_signals = trading_signals[["Date", "Close", "IsSignal"]]
		trading_signals.reset_index(drop=True, inplace=True)

		executor = Executor(current_app)
		
		lambda_ids = range(no_of_resources)
		
		executor.map(send_lambda_request, [price_history] * no_of_resources, [shots] * no_of_resources, [trading_signals] * no_of_resources, lambda_ids)
			
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
				'Args': ["spark-submit","--deploy-mode","cluster", "s3://cloudcomputingcw/emr/main.py", shots, str(no_of_resources)]
			    }
			},
		]
		
		
		
		response = emr_client.add_job_flow_steps(
   			 JobFlowId= emr_clusterid,
   			 Steps=Steps)
		
		print(response)
	
	
	#Prevent Race Condition on Cleanup Thread with Listener Thread by waiting for CleanupThread to finish
	cleanup_thread.join()
	
	listener_thread = Thread(target=listen_output_data)
	listener_thread.start()
	
	print("Its done")
	
def send_lambda_request(price_history, shots, trading_signals, lambda_id):
	
	global lambda_function_name
	
	payload = {
	   "price_history": price_history,
	   "shots": shots,
	   "data": trading_signals.to_json(),
	   "lambda_id": lambda_id
	}
	
	response = lambda_client.invoke(FunctionName=lambda_function_name,InvocationType='Event', Payload=json.dumps(payload).encode('utf-8'))
	#print(response)
	return response
	
	
@bp.route("/initialize", methods=["POST"])
def initialize_form():
	global resource_type
	global no_of_resources
	resource_type = request.form["resource-type"]
	
	no_of_resources = int(request.form["no_of_resources"])
	
	print(resource_type)
	print(no_of_resources)
	
	#if resource_type == "lambda":
		#create_lambda()
	if resource_type == "emr":
		create_emr(no_of_resources)
	
	return render_template("/initialize.html")
		

def create_lambda():
	lambda_client = boto3.client("lambda")
	
	response = lambda_client.create_function(
	  FunctionName = lambda_function_name,
	  Role = "arn:aws:iam::580126642516:role/LabRole",
	  Code = dict(ImageUri="580126642516.dkr.ecr.us-east-1.amazonaws.com/lambda_core:2.0"),
	  PackageType = "Image",
	  Timeout = 360)
		
	print(response)


def get_emr_status():
	emr_client = boto3.client('emr')
	
	cluster = emr_client.list_clusters(ClusterStates=["WAITING","RUNNING","STARTING"])
	
	#Assert only one cluster
	return cluster["Clusters"][0]["Id"] if len(cluster["Clusters"]) != 0 else None


def create_emr(no_of_resources):

	emr_clusterid = get_emr_status()
	
	if emr_clusterid != None:
		flash("EMR Cluster already running. Can't create a new one.")
		return

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
	         "Name":"Spark"
	      }])
	     
	      
	
	print("EMR Startup Done!!")
	#print(emr_clusterid)
	
	emr_clusterid = emr_clusterid["JobFlowId"]
	


def get_data_fromS3():

	s3_client = boto3.client('s3')

	response = s3_client.list_objects_v2(Bucket="cloudcomputingcw", Prefix="output/")
	csv_files = []
	
	#Flag variable for defining if the data is from Lambda or EMR, based on filename
	#This will determine whether to average the values or combine them
	isOutputFromLambda = True
		
	if "Contents" in response:
	
		contents = response["Contents"]
		
		for content in contents:
		
			output_key = content["Key"] 
			
			if "success" in output_key.lower():
				continue

			if "part" in output_key:
				isOutputFromLambda = False
		
			response = s3_client.get_object(Bucket="cloudcomputingcw", Key=output_key)
			payload = response["Body"].read()
			
			payload = pd.read_csv(BytesIO(payload))
			csv_files.append(payload)
			
			print("{0} found in S3 Bucket. Adding....".format(output_key))
				
				
				
										
		#Average the values	
		if isOutputFromLambda:
			
			output_df = pd.DataFrame(columns=["Date", "Var95", "Var99", "AVG_Var95", "AVG_Var99"])
			index = 0
			max_length = len(csv_files[0])
			while(max_length > index):				
				var95 = []
				var99 = []
				
				for csv in csv_files:
					Date = csv.at[index, "Date"]
					var95.append(csv.at[index, "var95"])
					var99.append(csv.at[index, "var99"])	
								
				output_df.loc[index] = [Date, mean(var95), mean(var99), 0, 0]
				index += 1
			
			output_df["AVG_Var95"] = mean(output_df["Var95"])
			output_df["AVG_Var99"] = mean(output_df["Var99"])
			print(output_df)
			return output_df
			
			
		#Append the files into one CSV	
		else:
			output_df = pd.DataFrame(columns=["Date", "Var95", "Var99"])
			
			for csv in csv_files:
				output_df = pd.concat([output_df, csv[["Date", "Var95", "Var99"]]])
				print(output_df)
							
						
			output_df["AVG_Var95"] = mean(output_df["Var95"])
			output_df["AVG_Var99"] = mean(output_df["Var99"])
			output_df.sort_values(by='Date', inplace=True)
			
			print(output_df)
			return output_df
	else:
		flash("No Risk data in S3 Bucket")
		return None
	
	
	
@bp.route('/parameters')
def parameters():
	return render_template("/parameters.html")
	
@bp.route('/output')
def output_dashboard():


	data = get_data_fromS3()
	
	return render_template("/output.html", data=data.values.tolist())
	
@bp.route('/audit')
def audit_dashboard():
	return render_template("/initialize.html")
