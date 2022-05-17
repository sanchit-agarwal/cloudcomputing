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
from datetime import datetime

bp = Blueprint('main', __name__)
lambda_function_name = "lambdacore"
no_of_resources = None
resource_type = None
emr_clusterid = None

cfg = botocore.config.Config(retries={'max_attempts': 0}, read_timeout=900, connect_timeout=900)

lambda_client = boto3.client("lambda", config=cfg)
s3_client = boto3.client("s3", config=cfg)
emr_client = boto3.client("emr", config=cfg)

@bp.route('/')
def initialize():
	return render_template("/initialize.html")
	
	
@bp.route('/terminate', methods=["POST"])
def terminate():
	
	emr_clusterid = get_emr_status()
	
	if emr_clusterid != None:
		#emr_client = boto3.client("emr")
		emr_client.terminate_job_flows(JobFlowIds=[emr_clusterid])
		print("EMR Cluster with JobFlow ID {0} tasked for termination".format(emr_clusterid))
		flash("EMR Cluster with JobFlow ID {0} tasked for termination".format(emr_clusterid))
		
	else:
		flash("No EMR Cluster found")

	return render_template("/initialize.html")
	
	
@bp.route("/simulate", methods=["POST"])
def simulate():
	price_history = request.form["price_history"]
	shots = request.form["shots"]
	trading_signal = request.form["trading_signal"]

	start_simulation(price_history, shots, trading_signal)	
	flash("Simulation Task sent to scalable services")
	
	return render_template("/parameters.html")
	
		
@bp.route('/parameters')
def parameters():
	return render_template("/parameters.html")
	
@bp.route('/output')
def output_dashboard():

	data = get_data_fromS3()
	
	if data is not None and len(data.values.tolist()) > 0:
		data = data.values.tolist()
	else:
		data = [[]]
		flash("Data not in S3 Bucket")
	
	return render_template("/output.html", data=data)
	
@bp.route('/audit')
def audit_dashboard():

	audits = get_audits()
	return render_template("/audit.html", data=audits)
	
	
@bp.route("/reset", methods=["POST"])
def reset():
	reset_analysis()
	flash("Previous risk calculations have been deleted")
	return render_template("/parameters.html")

	
@bp.route("/initialize", methods=["POST"])
def initialize_form():
	global resource_type
	global no_of_resources
	
	resource_type = request.form["resource-type"]
	no_of_resources = int(request.form["no_of_resources"])
	
	if resource_type == "emr":
		create_emr(no_of_resources)
	else:
		flash("Lambda functions are ready")
		
	return render_template("/initialize.html")
		

def get_audits():
	#s3_client = boto3.client("s3")
	
	response = s3_client.get_object(Bucket="cloudcomputingcw", Key="audit/log.csv")
	payload = response["Body"].read()
	payload = pd.read_csv(BytesIO(payload))
	
	return payload.values.tolist()

def write_to_s3(data, path):
	#s3 = boto3.client('s3')
	s3buffer = BytesIO()
	data.to_csv(s3buffer, index=False)
	response = s3_client.put_object(Bucket="cloudcomputingcw", Key=path, Body=s3buffer.getvalue())
	

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
	

def reset_analysis():
	#s3_client = boto3.client("s3")
	
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


def listen_output_data(price_history, shots, trading_signal):
	#s3_client = boto3.client("s3")
	output_key = None
	csv_files = []
	
	#Taking into account that each resource might write CSV files at different times
	unique_csv_files = set()
	max_tries = 10
	
	starttime = datetime.now()
	
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
				time.sleep(1)
				 
		else:
			print("No objects found in S3 Bucket yet")
			time.sleep(5)
	
	print("Read {0} objects".format(len(csv_files)))
	
	finaltime = int((datetime.now()  - starttime).total_seconds())
	
	capture_compute_cost(price_history, shots, trading_signal, unique_csv_files, finaltime)


def capture_compute_cost(price_history, shots, trading_signal, unique_csv_files, finaltime):

	#s3_client = boto3.client("s3")
	
	#Get average risk values
	
	csv_files = []
	isOutputFromLambda = True
	
	for output_key in unique_csv_files:
		
		response = s3_client.get_object(Bucket="cloudcomputingcw", Key=output_key)
		payload = response["Body"].read()
		
		payload = pd.read_csv(BytesIO(payload))
		csv_files.append(payload)
		
		print("{0} found in S3 Bucket. Adding....".format(output_key))
		
		if "part" in output_key:
			isOutputFromLambda = False
					
	meanVar95 = meanVar99 = 0.0	
													
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
		
		meanVar95 = mean(output_df["Var95"])
		meanVar99 = mean(output_df["Var99"])
		
		
	#Concatenate the files into one CSV	
	else:
		output_df = pd.DataFrame(columns=["Date", "Var95", "Var99"])
		
		for csv in csv_files:
			output_df = pd.concat([output_df, csv[["Date", "Var95", "Var99"]]])
			#print(output_df)
						
					
		meanVar95 = mean(output_df["Var95"])
		meanVar99 = mean(output_df["Var99"])
		
	global no_of_resources
	global resource_type
	
	log_string = "Number of Resources: {0} Signal Type: {1} Price History: {2} Shots: {3} Average 95% Confidence: {4} Average 99% Confidence: {5}".format(no_of_resources, trading_signal, price_history, shots, meanVar95, meanVar99)
	               
	
	write_log("Compute", resource_type, finaltime, log_string)
	
	
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
		
		#emr_client = boto3.client('emr')
		
		write_to_s3(trading_signals, "input/trading_signal.csv")
		
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
	
	listener_thread = Thread(target=listen_output_data, args=(price_history, shots, trading_signal,))
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
	
	return response
	
	

def get_emr_status(clusterstates=["WAITING","RUNNING","STARTING"]):
	#emr_client = boto3.client('emr')
	
	cluster = emr_client.list_clusters(ClusterStates=clusterstates)
	
	return cluster["Clusters"][0]["Id"] if len(cluster["Clusters"]) != 0 else None
	
	
def capture_startup_time(clusterid):
	starttime = datetime.now()
	
	status = None
	
	#It takes around 7-8 minutes for the EMR cluster to startup
	time.sleep(360)
	
	print("Audit Listener Thread awake from sleep. Now Listening")
	
	while status == None and (int((datetime.now() - starttime).total_seconds()) < 720):
		status = get_emr_status(["RUNNING", "WAITING"])
		print("EMR cluster not detected")
		time.sleep(2)
	
	print("EMR Cluster found")
	
	final_time = int((datetime.now() - starttime).total_seconds())
	write_log("Initialize", "EMR", final_time)


def write_log(category, service_type, time, log_string=" "):

	#s3_client = boto3.client("s3")
	auditFile = None
	
	#Get log file from S3
	response = s3_client.list_objects_v2(Bucket="cloudcomputingcw", Prefix="audit/log")
				
	if "Contents" in response:
		response = s3_client.get_object(Bucket="cloudcomputingcw", Key="audit/log.csv")
		
		payload = response["Body"].read()
		auditFile = pd.read_csv(BytesIO(payload))
		auditFile.reset_index(drop=True)
		#print(auditFile)
		
		
	#No Audit logfile created, so create a new one
	else:
		auditFile = pd.DataFrame(columns=["creation_date", "category", "resource_type", "time", "log_string"])
	
	
	new_data = pd.DataFrame([[datetime.now(), category, service_type, time, log_string]],
				 columns=["creation_date", "category", "resource_type", "time", "log_string"])
	
	auditFile = pd.concat([auditFile, new_data])
	auditFile.reset_index(drop=True)
	
	write_to_s3(auditFile, "audit/log.csv")




def create_emr(no_of_resources):

	emr_clusterid = get_emr_status()
	
	if emr_clusterid != None:
		flash("EMR Cluster already running. Can't create a new one.")
		return

	#emr_client = boto3.client('emr')
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
	     
	      
	emr_clusterid = emr_clusterid["JobFlowId"]
	
	
	audit_thread = Thread(target=capture_startup_time, args=(emr_clusterid,))
	audit_thread.start()
	
	
	print("EMR Cluster with JobFlow ID {0} tasked for creation".format(emr_clusterid))
	flash("EMR Cluster with JobFlow ID {0} tasked for creation".format(emr_clusterid))
	
	


def get_data_fromS3():

	#s3_client = boto3.client('s3')

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
			return output_df
			
			
		#Concatenate the files into one CSV	
		else:
			output_df = pd.DataFrame(columns=["Date", "Var95", "Var99"])
			
			for csv in csv_files:
				output_df = pd.concat([output_df, csv[["Date", "Var95", "Var99"]]])
				#print(output_df)
							
						
			output_df["AVG_Var95"] = mean(output_df["Var95"])
			output_df["AVG_Var99"] = mean(output_df["Var99"])
			output_df.sort_values(by='Date', inplace=True)
			
			return output_df
	else:
		flash("No data found in S3 Bucket")
		return None
	

