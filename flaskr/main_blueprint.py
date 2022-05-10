from flask import Blueprint, render_template, request, current_app
import boto3
import json
import botocore
from flask_executor import Executor


bp = Blueprint('main', __name__)
var95s = [] 
var99s = []
lambda_function_name = "LambdaCore"
no_of_resources = None


cfg = botocore.config.Config(retries={'max_attempts': 0}, read_timeout=900, connect_timeout=900, region_name="us-east-1" )


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
async def simulate():
	price_history = request.form["price_history"]
	shots = request.form["shots"]
	trading_signal = request.form["trading_signal"]


	print(price_history)
	print(shots)
	print(trading_signal)

	await start_simulation(price_history, shots, trading_signal)
	#create_emr()
	
	
	return "Success"
	
	
	
def start_simulation(price_history, shots, trading_signal):

















	#Lambda
	#TODO check for "pending" event
	lambda_client = boto3.client("lambda", config=cfg)

	lambda_url = lambda_client.list_function_url_configs(FunctionName=lambda_function_name)
	
	executor = Executor(current_app)
	global no_of_resources	
	no_of_resources = no_of_resources if no_of_resources != None else 3
	
	futures = executor.map(send_lambda_request, [lambda_url] * no_of_resources, [price_history] * no_of_resources, [shots] * no_of_resources, [trading_signal] * no_of_resources)
		
	for response in futures:
		print(response)
	
	
	print("Its done")
	return 0
	
def send_lambda_request(url, price_history, shots, trading_signal):
	lambda_client = boto3.client("lambda", config=cfg)
	payload = {
	   "price_history": price_history,
	   "shots": shots,
	   "trading_signal": trading_signal
	}
	
	response = lambda_client.invoke(FunctionName=lambda_function_name,InvocationType='Event', Payload=json.dumps(payload).encode('utf-8'))
	print(response)
	return response
	
	
@bp.route("/initialize", methods=["POST"])
def initialize_form():
	resource_type = request.form["resource-type"]
	global no_of_resources
	
	no_of_resources = int(request.form["no_of_resources"])
	
	print(resource_type)
	print(no_of_resources)
	
	create_lambda()	
	
	return render_template("/initialize.html")
		

def create_lambda():
	lambda_client = boto3.client("lambda")
	
	response = lambda_client.create_function(
	  FunctionName = lambda_function_name,
	  Role = "arn:aws:iam::580126642516:role/LabRole",
	  Code = dict(ImageUri="580126642516.dkr.ecr.us-east-1.amazonaws.com/lambda_core:1.0"),
	  PackageType = "Image",
	  Timeout = 360)
		
	print(response)



def create_emr(no_of_resources):

	emr_client = boto3.client('emr')
	instance_groups = []
	instance_groups.append(InstanceGroup(
	    num_instance=1,
	    role="MASTER",
	    type="m4.large",
	    market="ON_DEMAND",
	    name="Master Node"
	))
	instance_groups.append(InstanceGroup(
	    num_instance=no_of_resources-1,
	    role="CORE",
	    type="m4.large",
	    market="ON_DEMAND",
	    name="Worker Node"
	))
		
	
	
	clusterid = emr_client.run_job_flow(
	      "Trading Risk Simulation",
	      instance_groups = instance_groups,
	      action_on_failure="TERMINATE_CLUSTER",
	      log_uri = "s3://cloudcomputingcw/emr/logs/",
	      Steps = [
	          {
	             "Name": "Step 1",
	             "ActionOnFailure": "TERMINATE_CLUSTER",
	             "HadoopJarStep": {
	                 "Args": [
	                      "spark-submit",
	                      "--deploy-mode", "cluster",
	                      "--archives", "s3://cloudcomputingcw/emr/env.zip",
                              "s3://cloudcomputingcw/emr/main.py"
	                 ],
	                 "Jar": "command-runner.jar"
	             }
	          }
	       ]) 
	
	
	print(clusterid)
	
	
	
	
@bp.route('/parameters')
def parameters():
	return render_template("/parameters.html")
	
@bp.route('/output')
def output_dashboard():
	return render_template("/output.html", var95s=var95s, var99s=var99s)
	
@bp.route('/audit')
def audit_dashboard():
	return render_template("/initialize.html")
