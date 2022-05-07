from flask import Blueprint, render_template, request
import boto3
import json
import botocore

bp = Blueprint('main', __name__)
var95s = [] 
var99s = []


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

	await start_simulation()
	
	return render_template("/parameters.html")
	
	
	
async def start_simulation():
	#TODO check for "pending" event
	lambda_client = boto3.client("lambda", config=cfg)
	forbiddens = ["LightsailMonitoringFunction", "MainMonitoringFunction"]
	
	#TODO: Check for functions > 50
	lambda_functions = lambda_client.list_functions()	
	
	lambda_function_urls = []
	
	print("Started")
	
	for lambda_function in lambda_functions["Functions"]:
		name = lambda_function["FunctionName"]

		if name in forbiddens:
			continue
			
		response = lambda_client.invoke(FunctionName=name,InvocationType='RequestResponse')
		
		print(response)
		
		response_payload = json.loads(response['Payload'].read())
		
		var95s.append(response_payload["var95s"])
		var99s.append(response_payload["var99s"])
	
	print("Its done")
	
	
@bp.route("/initialize", methods=["POST"])
def initialize_form():
	resource_type = request.form["resource-type"]
	no_of_resources = request.form["no_of_resources"]
	
	print(resource_type)
	print(no_of_resources)
	
	create_lambda(int(no_of_resources))	
	
	return render_template("/initialize.html")
		

def create_lambda(no_of_resources):
	lambda_client = boto3.client("lambda")
	
	for i in range(no_of_resources):
		function_name = "lambdacore_" + str(i)
	
		response = lambda_client.create_function(
		  FunctionName = function_name,
		  Role = "arn:aws:iam::580126642516:role/LabRole",
		  Code = dict(ImageUri="580126642516.dkr.ecr.us-east-1.amazonaws.com/lambda_core:1.0"),
		  PackageType = "Image",
		  Timeout = 360
		)
		
		print(response)


	
@bp.route('/parameters')
def parameters():
	return render_template("/parameters.html")
	
@bp.route('/output')
def output_dashboard():
	return render_template("/output.html", var95s=var95s, var99s=var99s)
	
@bp.route('/audit')
def audit_dashboard():
	return render_template("/initialize.html")
