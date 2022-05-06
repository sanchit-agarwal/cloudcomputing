from flask import Blueprint, render_template, request
import boto3

bp = Blueprint('main', __name__)

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
		  Code = dict(ImageUri="580126642516.dkr.ecr.us-east-1.amazonaws.com/hello-world:latest"),
		  PackageType = "Image"
		)
		
		print(response)


	
@bp.route('/parameters')
def parameters():
	return render_template("/initialize.html")
	
@bp.route('/output')
def output_dashboard():
	return render_template("/initialize.html")
	
@bp.route('/audit')
def audit_dashboard():
	return render_template("/initialize.html")
