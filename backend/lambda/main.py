import sys

def lambda_handler(event, context):
	print("Hello peaches!! " + sys.version) 
	return {
	   "message": "Hello Bitches"
	}
