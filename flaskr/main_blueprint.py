from flask import Blueprint, render_template

bp = Blueprint('main', __name__)

@bp.route('/')
def initialize():
	return render_template("/initialize.html")
	
@bp.route('/parameters')
def parameters():
	return render_template("/initialize.html")
	
@bp.route('/output')
def output_dashboard():
	return render_template("/initialize.html")
	
@bp.route('/audit')
def audit_dashboard():
	return render_template("/initialize.html")
