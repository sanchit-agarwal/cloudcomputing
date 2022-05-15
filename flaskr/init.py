from flask import Flask
from . import main_blueprint


app = Flask(__name__)
app.secret_key = b'_5dfgtrdf45345^73@#4'
app.register_blueprint(main_blueprint.bp)
