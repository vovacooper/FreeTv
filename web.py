import os
import newrelic.agent
import os.path


APP_PATH = os.path.dirname(os.path.realpath(__file__))
from urlparse import urlparse
from classes.logger import logger
from flask import Flask, render_template, json, send_file, redirect, request, g, Response
from flask import flash, url_for, session

from flask.ext.compress import Compress
from datetime import timedelta
from classes.config import KEY

from modules.main_module import main_module



class CustomFlask(Flask):
    jinja_options = Flask.jinja_options.copy()
    jinja_options.update(dict(
        block_start_string='<%',
        block_end_string='%>',
        variable_start_string='%%',
        variable_end_string='%%',
        comment_start_string='<#',
        comment_end_string='#>',
    ))


app = CustomFlask(__name__)
# app = Flask(__name__)

app.jinja_env.autoescape = False


'''
Blueprints
'''

app.register_blueprint(main_module)

'''
Config
'''
app.config["SECRET_KEY"] = KEY
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=730)
app.config["SESSION_COOKIE_NAME"] = "impo_admin"

app.config["WTF_CSRF_ENABLED"] = False

'''
Login
'''

from flask.ext.login import LoginManager, login_required


login_manager = LoginManager()
login_manager.login_view = "/login"
login_manager.session_protection = None
login_manager.init_app(app)

Compress(app)



@app.route("/")
def index():
    return render_template("index.html")



@app.errorhandler(400)
def bad_request(error):
    return json.dumps({"error": str(error)}), 400


@app.errorhandler(401)
def unauthorized(error):
    return json.dumps({"error": str(error)}), 401


@app.errorhandler(404)
def not_found(error):
    return json.dumps({"error": str(error)}), 404


@app.errorhandler(500)
def server_error(error):
    logger.error(error)
    return json.dumps({"error": str(error)}), 500




'''
Main
'''
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
