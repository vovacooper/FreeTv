import datetime as dt
from classes.logger import logger
from flask import Blueprint, render_template, Response, request, json
from flask import Blueprint, request, render_template, send_file, Response, redirect, json
from flask import url_for, redirect
from flask.ext.login import login_required, current_user
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from flask.ext.login import current_user, login_user, logout_user

from classes.decorators import ssl_required
from classes.config import VERSION

main_module = Blueprint("main_module", __name__, url_prefix="/main")



@main_module.route('/')
def main():
    return 'ha'


