import datetime as dt
from classes.logger import logger
from flask import Blueprint, render_template, Response, request, json
from flask import Blueprint, request, render_template, send_file, Response, redirect, json
from flask import url_for, redirect
from flask.ext.login import login_required, current_user
from datetime import datetime, timedelta
from flask.ext.login import current_user, login_user, logout_user

from classes.decorators import ssl_required
from classes.config import VERSION

from providers.api_provider import ApiProvider

api_module = Blueprint("api_module", __name__, url_prefix="/api")


@api_module.route('/shows/<id>')
def api_shows(id):
    request_data = request.args

    api_provider = ApiProvider(request_data)  # get data from provider
    response_data = api_provider.get_show(id)
    # make json
    response_json = json.dumps(response_data)

    return response_json

@api_module.route('/shows')
def api_shows_id():
    genre = request.args.get('genre', None)
    alphabet = request.args.get('alphabet', None)
    request_data = request.args

    api_provider = ApiProvider(request_data)  # get data from provider
    response_data = api_provider.get_shows()
    # make json
    response_json = json.dumps(response_data)

    return response_json

