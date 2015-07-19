from classes.logger import logger
from classes.utils import Utils
from flask_login import current_user
from uuid import uuid4
from classes.mysql import *
import random
from datetime import datetime, timedelta
#import datetime
import json




class BaseProvider():
    def __init__(self, request_data):
        self._request_data = request_data
        self._response_data = \
            {
                "uid": ""
            }




if __name__ == "__main__":
    rp = BaseProvider({})

