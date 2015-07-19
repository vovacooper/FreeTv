import unittest
import requests
import time
from flask import json
from random import randint
from datetime import datetime


URL_BASE = "http://127.0.0.1:4000/ad-srv/performance/{0}"


class PerformanceTester():
    def get_users(self):
        params = \
            {
                "site_id": 1,
                "campaign_id": 6,
                "from_date": "2015-06-29",
                "to_date": "2015-06-29",
                "order_by": "total_rev",
                "order_type": "desc",
                "limit": 100,
                "offset": 0
            }
        result = requests.post(URL_BASE.format("get-site-campaign"), data=json.dumps(params))
        result_json = result.json()
        print result_json


if __name__ == "__main__":
    testr = PerformanceTester()
    testr.get_users()

