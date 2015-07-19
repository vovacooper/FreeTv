import requests
import json
import base64

from tldextract.tldextract import TLDExtract
from urlparse import urlparse
from random import choice
from paramiko import SSHClient, AutoAddPolicy
from decorators import memory_cached
from logger import *
from datetime import date, datetime, timedelta
import MySQLdb as mdb


class Utils:
    _tld_extract = TLDExtract()

    @staticmethod
    def get_domain_from_url(url):
        if not url or url == "":
            return None
        parsed_url = urlparse(url)
        domain = Utils._tld_extract(parsed_url.netloc)
        domain = domain.domain + "." + domain.suffix
        return domain

    @staticmethod
    def get_random(list):
        if not list:
            return list
        return choice(list)

    @staticmethod
    def to_base64(string):
        return base64.urlsafe_b64encode(string)

    @staticmethod
    def from_base64(string):
        if type(string) is unicode:
            string = str(string)
        return base64.urlsafe_b64decode(string)

    @staticmethod
    def execute_remote_commands(hostname, username, password, commands):
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(hostname, username=username, password=password)
        for command in commands:
            stdin, stdout, sterr = ssh.exec_command(command)
            stdout.channel.recv_exit_status()
        ssh.close()

    @staticmethod
    def get_date_table_names(table_name, days_ago):
        """
        :param table_name:the prefix
        :param days_ago: how many days
        :return: [
            table_name_date_1,
            table_name_date_2,
            ...
        ]
        """
        table_names = []
        for x in xrange(days_ago):
            name = "{0}_{1}".format(table_name, str(date.today() - timedelta(days=x)).replace("-", "_"))
            table_names.append(name)
        return table_names

    @staticmethod
    def get_date_range(start_date, end_date):
        """
        Example:
        for single_date in Utils.get_date_range(from_date, to_date):
            print single_date

        :param start_date:
        :param end_date:
        :return:
        """
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    @staticmethod
    def execute_stored_procedure(server, proc_name, params_list=[]):
        connection = server.raw_connection()
        try:
            cursor = connection.cursor(mdb.cursors.DictCursor)
            cursor.callproc(proc_name, params_list)
            results = list(cursor.fetchall())
            cursor.close()
            connection.commit()
            return results
        finally:
            connection.close()
