from config import IMPOSPACE_CONNECTION_STRNIG, STATS_CONNECTION_STRING
from sqlalchemy import *
from datetime import datetime
from classes.logger import logger


"""make a singleton connector to DB, there is no need for connecting more then once"""


class MySqlFactory:
    _instance = None

    def __new__(class_, *args, **kwargs):
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)
        return class_._instance

    impospace_server = create_engine(IMPOSPACE_CONNECTION_STRNIG, echo=False, convert_unicode=True)
    stats_server = create_engine(STATS_CONNECTION_STRING, echo=False, convert_unicode=True)

    """constructor"""

    def __init__(self):
        """
        dummy call for all tables to they will be created.
        """
        self.get_sites_table()
        self.get_revshare_table()
        self.get_users_table()
        # self.get_network_stats_table()
        self.get_network_aggregated_table()

    """destructor"""
    '''def __del__(self):
        self.impospace_db_connection.close()
        self.stats_db_connection.close()'''

    @staticmethod
    def get_sites_table():
        try:
            table = Table("sites", MetaData(bind=MySqlFactory.stats_server), autoload=True)
        except Exception:
            table = Table("sites", MetaData(bind=MySqlFactory.stats_server),
                          Column("id", Integer, primary_key=True),
                          Column("site_id", String(255), nullable=True),
                          Column("url", String(255), nullable=True),
                          Column("is_active", Boolean, nullable=True),
                          Column("date_created", DateTime(timezone=False), nullable=True, default=datetime.utcnow()),
                          mysql_engine="InnoDB")
            table.create(checkfirst=True)
        return table

    @staticmethod
    def get_revshare_table():
        try:
            table = Table("revshare", MetaData(bind=MySqlFactory.stats_server), autoload=True)
        except Exception:
            table = Table("revshare", MetaData(bind=MySqlFactory.stats_server),
                          Column("id", Integer, primary_key=True),
                          Column("site_id", String(255), nullable=True),
                          Column("user", String(255), nullable=True),
                          Column("revshare", Float, nullable=True),
                          Column("date_created", DateTime(timezone=False), nullable=True, default=datetime.utcnow()),
                          mysql_engine="InnoDB")
            table.create(checkfirst=True)
        return table

    @staticmethod
    def get_users_table():
        try:
            table = Table("users", MetaData(bind=MySqlFactory.stats_server), autoload=True)
        except Exception:
            table = Table("users", MetaData(bind=MySqlFactory.stats_server),
                          Column("id", Integer, primary_key=True),
                          Column("name", String(255), nullable=True),
                          Column("email", String(255), nullable=True),
                          Column("password", String(255), nullable=True),
                          Column("is_active", Boolean, nullable=True),
                          Column("is_admin", Boolean, nullable=True),
                          Column("date_created", DateTime(timezone=False), nullable=True, default=datetime.utcnow()),
                          mysql_engine="InnoDB")
            table.create(checkfirst=True)
        return table

    @staticmethod
    def get_network_stats_table(network_name):
        if type(network_name) is str:
            table_name = "network_" + network_name
        else:
            return None

        try:
            table = Table(table_name, MetaData(bind=MySqlFactory.stats_server), autoload=True)
        except Exception:
            table = Table(table_name, MetaData(bind=MySqlFactory.stats_server),
                          Column("id", Integer, primary_key=True),
                          Column("site_id", String(255), nullable=True),
                          Column("impressions", Integer, nullable=True),
                          Column("clicks", Integer, nullable=True),
                          Column("ctr", Float, nullable=True),
                          Column("revenue", Float, nullable=True),
                          Column("ecpm", Float, nullable=True),
                          Column("date_created", DateTime(timezone=False), nullable=True, default=datetime.utcnow()),
                          mysql_engine="InnoDB")
            table.create(checkfirst=True)
        return table

    @staticmethod
    def get_network_data_table():
        table_name = 'network_data'
        try:
            table = Table(table_name, MetaData(bind=MySqlFactory.stats_server), autoload=True)
        except Exception:
            logger.exception("there is no network_data table. please provide one!!!")
        return table

    @staticmethod
    def get_network_aggregated_table():
        table_name = "network_aggregated"
        try:
            table = Table(table_name, MetaData(bind=MySqlFactory.stats_server), autoload=True)
        except Exception:
            table = Table(table_name, MetaData(bind=MySqlFactory.stats_server),
                          Column("id", Integer, primary_key=True),
                          Column("site_id", String(255), nullable=True),

                          Column("total_page_views", Integer, nullable=True),
                          Column("adblocked_page_views", Integer, nullable=True),

                          Column("pops_impressions", Integer, nullable=True),
                          Column("pops_revenue", Float, nullable=True),
                          Column("pops_ecpm", Float, nullable=True),

                          Column("display_impressions", Integer, nullable=True),
                          Column("display_clicks", Integer, nullable=True),
                          Column("display_ctr", Float, nullable=True),
                          Column("display_revenue", Float, nullable=True),
                          Column("display_ecpm", Float, nullable=True),

                          Column("total_revenue", Float, nullable=True),

                          Column("date_created", DateTime(timezone=False), nullable=True, default=datetime.utcnow()),
                          mysql_engine="InnoDB")
            table.create(checkfirst=True)
        return table


"""
Create all tables in the DB!
"""
MySqlFactory()