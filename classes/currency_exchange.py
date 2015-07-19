import requests
import json
import datetime

from datetime import timedelta

from decorators import redis_cached
from logger import *
from sql_tables.services_tables import *
from datetime import datetime

CURRENCY_COMMISSION = 0.03


class CurrencyConverter():

    @classmethod
    def _upsert_currency_to_db(cls, currency, rate):
        """
        updates currency to coefficient if exists. if not exists creates that currency
        :param currency:
        :param coefficient:
        :return:
        """
        try:
            currency = currency.upper()
            table_name, table = ServicesTables.ensure_currency_exchange_rates_table()

            query = text("select * from {0} where currency = '{1}'".format(table_name, currency))
            response_db = list(ServicesTables.server.execute(query))

            if not response_db:
                insert_obj = \
                    {
                        "currency": currency,
                        "rate": rate,
                        "date_modified": datetime.utcnow()
                    }
                table.insert().values(insert_obj).execute()
            else:
                query = text("UPDATE {0} SET rate = {1},date_modified='{3}' WHERE currency = '{2}'"
                             .format(table_name, rate, currency, datetime.utcnow()))
                ServicesTables.server.execute(query)

        except Exception, e:
            logger.exception("Cant upsert currency {0} to DB.".format(currency))

    @classmethod
    def update_currency_to_db(cls):
        """
        updates currency exchange rates to currency_exchange_rates table
        where the base is USD

        the table:
            currency | rate | date_modified
               EUR   | 0.89 | -------
               XXX   | 0.12 | -------

        Example:
        to convert EUR to USD just
        USD = currency_in_euro/0.89
        """
        try:
            url = 'http://api.fixer.io/latest?base=USD'
            r = requests.get(url)
            api_data = json.loads(r.text)

            currency_rates = api_data['rates']

            for currency, rate in currency_rates.iteritems():
                CurrencyConverter._upsert_currency_to_db(str(currency), rate)

        except Exception, e:
            logger.exception("Cant get currency from service: 'http://fixer.io/' !!!!!!")

    @classmethod
    #@redis_cached(24 * 60 * 60)
    def get_currency_coefficients(cls):
        """
        :return:
        {
            AUD: 1.2895,
            BGN: 1.7434,
            ...
        }
        """
        try:
            table_name, table = ServicesTables.ensure_currency_exchange_rates_table()

            query = text("select * from {0}".format(table_name))
            response_db = list(ServicesTables.server.execute(query))

            if not response_db:
                logger.error("'response_db' in None, check what is going on with the DB!!!")
                CurrencyConverter.update_currency_to_db()
                return {}

            response = {}
            for db_row in response_db:
                response_dict = dict(db_row)
                response[response_dict['currency']] = response_dict['rate']

                delta_time = datetime.utcnow() - response_dict['date_modified']
                if delta_time > timedelta(hours=24):
                    CurrencyConverter.update_currency_to_db()
                    # recursive call for updated data
                    return CurrencyConverter.get_currency_coefficients()

            return response
        except Exception, e:
            logger.exception("cant get currency from DB")
            return {}

    @classmethod
    def convert(cls, from_currency, amount):
        """
        converts the convert_currency to USD

        :param from_currency: the currency to exchange
        :param amount: the amount
        :return: the amount in USD

        Example:
        currency_in_usd = CurrencyConverter.convert(EUR, currency_in_eur)
        """
        from_currency = from_currency.upper()
        if from_currency == 'USD':
            return amount

        currency_exchange_coefficient = CurrencyConverter.get_currency_coefficients().get(from_currency, None)
        if not currency_exchange_coefficient:
            logger.error("CurrencyConverter cant convert '{0}' currency to USD because there is no"
                             "currency_exchange_coefficient for this currency in the DB.".format(from_currency))
            return None

        amount_in_usd = amount / currency_exchange_coefficient
        return amount_in_usd * (1.0 - CURRENCY_COMMISSION)


if __name__ == "__main__":
    print CurrencyConverter.convert("EUR", 1)
    print CurrencyConverter.convert("eUr", 1)
    print CurrencyConverter.convert("ILS", 10)
    print CurrencyConverter.convert("BLA", 1)

