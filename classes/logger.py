import logging
import sys
import os
from os.path import basename
import logging.handlers
import time




LOG_PATH = "/var/log/impo_admin/"



# email logger
class TlsSMTPHandler(logging.handlers.SMTPHandler):
    def emit(self, record):
        """
        Emit a record.

        Format the record and send it to the specified addressees.
        """
        try:
            import smtplib
            import string  # for tls add this line

            try:
                from email.utils import formatdate
            except ImportError:
                formatdate = self.date_time
            port = self.mailport
            if not port:
                port = smtplib.SMTP_PORT
            smtp = smtplib.SMTP(self.mailhost, port)
            msg = self.format(record)
            msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n%s" % (
                self.fromaddr,
                string.join(self.toaddrs, ","),
                self.getSubject(record),
                formatdate(), msg)
            if self.username:
                smtp.ehlo()  # for tls add this line
                smtp.starttls()  # for tls add this line
                smtp.ehlo()  # for tls add this line
                smtp.login(self.username, self.password)
            smtp.sendmail(self.fromaddr, self.toaddrs, msg)
            smtp.quit()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)

logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)

print(LOG_PATH + basename(sys.argv[0]) + ".log")

fh = logging.FileHandler(LOG_PATH + basename(sys.argv[0]) + ".log")
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s %(module)s.%(funcName)s -> %(lineno)d-%(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s %(module)s.%(funcName)s -> %(lineno)d-%(levelname)s: %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

#email log
# gm = TlsSMTPHandler(("smtp.gmail.com", 587), 'vova@impospace.com', ['vova@impospace.com'], 'Error found!', ('vova@impospace.com', 'ImpoZerem^^^'))
#gm.setLevel(logging.ERROR)
#formatter = logging.Formatter("%(asctime)s %(module)s.%(funcName)s -> %(lineno)d-%(levelname)s: %(message)s")
#gm.setFormatter(formatter)
#logger.addHandler(gm)




'''
MAIN
'''
if __name__ == "__main__":
    logger.exception('Blat!!!!!!!!!-')
    logger.error('Blat!!!!!!!!!-')

    #logger.error("Adcash responce is [], dont expect it to work :(")
    try:
        1/0
    except:
        logger.exception('FFFFFFFFFFFFFFFFFFFFFFFUUUUUUUUUUUUUUUUUUUUUU-')
