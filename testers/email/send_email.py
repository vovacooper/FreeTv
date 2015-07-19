
import os
from os.path import abspath, exists
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

#from email.MIMEText import MIMEText

HOST = 'smtp.gmail.com'
PORT = '587'
USERNAME = 'vova@impospace.com'
PASSWORD = 'ImpoZerem^^^'

COMMASPACE = ', '

class Mail:
    @staticmethod
    def send_mail(to_list, from_address, subject, body):
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = from_address
        msg['To'] = COMMASPACE.join(to_list)

        #htmlBody = MIMEText(body, 'html')
        msg.attach(body)

        s = smtplib.SMTP()
        s.connect(HOST, PORT)

        s.ehlo()
        s.starttls()
        s.login(USERNAME, PASSWORD)

        s.sendmail("vovacooper@impospace.com", to_list, msg.as_string())
        s.quit()

########################################################################################################################
if __name__ == "__main__":

    f_path = abspath("email/contacts.txt")
    email_path = abspath("email/VladimirImpospace.html")
    emails = []
    if exists(f_path):
        with open(f_path) as f:
            emails = f.readlines()
            f.close()

    #emails = ["vovacooper@gmail.com","vovacooper@gmail.com","vovacooper@gmail.com","vovacooper@gmail.com","vovacooper@gmail.com","vovacooper@gmail.com",]
    for email_addr in emails:
        email_addr = email_addr.rstrip()

        time.sleep(5)

        print("+ sending mail to " + email_addr + ".")

        subject = "Unlock revenue from damage caused by adblocking to your website"
        fp = open(email_path)
        message = MIMEText(fp.read(), 'html')
        fp.close()
        Mail.send_mail([email_addr], "vova@impospace.com" , subject, message)

        print("- email to " + email_addr + " was sent.")