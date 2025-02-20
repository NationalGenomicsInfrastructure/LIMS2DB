import logging
import logging.handlers
import smtplib
from email.mime.text import MIMEText

import couchdb


# merges d2 in d1, keeps values from d1
def merge(d1, d2):
    """Will merge dictionary d2 into dictionary d1.
    On the case of finding the same key, the one in d1 will be used.
    :param d1: Dictionary object
    :param s2: Dictionary object
    """
    for key in d2:
        if key in d1:
            if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                merge(d1[key], d2[key])
            elif d1[key] == d2[key]:
                pass  # same leaf value
        else:
            d1[key] = d2[key]
    return d1


def setupLog(name, logfile):
    mainlog = logging.getLogger(name)
    mainlog.setLevel(level=logging.INFO)
    mfh = logging.handlers.RotatingFileHandler(logfile, maxBytes=209715200, backupCount=5)
    mft = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    mfh.setFormatter(mft)
    mainlog.addHandler(mfh)
    return mainlog


def formatStack(stack):
    formatted_error = []
    for trace in stack:
        formatted_error.append(f"File {trace[0]}: line {trace[1]} in {trace[2]}\n{trace[3]}")

    return "\n".join(formatted_error)


def setupServer(conf):
    db_conf = conf["statusdb"]
    url = "https://{}:{}@{}".format(db_conf["username"], db_conf["password"], db_conf["url"])
    return couchdb.Server(url)


def send_mail(subject, content, receiver):
    """Sends an email.
    :param str subject: Subject for the email
    :param str content: Content of the email
    :param str receiver: Address to send the email
    """
    if not receiver:
        raise SystemExit("No receiver was given to send mail")
    msg = MIMEText(content, "html")
    msg["Subject"] = f"LIMS2DB notification - {subject}"
    msg["From"] = "LIMS2DB@scilifelab.se"
    msg["To"] = receiver

    s = smtplib.SMTP("localhost")
    s.sendmail("LIMS2DB", [receiver], msg.as_string())
    s.quit()
