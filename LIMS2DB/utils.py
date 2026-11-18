import json
import logging
import logging.handlers
import smtplib
from email.mime.text import MIMEText
from importlib.resources import files

from ibmcloudant import CouchDbSessionAuthenticator, cloudant_v1


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


def load_couch_server(conf):
    """Loads the CouchDB server instance from the configuration.
    :param dict conf: Configuration dictionary containing statusdb settings
    :return: CouchDB server instance
    """
    db_conf = conf["statusdb"]
    couchdb = cloudant_v1.CloudantV1(authenticator=CouchDbSessionAuthenticator(db_conf["username"], db_conf["password"]))
    url = db_conf["url"]
    if not url.startswith("https://"):
        url = f"https://{url}"
    couchdb.set_service_url(url)
    return couchdb


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


def stillRunning(processList):
    ret = False
    for p in processList:
        if p.is_alive():
            ret = True

    return ret


def match_samples_to_validation_errors(common_fields, samples, validation_errors):
    """Match samples to validation errors based on sample_alias.

    :param samples: List of samples
    :param validation_errors: List of validation error dictionaries
    :return: List of samples with validation errors
    """
    samples_with_errors = {}
    for error in validation_errors:
        row_number = error.get("row")
        sample = samples[row_number - 1]
        field = error.get("field")
        if field in common_fields:
            if 'common' not in samples_with_errors:
                samples_with_errors['common'] = []
            if error["message"] not in samples_with_errors['common']:
                samples_with_errors['common'].append(error["message"])
        else:
            if sample not in samples_with_errors:
                samples_with_errors[sample] = []
            samples_with_errors[sample].append(error["message"])

    return samples_with_errors


def generate_tsv_template():
    """Generates a TSV template based on the genomics metadata template.
    :return: TSV template as a dictionary with default values
    """
    ena_tsv_metadata = json.loads(files("scilifelab_metadata_templates.genomics").joinpath("templates/genomics_technical_metadata_schema.json").read_text())
    tsv_file_headers = list(ena_tsv_metadata.get("properties", {}).keys())
    tsv_file_headers.remove("reverse_file_name")  # Remove reverse file name and md5 as they
    tsv_file_headers.remove("reverse_file_md5")  # will be added later in case of paired reads
    version = ena_tsv_metadata.get("version", "")
    template = {header: "" for header in tsv_file_headers}
    template["template_version"] = version
    return template


class QueueHandler(logging.Handler):
    """
    This handler sends events to a queue. Typically, it would be used together
    with a multiprocessing Queue to centralise logging to file in one process
    (in a multi-process application), so as to avoid file write contention
    between processes.

    This code is new in Python 3.2, but this class can be copy pasted into
    user code for use with earlier Python versions.
    """

    def __init__(self, queue):
        """
        Initialise an instance, using the passed queue.
        """
        logging.Handler.__init__(self)
        self.queue = queue

    def enqueue(self, record):
        """
        Enqueue a record.

        The base implementation uses put_nowait. You may want to override
        this method if you want to use blocking, timeouts or custom queue
        implementations.
        """
        self.queue.put_nowait(record)

    def prepare(self, record):
        """
        Prepares a record for queuing. The object returned by this method is
        enqueued.

        The base implementation formats the record to merge the message
        and arguments, and removes unpickleable items from the record
        in-place.

        You might want to override this method if you want to convert
        the record to a dict or JSON string, or send a modified copy
        of the record while leaving the original intact.
        """
        # The format operation gets traceback text into record.exc_text
        # (if there's exception data), and also puts the message into
        # record.message. We can then use this to replace the original
        # msg + args, as these might be unpickleable. We also zap the
        # exc_info attribute, as it's no longer needed and, if not None,
        # will typically not be pickleable.
        self.format(record)
        record.msg = record.message
        record.args = None
        record.exc_info = None
        return record

    def emit(self, record):
        """
        Emit a record.

        Writes the LogRecord to the queue, preparing it for pickling first.
        """
        try:
            self.enqueue(self.prepare(record))
        except Exception:
            self.handleError(record)
