import argparse
import os
import smtplib
from datetime import date, timedelta
from email.mime.text import MIMEText

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.lims import Lims
from statusdb.db.utils import load_couch_server


def main(args):
    lims = Lims(BASEURI, USERNAME, PASSWORD)

    sixMonthsAgo = date.today() - timedelta(weeks=26)
    yesterday = date.today() - timedelta(days=1)
    pjs = lims.get_projects(open_date=sixMonthsAgo.strftime("%Y-%m-%d"))
    statusdb = load_couch_server(args.conf)
    proj_id_view = statusdb["projects"].view("project/project_id")

    operator = "par.lundin@scilifelab.se"
    summary = {}
    project_types = [
        "Aggregate QC (DNA) 4.0",
        "Aggregate QC (Library Validation) 4.0",
        "Aggregate QC (RNA) 4.0",
        "AUTOMATED - NovaSeq Run (NovaSeq 6000 v2.0)",
        "AVITI Run v1.0",
        "Bcl Conversion & Demultiplexing (AVITI) v1.0",
        "Bcl Conversion & Demultiplexing (Illumina SBS) 4.0",
        "Illumina Sequencing (NextSeq) v1.0",
        "MiSeq Run (MiSeq) 4.0",
        "NovaSeqXPlus Run v1.0",
        "Project Summary 1.3",
    ]

    def clean_names(name):
        return name.replace("\u00f6", "o").replace("\u00e9", "e").replace("\u00e4", "a")

    def hasNGIRole(roles):
        for role in roles:
            if role.name in [
                "Facility Administrator",
                "Researcher",
                "System Administrator",
            ]:
                return True
        return False

    def get_email(fullname):
        # shotgun
        # In multipart names, the first token is taken as first name and the rest taken as surname
        names = fullname.split(" ", 1)
        try:
            researchers = lims.get_researchers(firstname=names[0], lastname=names[1])
        except:
            names = clean_names(fullname).split(" ", 1)
            researchers = lims.get_researchers(firstname=names[0], lastname=names[1])
        email = ""
        for r in researchers:
            try:
                if not r.account_locked and hasNGIRole(r.roles):
                    email = r.email
                    break
            # older Contacts would not have the account_locked field which would throw an AttributeError
            except AttributeError:
                continue

        return email

    for p in pjs:
        # Assuming this will be run on the early morning, this grabs all processes from the list that have been modified the day before
        pro = lims.get_processes(
            projectname=p.name,
            type=project_types,
            last_modified=yesterday.strftime("%Y-%m-%dT00:00:00Z"),
        )
        completed = []
        bfr = None
        lbr = None
        if pro:
            for pr in pro:
                date_start = None
                # Special case for the project summary
                if pr.type.name == "Project Summary 1.3":
                    if "Queued" in pr.udf and pr.udf["Queued"] == yesterday.strftime("%Y-%m-%d"):
                        completed.append(
                            {
                                "project": p.name,
                                "action": "has been queued",
                                "date": pr.udf["Queued"],
                                "techID": pr.udf["Signature Queued"],
                                "tech": pr.technician.first_name + " " + pr.technician.last_name,
                                "sum": True,
                            }
                        )
                    if "All samples sequenced" in pr.udf and pr.udf["All samples sequenced"] == yesterday.strftime("%Y-%m-%d"):
                        completed.append(
                            {
                                "project": p.name,
                                "action": "Has all its samples sequenced",
                                "date": pr.udf["All samples sequenced"],
                                "techID": pr.udf["Signature All samples sequenced"],
                                "tech": pr.technician.first_name + " " + pr.technician.last_name,
                                "sum": True,
                            }
                        )
                    if " All raw data delivered" in pr.udf and pr.udf[" All raw data delivered"] == yesterday.strftime("%Y-%m-%d"):
                        completed.append(
                            {
                                "project": p.name,
                                "action": "Has all its samples sequenced",
                                "date": pr.udf[" All raw data delivered"],
                                "techID": pr.udf["Signature  All raw data delivered"],
                                "tech": pr.technician.first_name + " " + pr.technician.last_name,
                                "sum": True,
                            }
                        )

                else:  # I don't want to combine this in a single elif because of the line 80, that must be done in the else, but regardless of the if
                    if "Run ID" in pr.udf:  # this is true for sequencing processes, and gives the actual starting date
                        date_start = pr.udf["Run ID"].split("_")[0]  # format is YYMMDD
                        date_start = date_start[:2] + "-" + date_start[2:4] + "-" + date_start[4:6]
                        if pr.date_run and date_start == pr.date_run[4:]:
                            date_start = None
                        else:
                            date_start = "20" + date_start  # now, the format is YYYY-MM-DD, assuming no prjects come from the 1990's or the next century...
                    completed.append(
                        {
                            "project": p.name,
                            "process": pr.type.name,
                            "limsid": pr.id,
                            "start": date_start,
                            "end": pr.date_run,
                            "tech": pr.technician.first_name + " " + pr.technician.last_name,
                            "sum": False,
                        }
                    )
            # catch closed projects
            if p.close_date and p.close_date == yesterday.strftime("%Y-%m-%d"):
                completed.append(
                    {
                        "project": p.name,
                        "action": "Has been closed",
                        "date": p.close_date,
                        "techID": "the responsible",
                        "sum": True,
                    }
                )

            if completed:  # If we actually have stuff to mail
                doc = statusdb["projects"].get(proj_id_view[p.id].rows[0].value)
                if "project_coordinator" in doc["details"]:
                    pc = doc["details"]["project_coordinator"]
                    summary[pc] = completed
                if "project_summary" in doc:
                    if "bioinfo_responsible" in doc["project_summary"]:
                        bfr = doc["project_summary"]["bioinfo_responsible"]
                        summary[bfr] = completed
                    if "lab_responsible" in doc["project_summary"]:
                        lbr = doc["project_summary"]["lab_responsible"]
                        summary[lbr] = completed

    control = ""
    for resp in summary:
        plist = set()  # no duplicates
        body = ""
        resp_email = get_email(resp)
        if resp_email:
            for struct in summary[resp]:
                if resp != struct.get("tech") and not struct["sum"]:
                    plist.add(struct["project"])
                    body += "In project {},  {} ({})".format(struct["project"], struct["process"], struct["limsid"])
                    if struct["start"] and yesterday.strftime("%Y-%m-%d") == struct["start"]:
                        body += "started on {}, ".format(struct["start"])
                    elif struct["end"]:
                        body += "ended on {}, ".format(struct["end"])
                    else:
                        body += "has been updated yesterday, "
                    body += "Done by {}\n".format(struct["tech"])
                elif struct["sum"]:
                    plist.add(struct["project"])
                    body += "Project {} {} on {} by {}\n".format(
                        struct["project"],
                        struct["action"],
                        struct["date"],
                        struct["techID"],
                    )
            if body != "":
                control += f"{resp_email} : {body}\n"
                body += f'\n\n--\nThis mail is an automated mail that is generated once a day and summarizes the events of the previous days in the lims, \
for the projects you are described as "Lab responsible", "Bioinfo Responsible" or "Project coordinator". You can send comments or suggestions to {operator}'
                msg = MIMEText(body)
                msg["Subject"] = "[Lims update] {}".format(" ".join(plist))
                msg["From"] = "Lims_monitor"
                try:
                    msg["To"] = resp_email
                except KeyError:
                    msg["To"] = operator
                    msg["Subject"] = f"[Lims update] Failed to send a mail to {resp}"

                s = smtplib.SMTP("localhost")
                s.sendmail("genologics-lims@scilifelab.se", msg["To"], msg.as_string())
                s.quit()

    ctrlmsg = MIMEText(control)
    ctrlmsg["Subject"] = "[Lims update] Control"
    ctrlmsg["From"] = "Lims_monitor"
    ctrlmsg["To"] = operator
    s = smtplib.SMTP("localhost")
    s.sendmail("genologics-lims@scilifelab.se", ctrlmsg["To"], ctrlmsg.as_string())
    s.quit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="mail the modifications in the lims for the last day to the responsibles declared in project summary")
    parser.add_argument(
        "-c",
        "--conf",
        default=os.path.join(os.environ["HOME"], "conf/LIMS2DB/post_process.yaml"),
        help="Config file.  Default: ~/conf/LIMS2DB/post_process.yaml",
    )
    parser.add_argument(
        "--test",
        "-t",
        dest="test",
        action="store_true",
        help="print and don't send mails",
    )

    args = parser.parse_args()

    main(args)
