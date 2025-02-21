#!/usr/bin/env python

"""Script to copy comments from the AggregateQC step in LIMS as
running notes to statusdb. Also notifies project coordinators

Should be run atleast daily as a cronjob
"""

import argparse
import datetime
import os

import genologics_sql.tables as tbls
import markdown
from genologics_sql.utils import get_session
from sqlalchemy import text
from sqlalchemy.orm import aliased
from statusdb.db.utils import load_couch_server

from LIMS2DB.utils import send_mail


def main(args):
    session = get_session()
    couch = load_couch_server(args.conf)
    db = couch["running_notes"]

    def get_researcher(userid):
        query = "select rs.* from principals pr \
                    inner join researcher rs on rs.researcherid=pr.researcherid \
                    where principalid=:pid;"
        return session.query(tbls.Researcher).from_statement(text(query)).params(pid=userid).first()

    def make_esc_running_note(
        researcher,
        reviewer,
        comment,
        date,
        processid,
        project,
        step_name,
        review_ask,
        samples,
    ):
        created_time = date.astimezone(datetime.timezone.utc)
        # Apparently inserting raw html works in markdown
        lims_link = f"<a href='https://ngi-lims-prod.scilifelab.se/clarity/work-complete/{processid}' target='_blank'>LIMS</a>"
        researcher_name = f"{researcher.firstname} {researcher.lastname}"

        if reviewer:
            reviewer_name = f"{reviewer.firstname} {reviewer.lastname}"
        if review_ask:
            comment_detail = f"(**{researcher_name} asked for review from {reviewer_name}**)"
            categories = ["Lab"]
        else:
            comment_detail = f"(**Reviewer {researcher_name} replied**)"
            categories = ["Administration", "Decision"]
        comment_detail = f"{comment_detail} \n **on {len(samples)} samples:** {', '.join(samples)}"
        newNote = {
            "_id": f"P{project}:{datetime.datetime.timestamp(created_time)}",
            "user": researcher_name,
            "email": researcher.email,
            "note": f"Comment from {step_name} ({lims_link}) {comment_detail} \n\n{comment}",
            "categories": categories,
            "note_type": "project",
            "parent": f"P{project}",
            "created_at_utc": created_time.isoformat(),
            "updated_at_utc": created_time.isoformat(),
            "projects": [f"P{project}"],
        }
        return newNote

    def update_note_db(note):
        updated = False
        note_existing = db.get(note["_id"])
        if "_rev" in note.keys():
            del note["_rev"]

        if note_existing:
            dict_note = dict(note_existing)
            if "_rev" in dict_note.keys():
                del dict_note["_rev"]
            if not dict_note == note:
                note_existing.update(note)
                db.save(note_existing)
                updated = True
        else:
            db.save(note)
            updated = True

        return updated

    def email_proj_coord(project, note, date):
        res = session.query(tbls.Project.name, tbls.Project.ownerid).filter(tbls.Project.projectid == project).first()
        if res:
            proj_coord = get_researcher(res.ownerid)
        else:
            proj_coord = "ngi-project-coordinators@scilifelab.se"

        time_in_format = datetime.datetime.strftime(date, "%a %b %d %Y, %I:%M:%S %p")

        html = (
            "<html>"
            "<body>"
            "<p>"
            f"A note has been created from LIMS in the project P{project}, {res.name}! The note is as follows</p>"
            "<blockquote>"
            '<div class="panel panel-default" style="border: 1px solid #e4e0e0; border-radius: 4px;">'
            '<div class="panel-heading" style="background-color: #f5f5f5; padding: 10px 15px;">'
            f'<a href="#">{note["user"]}</a> - <span>{time_in_format}</span> <span>{", ".join(note.get("categories"))}</span>'
            "</div>"
            '<div class="panel-body" style="padding: 15px;">'
            f"<p>{markdown.markdown(note.get('note'))}</p>"
            "</div></div></blockquote></body></html>"
        )

        send_mail(f"[LIMS] Running Note:P{project}, {res.name}", html, proj_coord.email)

    esc = aliased(tbls.EscalationEvent)
    sa = aliased(tbls.Sample)
    piot = aliased(tbls.ProcessIOTracker)
    asm = aliased(tbls.artifact_sample_map)

    # Assumed that it runs atleast once daily
    yesterday = datetime.date.today() - datetime.timedelta(days=1)

    # get aggregate QC comments for running notes
    escalations = (
        session.query(esc, sa)
        .join(piot, piot.processid == esc.processid)
        .join(asm, piot.inputartifactid == asm.columns["artifactid"])
        .join(sa, sa.processid == asm.columns["processid"])
        .filter(esc.lastmodifieddate > f"{yesterday}")
        .all()
    )

    projects = {}
    for escalation, sample in escalations:
        if not sample.projectid:
            continue
        query = f"select ps.name from escalationevent esc, process pr, protocolstep ps where esc.processid=pr.processid and pr.protocolstepid=ps.stepid and esc.processid={escalation.processid};"
        step_name = session.execute(text(query)).first()[0]
        owner = get_researcher(escalation.ownerid)
        reviewer = get_researcher(escalation.reviewerid)
        if sample.projectid in projects.keys() and escalation.eventid in projects[sample.projectid]:
            projects[sample.projectid][escalation.eventid]["samples"].append(sample.name)
        else:
            projects[sample.projectid] = {
                escalation.eventid: {
                    "samples": [sample.name],
                    "step": step_name,
                    "escalationcomment": escalation.escalationcomment,
                    "escalationdate": escalation.escalationdate,
                    "escalationprocessid": escalation.processid,
                    "owner": owner,
                    "reviewer": reviewer,
                }
            }

            if escalation.reviewdate:
                if not escalation.reviewcomment:
                    comment = "[No comments]"
                else:
                    comment = escalation.reviewcomment
                projects[sample.projectid][escalation.eventid]["review"] = {
                    "reviewdate": escalation.reviewdate,
                    "reviewcomment": comment,
                }
    for project in projects:
        for esceventid in projects[project]:
            escevent = projects[project][esceventid]
            escnote = make_esc_running_note(
                escevent["owner"],
                escevent["reviewer"],
                escevent["escalationcomment"],
                escevent["escalationdate"],
                escevent["escalationprocessid"],
                project,
                escevent["step"],
                True,
                escevent["samples"],
            )
            if update_note_db(escnote):
                email_proj_coord(project, escnote, escevent["escalationdate"])

            if "review" in escevent:
                revnote = make_esc_running_note(
                    escevent["reviewer"],
                    None,
                    escevent["review"]["reviewcomment"],
                    escevent["review"]["reviewdate"],
                    escevent["escalationprocessid"],
                    project,
                    escevent["step"],
                    False,
                    escevent["samples"],
                )

                if update_note_db(revnote):
                    email_proj_coord(project, revnote, escevent["review"]["reviewdate"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync the comments made in aggregate QC to project running notes")
    parser.add_argument(
        "-c",
        "--conf",
        default=os.path.join(os.environ["HOME"], "conf/LIMS2DB/post_process.yaml"),
        help="Config file.  Default: ~/conf/LIMS2DB/post_process.yaml",
    )
    args = parser.parse_args()

    main(args)
