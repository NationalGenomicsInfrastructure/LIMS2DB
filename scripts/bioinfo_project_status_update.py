#!/usr/bin/env python

import argparse
import os

import yaml
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Project
from genologics.lims import Lims
from requests.exceptions import HTTPError

import LIMS2DB.utils as lutils


def main(args):
    log = lutils.setupLog("bioinfologger", args.logfile)
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    with open(args.conf) as conf_file:
        conf = yaml.safe_load(conf_file)
    bioinfodb = lutils.setupServer(conf)["bioinfo_analysis"]
    open_projects = bioinfodb.view("latest_data/sample_id_open")

    for row in open_projects.rows:
        project_id = row.key[0]
        sample_id = row.key[3]
        close_date = None
        try:
            close_date = Project(lims=lims, id=project_id).close_date
        except HTTPError as e:
            if "404: Project not found" in str(e):
                log.error("Project " + project_id + " not found in LIMS")
                continue
        if close_date is not None:
            try:
                doc = bioinfodb.get(row.id)
            except Exception as e:
                log.error(e + "in Project " + project_id + " Sample " + sample_id + " while accessing doc from statusdb")
            doc["project_closed"] = True
            try:
                bioinfodb.save(doc)
                log.info("Updated Project " + project_id + " Sample " + sample_id)
            except Exception as e:
                log.error(e + "in Project " + project_id + " Sample " + sample_id + " while saving to statusdb")


if __name__ == "__main__":
    usage = "Usage:       python bioinfo_project_status_update.py [options]"
    parser = argparse.ArgumentParser(description=usage)

    parser.add_argument(
        "-c",
        "--conf",
        dest="conf",
        default=os.path.join(os.environ["HOME"], "opt/config/post_process.yaml"),
        help="Config file.  Default: ~/opt/config/post_process.yaml",
    )

    parser.add_argument(
        "-l",
        "--log",
        dest="logfile",
        default=os.path.join(os.environ["HOME"], "statusdb_bioinfo_closed.log"),
        help="log file.  Default: ~/statusdb_bioinfo_closed.log",
    )
    args = parser.parse_args()

    main(args)
