#!/usr/bin/env python
"""Script to load project info from Lims into the project database in statusdb.

Maya Brandi, Science for Life Laboratory, Stockholm, Sweden.
"""

import json
import logging
import logging.handlers
import multiprocessing as mp
import os
import queue as Queue
import sys
import time
import traceback
from argparse import ArgumentParser

import yaml
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.lims import Lims
from genologics_sql.queries import get_last_modified_projectids
from genologics_sql.tables import Project as DBProject
from genologics_sql.utils import get_configuration, get_session

from LIMS2DB.classes import ProjectSQL
from LIMS2DB.utils import QueueHandler, formatStack, load_couch_server, stillRunning


def main(options):
    conf = options.conf
    output_f = options.output_f
    with open(conf) as conf_file:
        couch_conf = yaml.load(conf_file, Loader=yaml.SafeLoader)
    couch = load_couch_server(couch_conf)
    mainlims = Lims(BASEURI, USERNAME, PASSWORD)
    lims_db = get_session()

    mainlog = logging.getLogger("psullogger")
    mainlog.setLevel(level=logging.INFO)
    mfh = logging.handlers.RotatingFileHandler(options.logfile, maxBytes=209715200, backupCount=5)
    mft = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    mfh.setFormatter(mft)
    mainlog.addHandler(mfh)

    # try getting orderportal config
    oconf = None
    try:
        with open(options.oconf) as ocf:
            oconf = yaml.load(ocf, Loader=yaml.SafeLoader)["order_portal"]
    except Exception as e:
        mainlog.warning(f"Loading orderportal config {options.oconf} failed due to {e}, so order information for project will not be updated")

    if options.project_name:
        host = get_configuration()["url"]
        pj_id = lims_db.query(DBProject.luid).filter(DBProject.name == options.project_name).scalar()
        if not pj_id:
            pj_id = options.project_name
        P = ProjectSQL(lims_db, mainlog, pj_id, host, couch, oconf)
        if options.upload:
            P.save(update_modification_time=not options.no_new_modification_time)
        else:
            if output_f is not None:
                with open(output_f, "w") as f:
                    f.write(json.dumps(P.obj))
            else:
                print(json.dumps(P.obj))

    else:
        projects = create_projects_list(options, lims_db, mainlims, mainlog)
        masterProcess(options, projects, mainlims, mainlog, oconf)
        lims_db.commit()
        lims_db.close()


def create_projects_list(options, db_session, lims, log):
    projects = []
    if options.all_projects:
        if options.hours:
            postgres_string = f"{options.hours} hours"
            project_ids = get_last_modified_projectids(db_session, postgres_string)
            valid_projects = db_session.query(DBProject).filter(DBProject.luid.in_(project_ids)).all()
            log.info(f"project list : {' '.join([p.luid for p in valid_projects])}")
            return valid_projects
        else:
            projects = db_session.query(DBProject).all()
            log.info(f"project list : {' '.join([p.luid for p in projects])}")
            return projects

    elif options.input:
        with open(options.input) as input_file:
            for pname in input_file:
                try:
                    projects.append(lims.get_projects(name=pname.rstrip())[0])
                except IndexError:
                    pass

        return projects


def processPSUL(options, queue, logqueue, oconf=None):
    with open(options.conf) as conf_file:
        couch_conf = yaml.load(conf_file, Loader=yaml.SafeLoader)
    couch = load_couch_server(couch_conf)
    db_session = get_session()
    work = True
    procName = mp.current_process().name
    proclog = logging.getLogger(procName)
    proclog.setLevel(level=logging.INFO)
    mfh = QueueHandler(logqueue)
    mft = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    mfh.setFormatter(mft)
    proclog.addHandler(mfh)
    # Not completely sure what this does, maybe trying to load balance?
    try:
        time.sleep(int(procName[8:]))
    except:
        time.sleep(1)

    while work:
        # grabs project from queue
        try:
            projname = queue.get(block=True, timeout=3)
            proclog.info(f"Starting work on {projname} ")
            proclog.info(f"Approximately {queue.qsize()} projects left in queue")
        except Queue.Empty:
            work = False
            proclog.info("exiting gracefully")
            break
        except NotImplementedError:
            # qsize failed, no big deal
            pass
        else:
            # locks the project : cannot be updated more than once.
            lockfile = os.path.join(options.lockdir, projname)
            if not os.path.exists(lockfile):
                try:
                    open(lockfile, "w").close()
                except:
                    proclog.error(f"cannot create lockfile {lockfile}")
                try:
                    pj_id = db_session.query(DBProject.luid).filter(DBProject.name == projname).scalar()
                    host = get_configuration()["url"]
                    P = ProjectSQL(db_session, proclog, pj_id, host, couch, oconf)
                    P.save()
                except:
                    error = sys.exc_info()
                    stack = traceback.extract_tb(error[2])
                    proclog.error(f"{error[0]}:{error[1]}\n{formatStack(stack)}")

                try:
                    os.remove(lockfile)
                except:
                    proclog.error(f"cannot remove lockfile {lockfile}")
            else:
                proclog.info(f"project {projname} is locked, skipping.")

            # signals to queue job is done
            queue.task_done()
    db_session.commit()
    db_session.close()


def masterProcess(options, projectList, mainlims, logger, oconf=None):
    projectsQueue = mp.JoinableQueue()
    logQueue = mp.Queue()
    childs = []
    # Initial step : order projects by sample number:
    logger.info("ordering the project list")
    orderedprojectlist = sorted(
        projectList,
        key=lambda x: (mainlims.get_sample_number(projectname=x.name)),
        reverse=True,
    )
    logger.info("done ordering the project list")
    # spawn a pool of processes, and pass them queue instance
    for i in range(options.processes):
        p = mp.Process(target=processPSUL, args=(options, projectsQueue, logQueue, oconf))
        p.start()
        childs.append(p)
    # populate queue with data
    for proj in orderedprojectlist:
        projectsQueue.put(proj.name)

    # wait on the queue until everything has been processed
    notDone = True
    while notDone:
        try:
            log = logQueue.get(False)
            logger.handle(log)
        except Queue.Empty:
            if not stillRunning(childs):
                notDone = False
                break


if __name__ == "__main__":
    usage = "Usage:       python project_summary_upload_LIMS.py [options]"
    parser = ArgumentParser(usage=usage)

    parser.add_argument(
        "-p",
        "--project",
        dest="project_name",
        default=None,
        help="eg: M.Uhlen_13_01. Dont use with -a flagg.",
    )

    parser.add_argument(
        "-a",
        "--all_projects",
        action="store_true",
        default=False,
        help=("Upload all Lims projects into couchDB.Don't use together with -f flag."),
    )
    parser.add_argument(
        "-c",
        "--conf",
        default=os.path.join(os.environ["HOME"], "conf/LIMS2DB/post_process.yaml"),
        help="Config file.  Default: ~/conf/LIMS2DB/post_process.yaml",
    )
    parser.add_argument(
        "--oconf",
        default=os.path.join(os.environ["HOME"], ".ngi_config/orderportal_cred.yaml"),
        help="Orderportal config file. Default: ~/.ngi_config/orderportal_cred.yaml",
    )
    parser.add_argument(
        "--no_upload",
        dest="upload",
        default=True,
        action="store_false",
        help=("Use this tag if project objects should not be uploaded, but printed to output_f, or to stdout. Only works with individual projects, not with -a."),
    )
    parser.add_argument(
        "--output_f",
        default=None,
        help="Output file that will be used only if --no_upload tag is used",
    )
    parser.add_argument(
        "-m",
        "--multiprocs",
        type=int,
        dest="processes",
        default=4,
        help="The number of processes that will be spawned. Will only work with -a",
    )
    parser.add_argument(
        "-l",
        "--logfile",
        default=os.path.expanduser("~/lims2db_projects.log"),
        help="log file that will be used. Default is $HOME/lims2db_projects.log",
    )
    parser.add_argument(
        "--lockdir",
        default=os.path.expanduser("~/psul_locks"),
        help=("Directory for handling the lock files to avoid multiple updates of one project. default is $HOME/psul_locks "),
    )
    parser.add_argument(
        "-j",
        "--hours",
        type=int,
        default=None,
        help=("only handle projects modified in the last X hours"),
    )
    parser.add_argument(
        "-i",
        "--input",
        default=None,
        help="path to the input file containing projects to update",
    )
    parser.add_argument(
        "--no_new_modification_time",
        action="store_true",
        help=("This updates documents without changing the modification time. Slightly dangerous, but useful e.g. when all projects would be updated."),
    )

    options = parser.parse_args()

    main(options)
