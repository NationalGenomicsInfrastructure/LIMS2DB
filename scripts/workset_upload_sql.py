import argparse
import os

import yaml
from genologics_sql.queries import get_last_modified_processes, get_processes_in_history
from genologics_sql.tables import Process
from genologics_sql.utils import get_session

import LIMS2DB.classes as lclasses
import LIMS2DB.objectsDB.process_categories as pc_cg
import LIMS2DB.parallel as lpar
import LIMS2DB.utils as lutils


def main(args):
    log = lutils.setupLog("worksetlogger", args.logfile)
    session = get_session()
    if args.ws:
        step = session.query(Process).filter_by(luid=args.ws).one()
        ws = lclasses.Workset_SQL(session, log, step)
        with open(args.conf) as conf_file:
            conf = yaml.load(conf_file, Loader=yaml.SafeLoader)
        couch = lutils.setupServer(conf)
        db = couch["worksets"]
        doc = {}
        for row in db.view("worksets/lims_id")[ws.obj["id"]]:
            doc = db.get(row.id)

        final_doc = lutils.merge(ws.obj, doc)

        db.save(final_doc)

    elif args.recent:
        recent_processes = get_last_modified_processes(
            session,
            list(pc_cg.AGRLIBVAL.keys()) + list(pc_cg.SEQUENCING.keys()) + list(pc_cg.WORKSET.keys()),
            args.interval,
        )
        processes_to_update = set()
        for p in recent_processes:
            if str(p.typeid) in list(pc_cg.WORKSET.keys()) and p.daterun:  # will only catch finished setup workset plate
                processes_to_update.add(p)
            else:
                processes_to_update.update(get_processes_in_history(session, p.processid, list(pc_cg.WORKSET.keys())))

        log.info(f"the following processes will be updated : {processes_to_update}")
        lpar.masterProcessSQL(args, processes_to_update, log)


if __name__ == "__main__":
    usage = "Usage:       python workset_upload_sql.py [options]"
    parser = argparse.ArgumentParser(description=usage)

    parser.add_argument(
        "-p",
        "--procs",
        dest="procs",
        type=int,
        default=8,
        help="number of processes to spawn",
    )

    parser.add_argument(
        "-a",
        "--all",
        dest="recent",
        action="store_true",
        default=False,
        help="tries to work on the recent worksets",
    )

    parser.add_argument(
        "-i",
        "--interval",
        dest="interval",
        default="2 hours",
        help="interval to look at to grab worksets",
    )

    parser.add_argument("-w", "--workset", dest="ws", default=None, help="tries to work on the given ws")

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
        default=os.path.join(os.environ["HOME"], "workset_upload.log"),
        help="log file.  Default: ~/workset_upload.log",
    )
    args = parser.parse_args()

    main(args)
