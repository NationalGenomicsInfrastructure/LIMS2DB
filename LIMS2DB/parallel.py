import logging
import multiprocessing as mp
import queue as Queue

import genologics_sql.tables as gt
import yaml
from genologics_sql.utils import get_session

import LIMS2DB.classes as lclasses
import LIMS2DB.utils as lutils


def masterProcessSQL(args, wslist, logger):
    worksetQueue = mp.JoinableQueue()
    logQueue = mp.Queue()
    childs = []
    procs_nb = 1
    if len(wslist) < args.procs:
        procs_nb = len(wslist)
    else:
        procs_nb = args.procs

    # spawn a pool of processes, and pass them queue instance
    for i in range(procs_nb):
        p = mp.Process(target=processWSULSQL, args=(args, worksetQueue, logQueue))
        p.start()
        childs.append(p)
    # populate queue with data
    for ws in wslist:
        worksetQueue.put(ws.processid)

    # wait on the queue until everything has been processed
    notDone = True
    while notDone:
        try:
            log = logQueue.get(False)
            logger.handle(log)
        except Queue.Empty:
            if not lutils.stillRunning(childs):
                notDone = False
                break


def processWSULSQL(args, queue, logqueue):
    work = True
    session = get_session()
    with open(args.conf) as conf_file:
        conf = yaml.load(conf_file, Loader=yaml.SafeLoader)
    couch = lutils.load_couch_server(conf)
    procName = mp.current_process().name
    proclog = logging.getLogger(procName)
    proclog.setLevel(level=logging.INFO)
    mfh = lutils.QueueHandler(logqueue)
    mft = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    mfh.setFormatter(mft)
    proclog.addHandler(mfh)

    while work:
        # grabs project from queue
        try:
            ws_id = queue.get(block=True, timeout=3)
            proclog.info(f"Starting work on {ws_id}")
        except Queue.Empty:
            work = False
            proclog.info("exiting gracefully")
            break
        else:
            step = session.query(gt.Process).filter(gt.Process.processid == int(ws_id)).one()
            ws = lclasses.Workset_SQL(session, proclog, step)
            doc = {}
            result = couch.post_view(
                db="worksets",
                ddoc="worksets",
                view="lims_id",
                key=ws.obj["id"],
                include_docs=True,
            ).get_result()["rows"]
            if result:
                doc = result[0]["doc"]
            if doc:
                final_doc = lutils.merge(ws.obj, doc)
            else:
                final_doc = ws.obj
            # clean possible name duplicates
            result = couch.post_view(
                db="worksets",
                ddoc="worksets",
                view="name",
                key=ws.obj["name"],
                include_docs=True,
            ).get_result()["rows"]
            if result:
                doc = result[0]["doc"]
                if doc["id"] != ws.obj["id"]:
                    proclog.warning(f"Duplicate name {doc['name']} for worksets {doc['id']} and {final_doc['id']}")
                    couch.delete_document(db="worksets", doc_id=doc["_id"], rev=doc["_rev"]).get_result()
            # upload the document
            couch.post_document(
                db="worksets",
                document=final_doc,
            ).get_result()
            proclog.info(f"updating {ws.obj['name']}")
            queue.task_done()
