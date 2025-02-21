from genologics_sql.queries import get_last_modified_processes
from genologics_sql.tables import Artifact, Container
from sqlalchemy import text

import LIMS2DB.objectsDB.process_categories as pc_cg


def create_lims_data_obj(session, pro):
    obj = {}
    obj["step_id"] = pro.luid

    # which container is used in this step ?
    query = f"select distinct ct.* from container ct\
             inner join containerplacement cp on cp.containerid=ct.containerid \
             inner join processiotracker piot on piot.inputartifactid=cp.processartifactid \
             where piot.processid = {pro.processid}::integer;"

    cont = session.query(Container).from_statement(text(query)).first()
    obj["container_id"] = cont.luid
    obj["container_name"] = cont.name

    # Fetch Run Type for MiSeq
    if pc_cg.SEQUENCING.get(str(pro.typeid), "") in ["MiSeq Run (MiSeq) 4.0"]:
        obj["run_type"] = pro.udf_dict["Run Type"]

    if pc_cg.SEQUENCING.get(str(pro.typeid), "") in [
        "AUTOMATED - NovaSeq Run (NovaSeq 6000 v2.0)",
        "AVITI Run v1.0",
        "Illumina Sequencing (NextSeq) v1.0",
        "NovaSeqXPlus Run v1.0",
    ]:
        # NovaSeq flowcell have the individual stats as output artifact
        query = f"select art.* from artifact art \
                 inner join outputmapping omap on omap.outputartifactid=art.artifactid \
                 inner join processiotracker piot on piot.trackerid=omap.trackerid \
                 where art.name LIKE 'Lane%' and piot.processid = {pro.processid}::integer;"
    else:
        # Which artifacts are updated in this step ?
        query = f"select distinct art.* from artifact art\
                 inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                 where piot.processid = {pro.processid}::integer;"

    obj["run_summary"] = {}
    arts = session.query(Artifact).from_statement(text(query)).all()
    for art in arts:
        if pc_cg.SEQUENCING.get(str(pro.typeid), "") in [
            "AUTOMATED - NovaSeq Run (NovaSeq 6000 v2.0)",
            "AVITI Run v1.0",
            "Illumina Sequencing (NextSeq) v1.0",
            "NovaSeqXPlus Run v1.0",
        ]:
            lane = art.name.replace("Lane ", "")
        else:
            lane = art.containerplacement.api_string.split(":")[0]
        # lane is a string
        if lane.isalpha():
            lane = str(ord(lane) - 64)
        obj["run_summary"][lane] = art.udf_dict
        obj["run_summary"][lane]["qc"] = art.qc_flag

    return obj


def get_sequencing_steps(session, interval="24 hours"):
    return get_last_modified_processes(session, list(pc_cg.SEQUENCING.keys()), interval)


def upload_to_couch(couch, runid, lims_data, pro):
    if pc_cg.SEQUENCING.get(str(pro.typeid), "") in [
        "AUTOMATED - NovaSeq Run (NovaSeq 6000 v2.0)",
        "Illumina Sequencing (NextSeq) v1.0",
        "MiSeq Run (MiSeq) 4.0",
        "NovaSeqXPlus Run v1.0",
    ]:
        dbname = "x_flowcells"
    elif pc_cg.SEQUENCING.get(str(pro.typeid), "") in ["AVITI Run v1.0"]:
        dbname = "element_runs"

    db = couch[dbname]
    view = db.view("info/id")
    doc = None
    for row in view[runid]:
        doc = db.get(row.value)

    if doc:
        running_notes = {}
        if "lims_data" in doc and "container_running_notes" in doc["lims_data"]:
            running_notes = doc["lims_data"].pop("container_running_notes")
        doc["lims_data"] = lims_data
        if running_notes:
            doc["lims_data"]["container_running_notes"] = running_notes
        db.save(doc)
