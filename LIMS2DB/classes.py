import copy
import http.client as http_client
import re
from datetime import datetime

from genologics_sql.queries import get_children_processes, get_processes_in_history
from genologics_sql.tables import (
    Artifact,
    Container,
    EscalationEvent,
    GlsFile,
    Process,
    Project,
    ReagentType,
    Researcher,
)
from requests import get as rget
from sqlalchemy import text
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

import LIMS2DB.objectsDB.process_categories as pc_cg
from LIMS2DB.diff import diff_objects
from LIMS2DB.utils import send_mail


class Workset:
    def __init__(self, lims, crawler, log):
        self.log = log
        self.name = set()
        self.lims = lims
        self.obj = {}
        # get the identifier
        outs = crawler.starting_proc.all_outputs()
        for out in outs:
            if out.type == "Analyte" and len(out.samples) == 1:
                try:
                    self.name.add(out.location[0].name)
                except:
                    self.log.warn(f"no name found for workset {out.id}")

        try:
            self.obj["name"] = self.name.pop()
        except:
            self.log.error(f"No name found for current workset {crawler.starting_proc.id}, might be an ongoing step.")
            raise NameError
        self.obj["technician"] = crawler.starting_proc.technician.initials
        self.obj["id"] = crawler.starting_proc.id
        self.obj["date_run"] = crawler.starting_proc.date_run
        # only get the latest aggregate qc date
        latest_date = 0
        for agr in crawler.libaggre:
            if agr.date_run > latest_date:
                latest_date = agr.date_run
        if not latest_date:
            latest_date = None
        self.obj["last_aggregate"] = latest_date
        pjs = {}
        for p in crawler.projects:
            pjs[p.id] = {}
            pjs[p.id]["name"] = p.name
            try:
                pjs[p.id]["library"] = p.udf["Library construction method"]
            except KeyError:
                pjs[p.id]["library"] = None
            try:
                pjs[p.id]["application"] = p.udf["Application"]
            except KeyError:
                pjs[p.id]["application"] = None
            try:
                pjs[p.id]["sequencing_setup"] = "{} {}".format(p.udf["Sequencing platform"], p.udf["Sequencing setup"])
            except KeyError:
                pjs[p.id]["sequencing_setup"] = None

            pjs[p.id]["samples"] = {}
            for sample in crawler.samples:
                if sample.project == p:
                    pjs[p.id]["samples"][sample.name] = {}
                    pjs[p.id]["samples"][sample.name]["library"] = {}
                    pjs[p.id]["samples"][sample.name]["sequencing"] = {}
                    try:
                        pjs[p.id]["samples"][sample.name]["customer_name"] = sample.udf["Customer Name"]
                    except KeyError:
                        pjs[p.id]["samples"][sample.name]["customer_name"] = None

                    pjs[p.id]["samples"][sample.name]["rec_ctrl"] = {}
                    for i in crawler.starting_proc.all_inputs():
                        if sample in i.samples:
                            pjs[p.id]["samples"][sample.name]["rec_ctrl"]["status"] = i.qc_flag

                    for output in crawler.starting_proc.all_outputs():
                        if output.type == "Analyte" and sample in output.samples:
                            pjs[p.id]["samples"][sample.name]["location"] = output.location[1]

                    for lib in sorted(crawler.libaggre, key=lambda l: l.date_run, reverse=True):
                        for inp in lib.all_inputs():
                            if sample in inp.samples:
                                onelib = {}
                                onelib["status"] = inp.qc_flag
                                onelib["art"] = inp.id
                                onelib["date"] = lib.date_run
                                onelib["name"] = lib.protocol_name
                                onelib["id"] = lib.id
                                if "Concentration" in inp.udf and "Conc. Units" in inp.udf:
                                    onelib["concentration"] = "{} {}".format(
                                        round(inp.udf["Concentration"], 2),
                                        inp.udf["Conc. Units"],
                                    )
                                if "Molar Conc. (nM)" in inp.udf:
                                    onelib["concentration"] = "{} nM".format(round(inp.udf["Molar Conc. (nM)"], 2))
                                if "Size (bp)" in inp.udf:
                                    onelib["size"] = round(inp.udf["Size (bp)"], 2)
                                if "NeoPrep Machine QC" in inp.udf and onelib["status"] == "UNKNOWN":
                                    onelib["status"] = inp.udf["NeoPrep Machine QC"]

                                pjs[p.id]["samples"][sample.name]["library"][lib.id] = onelib
                                if "library_status" not in pjs[p.id]["samples"][sample.name]:
                                    pjs[p.id]["samples"][sample.name]["library_status"] = inp.qc_flag

                    for seq in sorted(crawler.seq, key=lambda s: s.date_run, reverse=True):
                        for inp in seq.all_inputs():
                            if sample in inp.samples:
                                pjs[p.id]["samples"][sample.name]["sequencing"][seq.id] = {}
                                pjs[p.id]["samples"][sample.name]["sequencing"][seq.id]["status"] = inp.qc_flag
                                pjs[p.id]["samples"][sample.name]["sequencing"][seq.id]["date"] = seq.date_run
                                if "sequencing_status" not in pjs[p.id]["samples"][sample.name]:
                                    pjs[p.id]["samples"][sample.name]["sequencing_status"] = inp.qc_flag

        self.obj["projects"] = pjs


class LimsCrawler:
    def __init__(self, lims, starting_proc=None, starting_inputs=None):
        self.lims = lims
        self.starting_proc = starting_proc
        self.samples = set()
        self.projects = set()
        self.finlibinitqc = set()
        self.initqc = set()
        self.initaggr = set()
        self.pooling = set()
        self.preprepstart = set()
        self.prepstart = set()
        self.prepend = set()
        self.libval = set()
        self.finliblibval = set()
        self.libaggre = set()
        self.dilstart = set()
        self.seq = set()
        self.demux = set()
        self.caliper = set()
        self.projsum = set()
        self.inputs = set()
        if starting_proc:
            for i in starting_proc.all_inputs():
                if i.type == "Analyte":
                    self.samples.update(i.samples)
                    self.inputs.add(i)
        if starting_inputs:
            for i in starting_inputs:
                if i.type == "Analyte":
                    self.samples.update(i.samples)
                    self.inputs.add(i)
        for sample in self.samples:
            if sample.project:
                self.projects.add(sample.project)

    def crawl(self, starting_step=None):
        nextsteps = set()
        if not starting_step:
            if not self.starting_proc:
                for i in self.inputs:
                    if i.type == "Analyte" and (self.samples.intersection(i.samples)):
                        nextsteps.update(self.lims.get_processes(inputartifactlimsid=i.id))
            else:
                starting_step = self.starting_proc
        if starting_step:
            for o in starting_step.all_outputs():
                if o.type == "Analyte" and (self.samples.intersection(o.samples)):
                    nextsteps.update(self.lims.get_processes(inputartifactlimsid=o.id))
        for step in nextsteps:
            if step.type.name in list(pc_cg.PREPREPSTART.values()):
                self.preprepstart.add(step)
            elif step.type.name in list(pc_cg.PREPSTART.values()):
                self.prepstart.add(step)
            elif step.type.name in list(pc_cg.PREPEND.values()):
                self.prepend.add(step)
            elif step.type.name in list(pc_cg.LIBVAL.values()):
                self.libval.add(step)
            elif step.type.name in list(pc_cg.AGRLIBVAL.values()):
                self.libaggre.add(step)
            elif step.type.name in list(pc_cg.SEQUENCING.values()):
                self.seq.add(step)
            elif step.type.name in list(pc_cg.DEMULTIPLEX.values()):
                self.demux.add(step)
            elif step.type.name in list(pc_cg.INITALQCFINISHEDLIB.values()):
                self.finlibinitqc.add(step)
            elif step.type.name in list(pc_cg.INITALQC.values()):
                self.initqc.add(step)
            elif step.type.name in list(pc_cg.AGRINITQC.values()):
                self.initaggr.add(step)
            elif step.type.name in list(pc_cg.POOLING.values()):
                self.pooling.add(step)
            elif step.type.name in list(pc_cg.DILSTART.values()):
                self.dilstart.add(step)
            elif step.type.name in list(pc_cg.SUMMARY.values()):
                self.projsum.add(step)
            elif step.type.name in list(pc_cg.CALIPER.values()):
                self.caliper.add(step)

            # if the step has analytes as outputs
            if [x for x in step.all_outputs() if x.type == "Analyte"]:
                self.crawl(starting_step=step)


class Workset_SQL:
    def __init__(self, session, log, step):
        self.log = log
        self.start = step
        self.name = set()
        self.session = session
        self.obj = {}
        self.build()

    def extract_barcode(self, chain):
        barcode = ""
        bcp = re.compile(r"[ATCG\-]{4,}")
        TENX_SINGLE_PAT = re.compile(r"SI-(?:GA|NA)-[A-H][1-9][0-2]?")
        TENX_DUAL_PAT = re.compile(r"SI-(?:TT|NT|NN|TN|TS)-[A-H][1-9][0-2]?")
        SMARTSEQ_PAT = re.compile(r"SMARTSEQ[1-9]?-[1-9][0-9]?[A-P]")
        if "NoIndex" in chain:
            return chain
        if TENX_SINGLE_PAT.match(chain) or TENX_DUAL_PAT.match(chain) or SMARTSEQ_PAT.match(chain):
            return chain
        if "(" not in chain:
            barcode = chain
        else:
            pattern = re.compile(r"\(([A-Z\-]+)\)")
            matches = pattern.search(chain)
            if matches.group(1):
                barcode = matches.group(1)
        matches = bcp.match(barcode)
        if not matches:
            meta = self.session.query(ReagentType.meta_data).filter(ReagentType.name.like(f"%{barcode}%")).scalar()
            matches = bcp.search(meta)
            if matches:
                barcode = matches.group(0)
        return barcode

    def build(self):
        self.obj["id"] = self.start.luid
        self.obj["last_aggregate"] = None
        if self.start.daterun:
            self.obj["date_run"] = self.start.daterun.strftime("%Y-%m-%d")
        else:
            self.obj["date_run"] = None

        query = f"select distinct co.* from processiotracker pio \
                inner join outputmapping om on om.trackerid=pio.trackerid \
                inner join containerplacement cp on cp.processartifactid=om.outputartifactid \
                inner join container co on cp.containerid=co.containerid \
                where pio.processid = {self.start.processid};"
        self.container = self.session.query(Container).from_statement(text(query)).one()
        self.obj["name"] = self.container.name

        query = "select rs.initials from principals pr \
                inner join researcher rs on rs.researcherid=pr.researcherid \
                where principalid=:pid;"
        self.obj["technician"] = self.session.query(Researcher.initials).from_statement(text(query)).params(pid=self.start.ownerid).scalar()

        # main part
        self.obj["projects"] = {}
        query = f"select art.* from artifact art \
                inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                where piot.processid = {self.start.processid}"

        input_arts = self.session.query(Artifact).from_statement(text(query)).all()

        for inp in input_arts:
            sample = inp.samples[0]
            project = sample.project
            close_date = ""
            if not project:
                project_luid = "Control"
                application = "Control"
                name = "Control"
                library = ""
                library_option = ""
                sequencing_setup = ""
            else:
                project_luid = project.luid
                application = project.udf_dict.get("Application")
                name = project.name
                library = project.udf_dict.get("Library construction method")
                library_option = project.udf_dict.get("Library prep option")
                sequencing_setup = "{} {}".format(
                    project.udf_dict.get("Sequencing platform"),
                    project.udf_dict.get("Sequencing setup"),
                )
                if project.closedate:
                    close_date = project.closedate.strftime("%Y-%m-%d")
            if project_luid not in self.obj["projects"]:
                self.obj["projects"][project_luid] = {
                    "application": application,
                    "name": name,
                    "library": library,
                    "library_option": library_option,
                    "sequencing_setup": sequencing_setup,
                    "samples": {},
                }
                if close_date:
                    self.obj["projects"][project_luid]["close_date"] = close_date
            if sample.name not in self.obj["projects"][project_luid]["samples"]:
                self.obj["projects"][project_luid]["samples"][sample.name] = {
                    "customer_name": sample.udf_dict.get("Customer Name"),
                    "sequencing_status": "UNKNOWN",
                    "library_status": "UNKNOWN",
                    "rec_ctrl": {},
                    "library": {},
                    "sequencing": {},
                }

            self.obj["projects"][project_luid]["samples"][sample.name]["rec_ctrl"]["status"] = inp.qc_flag

            query = f"select art.* from artifact art \
            inner join outputmapping om on om.outputartifactid=art.artifactid \
            inner join processiotracker piot on piot.trackerid=om.trackerid \
            where piot.inputartifactid={inp.artifactid} and art.artifacttypeid=2 and piot.processid={self.start.processid};"

            # When one input artifact generates multiple output artifacts,
            # expand the input artifact with postfix _1, _2, etc
            outs = self.session.query(Artifact).from_statement(text(query)).all()
            rep_counter = 1
            for out in outs:
                if len(outs) > 1:
                    if self.obj["projects"][project_luid]["samples"].get(sample.name):
                        org_sample_obj = copy.deepcopy(self.obj["projects"][project_luid]["samples"][sample.name])
                        del self.obj["projects"][project_luid]["samples"][sample.name]
                    sample_name = sample.name + "_" + str(rep_counter)
                    self.obj["projects"][project_luid]["samples"][sample_name] = copy.deepcopy(org_sample_obj)
                    rep_counter += 1
                else:
                    sample_name = sample.name

                self.obj["projects"][project_luid]["samples"][sample_name]["location"] = out.containerplacement.api_string

                query = "select pc.* from process pc \
                        inner join processiotracker piot on piot.processid=pc.processid \
                        inner join artifact_ancestor_map aam on aam.artifactid=piot.inputartifactid \
                        where pc.typeid in ({agr_qc}) and aam.ancestorartifactid={out_art} order by daterun;".format(
                    agr_qc=",".join(list(pc_cg.AGRLIBVAL.keys())),
                    out_art=out.artifactid,
                )

                aggregates = self.session.query(Process).from_statement(text(query)).all()

                for agr in aggregates:
                    self.obj["projects"][project_luid]["samples"][sample_name]["library"][agr.luid] = {}
                    self.obj["projects"][project_luid]["samples"][sample_name]["library"][agr.luid]["id"] = agr.luid
                    self.obj["projects"][project_luid]["samples"][sample_name]["library"][agr.luid]["name"] = agr.protocolnameused
                    if agr.daterun is not None:
                        self.obj["projects"][project_luid]["samples"][sample_name]["library"][agr.luid]["date"] = agr.daterun.strftime("%Y-%m-%d")
                        if not self.obj["last_aggregate"] or datetime.strptime(self.obj["last_aggregate"], "%Y-%m-%d") < agr.daterun:
                            self.obj["last_aggregate"] = agr.daterun.strftime("%Y-%m-%d")
                    else:
                        self.obj["projects"][project_luid]["samples"][sample_name]["library"][agr.luid]["date"] = None

                    query = f"select art.* from artifact art \
                            inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                            inner join artifact_ancestor_map aam on aam.artifactid=art.artifactid \
                            where piot.processid={agr.processid} and aam.ancestorartifactid={out.artifactid};"

                    agr_inp = self.session.query(Artifact).from_statement(text(query)).one()
                    if agr.typeid == 806 and agr_inp.qc_flag == "UNKNOWN":
                        self.obj["projects"][project_luid]["samples"][sample_name]["library"][agr.luid]["status"] = agr_inp.udf_dict.get("NeoPrep Machine QC")
                        self.obj["projects"][project_luid]["samples"][sample_name]["library_status"] = agr_inp.udf_dict.get("NeoPrep Machine QC")
                    else:
                        self.obj["projects"][project_luid]["samples"][sample_name]["library"][agr.luid]["status"] = agr_inp.qc_flag
                        self.obj["projects"][project_luid]["samples"][sample_name]["library_status"] = agr_inp.qc_flag
                    self.obj["projects"][project_luid]["samples"][sample_name]["library"][agr.luid]["art"] = agr_inp.luid
                    if "Molar Conc. (nM)" in agr_inp.udf_dict:
                        self.obj["projects"][project_luid]["samples"][sample_name]["library"][agr.luid]["concentration"] = "{:.2f} nM".format(agr_inp.udf_dict["Molar Conc. (nM)"])
                    elif "Concentration" in agr_inp.udf_dict and "Conc. Units" in agr_inp.udf_dict:
                        self.obj["projects"][project_luid]["samples"][sample_name]["library"][agr.luid]["concentration"] = "{:.2f} {}".format(
                            agr_inp.udf_dict["Concentration"],
                            agr_inp.udf_dict["Conc. Units"],
                        )
                    if "Size (bp)" in agr_inp.udf_dict:
                        self.obj["projects"][project_luid]["samples"][sample_name]["library"][agr.luid]["size"] = round(agr_inp.udf_dict["Size (bp)"], 2)

                    # Fetch index (reagent_label) information
                    try:
                        artifacts = self.session.query(Artifact).from_statement(text(query)).all()
                        for art in artifacts:
                            if art.reagentlabels is not None and len(art.reagentlabels) == 1:
                                # If there are more than one reagent label, then I can't guess which one is the right one : the artifact is probably a pool
                                self.obj["projects"][project_luid]["samples"][sample_name]["library"][agr.luid]["index"] = self.extract_barcode(art.reagentlabels[0].name)
                    except AssertionError:
                        pass

                query = "select pc.* from process pc \
                        inner join processiotracker piot on piot.processid=pc.processid \
                        inner join artifact_ancestor_map aam on aam.artifactid=piot.inputartifactid \
                        where pc.typeid in ({seq}) and aam.ancestorartifactid={out_art} order by daterun;".format(seq=",".join(list(pc_cg.SEQUENCING.keys())), out_art=out.artifactid)

                sequencing = self.session.query(Process).from_statement(text(query)).all()
                for seq in sequencing:
                    if seq.daterun is not None:
                        self.obj["projects"][project_luid]["samples"][sample_name]["sequencing"][seq.luid] = {}
                        self.obj["projects"][project_luid]["samples"][sample_name]["sequencing"][seq.luid]["date"] = seq.daterun.strftime("%Y-%m-%d")

                        query = f"select art.* from artifact art \
                                inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                                inner join artifact_ancestor_map aam on aam.artifactid=art.artifactid \
                                where piot.processid={seq.processid} and aam.ancestorartifactid={out.artifactid};"

                        seq_inputs = self.session.query(Artifact).from_statement(text(query)).all()
                        seq_qc_flag = "UNKNOWN"
                        for seq_inp in seq_inputs:
                            if seq_qc_flag != "FAILED":  # failed stops sequencing update
                                seq_qc_flag = seq_inp.qc_flag

                        self.obj["projects"][project_luid]["samples"][sample_name]["sequencing"][seq.luid]["status"] = seq_qc_flag
                        # updates every time until the latest one, because of the order by in fetching sequencing processes.
                        self.obj["projects"][project_luid]["samples"][sample_name]["sequencing_status"] = seq_qc_flag


class ProjectSQL:
    def __init__(self, session, log, pid, host="genologics.scilifelab.se", couch=None, oconf=None):
        self.log = log
        self.pid = pid
        self.host = host
        self.name = set()
        self.session = session
        self.couch = couch
        self.oconf = oconf
        self.genstat_proj_url = "https://genomics-status.scilifelab.se/project/"
        self.obj = {}
        self.project = self.session.query(Project).filter(Project.luid == self.pid).one()
        self.build()

    def build(self):
        self.get_project_level()
        self.get_project_summary()
        self.get_escalations()
        self.get_samples()
        self.set_status()

    def save(self, update_modification_time=True):
        doc = None
        # When running for a single project, sometimes the connection is lost so retry
        try:
            self.couch["projects"]
        except http_client.BadStatusLine:
            self.log.warning(f"Access to couch failed before trying to save new doc for project {self.pid}")
            pass
        db = self.couch["projects"]
        view = db.view("project/project_id")
        for row in view[self.pid]:
            doc = db.get(row.id)
        if doc:
            fields_saved = [
                "_id",
                "_rev",
                "modification_time",
                "creation_time",
                "staged_files",
                "agreement_doc_id",
                "invoice_spec_generated",
                "invoice_spec_downloaded",
                "delivery_projects",
            ]
            details_saved = ["running_notes", "snic_checked", "latest_sticky_note"]

            fields_added_back = {}
            details_added_back = {}

            for field in fields_saved:
                fields_added_back[field] = doc.pop(field, None)

            for field in details_saved:
                details_added_back[field] = doc["details"].pop(field, None)

            diffs = diff_objects(doc, self.obj)
            if diffs:
                for field in fields_added_back:
                    if update_modification_time and field == "modification_time":
                        self.obj[field] = datetime.now().isoformat()
                        continue
                    if fields_added_back[field]:
                        self.obj[field] = fields_added_back[field]

                for field in details_added_back:
                    if details_added_back[field]:
                        self.obj["details"][field] = details_added_back[field]

                # Don't overwrite order portal details if have not been able to fetch them this round
                if self.obj["order_details"] == {} and doc["order_details"] != {}:
                    self.log.warn("Preventing order details to be overwritten since no details were fetched from order portal this round")
                    self.obj["order_details"] = doc["order_details"]

                self.log.info(f"Trying to save new doc for project {self.pid}")
                db.save(self.obj)
                if self.obj.get("details", {}).get("type", "") == "Application":
                    lib_method_text = f"Library method: {self.obj['details'].get('library_construction_method', 'N/A')}"
                    application = self.obj.get("details", {}).get("application", "")
                    is_single_cell = application == "RNA-seq (single cell)"
                    if is_single_cell:
                        single_cell_text = f"[Application: {application}]"
                    if "key  details contract_received" in diffs.keys():
                        genstat_url = f"{self.genstat_proj_url}{self.obj['project_id']}"
                        if diffs["key  details contract_received"][1] == "missing":
                            old_contract_received = diffs["key  details contract_received"][0]
                            msg = f"Contract received on {old_contract_received} deleted for applications project "
                            msg += f'<a href="{genstat_url}">{self.obj["project_id"]}, {self.obj["project_name"]}</a>[{lib_method_text}]\
                            {single_cell_text if is_single_cell else ""}.'
                        else:
                            contract_received = diffs["key  details contract_received"][1]
                            msg = "Contract received for applications project "
                            msg += f'<a href="{genstat_url}">{self.obj["project_id"]}, {self.obj["project_name"]}</a>[{lib_method_text}]\
                            {single_cell_text if is_single_cell else ""} on {contract_received}.'

                        if is_single_cell:
                            send_mail(f"Contract updated for single cell Project {self.obj['project_name']}", msg, "ngi_singlecell_projects@scilifelab.se")
                        else:
                            send_mail(
                                f"Contract updated for GA Project {self.obj['project_name']}",
                                msg,
                                "ngi_ga_projects@scilifelab.se",
                            )
            else:
                self.log.info(f"No modifications found for project {self.pid}")

        else:
            self.obj["creation_time"] = datetime.now().isoformat()
            self.obj["modification_time"] = self.obj["creation_time"]
            self.log.info(f"Trying to save new doc for project {self.pid}")
            db.save(self.obj)
            if self.obj.get("details", {}).get("type", "") == "Application":
                genstat_url = f"{self.genstat_proj_url}{self.obj['project_id']}"
                lib_method_text = f"Library method: {self.obj['details'].get('library_construction_method', 'N/A')}"
                msg = "New applications project created "
                msg += f'<a href="{genstat_url}">{self.obj["project_id"]}, {self.obj["project_name"]}</a>[{lib_method_text}].'
                send_mail(
                    f"GA Project created {self.obj['project_name']}",
                    msg,
                    "ngi_ga_projects@scilifelab.se",
                )

    def get_project_level(self):
        self.obj["entity_type"] = "project_summary"
        self.obj["source"] = "lims"
        self.obj["project_name"] = self.project.name
        self.obj["project_id"] = self.project.luid
        self.obj["application"] = self.project.udf_dict.get("Application")
        self.obj["contact"] = self.project.researcher.email
        if self.project.opendate:
            self.obj["open_date"] = self.project.opendate.strftime("%Y-%m-%d")
        if self.project.closedate:
            self.obj["close_date"] = self.project.closedate.strftime("%Y-%m-%d")
        if self.project.udf_dict.get("Delivery type"):
            self.obj["delivery_type"] = self.project.udf_dict.get("Delivery type")
        if self.project.udf_dict.get("Reference genome"):
            self.obj["reference_genome"] = self.project.udf_dict.get("Reference genome")
        self.obj["details"] = self.make_normalized_dict(self.project.udf_dict)
        self.obj["details"].pop("running_notes", None)
        self.obj["order_details"] = self.get_project_order()
        self.obj["affiliation"] = self.obj["order_details"].get("owner", {}).get("affiliation", "")
        lims_priority = {1: "Low", 5: "Standard", 10: "High"}  # as defined in LIMS
        if self.project.priority:
            self.obj["priority"] = lims_priority.get(self.project.priority, None)

    def get_project_summary(self):
        # get project summaries from project
        query = f"select distinct pr.* from process pr \
            inner join processiotracker piot on piot.processid=pr.processid \
            inner join artifact_sample_map asm on piot.inputartifactid=asm.artifactid \
            inner join sample sa on sa.processid=asm.processid \
            where sa.projectid = {self.project.projectid} and pr.typeid={list(pc_cg.SUMMARY.keys())[0]} order by createddate desc;"
        try:
            pjs = self.session.query(Process).from_statement(text(query)).all()
            self.obj["project_summary"] = self.make_normalized_dict(pjs[0].udf_dict)
            self.obj["project_summary_links"] = []
            for pj in pjs:
                status = "complete" if pj.workstatus == "COMPLETE" else "details"
                self.obj["project_summary_links"].append(
                    (
                        f"/clarity/work-{status}/{pj.processid}",
                        pj.createddate.strftime("%H:%M, %d %b %Y"),
                    )
                )
        except (NoResultFound, IndexError):
            self.log.info(f"No project summary found for project {self.project.projectid}")

    def get_escalations(self):
        # get EscalationEvents from Project
        query = f"select distinct esc.* from escalationevent esc \
                inner join processiotracker piot on piot.processid=esc.processid \
                inner join artifact_sample_map asm on piot.inputartifactid=asm.artifactid \
                inner join sample sa on sa.processid=asm.processid \
                where esc.reviewdate is NULL and sa.projectid = {self.project.projectid};"
        escalations = self.session.query(EscalationEvent).from_statement(text(query)).all()
        if escalations:
            esc_list = []
            for esc in escalations:
                # get requester and reviewer
                query = "select distinct r.* \
                        from researcher r \
                        inner join principals pr on pr.researcherid=r.researcherid \
                        where pr.principalid={requesterid};"
                requester = self.session.query(Researcher).from_statement(text(query.format(requesterid=esc.ownerid))).all()[0]
                reviewer = self.session.query(Researcher).from_statement(text(query.format(requesterid=esc.reviewerid))).all()[0]
                esc_list.append(
                    [
                        str(esc.processid),
                        f"{requester.firstname} {requester.lastname}",
                        f"{reviewer.firstname} {reviewer.lastname}",
                    ]
                )
            self.obj["escalations"] = esc_list

    def make_normalized_dict(self, d):
        ret = {}
        for kv in d.items():
            key = kv[0].lower().replace(" ", "_").replace(".", "")
            ret[key] = kv[1]
        return ret

    def get_project_order(self):
        # get project order details from orderportal
        proj_order_info = {}
        if self.oconf:
            try:
                proj_order_url = "{}/{}".format(
                    self.oconf["api_get_order_url"].rstrip("/"),
                    self.obj["details"]["portal_id"],
                )
                api_header = {"X-OrderPortal-API-key": self.oconf["api_token"]}
                full_order_info = rget(proj_order_url, headers=api_header).json()
                filter_keys = [
                    "created",
                    "modified",
                    "site",
                    "title",
                    "identifier",
                    {
                        "owner": ["name", "email"],
                        "fields": [
                            "seq_readlength_hiseqx",
                            "library_readymade",
                            "bx_exp",
                            "seq_instrument",
                            "project_lab_email",
                            "project_bx_email",
                            "project_lab_name",
                            "bx_data_delivery",
                            "sample_no",
                            "bioinformatics",
                            "sequencing",
                            "project_pi_name",
                            "bx_bp",
                            "project_desc",
                            "project_pi_email",
                        ],
                    },
                ]
                for fk in filter_keys:
                    if isinstance(fk, dict):
                        for k, vals in fk.items():
                            if k not in proj_order_info:
                                proj_order_info[k] = {}
                            for vk in vals:
                                proj_order_info[k][vk] = full_order_info.get(k, {}).get(vk)
                    else:
                        proj_order_info[fk] = full_order_info.get(fk)
                owner_url = full_order_info["owner"]["links"]["api"]["href"]
                owner_affiliation = rget(owner_url, headers=api_header).json().get("university", "")
                proj_order_info["owner"]["affiliation"] = owner_affiliation
            except Exception:
                self.log.warn(f"Not able to get update order info for project {self.project.name}")
        return proj_order_info

    def get_samples(self):
        self.obj["no_of_samples"] = len(self.project.samples)
        self.obj["samples"] = {}
        for sample in self.project.samples:
            self.obj["samples"][sample.name] = {}
            self.obj["samples"][sample.name]["scilife_name"] = sample.name
            self.obj["samples"][sample.name]["customer_name"] = sample.udf_dict.get("Customer Name")
            self.obj["samples"][sample.name]["details"] = self.make_normalized_dict(sample.udf_dict)

            self.get_initial_qc(sample)
            self.get_library_preps(sample)

    def get_initial_qc(self, sample):
        self.obj["samples"][sample.name]["initial_qc"] = {}
        # Get initial artifact for given sample
        query = f"select art.* from artifact art \
            inner join artifact_sample_map asm on asm.artifactid=art.artifactid \
            inner join sample sa on sa.processid=asm.processid \
            where sa.processid = {sample.processid} and art.isoriginal=True"
        try:
            initial_artifact = self.session.query(Artifact).from_statement(text(query)).one()
            self.obj["samples"][sample.name]["initial_plate_id"] = initial_artifact.containerplacement.container.luid
            self.obj["samples"][sample.name]["well_location"] = initial_artifact.containerplacement.api_string
            self.obj["samples"][sample.name]["initial_qc"]["initial_qc_status"] = initial_artifact.qc_flag
            self.obj["samples"][sample.name]["initial_qc"].update(self.make_normalized_dict(initial_artifact.udf_dict))
        except NoResultFound:
            self.log.info(f"did not find the initial artifact of sample {sample.name}")
        # get all initial QC processes for sample
        query = "select pr.* from process pr \
                inner join processiotracker piot on piot.processid=pr.processid \
                inner join artifact_sample_map asm on piot.inputartifactid=asm.artifactid \
                inner join sample sa on sa.processid=asm.processid \
                where sa.processid = {sapid} and pr.typeid in ({tid}) \
                order by pr.daterun;".format(
            sapid=sample.processid,
            tid=",".join(list(pc_cg.INITALQC.keys()) + list(pc_cg.INITALQCFINISHEDLIB.keys())),
        )
        try:
            oldest_qc = self.session.query(Process).from_statement(text(query)).first()
            if not oldest_qc:
                return None
            try:
                self.obj["samples"][sample.name]["initial_qc"]["start_date"] = oldest_qc.daterun.strftime("%Y-%m-%d")
                self.obj["samples"][sample.name]["first_initial_qc_start_date"] = oldest_qc.daterun.strftime("%Y-%m-%d")
            except AttributeError:
                self.obj["samples"][sample.name]["initial_qc"]["start_date"] = oldest_qc.createddate.strftime("%Y-%m-%d")
                self.obj["samples"][sample.name]["first_initial_qc_start_date"] = oldest_qc.createddate.strftime("%Y-%m-%d")

            try:
                if oldest_qc.daterun and datetime.strptime(self.obj["first_initial_qc"], "%Y-%m-%d") > oldest_qc.daterun:
                    self.obj["first_initial_qc"] = oldest_qc.daterun.strftime("%Y-%m-%d")
            except KeyError:
                try:
                    self.obj["first_initial_qc"] = oldest_qc.daterun.strftime("%Y-%m-%d")
                except AttributeError:
                    self.obj["first_initial_qc"] = oldest_qc.createddate.strftime("%Y-%m-%d")

            # get aggregate from init qc for sample
            query = "select pr.* from process pr \
                inner join processiotracker piot on piot.processid=pr.processid \
                inner join artifact_sample_map asm on piot.inputartifactid=asm.artifactid \
                inner join sample sa on sa.processid=asm.processid \
                where sa.processid = {sapid} and pr.typeid in ({tid}) \
                order by pr.daterun desc;".format(sapid=sample.processid, tid=",".join(list(pc_cg.AGRINITQC.keys())))
            try:
                youngest_aggregate = self.session.query(Process).from_statement(text(query)).first()
                try:
                    self.obj["samples"][sample.name]["initial_qc"]["finish_date"] = youngest_aggregate.daterun.strftime("%Y-%m-%d")
                except AttributeError:
                    pass
                self.obj["samples"][sample.name]["initial_qc"]["initials"] = youngest_aggregate.technician.researcher.initials
            except AttributeError:
                self.log.info(f"Didnt find an aggregate for Initial QC of sample {sample.name}")
        except AttributeError:
            self.log.info(f"Did not find any initial QC for sample {sample.name}")
        # get GlsFile for output artifact of a Fragment Analyzer process where its input is the initial artifact of a given sample
        query = "select gf.* from glsfile gf \
            inner join resultfile rf on rf.glsfileid=gf.fileid \
            inner join artifact art on rf.artifactid=art.artifactid \
            inner join outputmapping om on art.artifactid=om.outputartifactid \
            inner join processiotracker piot on piot.trackerid=om.trackerid \
            inner join artifact art2 on piot.inputartifactid=art2.artifactid \
            inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
            inner join process pr on piot.processid=pr.processid \
            inner join sample sa on sa.processid=asm.processid \
            where sa.processid = {sapid} and pr.typeid in ({tid}) and art2.isoriginal=True and art.name like '%Fragment Analyzer%{sname}' \
            order by pr.daterun desc;".format(
            sapid=sample.processid,
            tid=",".join(list(pc_cg.FRAGMENT_ANALYZER.keys())),
            sname=sample.name,
        )
        frag_an_file = self.session.query(GlsFile).from_statement(text(query)).first()
        # Special case for the OmniC Tissue and Lysate QC protocol
        if not frag_an_file:
            query = "select gf.* from glsfile gf \
                inner join resultfile rf on rf.glsfileid=gf.fileid \
                inner join artifact art on rf.artifactid=art.artifactid \
                inner join outputmapping om on art.artifactid=om.outputartifactid \
                inner join processiotracker piot on piot.trackerid=om.trackerid \
                inner join artifact art2 on piot.inputartifactid=art2.artifactid \
                inner join artifact_sample_map asm on art2.artifactid=asm.artifactid \
                inner join sample sa on sa.processid=asm.processid \
                inner join process pr on piot.processid=pr.processid \
                inner join processtype pt on pt.typeid=pr.typeid \
                inner join protocolstep ps on ps.processtypeid=pt.typeid \
                inner join labprotocol lp on lp.protocolid=ps.protocolid \
                where sa.processid = {sapid} and art.name like '%Fragment Analyzer%{sname}' and pr.typeid in ({tid}) and lp.protocolname='Tissue and Lysate QC' \
                order by pr.daterun desc;".format(
                sapid=sample.processid,
                tid=",".join(list(pc_cg.FRAGMENT_ANALYZER.keys())),
                sname=sample.name,
            )
            frag_an_file = self.session.query(GlsFile).from_statement(text(query)).first()
        if frag_an_file:
            self.obj["samples"][sample.name]["initial_qc"]["frag_an_image"] = f"https://{self.host}/api/v2/files/40-{frag_an_file.fileid}"
        else:
            self.log.info(f"Did not find an initial QC Fragment Analyzer for sample {sample.name}")
        # get GlsFile for output artifact of a Caliper process where its input is the initial artifact of a given sample
        query = "select gf.* from glsfile gf \
            inner join resultfile rf on rf.glsfileid=gf.fileid \
            inner join artifact art on rf.artifactid=art.artifactid \
            inner join outputmapping om on art.artifactid=om.outputartifactid \
            inner join processiotracker piot on piot.trackerid=om.trackerid \
            inner join artifact art2 on piot.inputartifactid=art2.artifactid \
            inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
            inner join process pr on piot.processid=pr.processid \
            inner join sample sa on sa.processid=asm.processid \
            where sa.processid = {sapid} and pr.typeid in ({tid}) and art2.isoriginal=True and art.name like '%CaliperGX%{sname}' \
            order by pr.daterun desc;".format(
            sapid=sample.processid,
            tid=",".join(list(pc_cg.CALIPER.keys())),
            sname=sample.name,
        )
        caliper_file = self.session.query(GlsFile).from_statement(text(query)).first()
        if caliper_file:
            self.obj["samples"][sample.name]["initial_qc"]["caliper_image"] = f"sftp://{self.host}/home/glsftp/{caliper_file.contenturi}"
        else:
            self.log.info(f"Did not find an initial QC Caliper for sample {sample.name}")

    def get_library_preps(self, sample):
        # first steps are either SetupWorksetPlate or Library Pooling Finished Libraries
        query = "select pr.* from process pr \
                inner join processiotracker piot on piot.processid=pr.processid \
                inner join artifact_sample_map asm on piot.inputartifactid=asm.artifactid \
                inner join sample sa on sa.processid=asm.processid \
                where sa.processid = {sapid} and pr.typeid in ({tid}) \
                order by pr.daterun;".format(
            sapid=sample.processid,
            tid=",".join(list(pc_cg.WORKSET.keys()) + list(pc_cg.PREPSTARTFINLIB.keys())),
        )  # Applications Generic Process
        lp_starts = self.session.query(Process).from_statement(text(query)).all()
        prepid = 64
        for one_libprep in lp_starts:
            if "library_prep" not in self.obj["samples"][sample.name]:
                self.obj["samples"][sample.name]["library_prep"] = {}

            # get all the output  artifacts of the libprep that match our sample
            query = f"select art.* from artifact art \
            inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
            inner join outputmapping om  on om.outputartifactid=art.artifactid \
            inner join processiotracker piot on piot.trackerid=om.trackerid \
            inner join sample sa on sa.processid=asm.processid \
            where sa.processid = {sample.processid} and piot.processid = {one_libprep.processid} and art.artifacttypeid = 2"
            lp_out_arts = self.session.query(Artifact).from_statement(text(query)).all()
            for one_libprep_art in lp_out_arts:
                prepid += 1
                prepname = chr(prepid)

                self.obj["samples"][sample.name]["library_prep"][prepname] = {}
                self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"] = {}
                self.obj["samples"][sample.name]["library_prep"][prepname]["sequenced_fc"] = []
                self.obj["samples"][sample.name]["library_prep"][prepname]["workset_setup"] = one_libprep.luid

                if str(one_libprep.typeid) in pc_cg.PREPSTARTFINLIB:
                    self.obj["isFinishedLib"] = True

                # get a list of all libprep start steps
                try:
                    libp = get_children_processes(
                        self.session,
                        one_libprep.processid,
                        pc_cg.PREPSTART,
                        sample=sample.processid,
                    )
                    older = libp[0]
                    for l in libp:
                        if (not older.daterun and l.daterun) or (l.daterun and older.daterun > l.daterun):
                            older = l
                    try:
                        self.obj["samples"][sample.name]["library_prep"][prepname]["prep_start_date"] = older.daterun.strftime("%Y-%m-%d")
                        if (
                            "first_prep_start_date" not in self.obj["samples"][sample.name]
                            or datetime.strptime(
                                self.obj["samples"][sample.name]["first_prep_start_date"],
                                "%Y-%m-%d",
                            )
                            > older.daterun
                        ):
                            self.obj["samples"][sample.name]["first_prep_start_date"] = older.daterun.strftime("%Y-%m-%d")
                        self.obj["samples"][sample.name]["library_prep"][prepname]["prep_start_date"] = older.daterun.strftime("%Y-%m-%d")
                    except AttributeError:
                        # Missing date run
                        pass
                except IndexError:
                    self.log.info(f"No libstart found for sample {sample.name}")
                    if str(one_libprep.typeid) in list(pc_cg.WORKSET.keys()):
                        if (
                            "first_prep_start_date" not in self.obj["samples"][sample.name]
                            or datetime.strptime(
                                self.obj["samples"][sample.name]["first_prep_start_date"],
                                "%Y-%m-%d",
                            )
                            > one_libprep.daterun
                        ):
                            self.obj["samples"][sample.name]["first_prep_start_date"] = one_libprep.daterun.strftime("%Y-%m-%d")
                        self.obj["samples"][sample.name]["library_prep"][prepname]["prep_start_date"] = one_libprep.daterun.strftime("%Y-%m-%d")
                pend = get_children_processes(
                    self.session,
                    one_libprep.processid,
                    pc_cg.PREPEND,
                    sample=sample.processid,
                )
                try:
                    recent = pend[0]
                    for l in pend:
                        if (not recent.daterun and l.daterun) or (l.daterun and recent.daterun < l.daterun):
                            recent = l
                    self.obj["samples"][sample.name]["library_prep"][prepname]["prep_finished_date"] = recent.daterun.strftime("%Y-%m-%d")
                    self.obj["samples"][sample.name]["library_prep"][prepname]["prep_id"] = recent.luid
                except (IndexError, AttributeError):
                    self.log.info(f"no prepend for sample {sample.name} prep {one_libprep.processid}")

                try:
                    agrlibvals = get_children_processes(
                        self.session,
                        one_libprep.processid,
                        list(pc_cg.AGRLIBVAL.keys()),
                        sample.processid,
                        "daterun desc",
                    )
                    agrlibval = None
                    for agrlv in agrlibvals:
                        # for small rna (and maybe others), there is more than one agrlibval, and I should not get the latest one,
                        # but the latest one that ran at sample level, not a pool level.
                        # get input artifact of a given process that belongs to sample
                        query = f"select art.* from artifact art \
                            inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                            inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                            inner join sample sa on sa.processid=asm.processid \
                            where sa.processid = {sample.processid} and piot.processid = {agrlv.processid}"
                        try:
                            inp_artifact = self.session.query(Artifact).from_statement(text(query)).first()

                            # Only skip the TruSeq small RNA protocol because we want the QC results of individual sample, not library pool
                            # For other protocols sample QC results should just copy the one of library pool
                            if (
                                len(inp_artifact.samples) > 1
                                and "by user" not in self.obj["details"]["library_construction_method"].lower()
                                and "in-house" not in self.obj["details"]["library_construction_method"].lower()
                                and "TruSeq small RNA" in self.obj["details"]["library_construction_method"]
                            ):
                                continue
                            else:
                                agrlibval = agrlv
                                break
                        except NoResultFound:
                            pass

                    # try and get seqruns for this library, this should work for most of the cases
                    # but not entirely sure if it would work for edgy cases
                    try:
                        query = "select distinct pro.* from process pro \
                                 inner join processiotracker piot on piot.processid = pro.processid \
                                 inner join artifact_ancestor_map aam on piot.inputartifactid = aam.artifactid \
                                 where pro.typeid in ({seq_step_id}) and aam.ancestorartifactid = {lib_art}".format(
                            seq_step_id=",".join(pc_cg.SEQUENCING.keys()),
                            lib_art=inp_artifact.artifactid,
                        )
                        seq_fcs = self.session.query(Process).from_statement(text(query)).all()
                        for seq in seq_fcs:
                            seq_fc_id = seq.udf_dict.get("Run ID")
                            if seq_fc_id and seq_fc_id not in self.obj["samples"][sample.name]["library_prep"][prepname]["sequenced_fc"]:
                                self.obj["samples"][sample.name]["library_prep"][prepname]["sequenced_fc"].append(seq_fc_id)
                    except Exception:
                        self.log.warn(f"Problem finding sequenced fc for sample {sample.name}")
                        pass

                    # Get barcode for finlib
                    if "by user" in self.obj["details"]["library_construction_method"].lower() or "in-house" in self.obj["details"]["library_construction_method"].lower():
                        # Get initial artifact for given sample
                        query = f"select art.* from artifact art \
                            inner join artifact_sample_map asm on asm.artifactid=art.artifactid \
                            inner join sample sa on sa.processid=asm.processid \
                            where sa.processid = {sample.processid} and art.isoriginal=True"
                        try:
                            initial_artifact = self.session.query(Artifact).from_statement(text(query)).one()
                            self.obj["samples"][sample.name]["library_prep"][prepname]["reagent_label"] = initial_artifact.reagentlabels[0].name
                            self.obj["samples"][sample.name]["library_prep"][prepname]["barcode"] = self.extract_barcode(initial_artifact.reagentlabels[0].name)
                        except:
                            pass

                    # raises AttributeError on no aggregate
                    self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid] = {}
                    try:
                        self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["finish_date"] = agrlibval.daterun.strftime("%Y-%m-%d")
                    except AttributeError:
                        pass
                    self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["initials"] = agrlibval.technician.researcher.initials
                    # get input artifact of a given process that belongs to sample and descends from one_lp_art
                    query = f"select art.* from artifact art \
                        inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                        inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                        inner join sample sa on sa.processid=asm.processid \
                        inner join artifact_ancestor_map aam on art.artifactid=aam.artifactid \
                        where sa.processid = {sample.processid} \
                        and piot.processid = {agrlibval.processid} \
                        and aam.ancestorartifactid={one_libprep_art.artifactid}"
                    try:
                        try:
                            inp_artifact = self.session.query(Artifact).from_statement(text(query)).one()
                        except MultipleResultsFound:
                            # this might happen when samples have been requeued and end up in the same aggragate QC as the originals.
                            # Select the artifact that has been routed to the next step. If there is more than one, take the most recent one.
                            artifacts = self.session.query(Artifact).from_statement(text(query)).all()
                            inp_artifact = None
                            date_routed = None
                            for art in artifacts:
                                for action in art.routes:
                                    if action.actiontype == "ADVANCE":
                                        if not date_routed or action.lastmodifieddate > date_routed:
                                            inp_artifact = art
                                            date_routed = action.lastmodifieddate
                            if not inp_artifact:
                                self.log.error(f"Multiple copies of the same sample {sample.name} found in step {sample.name},  None of them is routed. Skipping the libprep ")
                                continue
                        except NoResultFound:
                            # for the case of finished Libraries
                            query = f"select art.* from artifact art \
                                inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                                inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                                inner join sample sa on sa.processid=asm.processid \
                                where sa.processid = {sample.processid} and piot.processid = {agrlv.processid}"
                            inp_artifact = self.session.query(Artifact).from_statement(text(query)).first()

                        self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid].update(self.make_normalized_dict(inp_artifact.udf_dict))
                        self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["prep_status"] = inp_artifact.qc_flag
                        self.obj["samples"][sample.name]["library_prep"][prepname]["prep_status"] = inp_artifact.qc_flag
                        self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["well_location"] = inp_artifact.containerplacement.api_string
                        if (
                            "by user" not in self.obj["details"]["library_construction_method"].lower()
                            and "in-house" not in self.obj["details"]["library_construction_method"].lower()
                            and len(inp_artifact.reagentlabels) == 1
                        ):
                            # if finlib, these are already computed
                            self.obj["samples"][sample.name]["library_prep"][prepname]["reagent_label"] = inp_artifact.reagentlabels[0].name
                            self.obj["samples"][sample.name]["library_prep"][prepname]["barcode"] = self.extract_barcode(inp_artifact.reagentlabels[0].name)
                        elif (
                            "by user" not in self.obj["details"]["library_construction_method"].lower()
                            and "in-house" not in self.obj["details"]["library_construction_method"].lower()
                            and len(inp_artifact.reagentlabels) > 1
                        ):
                            # For cases that samples are indexed and pooled prior to Library QC
                            for iaa in inp_artifact.ancestors:
                                if iaa.reagentlabels and len(iaa.samples) == 1 and iaa.samples[0].name == sample.name:
                                    self.obj["samples"][sample.name]["library_prep"][prepname]["reagent_label"] = iaa.reagentlabels[0].name
                                    self.obj["samples"][sample.name]["library_prep"][prepname]["barcode"] = self.extract_barcode(iaa.reagentlabels[0].name)
                        # get libval steps from the same input art
                        query = "select pr.* from process pr \
                            inner join processiotracker piot on piot.processid=pr.processid \
                            where pr.typeid in ({dem}) and piot.inputartifactid={iaid} \
                            order by pr.daterun;".format(
                            dem=",".join(list(pc_cg.LIBVAL.keys())),
                            iaid=inp_artifact.artifactid,
                        )
                        libvals = self.session.query(Process).from_statement(text(query)).all()
                        try:
                            self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["start_date"] = libvals[0].daterun.strftime("%Y-%m-%d")
                        except IndexError:
                            self.log.info(f"no library validation steps found for sample {sample.name} prep {agrlibval.luid}")
                            try:
                                self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["start_date"] = agrlibval.daterun.strftime("%Y-%m-%d")
                            except AttributeError:
                                self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["start_date"] = agrlibval.createddate.strftime("%Y-%m-%d")
                        # get GlsFile for output artifact of a Fragment Analyzer process where its input is the initial artifact of a given sample
                        query = "select gf.* from glsfile gf \
                            inner join resultfile rf on rf.glsfileid=gf.fileid \
                            inner join artifact art on rf.artifactid=art.artifactid \
                            inner join outputmapping om on art.artifactid=om.outputartifactid \
                            inner join processiotracker piot on piot.trackerid=om.trackerid \
                            inner join artifact art2 on piot.inputartifactid=art2.artifactid \
                            inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                            inner join process pr on piot.processid=pr.processid \
                            inner join sample sa on sa.processid=asm.processid \
                            where sa.processid = {sapid} and pr.typeid in ({tid}) and art2.artifactid={inpid} and art.name like '%Fragment Analyzer%{sname}' \
                            order by pr.daterun desc;".format(
                            sapid=sample.processid,
                            tid=",".join(list(pc_cg.FRAGMENT_ANALYZER.keys())),
                            inpid=inp_artifact.artifactid,
                            sname=sample.name,
                        )
                        frag_an_file = self.session.query(GlsFile).from_statement(text(query)).first()
                        if frag_an_file:
                            self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["frag_an_image"] = (
                                f"https://{self.host}/api/v2/files/40-{frag_an_file.fileid}"
                            )
                        else:
                            self.log.info(f"Did not find a libprep Fragment Analyzer for sample {sample.name}")
                        # Get Ratio(%) from Fragment Analyzer QC
                        query = f"select art.* from artifact art \
                            inner join artifact_sample_map asm on art.artifactid=asm.artifactid \
                            inner join sample sa on sa.processid=asm.processid \
                            where sa.processid={sample.processid} and art.name like 'Fragment Analyzer%{sample.name}';"
                        frag_an_artifact = self.session.query(Artifact).from_statement(text(query)).all()
                        if frag_an_artifact:
                            frag_an_ratio = frag_an_artifact[0].udf_dict.get("Ratio (%)", "")
                            if frag_an_ratio:
                                self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["frag_an_ratio"] = frag_an_ratio
                        # get GlsFile for output artifact of a Caliper process where its input is given
                        query = "select gf.* from glsfile gf \
                            inner join resultfile rf on rf.glsfileid=gf.fileid \
                            inner join artifact art on rf.artifactid=art.artifactid \
                            inner join outputmapping om on art.artifactid=om.outputartifactid \
                            inner join processiotracker piot on piot.trackerid=om.trackerid \
                            inner join artifact art2 on piot.inputartifactid=art2.artifactid \
                            inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                            inner join process pr on piot.processid=pr.processid \
                            inner join sample sa on sa.processid=asm.processid \
                            where sa.processid = {sapid} and pr.typeid in ({tid}) and art2.artifactid={inpid} and art.name like '%CaliperGX%{sname}' \
                            order by pr.daterun desc;".format(
                            sapid=sample.processid,
                            inpid=inp_artifact.artifactid,
                            tid=",".join(list(pc_cg.CALIPER.keys())),
                            sname=sample.name,
                        )
                        try:
                            caliper_file = self.session.query(GlsFile).from_statement(text(query)).first()
                            self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["caliper_image"] = (
                                f"sftp://{self.host}/home/glsftp/{caliper_file.contenturi}"
                            )
                        except AttributeError:
                            self.log.info(f"Did not find a libprep caliper image for sample {sample.name}")
                        # handling neoprep
                        if "NeoPrep" in agrlibval.type.displayname:
                            try:
                                self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["concentration"] = inp_artifact.udf_dict["Normalized conc. (nM)"]
                                self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["conc_units"] = "nM"
                            except KeyError:
                                # The first neoprep projects did not go that well and have no concentration.
                                pass

                            # get output resultfile named like the sample of a Neoprep QC
                            query = f"select art.* from artifact art \
                                inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                                inner join outputmapping om on art.artifactid=om.outputartifactid \
                                inner join processiotracker piot on piot.trackerid=om.trackerid \
                                inner join sample sa on sa.processid=asm.processid \
                                where art.artifacttypeid = 1 \
                                and art.name like '%{sample.name}%' \
                                and sa.processid = {sample.processid}\
                                and piot.processid = {agrlibval.processid} \
                                and piot.inputartifactid = {inp_artifact.artifactid}"
                            try:
                                out_art = self.session.query(Artifact).from_statement(text(query)).one()
                                self.obj["samples"][sample.name]["library_prep"][prepname]["prep_status"] = out_art.qc_flag
                                self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["prep_status"] = out_art.qc_flag

                            except NoResultFound:
                                self.log.info(f"Did not find the output resultfile of the Neoprep step for sample {sample.name}")
                    except NoResultFound:
                        pass
                    # cleaning up
                    if "size_(bp)" in self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]:
                        self.obj["samples"][sample.name]["library_prep"][prepname]["library_validation"][agrlibval.luid]["average_size_bp"] = self.obj["samples"][sample.name]["library_prep"][
                            prepname
                        ]["library_validation"][agrlibval.luid]["size_(bp)"]
                except AttributeError:
                    self.log.info(f"No aggregate for sample {sample.name} prep {one_libprep.luid}")
                # get output analyte of a given process that belongs to sample and has one_libprep_art as ancestor
                # Here I commented out the old query from Denis that did not work any more, but I'd like to keep it in case anything is wrong
                # query = "select art.* from artifact art \
                # inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                # inner join outputmapping om on art.artifactid=om.outputartifactid \
                # inner join processiotracker piot on piot.trackerid=om.trackerid \
                # inner join sample sa on sa.processid=asm.processid \
                # inner join artifact_ancestor_map aam on art.artifactid=aam.artifactid \
                # where art.artifacttypeid = 2 \
                # and sa.processid = {sapid} \
                # and piot.processid = {agrid} \
                # and aam.ancestorartifactid = {libartid}".format(sapid=sample.processid, agrid=one_libprep.processid, libartid=one_libprep_art.artifactid)
                query = f"select art.* from artifact art \
                    inner join artifact_sample_map asm on art.artifactid=asm.artifactid \
                    inner join outputmapping om on om.outputartifactid=art.artifactid \
                    inner join processiotracker piot on piot.trackerid=om.trackerid \
                    inner join sample sa on sa.processid=asm.processid \
                    where sa.processid = {sample.processid} \
                    and piot.processid = {one_libprep.processid} \
                    and art.artifacttypeid = 2"
                try:
                    # out_artifact = self.session.query(Artifact).from_statement(text(query)).one() This is with the old query from Denis
                    out_artifact = self.session.query(Artifact).from_statement(text(query)).all()[0]
                    self.obj["samples"][sample.name]["library_prep"][prepname]["workset_name"] = out_artifact.containerplacement.container.name
                    self.obj["samples"][sample.name]["library_prep"][prepname]["amount_taken_(ng)"] = out_artifact.udf_dict.get("Amount taken (ng)")
                    self.obj["samples"][sample.name]["library_prep"][prepname]["amount_for_prep_(ng)"] = out_artifact.udf_dict.get("Amount for prep (ng)")
                    self.obj["samples"][sample.name]["library_prep"][prepname]["amount_for_prep_(fmol)"] = out_artifact.udf_dict.get("Amount for prep (fmol)")
                    self.obj["samples"][sample.name]["library_prep"][prepname]["amount_taken_from_plate_(ng)"] = out_artifact.udf_dict.get("Amount taken from plate (ng)")
                    self.obj["samples"][sample.name]["library_prep"][prepname]["volume_(ul)"] = out_artifact.udf_dict.get("Total Volume (uL)")

                except NoResultFound:
                    self.log.info(f"Did not find the output the Setup Workset Plate for sample {sample.name}")
                # preprep
                query = "select pr.* from process pr \
                    inner join processiotracker piot on piot.processid=pr.processid \
                    inner join artifact_sample_map asm on piot.inputartifactid=asm.artifactid \
                    inner join sample sa on sa.processid=asm.processid \
                    where sa.processid = {sapid} and pr.typeid in ({tid}) \
                    order by pr.daterun;".format(
                    sapid=sample.processid,
                    tid=",".join(list(pc_cg.PREPREPSTART.keys())),
                )
                try:
                    preprep = self.session.query(Process).from_statement(text(query)).first()
                    self.obj["samples"][sample.name]["library_prep"][prepname]["pre_prep_start_date"] = preprep.daterun.strftime("%Y-%m-%d")
                    if (
                        "first_prep_start_date" not in self.obj["samples"][sample.name]
                        or datetime.strptime(
                            self.obj["samples"][sample.name]["first_prep_start_date"],
                            "%Y-%m-%d",
                        )
                        > preprep.daterun
                    ):
                        self.obj["samples"][sample.name]["first_prep_start_date"] = preprep.daterun.strftime("%Y-%m-%d")
                except AttributeError:
                    self.log.info(f"Did not find a preprep for sample {sample.name}")

                # get seqruns
                seqs = get_children_processes(
                    self.session,
                    one_libprep.processid,
                    list(pc_cg.SEQUENCING.keys()),
                    sample=sample.processid,
                )
                for seq in seqs:
                    if "sample_run_metrics" not in self.obj["samples"][sample.name]["library_prep"][prepname]:
                        self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"] = {}
                    seqstarts = get_processes_in_history(
                        self.session,
                        seq.processid,
                        list(pc_cg.SEQSTART.keys()),
                        sample=sample.processid,
                    )
                    dilstarts = get_processes_in_history(
                        self.session,
                        seq.processid,
                        list(pc_cg.DILSTART.keys()),
                        sample=sample.processid,
                    )
                    # get all the input artifacts of the seqrun that match our sample and our libprep
                    query = f"select art.* from artifact art \
                    inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                    inner join processiotracker piot on piot.inputartifactid=art.artifactid \
                    inner join sample sa on sa.processid=asm.processid \
                    inner join artifact_ancestor_map aam on aam.artifactid=art.artifactid \
                    where sa.processid = {sample.processid} \
                    and piot.processid = {seq.processid} \
                    and aam.ancestorartifactid = {one_libprep_art.artifactid}"
                    inp_arts = self.session.query(Artifact).from_statement(text(query)).all()
                    for art in inp_arts:
                        # 2559 is ONT
                        if seq.typeid != 2559:
                            if seq.typeid == 46:
                                # miseq
                                lane = art.containerplacement.api_string.split(":")[1]
                            else:
                                lane = art.containerplacement.api_string.split(":")[0]
                            self.obj["sequencing_finished"] = seq.udf_dict.get("Finish Date")
                            try:
                                run_id = seq.udf_dict["Run ID"]
                                date = run_id.split("_")[0]
                                fcid = run_id.split("_")[3]
                                seqrun_barcode = self.obj["samples"][sample.name]["library_prep"][prepname]["barcode"]
                                samp_run_met_id = "_".join([lane, date, fcid, seqrun_barcode])
                                self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id] = {}
                                self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id]["sequencing_finish_date"] = seq.udf_dict.get("Finish Date")
                                self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id]["seq_qc_flag"] = art.qc_flag
                                try:
                                    self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id]["sequencing_start_date"] = seqstarts[0].daterun.strftime(
                                        "%Y-%m-%d"
                                    )
                                except AttributeError:
                                    self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id]["sequencing_start_date"] = seqstarts[0].createddate.strftime(
                                        "%Y-%m-%d"
                                    )
                                self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id]["sample_run_metrics_id"] = None  # Deprecated
                                try:
                                    self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id]["dillution_and_pooling_start_date"] = dilstarts[
                                        0
                                    ].daterun.strftime("%Y-%m-%d")
                                except AttributeError:
                                    self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id]["dillution_and_pooling_start_date"] = dilstarts[
                                        0
                                    ].createddate.strftime("%Y-%m-%d")
                                except IndexError:
                                    self.log.info(f"no dilution found for sequencing {seq.processid} of sample {sample.name}")
                                # get the associated demultiplexing step
                                query = f"select pr.* from process pr \
                                        inner join processiotracker piot on piot.processid=pr.processid \
                                        where pr.typeid={list(pc_cg.DEMULTIPLEX.keys())[0]} and piot.inputartifactid={art.artifactid};"
                                try:
                                    dem = self.session.query(Process).from_statement(text(query)).one()
                                    try:
                                        self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id]["sequencing_run_QC_finished"] = dem.daterun.strftime(
                                            "%Y-%m-%d"
                                        )
                                    except AttributeError:
                                        pass

                                    # get output resultfile named like the sample of a Demultiplex step
                                    query = f"select art.* from artifact art \
                                        inner join artifact_sample_map asm on  art.artifactid=asm.artifactid \
                                        inner join outputmapping om on art.artifactid=om.outputartifactid \
                                        inner join processiotracker piot on piot.trackerid=om.trackerid \
                                        inner join sample sa on sa.processid=asm.processid \
                                        inner join artifact_ancestor_map aam on art.artifactid= aam.artifactid \
                                        where art.artifacttypeid = 1 \
                                        and art.name like '%{sample.name}%' \
                                        and sa.processid = {sample.processid} \
                                        and piot.processid = {dem.processid}\
                                        and aam.ancestorartifactid = {art.artifactid};"
                                    out_arts = self.session.query(Artifact).from_statement(text(query)).all()
                                    cumulated_flag = "FAILED"
                                    for art in out_arts:
                                        if art.qc_flag == "PASSED":
                                            cumulated_flag = "PASSED"

                                    self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id]["dem_qc_flag"] = cumulated_flag

                                except NoResultFound:
                                    try:
                                        self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id]["sequencing_run_QC_finished"] = seq.daterun.strftime(
                                            "%Y-%m-%d"
                                        )
                                    except AttributeError:
                                        self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id]["sequencing_run_QC_finished"] = seq.createddate.strftime(
                                            "%Y-%m-%d"
                                        )

                                self.log.info(f"no demultiplexing found for sample {sample.name}, sequencing {seq.processid}")
                            except:
                                self.log.info(f"no run id for sequencing process {seq.luid}")
                        # If it is ONT
                        else:
                            run_name = art.udf_dict.get("ONT run name")
                            date = run_name.split("_")[0]
                            samp_run_met_id = run_name
                            self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id] = {}
                            self.obj["samples"][sample.name]["library_prep"][prepname]["sample_run_metrics"][samp_run_met_id]["sequencing_start_date"] = f"{date[:4]}-{date[4:6]}-{date[6:]}"

    def extract_barcode(self, chain):
        barcode = ""
        bcp = re.compile(r"[ATCG\-\_]{4,}")
        TENX_SINGLE_PAT = re.compile(r"SI-(?:GA|NA)-[A-H][1-9][0-2]?")
        TENX_DUAL_PAT = re.compile(r"SI-(?:TT|NT|NN|TN|TS)-[A-H][1-9][0-2]?")
        SMARTSEQ_PAT = re.compile(r"SMARTSEQ[1-9]?-[1-9][0-9]?[A-P]")
        if "NoIndex" in chain:
            return chain
        if TENX_SINGLE_PAT.match(chain) or TENX_DUAL_PAT.match(chain) or SMARTSEQ_PAT.match(chain):
            return chain
        if "(" not in chain:
            barcode = chain
        else:
            pattern = re.compile(r"\(([A-Z\-\_]+)\)")
            matches = pattern.search(chain)
            if matches.group(1):
                barcode = matches.group(1).replace("_", "-")
        matches = bcp.match(barcode)
        if not matches:
            meta = self.session.query(ReagentType.meta_data).filter(ReagentType.name.like(f"%{barcode}%")).scalar()
            matches = bcp.search(meta)
            if matches:
                barcode = matches.group(0).replace("_", "-")
        return barcode

    def set_status(self):
        proj_details = self.obj.get("details")
        status_fields = {}

        # Convenience string status field
        status_fields["status"] = None

        # Boolean status fields
        status_fields["aborted"] = False
        status_fields["closed"] = False
        status_fields["ongoing"] = False
        status_fields["open"] = False
        status_fields["pending"] = False
        status_fields["reception_control"] = False

        # Tags
        status_fields["need_review"] = False

        if proj_details.get("aborted"):
            status_fields["status"] = "Aborted"
            status_fields["aborted"] = True
            status_fields["closed"] = True
        else:
            if self.obj.get("close_date"):
                status_fields["status"] = "Closed"
                status_fields["closed"] = True
            else:
                if self.obj.get("open_date"):
                    if self.obj.get("escalations"):
                        status_fields["need_review"] = True

                    if proj_details.get("queued") or self.obj.get("project_summary", {}).get("queued"):
                        status_fields["status"] = "Ongoing"
                        status_fields["ongoing"] = True
                        status_fields["open"] = True
                    else:
                        status_fields["status"] = "Reception Control"
                        status_fields["reception_control"] = True
                        status_fields["open"] = True
                else:
                    status_fields["status"] = "Pending"
                    status_fields["pending"] = True

        self.obj["status_fields"] = status_fields
