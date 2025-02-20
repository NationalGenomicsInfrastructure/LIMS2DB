import http.client as http_client

from genologics_sql.utils import get_configuration, get_session

from LIMS2DB.utils import setupLog


def diff_project_objects(pj_id, couch, proj_db, logfile, oconf):
    # Import is put here to defer circular imports
    from LIMS2DB.classes import ProjectSQL

    log = setupLog(f"diff - {pj_id}", logfile)

    view = proj_db.view("projects/lims_followed")

    def fetch_project(pj_id):
        try:
            old_project_couchid = view[pj_id].rows[0].value
        except (KeyError, IndexError):
            log.error(f"No such project {pj_id}")
            return None
        return old_project_couchid

    try:
        old_project_couchid = fetch_project(pj_id)
    except http_client.BadStatusLine:
        log.error("BadStatusLine received after large project")
        # Retry
        old_project_couchid = fetch_project(pj_id)

    if old_project_couchid is None:
        return None

    old_project = proj_db.get(old_project_couchid)
    old_project.pop("_id", None)
    old_project.pop("_rev", None)
    old_project.pop("modification_time", None)
    old_project.pop("creation_time", None)
    old_project["details"].pop("running_notes", None)
    old_project["details"].pop("snic_checked", None)

    session = get_session()
    host = get_configuration()["url"]
    new_project = ProjectSQL(session, log, pj_id, host, couch, oconf)

    fediff = diff_objects(old_project, new_project.obj)

    return (fediff, old_project, new_project.obj)


def diff_objects(o1, o2, parent=""):
    diffs = {}

    for key in o1:
        if key in o2:
            if isinstance(o1[key], dict):
                more_diffs = diff_objects(o1[key], o2[key], f"{parent} {key}")
                diffs.update(more_diffs)
            else:
                if o1[key] != o2[key]:
                    diffs[f"{parent} {key}"] = [o1[key], o2[key]]

        else:
            if o1[key]:
                diffs[f"key {parent} {key}"] = [o1[key], "missing"]

    for key in o2:
        if key not in o1 and o2[key]:
            diffs[f"key {parent} {key}"] = ["missing", o2[key]]

    return diffs


if __name__ == "__main__":
    a = {"a": 1, "b": 2, "c": {"d": 3, "e": {"f": 5}}}
    b = {"a": 1, "b": 7, "c": {"d": 4, "e": {"f": 4}}}
    diffs = diff_objects(a, b)
    print(diffs)
