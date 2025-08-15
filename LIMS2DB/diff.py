from genologics_sql.utils import get_configuration, get_session
from ibm_cloud_sdk_core.api_exception import ApiException

from LIMS2DB.utils import setupLog


def diff_project_objects(pj_id, couch, logfile, oconf):
    # Import is put here to defer circular imports
    from LIMS2DB.classes import ProjectSQL

    log = setupLog(f"diff - {pj_id}", logfile)

    def fetch_project(pj_id):
        result = couch.post_view(
            db="projects",
            ddoc="projects",
            view="lims_followed",
            key=pj_id,
            include_docs=True,
        ).get_result()["rows"]
        if not result:
            log.error(f"No project found in couch for {pj_id}")
            return None
        return result[0]["doc"]

    try:
        old_project = fetch_project(pj_id)
    except ApiException:
        log.error("Connection issues after large project")
        # Retry
        old_project = fetch_project(pj_id)

    if old_project is None:
        return None

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
