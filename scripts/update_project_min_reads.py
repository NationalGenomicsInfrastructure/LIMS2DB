import argparse
import os

import yaml
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Project
from genologics.lims import Lims
from genologics_sql.queries import get_last_modified_projectids
from genologics_sql.utils import get_session

from LIMS2DB.utils import setupServer


def main(args):
    lims_db = get_session()
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    with open(args.conf) as cf:
        db_conf = yaml.load(cf, Loader=yaml.SafeLoader)
        couch = setupServer(db_conf)
    db = couch["expected_yields"]
    postgres_string = f"{args.hours} hours"
    project_ids = get_last_modified_projectids(lims_db, postgres_string)

    min_yields = {}
    for row in db.view("yields/min_yield"):
        db_key = " ".join(x if x else "" for x in row.key).strip()
        min_yields[db_key] = row.value

    for project in [Project(lims, id=x) for x in project_ids]:
        samples_count = 0
        samples = lims.get_samples(projectname=project.name)
        for sample in samples:
            if not ("Status (manual)" in sample.udf and sample.udf["Status (manual)"] == "Aborted"):
                samples_count += 1
        try:
            lanes_ordered = project.udf["Sequence units ordered (lanes)"]
            key = project.udf["Sequencing platform"]
        except:
            continue
        if key in min_yields:
            value = min_yields[key]
            try:
                project.udf["Reads Min"] = float(value) * lanes_ordered / samples_count
                project.put()
            except ZeroDivisionError:
                pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=2, help="Amount of hours to check for. Default=2")
    parser.add_argument(
        "--conf",
        default=os.path.join(os.environ["HOME"], "opt/config/post_process.yaml"),
        help="Amount of hours to check for. Default=2",
    )
    args = parser.parse_args()
    main(args)
