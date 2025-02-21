import argparse
import datetime
import os
import random

import yaml
from statusdb.db.utils import load_couch_server

import LIMS2DB.diff as df


def write_results_to_file(diffs, args):
    with open(args.resultfile, "w") as f:
        for proj_id, diff_tuple in diffs.items():
            if diff_tuple:
                f.write(f"Project {proj_id} :\n")
            for diff_key, diff_val in diff_tuple[0].items():
                f.write(f" {diff_key} : was {diff_val[0]}, is {diff_val[1]}\n")


def main(args):
    couch = load_couch_server(args.conf)
    proj_db = couch["projects"]

    with open(args.oconf) as ocf:
        oconf = yaml.load(ocf, Loader=yaml.SafeLoader)["order_portal"]

    diffs = {}
    if args.pj_id:
        diffs[args.pj_id] = df.diff_project_objects(args.pj_id, couch, proj_db, args.log, oconf)

    elif args.random:
        random.seed()
        closed_ids = []
        proj_db = couch["projects"]
        view = proj_db.view("project/summary")
        for row in view[["closed", ""] : ["closed", "ZZZZZZZZ"]]:
            if row.value.get("open_date", "0") > "2014-06-01":
                closed_ids.append(row.key[1])
        nb = int(len(closed_ids) / 10)
        picked_ids = random.sample(closed_ids, nb)
        for one_id in picked_ids:
            diffs[one_id] = df.diff_project_objects(one_id, couch, proj_db, args.log, oconf)
    else:
        view = proj_db.view("project/project_id")
        for row in view:
            proj_diff = df.diff_project_objects(row.key, couch, proj_db, args.log, oconf)
            if proj_diff is not None:
                diffs[row.key] = proj_diff

    write_results_to_file(diffs, args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare the results of the installed PSUL with the contents of the DB")
    parser.add_argument(
        "--conf",
        default=os.path.expanduser("~/opt/config/post_process.yaml"),
        help="configuration file path. default is ~/opt/config/post_process.yaml",
    )
    parser.add_argument(
        "--oconf",
        default=os.path.expanduser("~/conf/orderportal_cred.yaml"),
        help="Credentials for order portal",
    )
    parser.add_argument(
        "--log",
        "-l",
        dest="log",
        default=os.path.expanduser("~/psul_validation.log"),
        help="log file path. default is ~/psul_validation.log",
    )
    parser.add_argument(
        "--result",
        "-r",
        dest="resultfile",
        default=os.path.expanduser(f"~/psul_validations/{datetime.datetime.now().isoformat()}_psul_validation.out"),
        help="validation output path. default is ~/psul_validations/{date}_psul_validation.out",
    )
    parser.add_argument("--project", "-p", dest="pj_id", help="project id to perform the check")
    parser.add_argument(
        "--randomsample",
        "-s",
        action="store_true",
        dest="random",
        help="pick a random subset of projects to check",
    )

    args = parser.parse_args()

    main(args)
