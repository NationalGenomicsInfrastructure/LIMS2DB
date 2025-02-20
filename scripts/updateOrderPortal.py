"""Script to update projects from LIMS
to corresponding orders in Order Portal."""

import argparse
import json
import os
from datetime import date, timedelta

import genologics_sql.utils
import requests
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.lims import Lims, Project
from genologics_sql import queries

import LIMS2DB.utils as lutils


class Order_Portal_APIs:
    def __init__(self, url, headers, log):
        self.base_url = url
        self.headers = headers
        self.log = log
        self.lims = Lims(BASEURI, USERNAME, PASSWORD)

    def update_order_internal_id(self, open_date, dry_run, project_id):
        if project_id:
            pjs = [Project(self.lims, id=project_id)]
        else:
            pjs = self.lims.get_projects(open_date=open_date.strftime("%Y-%m-%d"))
        for project in pjs:
            try:
                pass
            except requests.exceptions.HTTPError:  # project does not exist in LIMS
                self.log.info(f"Project {project.id} not found in LIMS")
                continue

            try:
                ORDER_ID = project.udf["Portal ID"]
            except KeyError:
                continue
            if not ORDER_ID.startswith("NGI"):
                continue

            url = f"{self.base_url}/api/v1/order/{ORDER_ID}"
            data = {
                "fields": {
                    "project_ngi_name": project.name,
                    "project_ngi_identifier": project.id,
                }
            }
            if not dry_run:
                response = requests.post(url, headers=self.headers, json=data)
                assert response.status_code == 200, (
                    response.status_code,
                    response.reason,
                )

                self.log.info(f"Updated internal id for order: {ORDER_ID} - {project.id}")
            else:
                print(f"Dry run: {date.today()} Updated internal id for order: {ORDER_ID} - {project.id}")

    def update_order_status(self, project_id, dry_run):
        lims_db = genologics_sql.utils.get_session()
        pjs = set()
        if project_id:
            pjs.add(project_id)
        else:
            pjs = queries.get_last_modified_projectids(lims_db, "25 hours")
        for p in pjs:
            project = Project(self.lims, id=p)
            try:
                ORDER_ID = project.udf["Portal ID"]
            except KeyError:
                continue
            if not ORDER_ID.startswith("NGI"):
                continue
            url = f"{self.base_url}/api/v1/order/{ORDER_ID}"
            response = requests.get(url, headers=self.headers)
            data = ""
            try:
                data = response.json()
            except ValueError:  # In case a portal id does not exit on lims, skip the proj
                continue
            url = ""

            if data["status"] == "accepted":
                if project.udf.get("Aborted"):
                    url = data["links"]["aborted"]["href"]
                    status_set = "aborted"
                elif project.close_date:
                    url = data["links"]["closed"]["href"]
                    status_set = "closed"
                elif project.udf.get("Queued") and not project.close_date:
                    url = data["links"]["processing"]["href"]
                    status_set = "processing"
            if data["status"] == "processing":
                if project.udf.get("Aborted"):
                    url = data["links"]["aborted"]["href"]
                    status_set = "aborted"
                elif project.close_date:
                    url = data["links"]["closed"]["href"]
                    status_set = "closed"
            if url:
                if not dry_run:
                    # Order portal sends a mail to user on status change
                    response = requests.post(url, headers=self.headers)
                    assert response.status_code == 200, (
                        response.status_code,
                        response.reason,
                    )
                    self.log.info(f"Updated status for order {ORDER_ID} from {data['status']} to {status_set}")
                else:
                    print(f"Dry run: {date.today()} Updated status for order {ORDER_ID} from {data['status']} to {status_set}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "config",
        metavar="Path to config file",
        help="Path to config file with URL and API key in JSON format.",
    )
    parser.add_argument(
        "option",
        metavar="Option to update order",
        help="Choose which order fields to update, either OrderStatus or OrderInternalID",
    )
    parser.add_argument(
        "-l",
        "--log",
        dest="logfile",
        default=os.path.join(os.environ["HOME"], "log/LIMS2DB", "OrderPortal_update.log"),
        help="log file.  Default: ~/log/LIMS2DB/OrderPortal_update.log",
    )
    parser.add_argument(
        "-d",
        "--dryrun",
        action="store_true",
        dest="dryrun",
        default=False,
        help="dry run: no changes stored",
    )
    parser.add_argument(
        "-p",
        "--projectid",
        dest="project_id",
        default=None,
        help="Internal ID of the order to update",
    )
    args = parser.parse_args()
    log = lutils.setupLog("orderlogger", args.logfile)

    with open(args.config) as config_file:
        creds = json.load(config_file)
    OP_BASE_URL = creds["OrderPortal"].get("URL")  # Base URL for your OrderPortal instance.
    API_KEY = creds["OrderPortal"].get("API_KEY")  # API key for the user account.
    headers = {"X-OrderPortal-API-key": API_KEY}
    ord_port_apis = Order_Portal_APIs(OP_BASE_URL, headers, log)

    if args.option == "OrderStatus":
        ord_port_apis.update_order_status(args.project_id, args.dryrun)
    elif args.option == "OrderInternalID":
        ord_port_apis.update_order_internal_id(date.today() - timedelta(days=1), args.dryrun, args.project_id)
