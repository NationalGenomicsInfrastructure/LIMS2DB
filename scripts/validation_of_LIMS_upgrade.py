#!/usr/bin/env python

"""valitadion_of_LIMS_upgrade.py is a script to compare extraction output from lims stage
server and lims production server. The comparison is based on the objects created to build
documents in the projects database on status db. A recursive function compares all values
in the objects and any differing values or missing keys are logged in a validation log file.

Maya Brandi, Science for Life Laboratory, Stockholm, Sweden.
"""

usage = """

*****Recomended validation procedure:*****

Testing the script:

Test that the script is caching differences by changing something on the
stage server, eg. the value of the sample udf "status_(manual)". for some
project J.Doe_00_00. Then run the script with the -p flagg:

valitadion_of_LIMS_upgrade.py -p J.Doe_00_00

This should give the output:

Lims stage and Lims production are differing for proj J.Doe_00_00: True
Key status_(manual) differing: Lims production gives: Aborted. Lims stage gives In Progress.

Running the validation:

Run valitadion_of_LIMS_upgrade.py with the -a flagg and grep for "True" in
the logfile when the script is finished. It will take some hours to go through
all projects opened after jul 1

If you don't find anything when grepping for True in the log file, no differences
are found for any projects.

If you get output when grepping for True, there are differences. Then read the log
file to find what is differing.

"""
import sys
import os
import codecs
from optparse import OptionParser
from statusdb.db.utils import *
from LIMS2DB.objectsDB.functions import *
from pprint import pprint
from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
import LIMS2DB.objectsDB.objectsDB as DB
from datetime import date
lims = Lims('https://genologics.scilifelab.se:8443', USERNAME, PASSWORD)
lims_stage = Lims('https://genologics-stage.scilifelab.se:8443', USERNAME, PASSWORD)
import logging
import six
from six.moves import input


def comp_obj(stage, prod):
    """compares the two dictionaries obj and dbobj"""
    logging.info('project %s is being handeled' % stage['project_name'])
    diff = recursive_comp(stage, prod)
    logging.info('Lims stage and Lims production are differing for proj %s: %s' % ( stage['project_name'],diff))

def recursive_comp(stage, prod):
    diff = False
    keys = list(set(list(stage.keys()) + list(prod.keys())))
    for key in keys:
        if not (key in stage):
            logging.info('Key %s missing in Lims stage to db object ' % key)
            diff = True
        elif key not in prod:
            logging.info('Key %s missing in Lims production to db object ' % key)
            diff = True
        else:
            prod_val = prod[key]
            stage_val = stage[key]
            if (prod_val != stage_val):
                if (type(prod_val) is dict) and (type(stage_val) is dict):
                    diff = (diff and recursive_comp(stage_val, prod_val))
                else:
                    if 'genologics.scilifelab.se' in prod_val and 'genologics-stage.scilifelab.se' in stage_val:
                        stage_val.replace('genologics-stage.scilifelab.se', 'genologics.scilifelab.se')
                        if (prod_val != stage_val):
                            diff=True
                            logging.info('Key %s differing: Lims production gives: %s. Lims stage gives %s. ' %( key,prod_val,stage_val))
                        else:
                            diff=False
                    else:
                        diff = True
                        logging.info('Key %s differing: Lims production gives: %s. Lims stage gives %s. ' %( key,prod_val,stage_val))
    return diff

def  main(proj_name, all_projects, conf, only_closed):
    first_of_july = '2013-06-30'
    today = date.today()
    couch = load_couch_server(conf)
    if all_projects:
        projects = lims.get_projects()
        for proj in projects:
            closed = proj.close_date
            if not only_closed or (only_closed and closed):
                contin = True
            else:
                contin = False
            if contin:
                proj_name = proj.name
                try:
                    proj_stage = lims_stage.get_projects(name = proj_name)
                    if len(proj_stage)==0 :
                        logging.warning("""Found no projects on Lims stage with name %s""" % proj_name)
                    else:
                        proj_stage = proj_stage[0]
                        opened = proj.open_date
                        if opened:
                            if comp_dates(first_of_july, opened):
                                obj = DB.ProjectDB(lims, proj.id, None)
                                obj_stage = DB.ProjectDB(lims_stage, proj.id, None)
                                comp_obj(obj_stage.obj, obj.obj)
                                obj=None
                                obj_stage=None
                        else:
                            logging.info('Open date missing for project %s' % proj_name)
                except:
                    logging.info('Failed comparing stage and prod for proj %s' % proj_name)
    elif proj_name is not None:
        proj = lims.get_projects(name = proj_name)
        proj_stage = lims_stage.get_projects(name = proj_name)
        if (not proj) | (not proj_stage):
            logging.warning("""Found %s projects on Lims stage, and %s projects
                        on Lims production with project name %s""" % (str(len(proj_stage)), str(len(proj)), proj_name))
        else:
            proj = proj[0]
            proj_stage = proj_stage[0]
            opened = proj.open_date
            if opened:
                if comp_dates(first_of_july, opened):
                    cont = 'yes'
                else:
                    cont = input("""The project %s is opened before 2013-07-01.
                    Do you still want to load the data from lims into statusdb? (yes/no): """ % proj_name)
                if cont == 'yes':
                    obj = DB.ProjectDB(lims, proj.id, None)
                    obj_stage = DB.ProjectDB(lims_stage, proj.id, None)
                    comp_obj(obj_stage.obj, obj.obj)
            else:
                logging.info('Open date missing for project %s' % proj_name)

if __name__ == '__main__':
    parser = OptionParser(usage=usage)

    parser.add_option("-p", "--project", dest="project_name", default=None,
    help = "eg: M.Uhlen_13_01. Dont use with -a flagg.")

    parser.add_option("-a", "--all_projects", dest="all_projects", action="store_true", default=False,
    help = "Upload all Lims projects into couchDB. Don't use with -p flagg.")

    parser.add_option("-C", "--closed_projects", dest="closed_projects", action="store_true", default=False,
    help = "Upload only closed projects. Use with -a flagg.")

    parser.add_option("-c", "--conf", dest="conf",
    default=os.path.join(os.environ['HOME'],'opt/config/post_process.yaml'),
    help = "Config file.  Default: ~/opt/config/post_process.yaml")
    logging.basicConfig(filename='PSUL_validation.log',level=logging.INFO)

    (options, args) = parser.parse_args()
    main(options.project_name, options.all_projects, options.conf, options.closed_projects)
