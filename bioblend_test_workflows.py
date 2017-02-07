#!/usr/bin/env python
import argparse
import yaml
import re
import os
import sys
import glob
import json
import time
import random
import logging
import datetime
import subprocess
from six import iteritems, string_types
from bioblend import galaxy

logging.basicConfig(format='[%(asctime)s][%(lineno)d][%(module)s] %(message)s', level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("bioblend").setLevel(logging.WARNING)
NOW = datetime.datetime.now()
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

class Timer:
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start


class XUnitReportBuilder(object):
    XUNIT_TPL = """<?xml version="1.0" encoding="UTF-8"?>
    <testsuite name="{suite_name}" tests="{total}" errors="{errors}" failures="{failures}" skip="{skips}">
{test_cases}
    </testsuite>
    """

    TESTCASE_TPL = """        <testcase classname="{classname}" name="{name}" {time}>
{error}
        </testcase>"""

    ERROR_TPL = """            <error type="{test_name}" message="{errorMessage}">{errorDetails}
            </error>"""

    def __init__(self, suite_name):
        self.xunit_data = {
            'total': 0, 'errors': 0, 'failures': 0, 'skips': 0
        }
        self.test_cases = []
        self.suite_name = suite_name

    def ok(self, classname, test_name, time=0):
        logging.info("OK: [%s] %s", classname, test_name)
        self.xunit_data['total'] += 1
        self.__add_test(test_name, classname, errors="", time=time)

    def error(self, classname, test_name, errorMessage, errorDetails="", time=0):
        logging.info("ERROR: [%s] %s", classname, test_name)
        self.xunit_data['total'] += 1
        self.__add_test(test_name, classname, errors=self.ERROR_TPL.format(
            errorMessage=errorMessage, errorDetails=errorDetails, test_name=test_name), time=time)

    def failure(self, classname, test_name, errorMessage, errorDetails="", time=0):
        logging.info("FAIL: [%s] %s", classname, test_name)
        self.xunit_data['total'] += 1
        self.__add_test(test_name, classname, errors=self.ERROR_TPL.format(
            errorMessage=errorMessage, errorDetails=errorDetails, test_name=test_name), time=time)

    def skip(self, classname, test_name, time=0):
        logging.info("SKIP: [%s] %s", classname, test_name)
        self.xunit_data['skips'] += 1
        self.xunit_data['total'] += 1
        self.__add_test(test_name, classname, errors="            <skipped />", time=time)

    def __add_test(self, name, classname, errors, time=0):
        t = 'time="%s"' % time
        self.test_cases.append(
            self.TESTCASE_TPL.format(name=name, error=errors, classname=classname, time=t))

    def serialize(self):
        self.xunit_data['test_cases'] = '\n'.join(self.test_cases)
        self.xunit_data['suite_name'] = self.suite_name
        return self.XUNIT_TPL.format(**self.xunit_data)


xunit = XUnitReportBuilder('wf_tester')

def __main__():
    parser = argparse.ArgumentParser(description="""Script to run all workflows mentioned in workflows_to_test.
    It will import the shared workflows are create histories for each workflow run, prefixed with ``TEST_RUN_<date>:``
    Make sure the yaml has file names identical to those in the data library.""")

    parser.add_argument('-k', '--api-key', '--key', dest='key', metavar='your_api_key',
                        help='The account linked to this key needs to have admin right to upload by server path',
                        required=True)
    parser.add_argument('-u', '--url', dest='url', metavar="http://galaxy_url:port",
                        help="Be sure to specify the port on which galaxy is running",
                        default="http://usegalaxy.org")
    parser.add_argument("-d", "--data_library_id", dest='data_library_id', metavar='Data library ID',
                        help="Specify the ID of the data library in which the test dataset can be found",
                        default='TestingData')
    parser.add_argument('-w', "--yaml", "--workflow-inputs", dest="yaml", type=argparse.FileType('r'), metavar="Workflow input yaml file",
                        help="Specify a yaml file describing the worklfow to test and their inputs - see default",
                        default="testdata/workflow_example_parameters.yaml")
    parser.add_argument('-s', '--dry-run', dest="dry_run",
                        help="""Do not execute workflow, just show the call it would have made, helpful for identifying
                        the right parameters""", action="store_true", default=False)
    parser.add_argument('-x', '--xunit-output', dest="xunit_output", type=argparse.FileType('w'), default='report.xml',
                        help="""Location to store xunit report in""")
    args = parser.parse_args()

    workflows_to_test = yaml.load(args.yaml)

    gi = galaxy.GalaxyInstance(args.url, args.key)
    data_library = gi.libraries.get_libraries(library_id=args.data_library_id)
    test_workflows(gi, data_library, workflows_to_test, dry_run=args.dry_run)

    # Write out the report
    args.xunit_output.write(xunit.serialize())


def get_library(gio, data_library_name):
    return gio.libraries.get('a411ce27cdcc0a37')


def test_workflows(gi, data_library, workflows_to_test, dry_run=False):
    for wft in workflows_to_test:
        # Start time
        start_time = time.time()
        # Get our workflow info from the server
        wf = gi.workflows.get_workflows(workflow_id=wft['id'])[0]
        # Construct a hsitory name
        history_name = "TEST_RUN_%s: %s" % (time.strftime("%Y-%m-%d"), wf['name'])
        # Logging, in case anyone is watching.
        logging.info("Running workflow: %s with results to: %s" % (wf['name'], history_name))

        # Launch workflow
        invocation = gi.workflows.invoke_workflow(
            wf['id'],
            inputs=wft['inputs'],
            history_name=history_name,
        )

        result, result_extra = watch_workflow_invocation(gi, wf['id'], invocation['id'])
        # Finish time
        finish_time = time.time()
        if result == 'Success':
            xunit.ok('workflow_test', wf['name'], time=finish_time - start_time)
        else:
            xunit.failure('workflow_test', wf['name'], 'Workflow execution failed',
                          errorDetails=json.dumps(result_extra, indent=2),
                          time=finish_time - start_time)


def watch_workflow_invocation(gi, wf_id, invoke_id):
    latest_state = None
    while True:
        # Fetch the current state
        latest_state = gi.workflows.show_invocation(wf_id, invoke_id)
        # If it's scheduled, then let's look at steps. Otherwise steps probably don't exist yet.
        if latest_state['state'] == 'scheduled':
            steps = latest_state['steps']
            # Check if we're done / not
            all_done = True
            # Get step states
            states = [step['state'] for step in steps]
            # If any state is in error,
            logging.info("  Checking workflow %s states: %s", wf_id, '|'.join(map(str, states)))
            if any([state == 'error' for state in states]):
                # We bail
                return 'Fail', latest_state

            # If all OK
            if all([state is None or state == 'ok'
                    for state in states]):
                return "Success", None
                # We can finish
        time.sleep(5)
    return 'Fail', latest_state


if __name__ == "__main__":
    __main__()
