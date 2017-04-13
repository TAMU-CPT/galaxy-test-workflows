#!/usr/bin/env python
import argparse
import os
import time
import logging
import datetime
from run_wf import run_workflow
from run_wf import retrieve_and_rename
from run_wf import watch_workflow_invocation
from justbackoff import Backoff
from bioblend import galaxy
from xunit_wrapper import xunit, xunit_suite, xunit_dump


logging.basicConfig(format='[%(asctime)s][%(lineno)d][%(module)s] %(message)s', level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("bioblend").setLevel(logging.WARNING)
NOW = datetime.datetime.now()
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
backoff = Backoff(min_ms=100, max_ms=1000 * 60 * 5, factor=2, jitter=False)
BUILD_ID = os.environ.get('BUILD_NUMBER', 'Manual')

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
    parser.add_argument('-x', '--xunit-output', dest="xunit_output", type=argparse.FileType('w'), default='report.xml',
                        help="""Location to store xunit report in""")
    args = parser.parse_args()

    WORKFLOW_ID = 'ad86857bfadfed8c'
    gi = galaxy.GalaxyInstance(args.url, args.key)
    wf = gi.workflows.get_workflows(workflow_id=WORKFLOW_ID)[0]
    # inputMap = gi.workflows.show_workflow(WORKFLOW_ID)['inputs']
    # import json
    # print(json.dumps(inputMap, indent=2))
    # import sys; sys.exit()

    org_names = ('CCS',)
    # org_names = ('Soft', '2ww-3119', 'ISA', 'Inf_Still_Creek', 'J76', 'K6',
                 # 'K7', 'K8', 'MIS1-LT2', 'MIS3-3117', 'MP16', 'Pin', 'SCI',
                 # 'SCS', 'SL-Ken', 'ScaAbd', 'ScaApp', 'Sw1_3003', 'Sw2-Ken',
                 # 'UDP', '5ww_LT2')

    test_suites = []
    wf_invocations = []
    for name in org_names:
        hist = gi.histories.create_history(name='BuildID=%s WF=Structural Org=%s Source=Jenkins' % (BUILD_ID, name))
        gi.histories.create_history_tag(hist['id'], 'Automated')
        gi.histories.create_history_tag(hist['id'], 'Annotation')
        gi.histories.create_history_tag(hist['id'], 'BICH464')
        # Load the datasets into history
        datasets, fetch_test_cases = retrieve_and_rename(gi, hist, name)
        ts = xunit_suite('[%s] Fetching Data' % name, fetch_test_cases)
        test_suites.append(ts)

        # TODO: fix mapping to always work.
        # Map our inputs for invocation
        inputs = {
            '0': {
                'id': datasets['fasta']['id'],
                'src': 'hda',
            },
            '1': {
                'id': datasets['json']['id'],
                'src': 'hda',
            }
        }

        # Invoke Workflow
        wf_test_cases, watchable_invocation = run_workflow(gi, wf, inputs, hist)
        # Give galaxy time to process
        time.sleep(10)
        # Invoke Workflow test cases
        ts = xunit_suite('[%s] Invoking workflow' % name, wf_test_cases)
        test_suites.append(ts)
        # Store the invocation info for watching later.
        wf_invocations.append(watchable_invocation)

    invoke_test_cases = []
    for (wf_id, invoke_id) in wf_invocations:
        with xunit('galaxy', 'workflow_watch.%s.%s' % (wf_id, invoke_id)) as tc_watch:
            logging.info("Waiting on wf %s invocation %s", wf_id, invoke_id)
            watch_workflow_invocation(gi, wf_id, invoke_id)
        invoke_test_cases.append(tc_watch)
    ts = xunit_suite('[%s] Workflow Completion' % name, invoke_test_cases)
    args.xunit_output.write(xunit_dump(test_suites))


if __name__ == "__main__":
    __main__()
