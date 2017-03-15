#!/usr/bin/env python
import argparse
import os
import time
import logging
import datetime
from bioblend import galaxy
from xunit_wrapper import xunit, xunit_suite, xunit_dump

logging.basicConfig(format='[%(asctime)s][%(lineno)d][%(module)s] %(message)s', level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("bioblend").setLevel(logging.WARNING)
NOW = datetime.datetime.now()
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

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

    gi = galaxy.GalaxyInstance(args.url, args.key)
    hist = gi.histories.create_history('Load All Student Genomes')


    org_names = ('Soft', '2ww-3119', 'ISA', 'Inf_Still_Creek', 'J76', 'K6',
                 'K7', 'K8', 'MIS1-LT2', 'MIS3-3117', 'MP16', 'Pin', 'SCI',
                 'SCS', 'SL-Ken', 'ScaAbd', 'ScaApp', 'Sw1_3003', 'Sw2-Ken',
                 'UDP')

    test_suites = []
    for name in org_names:
        ts = retrieve_and_rename(gi, hist, name)
        test_suites.append(ts)
    args.xunit_output.write(xunit_dump(test_suites))


def retrieve_and_rename(gi, hist, ORG_NAME):
    logging.info("Retrieving and Renaming %s", ORG_NAME)
    # Now we'll run this tool
    with xunit('galaxy', 'launch_tool') as tc3:
        logging.info("Running tool")
        inputs = {
            'org_source|source_select': 'direct',
            'org_source|org_raw': ORG_NAME,
        }
        tool_run = gi.tools.run_tool(hist['id'], 'edu.tamu.cpt2.webapollo.export', inputs)
    # Now to correct the names

    with xunit('galaxy', 'watch_run') as tc4:
        (successful, msg) = watch_job_invocation(gi, tool_run['jobs'][0]['id'])

    rename_tcs = []
    logging.info("Run complete, renaming outputs")
    for dataset in tool_run['outputs']:
        if dataset['data_type'] == 'galaxy.datatypes.text.Json':
            name = '%s.json' % ORG_NAME
        elif dataset['data_type'] == 'galaxy.datatypes.sequence.Fasta':
            name = '%s.fasta' % ORG_NAME
        elif dataset['data_type'] == 'galaxy.datatypes.interval.Gff3':
            name = '%s.gff3' % ORG_NAME
        else:
            name = 'Unknown'
        logging.debug("Renaming %s (%s, %s) to %s", dataset['id'], dataset['data_type'], dataset['file_ext'], name)

        with xunit('galaxy', 'rename.%s' % dataset['file_ext']) as tmp_tc:
            (successful, msg) = watch_job_invocation(gi, tool_run['jobs'][0]['id'])
            gi.histories.update_dataset(hist['id'], dataset['id'], name=name)

        rename_tcs.append(tmp_tc)

    ts = xunit_suite('Fetching ' + ORG_NAME, [tc3, tc4] + rename_tcs)
    return ts

def watch_job_invocation(gi, job_id):
    latest_state = None
    while True:
        # Fetch the current state
        latest_state = gi.jobs.get_state(job_id)
        # If it's scheduled, then let's look at steps. Otherwise steps probably don't exist yet.
        logging.debug("Checking job %s state: %s", job_id, latest_state)
        if latest_state == 'error':
            return False, latest_state
        elif latest_state == 'ok':
            return True, None
        else:
            time.sleep(5)
    return False, latest_state


def watch_workflow_invocation(gi, wf_id, invoke_id):
    latest_state = None
    while True:
        # Fetch the current state
        latest_state = gi.workflows.show_invocation(wf_id, invoke_id)
        # If it's scheduled, then let's look at steps. Otherwise steps probably don't exist yet.
        if latest_state['state'] == 'scheduled':
            steps = latest_state['steps']
            # Get step states
            states = [step['state'] for step in steps]
            # If any state is in error,
            logging.info("Checking workflow %s states: %s", wf_id, '|'.join(map(str, states)))
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
