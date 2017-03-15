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
    wf = gi.workflows.get_workflows(workflow_id='784d1a320bd8a2b8')[0]

    org_names = ('Soft', '2ww-3119', 'ISA', 'Inf_Still_Creek', 'J76', 'K6',
                 'K7', 'K8', 'MIS1-LT2', 'MIS3-3117', 'MP16', 'Pin', 'SCI',
                 'SCS', 'SL-Ken', 'ScaAbd', 'ScaApp', 'Sw1_3003', 'Sw2-Ken',
                 'UDP')

    test_suites = []
    wf_invocations = []
    for name in org_names:
        hist = gi.histories.create_history(name='WF Auto Functional %s' % name)
        # Load the datasets into history
        datasets, fetch_test_cases = retrieve_and_rename(gi, hist, name)
        ts = xunit_suite('[%s] Fetching Data' % name, fetch_test_cases)
        test_suites.append(ts)

        # Map our inputs for invocation
        inputs = {
            '0': {
                'id': datasets['json']['id'],
                'src': 'hda',
            },
            '1': {
                'id': datasets['gff3']['id'],
                'src': 'hda',
            },
            '2': {
                'id': datasets['fasta']['id'],
                'src': 'hda',
            }
        }

        # Invoke Workflow
        wf_test_cases, watchable_invocation = run_workflow(gi, wf, inputs, hist)
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


def run_workflow(gi, wf, inputs, hist):
    test_cases = []

    with xunit('galaxy', 'workflow_launch') as tc_invoke:
        logging.info("Running wf %s in %s", wf['id'], hist['id'])
        invocation = gi.workflows.invoke_workflow(
            wf['id'],
            inputs=inputs,
            history_id=hist['id'],
        )
    test_cases.append(tc_invoke)
    watchable_invocation = (wf['id'], invocation['id'])

    return test_cases, watchable_invocation

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

    datasets = {}
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
        # Keep a copy by extension
        datasets[dataset['file_ext']] = dataset

    return datasets, [tc3]


def watch_job_invocation(gi, job_id):
    latest_state = None
    while True:
        # Fetch the current state
        latest_state = gi.jobs.get_state(job_id)
        # If it's scheduled, then let's look at steps. Otherwise steps probably don't exist yet.
        logging.debug("Checking job %s state: %s", job_id, latest_state)
        if latest_state == 'error':
            raise Exception(latest_state)
        elif latest_state == 'ok':
            return
        else:
            time.sleep(5)
    raise Exception(latest_state)


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
                raise Exception(latest_state)

            # If all OK
            if all([state is None or state == 'ok'
                    for state in states]):
                return
                # We can finish
        time.sleep(5)
    raise Exception(latest_state)


if __name__ == "__main__":
    __main__()
