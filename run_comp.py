#!/usr/bin/env python
import argparse
import os
import glob
import time
import logging
import datetime
from justbackoff import Backoff
from bioblend import galaxy
from xunit_wrapper import xunit, xunit_suite, xunit_dump


logging.basicConfig(format='[%(asctime)s][%(lineno)d][%(module)s] %(message)s', level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("bioblend").setLevel(logging.WARNING)
NOW = datetime.datetime.now()
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
backoff = Backoff(min_ms=100, max_ms=1000 * 60 * 5, factor=2, jitter=False)
BUILD_ID = os.environ.get('BUILD_NUMBER', 'Manual-%s' % NOW.strftime('%H:%M'))

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
    wf = gi.workflows.get_workflows(workflow_id='95c345e5129ac7f2')[0]

    org_names = ('Soft', '2ww-3119', 'ISA', 'Inf_Still_Creek', 'J76', 'K6',
                 'K7', 'K8', 'MIS1-LT2', 'MIS3-3117', 'MP16', 'Pin', 'SCI',
                 'SCS', 'SL-Ken', 'ScaAbd', 'ScaApp', 'Sw1_3003', 'Sw2-Ken',
                 'UDP', '5ww_LT2', 'Sw2-Np2', 'CCS')

    org_names = ('SCI',)

    wf_inputs = gi.workflows.show_workflow(wf['id'])['inputs']
    # import pprint; pprint.pprint(wf_inputs)
    # import sys; sys.exit()
    test_suites = []
    wf_invocations = []
    for name in org_names:
        try:
            hist = gi.histories.create_history(name='BuildID=%s WF=Comparative Org=%s Source=Jenkins' % (BUILD_ID, name))
            gi.histories.create_history_tag(hist['id'], 'Automated')
            gi.histories.create_history_tag(hist['id'], 'Annotation')
            gi.histories.create_history_tag(hist['id'], 'BICH464')
            # Load the datasets into history
            files = glob.glob('tmp/%s*' % name)
            for f in sorted(files):
                # Skip blastxml
                if '.NR.blastxml' in f: continue
                gi.tools.upload_file(f, hist['id'])

            datasets = gi.histories.show_history(hist['id'], contents=True)
            datasetMap = {
                dataset['name'].replace(name + '.', ''): dataset['id']
                for dataset in datasets
            }

            import pprint; pprint.pprint(datasetMap)

            # TODO: fix mapping to always work.
            # Map our inputs for invocation
            inputs = {
                '0': {
                    'id': datasetMap['fa'],
                    'src': 'hda',
                },
                '1': {
                    'id': datasetMap['gff3'],
                    'src': 'hda',
                },
                '2': {
                    'id': datasetMap['NT.blastxml'],
                    'src': 'hda',
                },
                '3': {
                    'id': datasetMap['NR.tsv'],
                    'src': 'hda',
                },
                '4': {
                    'id': datasetMap['PG.tsv'],
                    'src': 'hda',
                },
            }

            # Invoke Workflow
            wf_test_cases, watchable_invocation = run_workflow(gi, wf, inputs, hist)
            # Invoke Workflow test cases
            ts = xunit_suite('[%s] Invoking workflow' % name, wf_test_cases)
            test_suites.append(ts)

            # Store the invocation info for watching later.
            wf_invocations.append(watchable_invocation)
        except:
            pass

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


def watch_job_invocation(gi, job_id):
    latest_state = None
    prev_state = None

    while True:
        # Fetch the current state
        latest_state = gi.jobs.get_state(job_id)
        # If the state changes
        if latest_state != prev_state:
            # Reset the backoff
            backoff.reset()
        prev_state = latest_state

        # If it's scheduled, then let's look at steps. Otherwise steps probably don't exist yet.
        logging.debug("Checking job %s state: %s", job_id, latest_state)
        if latest_state == 'error':
            raise Exception(latest_state)
        elif latest_state == 'ok':
            return
        else:
            time.sleep(backoff.duration())
    raise Exception(latest_state)


def watch_workflow_invocation(gi, wf_id, invoke_id):
    latest_state = None
    prev_state = None
    while True:
        # Fetch the current state
        latest_state = gi.workflows.show_invocation(wf_id, invoke_id)
        # Get step states
        states = [step['state'] for step in latest_state['steps']]
        # Get a str based state representation
        state_rep = '|'.join(map(str, states))
        if state_rep != prev_state:
            backoff.reset()
        prev_state = state_rep

        # If it's scheduled, then let's look at steps. Otherwise steps probably don't exist yet.
        if latest_state['state'] == 'scheduled':
            # If any state is in error,
            logging.info("Checking workflow %s states: %s", wf_id, state_rep)
            if any([state == 'error' for state in states]):
                # We bail
                raise Exception(latest_state)

            # If all OK
            if all([state is None or state == 'ok'
                    for state in states]):
                return
                # We can finish
        time.sleep(backoff.duration())
    raise Exception(latest_state)


if __name__ == "__main__":
    __main__()
