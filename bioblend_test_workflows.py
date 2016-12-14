#!/usr/bin/env python

import os, sys, time
import argparse

sys.path.insert(0, os.path.dirname(__file__))
import logging
import json, re
from six import iteritems, string_types
from bioblend.galaxy.objects import *
from bioblend.galaxy.client import ConnectionError

logging.basicConfig(format='[%(asctime)s][%(lineno)d]-[%(module)s] %(message)s', level=logging.DEBUG)

logging.getLogger("requests").setLevel(logging.WARNING)


# logging.getLogger("bioblend").setLevel(logging.WARNING)


def __main__():
    parser = argparse.ArgumentParser(description="""Script to run all workflows mentioned in workflows_to_test.
    It will import the shared workflows are create histories for each workflow run, prefixed with ``TEST_RUN_<date>:``
    Make sure the json has file names identical to those in the data library.""")

    parser.add_argument('-k', '--api-key', '--key', dest='key', metavar='your_api_key',
                        help='The account linked to this key needs to have admin right to upload by server path',
                        required=True)
    parser.add_argument('-u', '--url', dest='url', metavar="http://galaxy_url:port",
                        help="Be sure to specify the port on which galaxy is running",
                        default="http://usegalaxy.org")
    parser.add_argument("-d", "--data_library_name", dest='data_library_name', metavar='Data library name',
                        help="Specify the name of the data library in which the test dataset can be found",
                        default='TestingData')
    parser.add_argument('-w', "--json", "--workflow-inputs", dest="json", metavar="Workflow input json file",
                        help="Specify a json file describing the worklfow to test and their inputs - see default",
                        default="testdata/workflow_example_parameters.json")
    parser.add_argument('-s', '--dry-run', dest="dry_run",
                        help="""Do not execute workflow, just show the call it would have made, helpful for identifying
                        the right parameters""", action="store_true", default=False)
    args = parser.parse_args()

    with open(args.json, 'rb') as f:
        workflows_to_test = json.load(f)

    gio = GalaxyInstance(args.url, args.key)
    data_library = get_library(gio, args.data_library_name)
    test_workflows(gio, data_library, workflows_to_test, dry_run=args.dry_run)


def get_library(gio, data_library_name):
    data_library = None
    for lib in gio.libraries.list():
        if lib.name == data_library_name:
            return lib
    if data_library == None:
        logging.error("Could not find data library: %s" % data_library_name)
        sys.exit()


def test_workflows(gio, data_library, workflows_to_test, dry_run=False):
    for wft in workflows_to_test:
        if 'run' in wft and wft['run'] in [False, "False"]:
            logging.info("Will NOT run %s" % wft['name'])
            continue
        logging.info("Will RUN %s" % wft['name'])
        logging.debug(wft)
        wf_dataset_map = []
        wf = get_workflow(gio, wft)
        if wf == None:
            continue
        if not wf.is_runnable:
            logging.error("Broken workflow: %s" % wf.name)
            logging.error("Missing tool ids: %s" % ",".join(wf.missing_ids))
            continue
        try:
            wf_dataset_map = match_inputs(wf, wft['inputs'], data_library)
        except Exception as e:
            logging.error(e)
            continue
        logging.debug(wf_dataset_map)

        params = fix_workflow_parameters(wf, wft)

        history_name = "TEST_RUN_%s: %s" % (time.strftime("%Y-%m-%d"), wf.name)
        logging.info("Running workflow: %s with results to: %s" % (wf.name, history_name))

        wf_run = None
        if dry_run:
            logging.info("DRY RUN, would have executed workflow '%s' with these steps:\n%s" % (wf.name, wf))
            logging.info(" with following parameters:\n%s" % params)
            if 'replacement_params' in wft:
                logging.info("and replacement params:\n%s" % (wft['replacement_params']))
            continue
        try:
            wf_run = wf.run(
                input_map=wf_dataset_map,
                history=history_name,
                params=params,
                import_inputs=True,
                # replacement_params=wft['replacement_params']
            )
        except ConnectionError:
            logging.exception("Connection Error")
        except Exception:
            logging.exception("error")
        else:
            logging.debug(wf_run)
            logging.info("Succesfully started workflow: %s" % wf.name)


def match_inputs(wf, wft_inputs, data_library):
    wf_dataset_map = {}
    for wft_input in wft_inputs:
        input_id = None
        if 'type' in wft_input and wft_input['type'] == 'ldda':
            datasets = data_library.get_datasets(name=wft_input['value'])

            if len(datasets) > 1:
                raise Exception("Dataset name not unique: %s " % wft_input['value'])
            elif len(datasets) == 0:
                raise Exception("Input dataset not found: %s, dataset name: %s" % (wft_input['name'],
                                                                                   wft_input['value']))
            wf_dataset_map[wft_input['name']] = datasets[0]
    return wf_dataset_map


def get_workflow(gio, wft):
    valid_workflows = []
    # Have to be careful here. The oo function:
    # gio.workflows.list() would return an error on workflows with an unconnected input dataset,
    # this might indicate a broken workflow.., but one might have multiple workflows with the same name
    # of which one is broken
    workflows = []
    try:
        if 'published' in wft and wft['published']:
            workflows = gio.workflows.list(name=wft['name'], published=True)
        else:
            workflows = gio.workflows.list(name=wft['name'])
    except Exception:
        raise

    for wf in workflows:
        # Although deleted workflows should not be here, it doesn't hurt to recheck..
        if not wf.deleted and wf.published:
            do_import = True
            # check if latest id are same
            for own_wf in gio.workflows.list(name='imported: %s' % wf.name):
                if own_wf.wrapped['latest_workflow_uuid'] == wf.wrapped['latest_workflow_uuid']:
                    do_import = False
                    valid_workflows.append(own_wf)
            if do_import:
                gio.gi.workflows.import_shared_workflow(wf.id)
                for imp_wf in gio.workflows.list(name='imported: %s' % wf.name):
                    if imp_wf.wrapped['latest_workflow_uuid'] == wf.wrapped['latest_workflow_uuid']:
                        valid_workflows.append(imp_wf)
        elif not wf.deleted and not wf.published:
            valid_workflows.append(wf)
    logging.debug(valid_workflows)
    if len(valid_workflows) == 0:
        logging.error("Could not find workflow: %s" % wft['name'])
        return None
    elif len(valid_workflows) > 1:
        logging.error("Got multiple workflows, please try to be more specific in the filter or naming.\
        You might need to rename your workflows!")
        return None
    else:
        logging.info("Found workflow: %s" % wft['name'])
        return valid_workflows[0]


def fix_workflow_parameters(wf, wft):
    """
    PARAMS = {STEP_ID: PARAM_DICT, ...}
    PARAM_DICT = {NAME: VALUE, ...}
    """
    replace_params = {}
    if 'replacement_params' in wft:
        replace_params = simple_fixup(wf, wft)
    else:
        wft['replacement_params'] = {}
    params = replace_params
    if 'params' in wft:
        logging.debug("In function fix_workflow_parameters, starting tool_params_to_step")
        params = tool_params_to_step(wf, wft)

    for step_id in replace_params.keys():
        if step_id not in params:
            params[step_id] = replace_params[step_id]
        else:
            params[step_id].update(replace_params[step_id])
    return params


def tool_params_to_step(wf, wft):
    params = {}
    logging.debug("in function tool_params_to_step")
    for tool_key, user_param_dict in iteritems(wft["params"]):
        for id, step in iteritems(wf.steps):
            if step.tool_id == tool_key:
                params[id] = user_param_dict
    return params


def simple_fixup(wf, wft={'replacement_params': {'transferdirectory': 'CHEERS', 'expName': 'x'}}):
    logging.info(wft['replacement_params'])
    params = {}
    for id, step in iteritems(wf.steps):
        # json_step_tool_inputs = json.dumps(step.tool_inputs)
        params[id] = {}
        for expression, replacement in iteritems(wft['replacement_params']):
            rex = re.compile("\$\{%s\}" % expression)
            params[id] = fixup(step.tool_inputs, rex, replacement, param_dict=params[id])
    return params


def fixup(tool_input, rex, replacement, traceback=[], param_dict={}):
    for k, v in iteritems(tool_input):
        traceback.append(k)
        logging.debug(k)
        logging.debug(v)
        if type(tool_input[k]) is dict:
            fixup(tool_input[k], rex, replacement, traceback, param_dict)
        elif tool_input[k] != None and isinstance(tool_input[k], string_types):
            logging.debug(tool_input[k])
            if rex.search(tool_input[k]):
                param_key = "|".join(traceback)
                logging.debug(param_key)
                if param_key in param_dict:
                    param_dict[param_key] = rex.sub(replacement, param_dict[param_key])
                else:
                    param_dict[param_key] = rex.sub(replacement, tool_input[k])
                traceback = []
        else:
            traceback = []
    return param_dict


if __name__ == "__main__":
    __main__()
