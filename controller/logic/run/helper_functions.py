from django.conf import settings
from django.urls import reverse
from django.utils import timezone

import controller.logic.workflow.data_access_operations as workflow_dao
import controller.logic.simulated_run.data_access_operations as simulated_run_dao
import controller.logic.run.components as run_components
import controller.logic.run.data_access_operations as run_dao
import controller.logic.run.helper_functions as run_helper_functions
import controller.logic.job.components as job_components
import controller.logic.job.data_access_operations as job_dao
import controller.logic.job.helper_functions as job_helper_functions
from controller.logic.common_logic_operations import multiple_replace
from controller.logic.common_logic_operations import cantor_pairing, get_run_dir_path, get_run_prefix_table_name

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from collections import OrderedDict
from pathlib import Path
import requests
from requests import Session
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import copy, threading, time, random, base64, boto3
import json


def get_execution_order(copy_dag: run_components.DiGraph):
    """Implementation of Kahn's algorithm - will modify dag in-place"""

    linearized_dag_nodes = []  # list that will contain the sorted elements
    root_nodes = set()  # nodes with no incoming edge
    for node in copy_dag.nodes:
        flag_root = True
        # does this node contain atleast one incoming edge?
        if copy_dag.get_incoming_nodes(node):
            flag_root = False
        # no incoming edge to this node
        if flag_root:
            root_nodes.add(node)

    while len(root_nodes):  # set root_nodes is not empty
        # remove a node n from root_nodes
        n = root_nodes.pop()
        # add n to linearized_dag_nodes
        linearized_dag_nodes.append(n)
        # for each node m with an edge e from n to m do
        outgoing_nodes = copy_dag.get_outgoing_nodes(n)
        for m in outgoing_nodes:
            e = copy_dag.get_edge(n, m)
            # remove edge e from the graph
            copy_dag.remove_edge(e)
            # if m has no other incoming edges then
            incoming_nodes = copy_dag.get_incoming_nodes(m)
            if not incoming_nodes:
                root_nodes.add(m)

    if copy_dag.edges:  # graph has cycles
        raise ValueError("Error while traversing DAG: DAG has edges")
    else:
        return linearized_dag_nodes


def get_copy(obj_run_dag: run_components.DiGraph):
    """Get copy of dag"""
    new_dag: run_components.DiGraph = run_components.DiGraph()
    for node in obj_run_dag.nodes:
        new_node = run_components.Node(
            int(node.id),
            str(node.name),
            str(node.type)
        )
        new_dag.add_node(new_node)
    for edge in obj_run_dag.edges:
        origin_node = edge.origin
        destination_node = edge.destination
        new_origin_node = run_components.Node(
            int(origin_node.id),
            str(origin_node.name),
            str(origin_node.type)
        )
        new_destination_node = run_components.Node(
            int(destination_node.id),
            str(destination_node.name),
            str(destination_node.type)
        )
        new_edge = run_components.Edge(new_origin_node, new_destination_node)
        new_dag.add_edge(new_edge)
    return new_dag


def extract_dag_data_for_processing_3a_kn_job(
        incoming_nodes: set,
        outgoing_nodes: set,
        run_prefix_table_name: str,
        run_dir_path: Path,
        obj_run: run_components.Run
):
    """Helper function for processing the 3a_kn job"""

    relevant_data = dict()  # this variable will be returned

    parameters = dict()
    instructions_file_path = None
    layout_file_path = None
    data_table_name = None
    annotations_per_tuple_per_worker_table_name = None
    aggregated_annotations_table_name = None

    # get the constituent files uploaded under this workflow
    list_files = workflow_dao.find_all_files(
        user_id=obj_run.user_id,
        project_id=obj_run.project_id,
        workflow_id=obj_run.workflow_id
    )
    map_name_vs_type: dict = {}
    for f in list_files:
        f_path: Path = Path(f.file_path_str)
        map_name_vs_type[f_path.name] = f.type

    for input_node in incoming_nodes:
        if '=' in input_node.name:
            # key-value pair
            key_value = input_node.name.split("=", 1)
            key_value = [x.strip() for x in key_value]
            parameters[key_value[0]] = key_value[1]
        elif input_node.name.startswith('"') and input_node.name.endswith('"'):
            file_name = input_node.name[1:-1]
            file_type = map_name_vs_type.get(file_name)
            # get the file path from the run directory
            for file_path in run_dir_path.iterdir():    # scan run dir
                if file_path.is_file(): # scan for files
                    if file_path.name == file_name:  # file search match
                        if file_type == settings.UPLOADED_FILE_TYPES[2]:    # inst
                            instructions_file_path = file_path
                            # print(instructions_file_path)
                        elif file_type == settings.UPLOADED_FILE_TYPES[3]:  # layout
                            layout_file_path = file_path
                            # print(layout_file_path)
        else:
            # input data table name
            data_table_name = run_prefix_table_name + input_node.name

    for output_node in outgoing_nodes:
        if output_node.type == 'data:0':
            # annotation per tuple per worker (B type) table
            annotations_per_tuple_per_worker_table_name = run_prefix_table_name + output_node.name
        elif output_node.type == 'data:1':
            # aggregated annotations per tuple (C type) table
            aggregated_annotations_table_name = run_prefix_table_name + output_node.name

    relevant_data['data_table_name'] = data_table_name
    relevant_data['instructions_file_path'] = instructions_file_path
    relevant_data['layout_file_path'] = layout_file_path
    relevant_data['parameters'] = parameters
    relevant_data['annotations_per_tuple_per_worker_table_name'] = annotations_per_tuple_per_worker_table_name
    relevant_data['aggregated_annotations_table_name'] = aggregated_annotations_table_name
    return relevant_data


def extract_dag_data_for_processing_read_table_job(
        incoming_nodes: set,
        outgoing_nodes: set,
        run_prefix_table_name: str
):
    """Helper function for processing read_table job"""
    relevant_data = dict()  # answer variable

    input_file_node: run_components.Node = next(iter(incoming_nodes))
    input_file_name: str = input_file_node.name
    input_file_name = input_file_name[1:-1]  # input file name to read_table was a string literal by allowed semantics
    output_table_node: run_components.Node = next(iter(outgoing_nodes))
    output_table_name: str = run_prefix_table_name + output_table_node.name
    relevant_data['input_file_name'] = input_file_name
    relevant_data['output_table_name'] = output_table_name
    return relevant_data


def extract_dag_data_for_processing_sample_random_job(
        incoming_nodes: set,
        outgoing_nodes: set,
        run_prefix_table_name: str
):
    """Helper function for processing sample_random job"""
    relevant_data = dict()  # answer variable

    input_table_name = None
    sample_size = 0
    for input_node in incoming_nodes:
        if '=' in input_node.name:  # this node represents sample size
            # key-value pair
            key_value = input_node.name.split("=")
            key_value = [x.strip() for x in key_value]
            if key_value[0] == 'n':
                sample_size = int(key_value[1])
        else:
            # input data table name
            input_table_name = run_prefix_table_name + input_node.name
    output_table_node: run_components.Node = next(iter(outgoing_nodes))
    output_table_name: str = run_prefix_table_name + output_table_node.name
    relevant_data['input_table_name'] = input_table_name
    relevant_data['sample_size'] = sample_size
    relevant_data['output_table_name'] = output_table_name
    return relevant_data


def extract_dag_data_for_processing_write_table_job(
        incoming_nodes: set,
        outgoing_nodes: set,
        run_prefix_table_name: str,
        run_dir_path: Path
):
    """Helper function for processing write_table job"""
    relevant_data = dict()  # answer variable

    input_table_name = None
    output_file_path = None
    input_table_node: run_components.Node = next(iter(incoming_nodes))
    input_table_name: str = run_prefix_table_name + input_table_node.name
    output_file_node: run_components.Node = next(iter(outgoing_nodes))
    file_key_value = output_file_node.name.strip().split('=')
    output_file_name = ''
    if file_key_value[0].strip() == 'file':
        output_file_name = file_key_value[1].strip()
    output_file_name = output_file_name[1:-1]  # a string literal so remove surrounding quotes
    # get the output file path from the run directory
    output_file_path = run_dir_path.joinpath(output_file_name)
    relevant_data['input_table_name'] = input_table_name
    relevant_data['output_file_path'] = output_file_path
    return relevant_data


def extract_dag_data_for_processing_exec_sql_job(
        incoming_nodes: set,
        outgoing_nodes: set,
        run_prefix_table_name: str
):
    """Helper function for processing exec_sql job"""
    relevant_data = dict()  # answer variable

    input_variables = []
    query = ''
    output_variables = []
    for input_node in incoming_nodes:
        if '=' in input_node.name:  # this node represents either queries or mapping to output variables
            key_value = input_node.name.split("=", 1)  # key-value pair split based on first occurence of =
            key_value = [x.strip() for x in key_value]
            if key_value[0] == 'query':
                query = str(key_value[1]).strip()
                query = query[1:-1]  # removing starting and trailing " to remove nested strings
        else:
            # input variable name
            input_variables.append(input_node.name.strip())

    # get output variable names
    for output_table_node in outgoing_nodes:
        output_variables.append(output_table_node.name.strip())

    if len(output_variables) > 1:
        raise ValueError("Error: more than one output variable for exec_sql operator")

    # go through input variables, find them in query and replace occurrences with run prefixed table names
    input_replaced_query = ''
    input_tables = []
    output_table = None
    modified_query = copy.deepcopy(query)
    replacements = dict()
    for variable in input_variables:
        input_table = run_prefix_table_name + variable
        input_tables.append(input_table)
        replacements[variable] = input_table
    modified_query = multiple_replace(modified_query, replacements)
    input_replaced_query = modified_query
    for variable in output_variables:
        output_table = run_prefix_table_name + variable

    relevant_data['input_replaced_query'] = input_replaced_query
    relevant_data['input_tables'] = input_tables
    relevant_data['output_table'] = output_table
    return relevant_data


def progress_dag(
        obj_run: run_components.Run,
        original_dag: run_components.DiGraph,
        operator_nodes_in_execution_order: list,
        mapping_node_id_vs_job_id: dict,
        id_field_name: str=None
):
    """Execute dag based on the execution order"""
    run_prefix_table_name = get_run_prefix_table_name(obj_run=obj_run)
    run_dir_path = get_run_dir_path(obj_run=obj_run)
    explore_dag = True
    for node in operator_nodes_in_execution_order:
        # get input and output nodes
        incoming_nodes = original_dag.get_incoming_nodes(node)
        outgoing_nodes = original_dag.get_outgoing_nodes(node)
        job_id = mapping_node_id_vs_job_id[node.id]  # get the corresponding o-o mapped job id
        obj_job: job_components.Job = job_dao.find_job(
            job_id=job_id,
            run_id=obj_run.id,
            workflow_id=obj_run.workflow_id,
            project_id=obj_run.project_id,
            user_id=obj_run.user_id
        )
        if node.name == settings.HUMAN_OPERATORS[0]:    # "3a_kn"
            # collect info for processing this operator
            information: dict = run_helper_functions.extract_dag_data_for_processing_3a_kn_job(
                incoming_nodes=incoming_nodes, outgoing_nodes=outgoing_nodes,
                run_prefix_table_name=run_prefix_table_name, run_dir_path=run_dir_path, obj_run=obj_run
            )
            # pass this info to an internal procedure
            submitted: bool = job_helper_functions.process_3a_kn(
                data_table_name=information.get('data_table_name'),
                instructions_file_path=information.get('instructions_file_path'),
                layout_file_path=information.get('layout_file_path'),
                configuration=information.get('parameters'),
                obj_job=obj_job,
                annotations_per_tuple_per_worker_table_name=information.get(
                    'annotations_per_tuple_per_worker_table_name'),
                aggregated_annotations_table_name=information.get('aggregated_annotations_table_name'),
                id_field_name=id_field_name
            )
            if submitted:   # job submitted to cymphony dashboard
                # if run is a simulation run, simulate workers on this 3a_kn job
                # these workers will take the job to completion which will then progress dag forward
                if obj_run.type == settings.RUN_TYPES[0]:  # run is of type simulation
                    # 1. load job specific simulation params into job_simulation_params
                    # overall parameters for simulating job, specified by the requester
                    job_simulation_params: dict = simulated_run_dao.load_job_simulation_parameters(obj_job=obj_job)
                    # 2. prepare empty tables for information of simulation of workers at the overall & individual level
                    # parameters corresponding to overall simulation of workers
                    simulated_run_dao.create_table_parameters_simulation_workers_job(obj_job=obj_job)
                    # statistics corresponding to overall simulation of workers
                    simulated_run_dao.create_table_statistics_simulation_workers_job(obj_job=obj_job)
                    # parameters corresponding to each individual worker hitting the job
                    simulated_run_dao.create_table_parameters_workers_job(obj_job=obj_job)
                    # statistics corresponding to each simulated worker hitting the job
                    simulated_run_dao.create_table_statistics_workers_job(obj_job=obj_job)
                    # 3. the size (rows) of data input to this job, required in the offloaded worker procedure
                    # will be used for simulating accuracy
                    size_data_job = simulated_run_dao.get_size_data(data_table_name=information.get('data_table_name'))
                    # 4. offload to procedure (it will generate workers and hit cymphony)
                    x = threading.Thread(target=simulate_workers_on_job, args=(job_simulation_params, obj_job, size_data_job))
                    x.start()
                # do not progress dag yourself, workers (human or synthetic) will make that happen
                explore_dag = False
            else:
                # input data was empty, so job wasn't submitted to dashboard, and 3a_kn has been processed to completion
                # in short, 3a_kn was processed like it was an automatic operator
                # progress dag forward
                explore_dag = True
        elif node.name == settings.HUMAN_OPERATORS[2]:    # "3a_knlm"
            # collect info for processing this operator
            information: dict = run_helper_functions.extract_dag_data_for_processing_3a_kn_job(
                incoming_nodes=incoming_nodes, outgoing_nodes=outgoing_nodes,
                run_prefix_table_name=run_prefix_table_name, run_dir_path=run_dir_path, obj_run=obj_run
            )
            # pass this info to an internal procedure
            submitted: bool = job_helper_functions.process_3a_kn(
                data_table_name=information.get('data_table_name'),
                instructions_file_path=information.get('instructions_file_path'),
                layout_file_path=information.get('layout_file_path'),
                configuration=information.get('parameters'),
                obj_job=obj_job,
                annotations_per_tuple_per_worker_table_name=information.get(
                    'annotations_per_tuple_per_worker_table_name'),
                aggregated_annotations_table_name=information.get('aggregated_annotations_table_name'),
                id_field_name=id_field_name
            )
            if submitted:   # job submitted to cymphony dashboard
                # if run is a simulation run, simulate workers on this 3a_knlm job
                # these workers will take the job to completion which will then progress dag forward
                if obj_run.type == settings.RUN_TYPES[0]:  # run is of type simulation
                    # 1. load job specific simulation params into job_simulation_params
                    # overall parameters for simulating job, specified by the requester
                    job_simulation_params: dict = simulated_run_dao.load_job_simulation_parameters(obj_job=obj_job)
                    # 2. prepare empty tables for information of simulation of workers at the overall & individual level
                    # parameters corresponding to overall simulation of workers
                    simulated_run_dao.create_table_parameters_simulation_workers_job(obj_job=obj_job)
                    # statistics corresponding to overall simulation of workers
                    simulated_run_dao.create_table_statistics_simulation_workers_job(obj_job=obj_job)
                    # parameters corresponding to each individual worker hitting the job
                    simulated_run_dao.create_table_parameters_workers_job(obj_job=obj_job)
                    # statistics corresponding to each simulated worker hitting the job
                    simulated_run_dao.create_table_statistics_workers_job(obj_job=obj_job)
                    # 3. the size (rows) of data input to this job, required in the offloaded worker procedure
                    # will be used for simulating accuracy
                    size_data_job = simulated_run_dao.get_size_data(data_table_name=information.get('data_table_name'))
                    # 4. offload to procedure (it will generate workers and hit cymphony)
                    # TODO: yet to verify for 3a_knlm
                    x = threading.Thread(target=simulate_workers_on_job, args=(job_simulation_params, obj_job, size_data_job))
                    x.start()
                # do not progress dag yourself, workers (human or synthetic) will make that happen
                explore_dag = False
            else:
                # input data was empty, so job wasn't submitted to dashboard, and 3a_knlm has been processed to completion
                # in short, 3a_knlm was processed like it was an automatic operator
                # progress dag forward
                explore_dag = True
        elif node.name == settings.HUMAN_OPERATORS[1]:  # "3a_amt"
            # load amt credentials
            encrypted_run_amt_credentials: dict = run_dao.load_run_amt_credentials(obj_run=obj_run)
            # decrypt the credentials
            f: Fernet = run_helper_functions.create_fernet_for_amt_credentials(obj_run=obj_run)
            amt_credentials: dict = {}
            for key, value in encrypted_run_amt_credentials.items():
                str_token: str = value
                bytes_token: bytes = bytes(str_token, encoding=settings.AMT_CREDENTIALS_ENCODING)
                decrypted_bytes: bytes = f.decrypt(bytes_token)
                amt_credentials[key] = decrypted_bytes.decode(encoding=settings.AMT_CREDENTIALS_ENCODING)
            # collect info for processing this operator
            information: dict = run_helper_functions.extract_dag_data_for_processing_3a_amt_job(
                incoming_nodes=incoming_nodes,
                outgoing_nodes=outgoing_nodes,
                run_prefix_table_name=run_prefix_table_name,
                run_dir_path=run_dir_path,
                obj_run=obj_run
            )
            # pass this info to an internal procedure
            mturk_client: boto3.client = job_helper_functions.process_3a_amt(
                amt_credentials=amt_credentials,
                data_table_name=information.get('data_table_name'),
                instructions_file_path=information.get('instructions_file_path'),
                layout_file_path=information.get('layout_file_path'),
                configuration=information.get('parameters'),
                obj_job=obj_job,
                annotations_per_tuple_per_worker_table_name=information.get(
                    'annotations_per_tuple_per_worker_table_name'),
                aggregated_annotations_table_name=information.get('aggregated_annotations_table_name')
            )
            # if we connected to amt, this means input data was non-empty and we wrapped it in hits and pushed to amt
            if mturk_client:
                # offload to procedure
                # (it will ping amt for this job, collect amt annotations, aggregate them,
                # do final bookkeeping for this job, and progress dag)
                x = threading.Thread(target=job_helper_functions.coordinate_cymphony_amt, args=(obj_job, mturk_client))
                x.start()
                # do not progress dag
                explore_dag = False
            else:
                # input data was empty, amt wasn't connected with, and 3a_amt has been processed to completion
                # in short, 3a_amt was processed like it was an automatic operator
                # progress dag forward
                explore_dag = True
        elif node.name == settings.AUTOMATIC_OPERATORS[0]:  # "read_table"
            # syntax and semantic verification has already happened before, so need to check if value at index exists
            information: dict = run_helper_functions.extract_dag_data_for_processing_read_table_job(
                incoming_nodes=incoming_nodes, outgoing_nodes=outgoing_nodes,
                run_prefix_table_name=run_prefix_table_name
            )
            # pass this info to an internal procedure
            job_helper_functions.process_read_table(
                input_file_name=information.get('input_file_name'),
                obj_job=obj_job,
                output_data_table_name=information.get('output_table_name')
            )
            explore_dag = True
        elif node.name == settings.AUTOMATIC_OPERATORS[1]:  # "sample_random"
            # semantic checking done before, so if else below should be good i think
            information: dict = run_helper_functions.extract_dag_data_for_processing_sample_random_job(
                incoming_nodes=incoming_nodes, outgoing_nodes=outgoing_nodes,
                run_prefix_table_name=run_prefix_table_name
            )
            # pass this info to an internal procedure
            job_helper_functions.process_sample_random(
                input_table_name=information.get('input_table_name'),
                sample_size=information.get('sample_size'),
                obj_job=obj_job,
                output_table_name=information.get('output_table_name')
            )
            explore_dag = True
        elif node.name == settings.AUTOMATIC_OPERATORS[3]:  # "write_table"
            information: dict = run_helper_functions.extract_dag_data_for_processing_write_table_job(
                incoming_nodes=incoming_nodes, outgoing_nodes=outgoing_nodes,
                run_prefix_table_name=run_prefix_table_name, run_dir_path=run_dir_path
            )
            # pass this info to an internal procedure
            job_helper_functions.process_write_table(
                input_table_name=information.get('input_table_name'),
                obj_job=obj_job,
                output_file_path=information.get('output_file_path')
            )
            explore_dag = True
        elif node.name == settings.AUTOMATIC_OPERATORS[2]:  # "exec_sql"
            # Inputs to exec_sql:
            # first input to exec_sql in the cy program are some input variables:
            # (had been defined earlier in the cy program and supplied as arguments in this exec_sql statement)
            # second input to exec_sql in the cy program is the sql query
            # output variables are the ones that may be used later in the cy program
            information: dict = run_helper_functions.extract_dag_data_for_processing_exec_sql_job(
                incoming_nodes=incoming_nodes,
                outgoing_nodes=outgoing_nodes,
                run_prefix_table_name=run_prefix_table_name
            )
            # pass this info to an internal procedure
            job_helper_functions.process_exec_sql(
                query=information.get('input_replaced_query'),
                input_tables=information.get('input_tables'),
                output_table=information.get('output_table'),
                obj_job=obj_job
            )
            explore_dag = True
        if explore_dag:  # means last operator completed (automatic operator)
            # dump output tables as files
            # go over output nodes (for all except write_table operator), extract their names, add run_prefix to them
            # copy the run prefixed table names to file named output_node.name, inside the run_dir_path path.
            if not (node.name == settings.AUTOMATIC_OPERATORS[3]):  # not "write_table"
                dump_data_nodes(
                    data_nodes=outgoing_nodes,
                    run_prefix_table_name=run_prefix_table_name,
                    run_dir_path=run_dir_path
                )
            continue
        else:
            break
    # progressed dag as far as possible
    # if explore dag still true, implies the exploration contained automatic operators only which are all complete now
    # if explore dag is false, this means the exploration hit a manual operator
    return explore_dag


def complete_processing_job_and_progress_dag(
        this_job: job_components.Job
):
    """
    Performs the following steps
    1. Load dag related data
    2. complete the present 3a_kn job
        (lock job in all_jobs, copy job level tables to run level tables, edit job status in all_jobs)
    3. dump run level tables to files if option enabled in config
    4. progress dag further
    """
    # 1. load dag
    obj_run: run_components.Run = run_dao.find_run(
        run_id=this_job.run_id, workflow_id=this_job.workflow_id, project_id=this_job.project_id, user_id=this_job.user_id
    )
    obj_run_dag: run_components.DiGraph = run_dao.load_dag(obj_run=obj_run)
    # 2. load mapping to get node of this job
    mapping_node_id_vs_job_id = run_dao.load_mapping_node_vs_job(obj_run=obj_run)
    # 3 progress dag from that node onwards,
    # 3.1 while copying files and "completing" this job.
    # get the execution order of nodes in dag (this is called linearlized dag)
    nodes_execution_order: list = []
    mapping_node_id_vs_position: OrderedDict = run_dao.load_execution_order(obj_run=obj_run)
    for node_id, position in mapping_node_id_vs_position.items():
        node_id = int(node_id)
        obj_node = obj_run_dag.search_node_by_id(node_id=node_id)
        nodes_execution_order.append(obj_node)
    # print([str(i) for i in nodes_execution_order])
    # extract operator nodes from linearized dag
    operator_nodes_in_execution_order = []
    for node in nodes_execution_order:
        if node.type == "operator":
            operator_nodes_in_execution_order.append(node)
    # jobs are already initialized and linked to operator nodes
    # pass over operator nodes again in execution order, this time to execute
    # this time we have the node vs job mapping as well
    # prepare nodes in execution order. (only those nodes including this job's or the one's after this one)
    this_job_node_id = None
    for node_id, job_id in mapping_node_id_vs_job_id.items():
        if job_id == this_job.id:
            this_job_node_id = node_id
    this_job_node = None
    next_operator_nodes_in_execution_order = []
    flag_hit_this_job_node = False
    for node in operator_nodes_in_execution_order:
        if flag_hit_this_job_node:
            next_operator_nodes_in_execution_order.append(node)
        if node.id == this_job_node_id:
            flag_hit_this_job_node = True
            this_job_node = node
    # "complete" this job
    run_prefix_table_name = get_run_prefix_table_name(obj_run=obj_run)
    run_dir_path = get_run_dir_path(obj_run=obj_run)
    outgoing_nodes = obj_run_dag.get_outgoing_nodes(this_job_node)
    annotations_per_tuple_per_worker_table_name = None
    aggregated_annotations_table_name = None
    for output_node in outgoing_nodes:
        if output_node.type == 'data:0':
            # annotation per tuple per worker (B type) table
            annotations_per_tuple_per_worker_table_name = run_prefix_table_name + output_node.name
        elif output_node.type == 'data:1':
            # aggregated annotations per tuple (C type) table
            aggregated_annotations_table_name = run_prefix_table_name + output_node.name
    if this_job.name == settings.HUMAN_OPERATORS[0] or this_job.name == settings.HUMAN_OPERATORS[2]:    # 3a_kn or 3a_knlm job
        processed_3a_kn_part_2: bool = job_helper_functions.process_3a_kn_part_2(
            obj_job=this_job,
            annotations_per_tuple_per_worker_table_name=annotations_per_tuple_per_worker_table_name,
            aggregated_annotations_table_name=aggregated_annotations_table_name
        )
        # if could not process second part of 3a_kn in case it was already completed
        if not processed_3a_kn_part_2:
            # do not process further logic and do not progress dag further
            return
    else:  # 3a_amt job
        processed_3a_amt_part_2: bool = job_helper_functions.process_3a_amt_part_2(
            obj_job=this_job,
            annotations_per_tuple_per_worker_table_name=annotations_per_tuple_per_worker_table_name,
            aggregated_annotations_table_name=aggregated_annotations_table_name
        )
        # This piece of code cannot be called twice, since there is only one collector process for amt annotations.
        # no need to check `if not processed_3a_amt_part_2` like in the case of 3a_kn above
    # dump the output nodes of this just completed 3a_kn/3a_knlm/3a_amt operator, to files in the run directory
    dump_data_nodes(
        data_nodes=outgoing_nodes,
        run_prefix_table_name=run_prefix_table_name,
        run_dir_path=run_dir_path
    )
    # 3.2 progress the dag further
    # Three cases to handle,
    # 1. no more operator (apart from the this job's operator that we just processed above)
    # 2. all automatic operators, leading to the completion of run
    # 3. hitting human operator and putting it in running status
    # the goal while handling above 3 is:
    # not returning to requester, but to continue the worker interaction back to job business logic.
    # see the "repeat" snippet in doc for additional details.
    explore_dag: bool = run_helper_functions.progress_dag(
        obj_run=obj_run,
        original_dag=obj_run_dag,
        operator_nodes_in_execution_order=next_operator_nodes_in_execution_order,
        mapping_node_id_vs_job_id=mapping_node_id_vs_job_id
    )
    # all operators exhausted
    if explore_dag:
        # run.status = "completed" in all_runs
        obj_run.status = settings.RUN_STATUS[2]     # "COMPLETED"
        run_dao.edit_run(obj_run=obj_run)
        print('RUN COMPLETED')
        end_ts = time.time()
        print('SIMULATED RUN COMPLETED at: ', end_ts)
        send_completion_notification(obj_run=obj_run)
        return
    # or human operator caused a break in exploration
    else:
        return


def dump_data_nodes(data_nodes, run_prefix_table_name: str, run_dir_path: Path):
    """Dump tabular data to files"""
    flag_dump: bool = settings.DUMP_OPERATOR_OUTPUTS
    if flag_dump:
        for data_node in data_nodes:
            src_table_name = run_prefix_table_name + data_node.name
            # get the dest file path from the run directory
            dest_file_name = data_node.name
            dest_file_path = run_dir_path.joinpath(dest_file_name)
            job_dao.create_file_from_table(source_table_name=src_table_name, destination_file_path=dest_file_path)
    return


def simulate_workers_on_job(simulation_parameters_dict: dict, obj_job: job_components.Job, size_data_job: int):
    """Generates synthetic workers and makes them work on the job based on the specified simulation params"""
    min_loop_times = int(simulation_parameters_dict['min_loop_times'])
    max_loop_times = int(simulation_parameters_dict['max_loop_times'])
    loop_times = int(random.uniform(min_loop_times, max_loop_times))
    min_workers_per_burst = int(simulation_parameters_dict['min_workers_per_burst'])
    max_workers_per_burst = int(simulation_parameters_dict['max_workers_per_burst'])
    min_time_gap_in_loop_points = int(simulation_parameters_dict['min_time_gap_in_loop_points'])
    max_time_gap_in_loop_points = int(simulation_parameters_dict['max_time_gap_in_loop_points'])
    # will store these in the db, just after simulating all workers below
    parameters_simulation_workers: list = []
    statistics_simulation_workers: list = []
    for i in range(0, loop_times):
        start_ts = time.time()
        parameters_info: dict = {}
        statistics_info: dict = {}
        # loop point (burst) identifier
        parameters_info['identifier'] = i
        statistics_info['identifier'] = i
        workers_in_this_burst = int(random.uniform(min_workers_per_burst, max_workers_per_burst))
        parameters_info['number_workers'] = workers_in_this_burst
        j = 0
        while j < workers_in_this_burst:
            y = threading.Thread(
                target=run_worker_pipeline,
                args=(simulation_parameters_dict, obj_job, size_data_job)
            )
            y.start()
            j = j + 1
        # TODO: this should be number of actual spawned (signed up) workers in this loop point
        # hence, it is dependent on all worker pipelines merging back into this code here.
        statistics_info['number_workers'] = workers_in_this_burst
        time_gap = int(random.uniform(min_time_gap_in_loop_points, max_time_gap_in_loop_points))
        parameters_info['time_gap'] = time_gap
        parameters_simulation_workers.append(parameters_info)
        time.sleep(time_gap)
        end_ts = time.time()
        time_elapsed = int(end_ts - start_ts)    # time between this and next loop points (in seconds)
        statistics_info['time_gap'] = time_elapsed
        statistics_simulation_workers.append(statistics_info)
    # store parameters corresponding to simulation of workers
    # these parameters got decided based on the broad ranged job simulation parameters
    simulated_run_dao.store_parameters_simulation_workers_job(
        parameters_simulation_workers=parameters_simulation_workers,
        obj_job=obj_job
    )
    # store statistics corresponding to simulation of workers
    # these statistics were computed based on actual execution
    simulated_run_dao.store_statistics_simulation_workers_job(
        statistics_simulation_workers=statistics_simulation_workers,
        obj_job=obj_job
    )


def run_worker_pipeline(simulation_parameters_dict: dict, obj_job: job_components.Job, size_data_job: int):
    """Simulating the working of one single synthetic worker"""
    job_category = None
    if obj_job.name == settings.HUMAN_OPERATORS[0]:    # 3a_kn job
        job_category = 'job_3a_kn'
    elif obj_job.name == settings.HUMAN_OPERATORS[2]:    # 3a_knlm job
        job_category = 'job_3a_knlm'

    min_worker_annotation_time = int(simulation_parameters_dict['min_worker_annotation_time'])
    max_worker_annotation_time = int(simulation_parameters_dict['max_worker_annotation_time'])
    min_worker_accuracy = float(simulation_parameters_dict['min_worker_accuracy'])
    max_worker_accuracy = float(simulation_parameters_dict['max_worker_accuracy'])
    # worker characteristics
    worker_annotation_time = int(random.uniform(min_worker_annotation_time, max_worker_annotation_time))
    worker_accuracy = random.uniform(min_worker_accuracy, max_worker_accuracy)
    user_number = random.randint(settings.MIN_WORKER_NUMBER_FOR_JOB_SIGNUP, settings.MAX_WORKER_NUMBER_FOR_JOB_SIGNUP)
    # target characteristics
    target_url = settings.TARGET_CYMPHONY_URL
    # initializations
    s = Session()
    submit_retries = Retry(
        total=settings.RETRIES_TOTAL,
        backoff_factor=settings.RETRIES_BACKOFF_FACTOR,
        status_forcelist=settings.RETRIES_STATUS_FORCELIST,
        method_whitelist=settings.RETRIES_METHOD_WHITELIST
    )
    s.mount(target_url, HTTPAdapter(max_retries=submit_retries))
    s.headers = {'User-Agent': settings.USER_AGENT}
    user_name = 'synthetic_worker_' + str(user_number) + 'for_' + 'u_' + str(obj_job.user_id) + 'p_' + str(
        obj_job.project_id) + \
                'w_' + str(obj_job.workflow_id) + 'r_' + str(obj_job.run_id) + 'j_' + str(obj_job.id)
    password = settings.SYNTHETIC_WORKER_PASSWORD

    # 1. signup
    signup_url = target_url + '/one_step_register/'     # This url skips the activation link verification process
    signup_data = {
        'first_name': 'synthetic_worker_first_name_' + str(user_number),
        'last_name': 'synthetic_worker_last_name_' + str(user_number),
        # TODO (bypassed it instead): will have to disable email uniqueness and verification check
        'email': user_name + '@email.com',
        'username': user_name,
        'password1': password,
        'password2': password,
    }
    call_signup_response = call_signup(s, signup_url, signup_data)
    signup_response_url = call_signup_response.url
    signup_page_url = target_url + reverse('account:django_registration_one_step_register')
    if signup_response_url == signup_page_url:
        # sign up did not go through, so the response landed on the sign up page again
        print('Request url: ', signup_url)
        print('Response resulting url: ', signup_response_url)
        print('Signup page url: ', signup_page_url)
        print('Comparison: ', signup_response_url == signup_page_url)
        return  # gracefully exiting thread because of worker user_name conflict or some other signup error

    # 2. login
    # not needed since one_step_register based signup logs the worker in automatically.
    # login_url = target_url + '/login/'
    # login_data = {
    #     'username': user_name,
    #     'password': password,
    # }
    # call_login(s, login_url, login_data)

    # 3. dashboard
    call_dashboard(s, target_url)

    # 4. job index
    job_index_url = target_url + '/controller/?category=' + job_category + '&action=index'
    call_job_index(s, job_index_url)

    # basic computation for determining number of labels to match in simulation
    target_p = worker_accuracy
    total_size_tuples = size_data_job
    target_total_matches = int(target_p * float(total_size_tuples))
    target_total_non_matches = int(total_size_tuples) - target_total_matches
    total_matches_so_far = 0
    total_annotated_so_far = 0
    total_time_elapsed_all_submits = 0
    total_submits_made = 0
    time_elapsed_first_assignment = 0

    while True:
        # 5. call work on job
        start_ts = time.time_ns()
        work_on_job_url = target_url + \
                          '/controller/?category=' + job_category + '&action=work&uid={0}&pid={1}&wid={2}&rid={3}&jid={4}'.format(
                              obj_job.user_id, obj_job.project_id, obj_job.workflow_id, obj_job.run_id, obj_job.id
                          )
        call_work_on_job_response = call_work_on_job(
            s, work_on_job_url
        )
        end_ts = time.time_ns()
        time_elapsed_first_assignment = end_ts - start_ts
        call_work_on_job_json_response = call_work_on_job_response.json()
        work_response_information: dict = {}
        for key, value in call_work_on_job_json_response.items():
            work_response_information[key] = value
        worker_code = work_response_information.get('worker_code')
        if worker_code == settings.WORKER_CODES['QUIT']:
            # store worker parameters that got decided based on simulation parameters
            simulated_run_dao.store_parameters_worker_job(
                worker_username=user_name, worker_reliability=worker_accuracy,
                worker_annotation_time=worker_annotation_time, obj_job=obj_job
            )
            # compute statistics of worker, pertaining to worker's interaction with cymphony
            (precision, recall) = compute_worker_statistics(
                time_elapsed_first_assignment=time_elapsed_first_assignment,
                total_submits_made=total_submits_made,
                total_time_elapsed_all_submits=total_time_elapsed_all_submits,
                target_total_matches=target_total_matches,
                total_matches_so_far=total_matches_so_far,
                total_size_tuples=total_size_tuples,
                total_annotated_so_far=total_annotated_so_far,
                accuracy=worker_accuracy
            )
            # store the above calculated worker statistics
            simulated_run_dao.store_statistics_worker_job(
                worker_username=user_name, worker_precision=precision,
                worker_recall=recall, obj_job=obj_job
            )
            # worker has finished interacting with cymphony
            # print("Thread finishing for worker with username: ", user_name)
            return
        elif worker_code == settings.WORKER_CODES['ANNOTATE']:
            # get gold label and available answer options from the 'work on job' response
            header_value_dict = work_response_information.get('header_value_dict')
            gold_label = header_value_dict.get(settings.GOLD_LABEL_COLUMN_NAME)
            # print('gold_label: ', gold_label)
            task_option_list = work_response_information.get('task_option_list')
            # print('task_option_list: ', task_option_list)
            while True:
                # decide on the label to annotate with
                label_to_annotate_with = None
                # worker takes some time to decided his/her choice
                time.sleep(worker_annotation_time)
                # label_annotate_with is set to gold_label or not based on below computation
                # once hit intended number of matches, always go into the non-matching section of code,
                # once hit the number of non-matches, always go into the matching section.
                # This will ensure the actual worker accuracy to be exactly equal to
                #   intended accuracy in case worker annotates all tuples.
                # If worker does not get to annotate all tuples in data, probability based matching is done,
                #   which is good since it will not allow first all yes, then all no,
                #   but all no got eliminated because worker never got to annotate later tuples.
                # So, probability based matching is the best possible case already in that scenario,
                #   and will ensure worker accuracy to be closest to intended accuracy of worker.
                total_non_matches_so_far = total_annotated_so_far - total_matches_so_far
                if total_non_matches_so_far >= target_total_non_matches:
                    # print("Non matches had exhausted.")
                    # all intended non-matches exhausted, choose to match
                    label_to_annotate_with = gold_label
                    total_matches_so_far = total_matches_so_far + 1
                elif total_matches_so_far >= target_total_matches:
                    # print("Matches had exhausted.")
                    # all intended matches exhausted, choose to not match
                    # any label (out of the available options) but the gold label
                    if task_option_list is None:    # free text answer
                        label_to_annotate_with = gold_label + str(random.randint(1,10))
                    else:   # answer has to be from a set of choices
                        choices: list = task_option_list.copy()
                        choices.remove(gold_label)
                        label_list: list = random.choices(choices, k=1)
                        label_to_annotate_with = label_list[0]
                else:  # edge cases did not get hit, so go with normal case of picking based on probability.
                    # print("Probability based choice selection")
                    match = random.choices([0, 1], weights=(target_total_non_matches, target_total_matches), k=1)
                    if match[0] == 1:
                        label_to_annotate_with = gold_label
                        total_matches_so_far = total_matches_so_far + 1
                    else:
                        # any label (out of the available options) but the gold label
                        if task_option_list is None:  # free text answer
                            label_to_annotate_with = gold_label + str(random.randint(1, 10))
                        else:  # answer has to be from a set of choices
                            choices = task_option_list.copy()
                            choices.remove(gold_label)
                            label_list: list = random.choices(choices, k=1)
                            label_to_annotate_with = label_list[0]
                # print('Total annotated so far: ', total_annotated_so_far)
                total_annotated_so_far = total_annotated_so_far + 1
                # send the label to annotate with as well
                submit_start_ts = time.time_ns()
                submit_annotation_url = target_url + '/controller/?category=' + job_category + '&action=process_annotation'
                call_submit_annotation_response = call_submit_annotation(
                    s,
                    submit_annotation_url,
                    label_to_annotate_with
                )
                submit_end_ts = time.time_ns()
                time_elapsed_this_submit = submit_end_ts - submit_start_ts
                total_time_elapsed_all_submits = total_time_elapsed_all_submits + time_elapsed_this_submit
                total_submits_made = total_submits_made + 1
                # analyze the "submit" response
                call_submit_annotation_json_response = call_submit_annotation_response.json()
                submit_annotation_response_information: dict = {}
                for key, value in call_submit_annotation_json_response.items():
                    submit_annotation_response_information[key] = value
                worker_code = submit_annotation_response_information.get('worker_code')
                if worker_code == settings.WORKER_CODES['QUIT']:
                    # store worker parameters that got decided based on simulation parameters
                    simulated_run_dao.store_parameters_worker_job(
                        worker_username=user_name, worker_reliability=worker_accuracy,
                        worker_annotation_time=worker_annotation_time, obj_job=obj_job
                    )
                    # compute statistics of worker, pertaining to worker's interaction with cymphony
                    (precision, recall) = compute_worker_statistics(
                        time_elapsed_first_assignment=time_elapsed_first_assignment,
                        total_submits_made=total_submits_made,
                        total_time_elapsed_all_submits=total_time_elapsed_all_submits,
                        target_total_matches=target_total_matches,
                        total_matches_so_far=total_matches_so_far,
                        total_size_tuples=total_size_tuples,
                        total_annotated_so_far=total_annotated_so_far,
                        accuracy=worker_accuracy
                    )
                    # store the above calculated worker statistics
                    simulated_run_dao.store_statistics_worker_job(
                        worker_username=user_name, worker_precision=precision,
                        worker_recall=recall, obj_job=obj_job
                    )
                    # worker has finished interacting with cymphony
                    # print("Thread finishing for worker with username: ", user_name)
                    return
                elif worker_code == settings.WORKER_CODES['ANNOTATE']:
                    header_value_dict = submit_annotation_response_information.get('header_value_dict')
                    gold_label = header_value_dict.get(settings.GOLD_LABEL_COLUMN_NAME)
                    # print('gold_label: ', gold_label)
                    task_option_list = submit_annotation_response_information.get('task_option_list')
                    # print('task_option_list: ', task_option_list)
                    continue
                else: # retry annotating after some time, but will have to go through work on job this time
                    break
        else:   # retry working on job after some time
            pass
        delay = random.randint(settings.MIN_WORKER_RETRY_DELAY, settings.MAX_WORKER_RETRY_DELAY)   # eg: 1 minute delay window if (1, 60)
        # print('worker with username: ', user_name, ' will retry after ', delay, ' seconds')
        time.sleep(delay)
        # print('worker with username: ', user_name, ' is retrying now after a delay of ', delay, ' seconds ')
    # once this function ends we either need to pass the session up to the
    # calling function or it will be gone forever
    # Let's let it shut down forever, since every worker should have a different requests session
    return


# 1. signup
def call_signup(s, url, data):
    method = 'POST'
    signup_response = s.post(url, data=data)
    return signup_response


# 2. login
def call_login(s, url, data):
    method = 'POST'
    s.auth = (data['username'], data['password'])
    login_response = s.post(url, data=data)
    return login_response


# 3. dashboard
def call_dashboard(s, url):
    data = None
    dashboard_response = s.get(url, data=data)
    return dashboard_response


# 4. job index
def call_job_index(s, url):
    method = 'GET'
    data = None
    jobs_listing_response = s.get(url, data=data)
    return jobs_listing_response


# 5. work on specific job
def call_work_on_job(s, url):
    method = 'GET'
    data = None
    task_to_be_annotated_response = s.get(url, data=data)
    return task_to_be_annotated_response


# 6. submit annotation
def call_submit_annotation(s, url, label_to_annotate_with):
    method = 'POST'
    data = {
        'choice': label_to_annotate_with
    }
    task_annotated_response = s.post(url, data=data)
    return task_annotated_response


def compute_worker_statistics(
        time_elapsed_first_assignment,
        total_submits_made, total_time_elapsed_all_submits,
        target_total_matches, total_matches_so_far,
        total_size_tuples, total_annotated_so_far,
        accuracy
):
    print('*****First work on job response time (returning an assignment) nanoseconds*****',
          time_elapsed_first_assignment)
    # performance
    if total_submits_made > 0:
        print('Average Submit-Assign cycle time (in ns): ', float(total_time_elapsed_all_submits) / total_submits_made)
    else:
        print('Could not compute average submit-assign cycle time because of 0 submissions (annotations).')
    # exact intention implementation
    # to experimentally verify the edge case matching + probability based matching accuracy
    print('Total intended matches decided from worker accuracy parameter: ', target_total_matches)
    print('Total matches actually made by worker: ', total_matches_so_far)
    # recall
    print('Total annotated by worker: ', total_annotated_so_far)
    print('Total size(tuples) of data belonging to the job: ', total_size_tuples)
    recall: float = -1.0
    if total_size_tuples > 0:
        recall = float(total_annotated_so_far) / total_size_tuples
        print('Fraction of total tuples annotated by this worker: ', recall)
    else:
        recall = -1.0
    # precision
    precision: float = -1.0
    print('Intended accuracy of worker based on supplied parameter: ', accuracy)
    if total_annotated_so_far > 0:
        precision = float(total_matches_so_far) / total_annotated_so_far
        print('Actual accuracy of worker: ', precision)
    else:
        precision = -1.0
        print('Could not compute actual accuracy of worker because of 0 annotations.')
    return precision, recall


def create_fernet_for_amt_credentials(obj_run: run_components.Run):
    """
    Create fernet for amt credentials.
    Fernet will be used to encrypt/decrypt the amt credentials for a run
    Details: https://cryptography.io/en/latest/fernet/
    """

    amt_password = settings.AMT_PASSWORD
    password = bytes(amt_password, encoding=settings.AMT_PASSWORD_ENCODING)
    '''
    decide the salt (should be unique for each run)
    usually, it is chosen as a random number, and stored in db in plaintext
    i will instead derive the salt (so i don't have to store it in db) from the run's identifying ids as below
    i.e. salt should be unique for an ordered combination of user_id, project_id, workflow_id, run_id
    we will use a pairing function called Cantor pairing.
    more details: https://en.wikipedia.org/wiki/Pairing_function and
    https://math.stackexchange.com/questions/23503/create-unique-number-from-2-numbers
    '''
    user_project_paired = cantor_pairing(obj_run.user_id, obj_run.project_id)
    user_project_workflow_paired = cantor_pairing(user_project_paired, obj_run.workflow_id)
    salt = str(cantor_pairing(user_project_workflow_paired, obj_run.id))
    salt = bytes(salt, encoding=settings.AMT_SALT_ENCODING)

    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=settings.KDF_LENGTH,
        salt=salt,
        iterations=settings.KDF_ITERATIONS,
    )
    key = base64.urlsafe_b64encode(kdf.derive(password))
    f = Fernet(key)
    return f


def extract_dag_data_for_processing_3a_amt_job(
        incoming_nodes: set, outgoing_nodes: set,
        run_prefix_table_name: str, run_dir_path: Path, obj_run: run_components.Run
):
    """Helper function for processing 3a_amt job"""
    # although the body of this function is same as the function - extract_dag_data_for_processing_3a_kn_job,
    # I have kept this function separate since there might be a chance that either 3a_kn or 3a_amt signature changes
    # in the future, so to accommodate that, the body of either function will have to be changed.
    relevant_data = dict()  # answer variable

    parameters = dict()
    instructions_file_path = None
    layout_file_path = None
    data_table_name = None
    annotations_per_tuple_per_worker_table_name = None
    aggregated_annotations_table_name = None
    # get the constituent files uploaded under this workflow
    list_files = workflow_dao.find_all_files(
        user_id=obj_run.user_id,
        project_id=obj_run.project_id,
        workflow_id=obj_run.workflow_id
    )
    map_name_vs_type: dict = {}
    for f in list_files:
        f_path: Path = Path(f.file_path_str)
        map_name_vs_type[f_path.name] = f.type
    for input_node in incoming_nodes:
        if '=' in input_node.name:
            # key-value pair
            key_value = input_node.name.split("=", 1)
            key_value = [x.strip() for x in key_value]
            parameters[key_value[0]] = key_value[1]
        elif input_node.name.startswith('"') and input_node.name.endswith('"'):
            file_name = input_node.name[1:-1]
            file_type = map_name_vs_type.get(file_name)
            # get the file path from the run directory
            for file_path in run_dir_path.iterdir():  # scan run dir
                if file_path.is_file():  # scan for files
                    if file_path.name == file_name:  # file search match
                        if file_type == settings.UPLOADED_FILE_TYPES[2]:  # inst
                            instructions_file_path = file_path
                        elif file_type == settings.UPLOADED_FILE_TYPES[3]:  # layout
                            layout_file_path = file_path
        else:
            # input data table name
            data_table_name = run_prefix_table_name + input_node.name
    for output_node in outgoing_nodes:
        if output_node.type == 'data:0':
            # annotation per tuple per worker (B type) table
            annotations_per_tuple_per_worker_table_name = run_prefix_table_name + output_node.name
        elif output_node.type == 'data:1':
            # aggregated annotations per tuple (C type) table
            aggregated_annotations_table_name = run_prefix_table_name + output_node.name
    relevant_data['data_table_name'] = data_table_name
    relevant_data['instructions_file_path'] = instructions_file_path
    relevant_data['layout_file_path'] = layout_file_path
    relevant_data['parameters'] = parameters
    relevant_data['annotations_per_tuple_per_worker_table_name'] = annotations_per_tuple_per_worker_table_name
    relevant_data['aggregated_annotations_table_name'] = aggregated_annotations_table_name
    return relevant_data


def get_run_identifiers(request):
    """
    Return the set of identifiers which identify a run
    :param
        request: HttpRequest
    :return:
        user_id: int
        project_id: int
        workflow_id: int
        run_id: int
    """
    user_id = request.user.id
    project_id = request.GET.get('pid', -1)
    workflow_id = request.GET.get('wid', -1)
    run_id = request.GET.get('rid', -1)
    return user_id, project_id, workflow_id, run_id


def send_completion_notification(obj_run: run_components.Run):
    """Constructs and sends the webhook notification to the client's URL."""

    if not obj_run.notification_url:
        print("No notification URL found for run: ", obj_run.id)
        return False
    
    webhook_url = obj_run.notification_url

    # 1. Construct the payload
    # Iterate on dir files
    # list_file_names = []
    # run_dir_path: Path = get_run_dir_path(obj_run=obj_run)
    # if run_dir_path.is_dir():
    #     for file_path in run_dir_path.iterdir():
    #         if file_path.is_file():
    #             # add this file name and its path (with download link)
    #             list_file_names.append(file_path.name)
    composite_run_id = f"{obj_run.user_id}.{obj_run.project_id}.{obj_run.workflow_id}.{obj_run.id}"
    payload = {
        'run_id': composite_run_id,
        'status': obj_run.status,
        # 'list_file_names': list_file_names,
        'completed_at': timezone.now().isoformat()
    }
    # Convert the payload to JSON
    json_payload = json.dumps(payload)
    # # Use compact separators to minimize payload size (optional optimization)
    # json_payload = json.dumps(payload, separators=(',', ':'))

    # 2. Set headers
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Cymphony-Webhook/1.0'
    }

    # 3. Send the request
    try:
        response = requests.post(webhook_url, headers=headers, data=json_payload, timeout=30)
        # check for successful status codes 
        response.raise_for_status()
        print(f"Successfully sent notification for run {composite_run_id} to {webhook_url}. Status: {response.status_code}")
        return True
    except requests.exceptions.RequestException as e:
        # This is where an asynchronous retry mechanism should be triggered.
        print(f"Failed to send notification for run {composite_run_id} to {webhook_url}. Error: {e}")
        return False
    except Exception as e:
        print(f"Error sending completion notification for run {composite_run_id} to {webhook_url}. Error: {e}")
        return False

