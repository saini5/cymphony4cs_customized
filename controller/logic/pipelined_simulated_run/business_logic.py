from django.http import HttpResponse, HttpRequest, JsonResponse, FileResponse
from django.template import loader

from pathlib import Path
from django.conf import settings

import controller.logic.workflow.data_access_operations as workflow_dao

import controller.logic.pipelined_simulated_run.helper_functions as pipelined_simulated_run_helper_functions

import controller.logic.simulated_run.data_access_operations as simulated_run_dao

import controller.logic.run.components as run_components
import controller.logic.run.data_access_operations as run_dao
import controller.logic.run.helper_functions as run_helper_functions

import controller.logic.job.components as job_components
import controller.logic.job.data_access_operations as job_dao

from controller.logic.common_logic_operations import get_workflow_dir_path, get_run_dir_path

import random
from collections import OrderedDict
import time


def index(request: HttpRequest):
    """Return the pipelined simulated run listing under this workflow and options to manipulate them"""

    run_type = settings.RUN_TYPES[2]  # pipelined simulation run

    # get this user and the selected workflow id
    user_id = request.user.id
    project_id = request.GET.get('pid', -1)
    workflow_id = request.GET.get('wid', -1)

    # get the workflow
    obj_workflow = workflow_dao.find_workflow(workflow_id=workflow_id, project_id=project_id, user_id=user_id)

    # get the constituent runs under this workflow
    list_pipelined_simulated_runs = run_dao.find_all_runs(workflow_id=workflow_id, project_id=project_id, user_id=user_id, run_type=run_type)

    # show on screen via the response
    context = {
        'section': 'requester',
        'obj_workflow': obj_workflow,
        'list_pipelined_simulated_runs': list_pipelined_simulated_runs
    }
    template = loader.get_template('controller/pipelined_simulated_run/pipelined_simulated_run_management_index.html')
    response = HttpResponse(template.render(context, request))
    return response


def create(request: HttpRequest):

    run_type = settings.RUN_TYPES[2]  # pipelined simulated run

    # get this user, project and workflow ids
    user_id = request.user.id
    project_id = int(request.GET.get('pid', -1))
    workflow_id = int(request.GET.get('wid', -1))

    if request.method == 'GET':  # return a form if the user intends to create a new pipelined simulated run
        context = {
            'section': 'requester',
            'project_id': project_id,
            'workflow_id': workflow_id
        }
        template = loader.get_template('controller/pipelined_simulated_run/create_pipelined_simulated_run.html')
        response = HttpResponse(template.render(context, request))
        return response

    elif request.method == 'POST':  # create pipelined simulated run based on form response
        run_id = request.GET.get('rid', -1)
        if run_id == -1:    # no run id specified
            # encapsulate the run details, and create a new run
            obj_pipelined_simulated_run = run_components.Run(
                workflow_id=workflow_id,
                project_id=project_id,
                user_id=request.user.id,
                run_name=request.POST['p_s_r_name'],
                run_description=request.POST['p_s_r_desc'],
                run_type=run_type,
                run_status=settings.RUN_STATUS[0]   # "IDLE"
            )

            # 1. store entry in db
            pipelined_simulated_run_id = run_dao.create_run(obj_run=obj_pipelined_simulated_run)
            obj_pipelined_simulated_run.id = pipelined_simulated_run_id

            # 2. create new directory and copy files from workflow directory to the new directory

            # get the dir path of files in the workflow
            obj_workflow = workflow_dao.find_workflow(workflow_id=workflow_id, project_id=project_id, user_id=user_id)
            workflow_dir_path = get_workflow_dir_path(obj_workflow=obj_workflow)

            # get the target path
            obj_pipelined_simulated_run = run_dao.find_run(run_id=pipelined_simulated_run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
            # simulated_run_dir_path = Path(workflow_dir_path).joinpath('r' + str(obj_simulated_run.id))
            pipelined_simulated_run_dir_path = get_run_dir_path(obj_run=obj_pipelined_simulated_run)
            run_dao.copy_source_contents_to_target_dir(source_dir=workflow_dir_path, target_dir=pipelined_simulated_run_dir_path)

            # 3. parse run>workflow.cy and build dag
            obj_pipelined_simulated_run_dag = run_dao.parse(run_dir_path=pipelined_simulated_run_dir_path)
            print(obj_pipelined_simulated_run_dag)

            # 4. store the dag in db
            run_dao.store_dag(obj_run=obj_pipelined_simulated_run, obj_run_dag=obj_pipelined_simulated_run_dag)

            # 5. get the execution order of nodes in dag (this is called linearlized dag)

            # preserve original dag before getting order
            original_dag = run_helper_functions.get_copy(obj_pipelined_simulated_run_dag)
            print(original_dag)

            # get execution order - this modifies dag as well, that's why preserved original dag before
            nodes_execution_order = run_helper_functions.get_execution_order(obj_pipelined_simulated_run_dag)   # this contains data nodes as well as operator nodes
            print([str(i) for i in nodes_execution_order])

            # store execution order for later use
            mapping_node_id_vs_position = dict()
            index = 0
            for node in nodes_execution_order:
                mapping_node_id_vs_position[node.id] = index
                index = index + 1

            run_dao.store_execution_order(mapping_node_id_vs_position=mapping_node_id_vs_position, obj_run=obj_pipelined_simulated_run)

            # 6. pass over operator nodes to initialize jobs

            # extract operator nodes from linearized dag
            operator_nodes_in_execution_order = []
            for node in nodes_execution_order:
                if node.type == "operator":
                    operator_nodes_in_execution_order.append(node)

            mapping_node_id_vs_job_id = dict()  # node id vs job
            for node in operator_nodes_in_execution_order:
                if node.name in settings.HUMAN_OPERATORS:
                    job_type = settings.OPERATOR_TYPES[1]   # "human"
                else:
                    job_type = settings.OPERATOR_TYPES[0]   # "automatic"
                # encapsulate the job (operator) details
                obj_job = job_components.Job(
                    run_id=obj_pipelined_simulated_run.id,
                    workflow_id=obj_pipelined_simulated_run.workflow_id,
                    project_id=obj_pipelined_simulated_run.project_id,
                    user_id=obj_pipelined_simulated_run.user_id,
                    job_name=node.name,
                    job_type=job_type
                )
                job_id = job_dao.create_job(obj_job=obj_job)
                obj_job.id = job_id
                mapping_node_id_vs_job_id[node.id] = obj_job.id
            run_dao.store_mapping_node_vs_job(mapping_node_id_vs_job_id=mapping_node_id_vs_job_id, obj_run=obj_pipelined_simulated_run)

            # requester will be able to differentiate between two 3a_kn nodes by means of their input and output nodes
            # cymphony will use the job_id tied to each such bundle of nodes, to store the requester supplied params against the right job
            mapping_job_id_vs_node_bundle = {}
            for node_id, job_id in mapping_node_id_vs_job_id.items():
                obj_job: job_components.Job = job_dao.find_job(job_id=job_id, run_id=pipelined_simulated_run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
                if obj_job.type == settings.OPERATOR_TYPES[1] and obj_job.name == settings.HUMAN_OPERATORS[0]: # human, and 3a_kn job
                    node_bundle = {}
                    node_corresponding_to_job: run_components.Node = original_dag.search_node_by_id(node_id=node_id)
                    input_nodes: list = original_dag.get_incoming_nodes(node=node_corresponding_to_job)
                    output_nodes: list = original_dag.get_outgoing_nodes(node=node_corresponding_to_job)
                    node_bundle['node'] = node_corresponding_to_job
                    node_bundle['input_nodes'] = input_nodes
                    node_bundle['output_nodes'] = output_nodes
                    mapping_job_id_vs_node_bundle[obj_job.id] = node_bundle

            # return response
            context = {
                'section': 'requester',
                'pipelined_simulated_run_id': obj_pipelined_simulated_run.id,
                'pipelined_simulated_run_name': obj_pipelined_simulated_run.name,
                'workflow_id': obj_pipelined_simulated_run.workflow_id,
                'project_id': obj_pipelined_simulated_run.project_id,
                'mapping_job_id_vs_node_bundle': mapping_job_id_vs_node_bundle
            }
            # for api requests such as by external web client
            job_info: dict = {}
            for job_id, node_bundle in mapping_job_id_vs_node_bundle.items():
                node_info: dict = {}
                for key, value in node_bundle.items():
                    if key == 'node':
                        node_info[key] = value.name
                    elif key == 'input_nodes':
                        list_input_nodes = []
                        for n in value:
                            list_input_nodes.append(n.name)
                        node_info[key] = list_input_nodes
                    elif key == 'output_nodes':
                        list_output_nodes = []
                        for n in value:
                            list_output_nodes.append(n.name)
                        node_info[key] = list_output_nodes
                job_info[job_id] = node_info

            context_for_api = {
                'section': 'requester',
                'simulated_run_id': obj_pipelined_simulated_run.id,
                'simulated_run_name': obj_pipelined_simulated_run.name,
                'workflow_id': obj_pipelined_simulated_run.workflow_id,
                'project_id': obj_pipelined_simulated_run.project_id,
                'job_info': job_info
            }
            if 'python' in request.headers.get('User-Agent'):
                return JsonResponse(context_for_api)
            # usual case: for requests via GUI (browser)
            template = loader.get_template('controller/pipelined_simulated_run/pipelined_simulated_run_creation_parameters.html')
            response = HttpResponse(template.render(context, request))
            return response

        else:       # run_id was specified

            # 0. get the pipelined simulated run and put run in running
            obj_pipelined_simulated_run: run_components.Run = run_dao.find_run(run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
            obj_pipelined_simulated_run.status = settings.RUN_STATUS[1]   # "RUNNING"
            run_dao.edit_run(obj_pipelined_simulated_run)
            start_ts = time.time()
            print('PIPELINED SIMULATED RUN STARTED at: ', start_ts)

            # 1. load dag
            obj_pipelined_simulated_run_dag: run_components.DiGraph = run_dao.load_dag(obj_run=obj_pipelined_simulated_run)

            # 2. load node-job mapping
            mapping_node_id_vs_job_id = run_dao.load_mapping_node_vs_job(obj_run=obj_pipelined_simulated_run)

            # 3. load the execution order of nodes in dag (this is called linearlized dag)
            nodes_execution_order: list = []
            mapping_node_id_vs_position: OrderedDict = run_dao.load_execution_order(obj_run=obj_pipelined_simulated_run)
            for node_id, position in mapping_node_id_vs_position.items():
                node_id = int(node_id)
                obj_node = obj_pipelined_simulated_run_dag.search_node_by_id(node_id=node_id)
                nodes_execution_order.append(obj_node)
            print([str(i) for i in nodes_execution_order])

            # extract operator nodes from linearized dag
            operator_nodes_in_execution_order = []
            for node in nodes_execution_order:
                if node.type == "operator":
                    operator_nodes_in_execution_order.append(node)

            # 4. get simulation params
            mapping_job_id_vs_simulation_params: dict = {}
            for node_id, job_id in mapping_node_id_vs_job_id.items():
                obj_job: job_components.Job = job_dao.find_job(job_id=job_id, run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
                if obj_job.type == settings.OPERATOR_TYPES[1] and obj_job.name == settings.HUMAN_OPERATORS[0]: # human, and 3a_kn job
                    job_simulation_params: dict = {}

                    min_loop_times_identifier = 'min_loop_times_job_' + str(job_id)
                    job_simulation_params['min_loop_times'] = request.POST[min_loop_times_identifier]
                    max_loop_times_identifier = 'max_loop_times_job_' + str(job_id)
                    job_simulation_params['max_loop_times'] = request.POST[max_loop_times_identifier]

                    min_workers_per_burst_identifier = 'min_workers_per_burst_job_' + str(job_id)
                    job_simulation_params['min_workers_per_burst'] = request.POST[min_workers_per_burst_identifier]
                    max_workers_per_burst_identifier = 'max_workers_per_burst_job_' + str(job_id)
                    job_simulation_params['max_workers_per_burst'] = request.POST[max_workers_per_burst_identifier]

                    min_time_gap_in_loop_points_identifier = 'min_time_gap_in_loop_points_job_' + str(job_id)
                    job_simulation_params['min_time_gap_in_loop_points'] = request.POST[min_time_gap_in_loop_points_identifier]
                    max_time_gap_in_loop_points_identifier = 'max_time_gap_in_loop_points_job_' + str(job_id)
                    job_simulation_params['max_time_gap_in_loop_points'] = request.POST[max_time_gap_in_loop_points_identifier]

                    min_worker_annotation_time_identifier = 'min_worker_annotation_time_job_' + str(job_id)
                    job_simulation_params['min_worker_annotation_time'] = request.POST[min_worker_annotation_time_identifier]
                    max_worker_annotation_time_identifier = 'max_worker_annotation_time_job_' + str(job_id)
                    job_simulation_params['max_worker_annotation_time'] = request.POST[max_worker_annotation_time_identifier]

                    min_worker_accuracy_identifier = 'min_worker_accuracy_job_' + str(job_id)
                    job_simulation_params['min_worker_accuracy'] = request.POST[min_worker_accuracy_identifier]
                    max_worker_accuracy_identifier = 'max_worker_accuracy_job_' + str(job_id)
                    job_simulation_params['max_worker_accuracy'] = request.POST[max_worker_accuracy_identifier]

                    mapping_job_id_vs_simulation_params[job_id] = job_simulation_params

            # 5. store simulation params
            for job_id, job_simulation_params in mapping_job_id_vs_simulation_params.items():
                obj_job: job_components.Job = job_dao.find_job(job_id=job_id, run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
                simulated_run_dao.store_job_simulation_parameters(simulation_parameters=job_simulation_params, obj_job=obj_job)

            # 6. pass over operator nodes again in execution order, this time to execute
            explore_dag: bool = pipelined_simulated_run_helper_functions.progress_dag(
                obj_run=obj_pipelined_simulated_run,
                original_dag=obj_pipelined_simulated_run_dag,
                operator_nodes_in_execution_order=operator_nodes_in_execution_order,
                mapping_node_id_vs_job_id=mapping_node_id_vs_job_id
            )

            # either all operators exhausted
            if explore_dag:
                # run.status = "completed" in all_runs
                obj_pipelined_simulated_run.status = settings.RUN_STATUS[2]   # "COMPLETED"
                run_dao.edit_run(obj_run=obj_pipelined_simulated_run)
                message = "Simulated run completed as well because it contained all automatic operators. \n " \
                          "You can check run results in view dashboard next to this run on the simulated run listing page."
            # or human operator caused a break in exploration
            else:
                message = "A human (synthetic worker in this case) dependent operator is in progress. \n " \
                          "You can check run progress in view dashboard next to this run on the simulated run listing page."

            # return response
            context = {
                'section': 'requester',
                'pipelined_simulated_run_id': obj_pipelined_simulated_run.id,
                'pipelined_simulated_run_name': obj_pipelined_simulated_run.name,
                'workflow_id': obj_pipelined_simulated_run.workflow_id,
                'project_id': obj_pipelined_simulated_run.project_id,
                'message': message
            }
            # for api requests such as by external web client
            if 'python' in request.headers.get('User-Agent'):
                return JsonResponse(context)
            # usual case: for requests via GUI (browser)
            template = loader.get_template('controller/pipelined_simulated_run/pipelined_simulated_run_created.html')
            response = HttpResponse(template.render(context, request))
            return response