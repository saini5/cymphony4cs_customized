from django.http import HttpResponse, HttpRequest, JsonResponse, FileResponse
from django.template import loader
from django.conf import settings

import controller.logic.workflow.helper_functions as workflow_helper_functions
import controller.logic.workflow.data_access_operations as workflow_dao
import controller.logic.simulated_run.data_access_operations as simulated_run_dao
import controller.logic.run.components as run_components
import controller.logic.run.data_access_operations as run_dao
import controller.logic.run.helper_functions as run_helper_functions
import controller.logic.job.components as job_components
import controller.logic.job.data_access_operations as job_dao
from controller.logic.common_logic_operations import get_workflow_dir_path, get_run_dir_path

import time
from collections import OrderedDict
from pathlib import Path


def index(request: HttpRequest):
    """Return the simulated runs within the workflow"""

    run_type = settings.RUN_TYPES[0]  # simulation run

    # get the workflow identifiers
    user_id, project_id, workflow_id = workflow_helper_functions.get_workflow_identifiers(request)

    # get the workflow
    obj_workflow = workflow_dao.find_workflow(workflow_id=workflow_id, project_id=project_id, user_id=user_id)

    # get the constituent runs under this workflow
    list_simulated_runs = run_dao.find_all_runs(
        workflow_id=workflow_id,
        project_id=project_id,
        user_id=user_id,
        run_type=run_type
    )

    # show on screen via the response
    context = {
        'section': 'requester',
        'obj_workflow': obj_workflow,
        'list_simulated_runs': list_simulated_runs
    }
    template = loader.get_template('controller/simulated_run/simulated_run_management_index.html')
    response = HttpResponse(template.render(context, request))
    return response


def create(request: HttpRequest):
    """Return a simulated run creation form, or create simulated run based on filled form"""

    run_type = settings.RUN_TYPES[0]  # simulated run

    # get this user, project and workflow ids
    user_id, project_id, workflow_id = workflow_helper_functions.get_workflow_identifiers(request)

    if request.method == 'GET':  # return a form if the user intends to create a new simulated run
        context = {
            'section': 'requester',
            'project_id': project_id,
            'workflow_id': workflow_id
        }
        template = loader.get_template('controller/simulated_run/create_simulated_run.html')
        response = HttpResponse(template.render(context, request))
        return response

    elif request.method == 'POST':  # create simulated run based on form response
        run_id = request.GET.get('rid', -1)
        if run_id == -1:    # no run id specified
            # encapsulate the run details, and create a new run
            obj_simulated_run = run_components.Run(
                workflow_id=workflow_id,
                project_id=project_id,
                user_id=request.user.id,
                run_name=request.POST['s_r_name'],
                run_description=request.POST['s_r_desc'],
                run_type=run_type,
                run_status=settings.RUN_STATUS[0]   # "IDLE"
            )

            # 1. store entry in db
            simulated_run_id = run_dao.create_run(obj_run=obj_simulated_run)
            obj_simulated_run.id = simulated_run_id

            # 2. create new directory and copy files from workflow directory to the new directory
            # get the dir path of files in the workflow
            obj_workflow = workflow_dao.find_workflow(workflow_id=workflow_id, project_id=project_id, user_id=user_id)
            workflow_dir_path = get_workflow_dir_path(obj_workflow=obj_workflow)
            # get the target path
            obj_simulated_run = run_dao.find_run(
                run_id=simulated_run_id,
                workflow_id=workflow_id,
                project_id=project_id,
                user_id=user_id
            )
            # simulated_run_dir_path = Path(workflow_dir_path).joinpath('r' + str(obj_simulated_run.id))
            simulated_run_dir_path = get_run_dir_path(obj_run=obj_simulated_run)
            run_dao.copy_source_contents_to_target_dir(source_dir=workflow_dir_path, target_dir=simulated_run_dir_path)

            # 3. parse run>workflow.cy and build dag
            obj_simulated_run_dag = run_dao.parse(run_dir_path=simulated_run_dir_path)
            print(obj_simulated_run_dag)

            # 4. store the dag in db
            run_dao.store_dag(obj_run=obj_simulated_run, obj_run_dag=obj_simulated_run_dag)

            # 5. get the execution order of nodes in dag (this is called linearized dag)
            # preserve original dag before getting order
            original_dag = run_helper_functions.get_copy(obj_simulated_run_dag)
            print(original_dag)
            # get execution order - this modifies dag as well, that's why preserved original dag before
            nodes_execution_order = run_helper_functions.get_execution_order(obj_simulated_run_dag)
            print([str(i) for i in nodes_execution_order])
            # store execution order for later use
            mapping_node_id_vs_position = dict()
            index = 0
            for node in nodes_execution_order:
                mapping_node_id_vs_position[node.id] = index
                index = index + 1
            run_dao.store_execution_order(mapping_node_id_vs_position=mapping_node_id_vs_position, obj_run=obj_simulated_run)

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
                    run_id=obj_simulated_run.id,
                    workflow_id=obj_simulated_run.workflow_id,
                    project_id=obj_simulated_run.project_id,
                    user_id=obj_simulated_run.user_id,
                    job_name=node.name,
                    job_type=job_type
                )
                job_id = job_dao.create_job(obj_job=obj_job)
                obj_job.id = job_id
                mapping_node_id_vs_job_id[node.id] = obj_job.id
            run_dao.store_mapping_node_vs_job(mapping_node_id_vs_job_id=mapping_node_id_vs_job_id, obj_run=obj_simulated_run)

            # requester will be able to differentiate between two 3a_kn nodes by means of their input and output nodes
            # cymphony will use the job_id tied to each such bundle of nodes, to store the requester supplied params against the right job
            mapping_job_id_vs_node_bundle = {}
            for node_id, job_id in mapping_node_id_vs_job_id.items():
                obj_job: job_components.Job = job_dao.find_job(
                    job_id=job_id,
                    run_id=simulated_run_id,
                    workflow_id=workflow_id,
                    project_id=project_id,
                    user_id=user_id
                )
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
                'simulated_run_id': obj_simulated_run.id,
                'simulated_run_name': obj_simulated_run.name,
                'workflow_id': obj_simulated_run.workflow_id,
                'project_id': obj_simulated_run.project_id,
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
                'simulated_run_id': obj_simulated_run.id,
                'simulated_run_name': obj_simulated_run.name,
                'workflow_id': obj_simulated_run.workflow_id,
                'project_id': obj_simulated_run.project_id,
                'job_info': job_info,
                'status': obj_simulated_run.status
            }
            if 'python' in request.headers.get('User-Agent'):
                return JsonResponse(context_for_api)
            # usual case: for requests via GUI (browser)
            template = loader.get_template('controller/simulated_run/simulated_run_creation_parameters.html')
            response = HttpResponse(template.render(context, request))
            return response
        else:       # run_id was specified
            # 0. get the simulated run and put run in running
            obj_simulated_run: run_components.Run = run_dao.find_run(run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
            obj_simulated_run.status = settings.RUN_STATUS[1]   # "RUNNING"
            run_dao.edit_run(obj_simulated_run)
            print('SIMULATED RUN STARTED')
            start_ts = time.time()
            print('SIMULATED RUN STARTED at: ', start_ts)
            # 1. load dag
            obj_simulated_run_dag: run_components.DiGraph = run_dao.load_dag(obj_run=obj_simulated_run)

            # 2. load node-job mapping
            mapping_node_id_vs_job_id = run_dao.load_mapping_node_vs_job(obj_run=obj_simulated_run)

            # 3. load the execution order of nodes in dag (this is called linearlized dag)
            nodes_execution_order: list = []
            mapping_node_id_vs_position: OrderedDict = run_dao.load_execution_order(obj_run=obj_simulated_run)
            for node_id, position in mapping_node_id_vs_position.items():
                node_id = int(node_id)
                obj_node = obj_simulated_run_dag.search_node_by_id(node_id=node_id)
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
                obj_job: job_components.Job = job_dao.find_job(
                    job_id=job_id,
                    run_id=run_id,
                    workflow_id=workflow_id,
                    project_id=project_id,
                    user_id=user_id
                )
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
                obj_job: job_components.Job = job_dao.find_job(
                    job_id=job_id,
                    run_id=run_id,
                    workflow_id=workflow_id,
                    project_id=project_id,
                    user_id=user_id
                )
                simulated_run_dao.store_job_simulation_parameters(
                    simulation_parameters=job_simulation_params,
                    obj_job=obj_job
                )

            # 6. pass over operator nodes again in execution order, this time to execute
            explore_dag: bool = run_helper_functions.progress_dag(
                obj_run=obj_simulated_run,
                original_dag=obj_simulated_run_dag,
                operator_nodes_in_execution_order=operator_nodes_in_execution_order,
                mapping_node_id_vs_job_id=mapping_node_id_vs_job_id
            )

            # either all operators exhausted
            if explore_dag:
                # run.status = "completed" in all_runs table
                obj_simulated_run.status = settings.RUN_STATUS[2]   # "COMPLETED"
                run_dao.edit_run(obj_run=obj_simulated_run)
                # message = "Simulated run completed as well because it contained all automatic operators. \n " \
                #      "You can check run results in view dashboard next to this run on the simulated run listing page."
                message = "Simulated run completed!"
            # or human operator caused a break in exploration
            else:
                message = "Simulated run initiated. The run will complete once synthetic workers are done annotating."
                # message = "A human (synthetic worker in this case) dependent operator is in progress. \n " \
                #     "You can check run progress in view dashboard next to this run on the simulated run listing page."

            # return response
            context = {
                'section': 'requester',
                'simulated_run_id': obj_simulated_run.id,
                'simulated_run_name': obj_simulated_run.name,
                'workflow_id': obj_simulated_run.workflow_id,
                'project_id': obj_simulated_run.project_id,
                'message': message,
                'status': obj_simulated_run.status
            }
            # for api requests such as by external web client
            if 'python' in request.headers.get('User-Agent'):
                return JsonResponse(context)
            # usual case: for requests via GUI (browser)
            template = loader.get_template('controller/simulated_run/simulated_run_created.html')
            response = HttpResponse(template.render(context, request))
            return response


# TODO: view dashboard is not complete yet, but whatever has been coded is accurate.
def view_statistics(request:HttpRequest):
    """Return the details of the specific simulated run"""

    user_id, project_id, workflow_id, run_id = run_helper_functions.get_run_identifiers(request)

    # get the simulated run
    obj_simulated_run: run_components.Run = run_dao.find_run(
        run_id=run_id,
        workflow_id=workflow_id,
        project_id=project_id,
        user_id=user_id
    )

    # get the progress information of this run
    if obj_simulated_run.status == settings.RUN_STATUS[1]:   # "RUNNING"
        progress_message = 'We do not fetch run progress information right now.'
        # TODO: fetch the information and show as a dashboard
    # get the run results folder to show as a downloadable button
    else:
        # run is completed or aborted, show full results or partial results to downlaod
        # TODO: fetch the results and show on the dashboard
        pass

    # if simulated run is in any possible case (running, aborted or completed),
    # we will capture worker simulation parameters and statistics on constituent human jobs
    job_type = settings.OPERATOR_TYPES[1]   # human jobs
    human_jobs = job_dao.find_all_jobs_under_run(job_type=job_type, run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)

    # parameters: dict = dict()   # bundle of all simulation parameters supplied by requester for this run
    # for obj_job in human_jobs:
    #     parameters_job: dict = simulated_run_dao.load_job_simulation_parameters(
    #         obj_job=obj_job
    #     )   # bundle of all simulation parameters supplied for this job
    #     parameters[obj_job] = parameters_job

    statistics: dict = dict()   # bundle of all statistics captured in this run
    for obj_job in human_jobs:
        statistics_job: dict = dict()   # bundle of all statistics captured in this job
        # time from first assignment in job to last aggregation
        statistics_job['e2e_time'] = 'na'
        # accuracy of aggregated labels
        statistics_job['accuracy'] = 'na'
        # worker who got to annotate least number of tuples, annotated min_annotations_per_worker tuples
        statistics_job['min_annotations_per_worker'] = 'na'
        # worker who got to annotate most number of tuples, annotated max_annotations_per_worker tuples
        statistics_job['max_annotations_per_worker'] = 'na'
        # on average, a worker has annotated avg_annotations_per_worker
        statistics_job['avg_annotations_per_worker'] = 'na'
        # number of workers who got to make at least one annotation
        statistics_job['num_workers_who_annotated'] = 'na'

        if obj_job.status == settings.JOB_STATUS[2]:    # "COMPLETED"
            # capture e2e time of job
            statistics_job['e2e_time'] = simulated_run_dao.get_e2e_time(obj_job=obj_job)
            # capture accuracy of aggregated labels
            statistics_job['accuracy'] = simulated_run_dao.get_accuracy(obj_job=obj_job)
            # capture stats about annotations by workers
            (min_annotations_per_worker, max_annotations_per_worker, avg_annotations_per_worker,
             num_workers_who_annotated) = \
                simulated_run_dao.get_stats_of_worker_annotations(obj_job)
            statistics_job['min_annotations_per_worker'] = min_annotations_per_worker
            statistics_job['max_annotations_per_worker'] = max_annotations_per_worker
            statistics_job['avg_annotations_per_worker'] = avg_annotations_per_worker
            statistics_job['num_workers_who_annotated'] = num_workers_who_annotated
        elif obj_job.status == settings.JOB_STATUS[3]:    # "ABORTED"
            # capture e2e time of job
            statistics_job['e2e_time'] = simulated_run_dao.get_e2e_time(obj_job=obj_job)
            # capture accuracy of so far aggregated labels
            statistics_job['accuracy'] = simulated_run_dao.get_accuracy(obj_job=obj_job)
            # capture stats about annotations by workers so far
            (min_annotations_per_worker, max_annotations_per_worker, avg_annotations_per_worker,
             num_workers_who_annotated) = \
                simulated_run_dao.get_stats_of_worker_annotations(obj_job)
            statistics_job['min_annotations_per_worker'] = min_annotations_per_worker
            statistics_job['max_annotations_per_worker'] = max_annotations_per_worker
            statistics_job['avg_annotations_per_worker'] = avg_annotations_per_worker
            statistics_job['num_workers_who_annotated'] = num_workers_who_annotated
        elif obj_job.status == settings.JOB_STATUS[1]:  # "RUNNING"
            # cannot capture e2e time of job since it hasn't completed
            # capture accuracy of so far aggregated labels
            statistics_job['accuracy'] = simulated_run_dao.get_accuracy(obj_job=obj_job)
            # capture stats about annotations by workers so far
            (min_annotations_per_worker, max_annotations_per_worker, avg_annotations_per_worker, num_workers_who_annotated) = \
                simulated_run_dao.get_stats_of_worker_annotations(obj_job)
            statistics_job['min_annotations_per_worker'] = min_annotations_per_worker
            statistics_job['max_annotations_per_worker'] = max_annotations_per_worker
            statistics_job['avg_annotations_per_worker'] = avg_annotations_per_worker
            statistics_job['num_workers_who_annotated'] = num_workers_who_annotated
        elif obj_job.status == settings.JOB_STATUS[0]:  # "IDLE"
            # no job specific tables for this table would have been created, so no statistics can be captured
            pass

        statistics[obj_job] = statistics_job

    # # preparing in a concise format to use easily in template html
    # parameters_and_statistics: OrderedDict = OrderedDict()
    # for obj_job in human_jobs:
    #     parameters_and_statistics_job: OrderedDict = OrderedDict()
    #     parameters_job: dict = parameters[obj_job]
    #     statistics_job: dict = statistics[obj_job]
    #     for key, value in parameters_job.items():
    #         parameters_and_statistics_job[key] = value
    #     for key, value in statistics_job.items():
    #         parameters_and_statistics_job[key] = value
    #     parameters_and_statistics[obj_job] = parameters_and_statistics_job

    # TODO: right now, only showing statistics (and not parameters) in the GUI screen
    # return screen with two sections
    context = {
        'section': 'requester',
        'obj_simulated_run': obj_simulated_run,
        'human_jobs': human_jobs,
        # 'parameters': parameters,
        'statistics': statistics,
        # 'parameters_and_statistics': parameters_and_statistics
    }
    template = loader.get_template('controller/simulated_run/view_simulated_run_statistics.html')
    response = HttpResponse(template.render(context, request))
    return response


def view(request:HttpRequest):
    """Return the details of the specific simulated run"""
    user_id, project_id, workflow_id, run_id = run_helper_functions.get_run_identifiers(request)
    list_file_names = []
    # get the run
    obj_simulated_run: run_components.Run = run_dao.find_run(
        run_id=run_id,
        workflow_id=workflow_id,
        project_id=project_id,
        user_id=user_id
    )
    # get the run directory path
    simulated_run_dir_path: Path = get_run_dir_path(obj_run=obj_simulated_run)
    # get the progress information of this run
    if obj_simulated_run.status == settings.RUN_STATUS[1]:   # "RUNNING"
        progress_message = 'Run is still in progress.'
    else:
        # run is completed or aborted
        progress_message = 'Run has been completed or aborted.'
    # iterate on dir files
    if simulated_run_dir_path.is_dir():
        for file_path in simulated_run_dir_path.iterdir():
            if file_path.is_file():
                # add this file name and its path (with download link)
                list_file_names.append(file_path.name)
    # return response
    context = {
        'section': 'requester',
        'simulated_run_id': obj_simulated_run.id,
        'simulated_run_name': obj_simulated_run.name,
        'simulated_run_description': obj_simulated_run.description,
        'simulated_run_date_creation': obj_simulated_run.date_creation,
        'workflow_id': obj_simulated_run.workflow_id,
        'project_id': obj_simulated_run.project_id,
        'list_file_names': list_file_names,
        'message': progress_message,
        'status': obj_simulated_run.status
    }
    # for api requests such as by external web client
    if 'python' in request.headers.get('User-Agent'):
        return JsonResponse(context)
    # usual case: for requests via GUI
    template = loader.get_template('controller/simulated_run/view_simulated_run.html')
    response = HttpResponse(template.render(context, request))
    return response


def download_file(request:HttpRequest):
    """Download a specific file pertaining to simulated run"""
    user_id, project_id, workflow_id, run_id = run_helper_functions.get_run_identifiers(request)

    file_name = request.GET.get('fname', None)

    # get the run
    obj_simulated_run: run_components.Run = run_dao.find_run(
        run_id=run_id,
        workflow_id=workflow_id,
        project_id=project_id,
        user_id=user_id
    )

    # get the run directory path
    simulated_run_dir_path: Path = get_run_dir_path(obj_run=obj_simulated_run)

    file_path: Path = simulated_run_dir_path.joinpath(file_name)
    print(file_path)
    if not file_path.is_file():
        raise ValueError("File not present!")

    response = FileResponse(open(file_path, 'rb'))
    # response = FileResponse(open(file_path, 'rb'), as_attachment=True)
    return response
