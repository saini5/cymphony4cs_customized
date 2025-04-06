from django.http import HttpResponse, HttpRequest, JsonResponse, FileResponse
from django.template import loader
from django.conf import settings

import controller.logic.run.components as run_components
import controller.logic.run.data_access_operations as run_dao
import controller.logic.run.helper_functions as run_helper_functions
import controller.logic.workflow.helper_functions as workflow_helper_functions
import controller.logic.workflow.data_access_operations as workflow_dao
import controller.logic.job.components as job_components
import controller.logic.job.data_access_operations as job_dao
from controller.logic.common_logic_operations import get_workflow_dir_path, get_run_dir_path

from collections import OrderedDict
from cryptography.fernet import Fernet
from pathlib import Path


def index(request: HttpRequest):
    """Return the run listing under this workflow and options to manipulate them"""

    run_type = settings.RUN_TYPES[1]  # human run

    # get this user and the selected workflow id
    user_id, project_id, workflow_id = workflow_helper_functions.get_workflow_identifiers(request)

    # get the workflow
    obj_workflow = workflow_dao.find_workflow(workflow_id=workflow_id, project_id=project_id, user_id=user_id)

    # get the constituent runs under this workflow
    list_runs = run_dao.find_all_runs(
        workflow_id=workflow_id,
        project_id=project_id,
        user_id=user_id,
        run_type=run_type
    )

    # show on screen via the response
    context = {
        'section': 'requester',
        'obj_workflow': obj_workflow,
        'list_runs': list_runs
    }
    template = loader.get_template('controller/run/run_management_index.html')
    response = HttpResponse(template.render(context, request))
    return response


def create(request: HttpRequest):
    """Return a run creation form, or create run based on filled form"""

    run_type = settings.RUN_TYPES[1]  # human run

    # get this user, project and workflow ids
    user_id, project_id, workflow_id = workflow_helper_functions.get_workflow_identifiers(request)

    if request.method == 'GET':    # return a form if the user intends to create a new run
        context = {
            'section': 'requester',
            'project_id': project_id,
            'workflow_id': workflow_id
        }
        template = loader.get_template('controller/run/create_run.html')
        response = HttpResponse(template.render(context, request))
        return response

    elif request.method == 'POST':  # create run based on form response
        run_id = request.GET.get('rid', -1)
        if run_id == -1:  # no run id specified
            # encapsulate the run details
            obj_run = run_components.Run(
                workflow_id=workflow_id,
                project_id=project_id,
                user_id=request.user.id,
                run_name=request.POST['rname'],
                run_description=request.POST['rdesc'],
                run_type=run_type,   # didn't need to supply it as a run is "human" by default already
                run_status=settings.RUN_STATUS[0]   # "IDLE"
            )

            # 1. store entry in db
            run_id = run_dao.create_run(obj_run)
            obj_run.id = run_id

            # 2. create new directory and copy files from workflow directory to the new directory
            # get the dir path of files in the workflow
            obj_workflow = workflow_dao.find_workflow(workflow_id=workflow_id, project_id=project_id, user_id=user_id)
            dir_path = get_workflow_dir_path(obj_workflow=obj_workflow)
            # get the target path
            obj_run = run_dao.find_run(run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
            # run_dir_path = Path(dir_path).joinpath('r' + str(obj_run.id))
            run_dir_path = get_run_dir_path(obj_run=obj_run)
            run_dao.copy_source_contents_to_target_dir(source_dir=dir_path, target_dir=run_dir_path)

            # 3. parse run>workflow.cy and build dag
            obj_run_dag = run_dao.parse(run_dir_path)
            print(obj_run_dag)

            # 4. store the dag in db
            run_dao.store_dag(obj_run, obj_run_dag)

            # 5. get the execution order of nodes in dag (this is called linearized dag)
            # preserve original dag before getting order
            original_dag = run_helper_functions.get_copy(obj_run_dag)
            print(original_dag)
            # get execution order - this modifies dag as well
            nodes_execution_order = run_helper_functions.get_execution_order(obj_run_dag)  # contains data nodes as well
            print([str(i) for i in nodes_execution_order])
            # store execution order for later use
            mapping_node_id_vs_position = dict()
            index = 0
            for node in nodes_execution_order:
                mapping_node_id_vs_position[node.id] = index
                index = index + 1
            run_dao.store_execution_order(mapping_node_id_vs_position=mapping_node_id_vs_position,
                                          obj_run=obj_run)

            # 6. pass over operator nodes to initialize jobs
            # extract operator nodes from linearized dag
            operator_nodes_in_execution_order = []
            for node in nodes_execution_order:
                if node.type == "operator":
                    operator_nodes_in_execution_order.append(node)
            mapping_node_id_vs_job_id = dict()       # node id vs job
            for node in operator_nodes_in_execution_order:
                if node.name in settings.HUMAN_OPERATORS:
                    job_type = settings.OPERATOR_TYPES[1]   # "human"
                else:
                    job_type = settings.OPERATOR_TYPES[0]   # "automatic"
                # encapsulate the job (operator) details
                obj_job = job_components.Job(
                    run_id=obj_run.id,
                    workflow_id=obj_run.workflow_id,
                    project_id=obj_run.project_id,
                    user_id=obj_run.user_id,
                    job_name=node.name,
                    job_type=job_type
                )
                job_id = job_dao.create_job(obj_job=obj_job)
                obj_job.id = job_id
                mapping_node_id_vs_job_id[node.id] = obj_job.id
            run_dao.store_mapping_node_vs_job(mapping_node_id_vs_job_id=mapping_node_id_vs_job_id, obj_run=obj_run)

            # 7. check if at least one job with job name as 3a_amt
            flag_3a_amt = False
            for node_id, job_id in mapping_node_id_vs_job_id.items():
                obj_job: job_components.Job = job_dao.find_job(job_id=job_id, run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
                if obj_job.type == settings.OPERATOR_TYPES[1] and obj_job.name == settings.HUMAN_OPERATORS[1]:  # human, and 3a_amt job
                    flag_3a_amt = True

            if flag_3a_amt:
                # need to get aws credentials from requester
                # 1. prepare html page to fetch aws credentials (embedded with the run_id)
                # 2. return the html page
                # return response
                context = {
                    'section': 'requester',
                    'run_id': obj_run.id,
                    'run_name': obj_run.name,
                    'workflow_id': obj_run.workflow_id,
                    'project_id': obj_run.project_id,
                    'flag_3a_amt': flag_3a_amt
                }
                # for api requests such as by external web client
                if 'python' in request.headers.get('User-Agent'):
                    return JsonResponse(context)
                # usual case: for requests via GUI (browser)
                template = loader.get_template('controller/run/run_creation_amt_parameters.html')
                response = HttpResponse(template.render(context, request))
                return response
            else:   # no 3a_amt job present, progress dag without any interruptions
                # 8. pass over operator nodes again in execution order, this time to execute
                # this time we have the node vs job mapping as well
                explore_dag: bool = run_helper_functions.progress_dag(
                    obj_run=obj_run,
                    original_dag=original_dag,
                    operator_nodes_in_execution_order=operator_nodes_in_execution_order,
                    mapping_node_id_vs_job_id=mapping_node_id_vs_job_id
                )
                # either all operators exhausted
                if explore_dag:
                    # run.status = "completed" in all_runs
                    obj_run.status = settings.RUN_STATUS[2]     # "COMPLETED"
                    run_dao.edit_run(obj_run=obj_run)
                    message = "Run completed!"
                    # message = "Run completed as well because it contained all automatic operators. \n " \
                    #           "You can check run results in view dashboard next to this run on the run listing page."
                # or human operator caused a break in exploration
                else:
                    message = "Run initiated. The run will complete once human workers are done annotating."
                    # message = "A human (worker) dependent operator is in progress. \n " \
                    #           "You can check run progress in view dashboard next to this run on the run listing page."

                # return response
                context = {
                    'section': 'requester',
                    'run_id': obj_run.id,
                    'run_name': obj_run.name,
                    'workflow_id': obj_run.workflow_id,
                    'project_id': obj_run.project_id,
                    'flag_3a_amt': flag_3a_amt,
                    'message': message
                }
                # for api requests such as by external web client
                if 'python' in request.headers.get('User-Agent'):
                    return JsonResponse(context)
                # usual case: for requests via GUI (browser)
                template = loader.get_template('controller/run/run_created.html')
                response = HttpResponse(template.render(context, request))
                return response

        else:  # run_id was specified
            # 1. get the run and put run in running
            obj_run: run_components.Run = run_dao.find_run(
                run_id=run_id,
                workflow_id=workflow_id,
                project_id=project_id,
                user_id=user_id
            )
            obj_run.status = settings.RUN_STATUS[1]     # "RUNNING"
            run_dao.edit_run(obj_run)

            # 2. load dag
            obj_run_dag: run_components.DiGraph = run_dao.load_dag(obj_run=obj_run)

            # 3. load node-job mapping
            mapping_node_id_vs_job_id = run_dao.load_mapping_node_vs_job(obj_run=obj_run)

            # 4. load the execution order of nodes in dag (this is called linearlized dag)
            nodes_execution_order: list = []
            mapping_node_id_vs_position: OrderedDict = run_dao.load_execution_order(obj_run=obj_run)
            for node_id, position in mapping_node_id_vs_position.items():
                node_id = int(node_id)
                obj_node = obj_run_dag.search_node_by_id(node_id=node_id)
                nodes_execution_order.append(obj_node)
            print([str(i) for i in nodes_execution_order])
            # extract operator nodes from linearized dag
            operator_nodes_in_execution_order = []
            for node in nodes_execution_order:
                if node.type == "operator":
                    operator_nodes_in_execution_order.append(node)

            # 5. get amt params
            run_amt_credentials: dict = {}
            # iam_user_name_field_identifier = 'iam_user_name'
            # run_amt_credentials['iam_user_name'] = request.POST[iam_user_name_field_identifier]
            access_key_id_field_identifier = 'access_key_id'
            run_amt_credentials['access_key_id'] = request.POST[access_key_id_field_identifier]
            secret_access_key_field_identifier = 'secret_access_key'
            run_amt_credentials['secret_access_key'] = request.POST[secret_access_key_field_identifier]

            # 6. encrypt credentials symmetrically, so you can decrypt later while executing 3a_amt job in dag
            # TODO: there should be ways to make the encryption more secure, or better way to store amt credentials.
            #  basic encryption as below should be good for now
            f: Fernet = run_helper_functions.create_fernet_for_amt_credentials(obj_run=obj_run)
            # token = f.encrypt(b"Secret message!")
            encrypted_run_amt_credentials: dict = {}
            for key, value in run_amt_credentials.items():
                token: bytes = f.encrypt(
                    bytes(value, encoding=settings.AMT_CREDENTIALS_ENCODING)
                )
                str_token: str = token.decode(settings.AMT_CREDENTIALS_ENCODING)
                encrypted_run_amt_credentials[key] = str_token

            # 7. store encrypted amt credentials
            run_dao.store_run_amt_credentials(
                amt_credentials=encrypted_run_amt_credentials,
                obj_run=obj_run
            )

            # 8. pass over operator nodes again in execution order, this time to execute
            explore_dag: bool = run_helper_functions.progress_dag(
                obj_run=obj_run,
                original_dag=obj_run_dag,
                operator_nodes_in_execution_order=operator_nodes_in_execution_order,
                mapping_node_id_vs_job_id=mapping_node_id_vs_job_id
            )
            # either all operators exhausted
            if explore_dag:
                ''' 
                Note: ideally, should not enter here, 
                since there should be at least one 3a_amt operator, 
                given that we entered in this "run_id was specified" code snippet
                '''
                # run.status = "completed" in all_runs
                obj_run.status = settings.RUN_STATUS[2]     # "COMPLETED"
                run_dao.edit_run(obj_run=obj_run)
                message = "Run completed!"
                # message = "Run completed as well because it contained all automatic operators. \n " \
                #           "You can check run results in view dashboard next to this run on the run listing page."
            # or human operator caused a break in exploration
            else:
                message = "Run initiated. The run will complete once human workers are done annotating."
                # message = "A human (worker) dependent operator is in progress. \n " \
                #           "You can check run progress in view dashboard next to this run on the run listing page."

            # return response
            context = {
                'section': 'requester',
                'run_id': obj_run.id,
                'run_name': obj_run.name,
                'workflow_id': obj_run.workflow_id,
                'project_id': obj_run.project_id,
                'message': message
            }
            # for api requests such as by external web client
            if 'python' in request.headers.get('User-Agent'):
                return JsonResponse(context)
            # usual case: for requests via GUI (browser)
            template = loader.get_template('controller/run/run_created.html')
            response = HttpResponse(template.render(context, request))
            return response


def view(request:HttpRequest):
    """Return the details of the specific run"""
    user_id, project_id, workflow_id, run_id = run_helper_functions.get_run_identifiers(request)

    list_file_names = []

    # get the run
    obj_run: run_components.Run = run_dao.find_run(run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)

    # get the run directory path
    run_dir_path: Path = get_run_dir_path(obj_run=obj_run)

    # get the progress information of this run
    if obj_run.status == settings.RUN_STATUS[1]:   # "RUNNING"
        progress_message = 'Run is still in progress.'
    else:
        # run is completed or aborted
        progress_message = 'Run has been completed or aborted.'

    # iterate on dir files
    if run_dir_path.is_dir():
        for file_path in run_dir_path.iterdir():
            if file_path.is_file():
                # add this file name and its path (with download link)
                list_file_names.append(file_path.name)

    # return response
    context = {
        'section': 'requester',
        'run_id': obj_run.id,
        'run_name': obj_run.name,
        'run_description': obj_run.description,
        'run_date_creation': obj_run.date_creation,
        'workflow_id': obj_run.workflow_id,
        'project_id': obj_run.project_id,
        'list_file_names': list_file_names,
        'message': progress_message
    }
    # for api requests such as by external web client
    if 'python' in request.headers.get('User-Agent'):
        return JsonResponse(context)
    # usual case: for requests via GUI
    template = loader.get_template('controller/run/view_run.html')
    response = HttpResponse(template.render(context, request))
    return response


def download_file(request:HttpRequest):
    """Download a specific file pertaining to run"""
    user_id, project_id, workflow_id, run_id = run_helper_functions.get_run_identifiers(request)

    file_name = request.GET.get('fname', None)

    # get the run
    obj_run: run_components.Run = run_dao.find_run(run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)

    # get the run directory path
    run_dir_path: Path = get_run_dir_path(obj_run=obj_run)

    file_path: Path = run_dir_path.joinpath(file_name)
    print(file_path)
    if not file_path.is_file():
        raise ValueError("File not present!")

    response = FileResponse(open(file_path, 'rb'))
    # response = FileResponse(open(file_path, 'rb'), as_attachment=True)
    return response

