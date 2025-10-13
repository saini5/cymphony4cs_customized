from django.http import HttpResponse, HttpRequest, JsonResponse, FileResponse
from django.template import loader
from django.conf import settings

import controller.logic.run.components as run_components
import controller.logic.run.data_access_operations as run_dao
import controller.logic.run.helper_functions as run_helper_functions
import controller.logic.workflow.components as workflow_components
import controller.logic.workflow.helper_functions as workflow_helper_functions
import controller.logic.workflow.data_access_operations as workflow_dao
import controller.logic.project.components as project_components
import controller.logic.project.data_access_operations as project_dao
import controller.logic.job.components as job_components
import controller.logic.job.data_access_operations as job_dao
from controller.logic.common_logic_operations import get_workflow_dir_path, get_run_dir_path, get_job_prefix_table_name

from collections import OrderedDict
from cryptography.fernet import Fernet
from pathlib import Path

import io
import zipfile
import json 
import logging

def create(request: HttpRequest):
    """
    Create a run for the external curation process
    Inputs:
    1. Workflow File (.cy file)
    2. Data File (.csv file)
    3. Data File's ID field name
    4. Instructions File (.html file)
    5. Layout File (.html file)
    6. Notification URL

    Returns:
    1. Run ID (composite - user_id . project_id . workflow_id . run_id)
    2. Status of the request - success, fail, other error
    3. Message - Any details pertaining to the request
    """
    
    try:
        # Capture the inputs
        workflow_file = request.FILES.get('workflow_file')
        data_file = request.FILES.get('data_file')
        id_field_name = request.POST.get('id_field_name', None)
        instructions_file = request.FILES.get('instructions_file')
        # Layout file is optional
        layout_file = request.FILES.get('layout_file', None)
        notification_url = request.POST.get('notification_url', None)
        user_id = request.user.id
        uploaded_file_listing = [workflow_file, data_file, instructions_file, layout_file]

        # Create a new project
        obj_project = project_components.Project(
            user_id=user_id,
            project_name='Default Project',
            project_description='For curation runs'
        )
        project_id = project_dao.create_project(obj_project)
        obj_project.id = project_id

        # Create a new workflow
        obj_workflow = workflow_components.Workflow(
            project_id=project_id,
            user_id=user_id,
            workflow_name='Default Workflow',
            workflow_description='For curation runs'
        )
        workflow_id = workflow_dao.create_workflow(obj_workflow)
        obj_workflow.id = workflow_id
        
        # For each file, upload to the workflow
        workflow_dir_path = get_workflow_dir_path(obj_workflow=obj_workflow)
        for f in uploaded_file_listing:
            if f is None:
                continue
            file_path = workflow_dir_path.joinpath(f.name)
            extension = file_path.suffix
            file_type = None
            if extension == '.cy':
                file_type = settings.UPLOADED_FILE_TYPES[0]
            elif extension == '.html':
                file_contents = f.read()
                file_contents = file_contents.decode()  # converting bytes to string
                if settings.SHORT_INSTRUCTIONS_BEGIN in file_contents and \
                        settings.SHORT_INSTRUCTIONS_END in file_contents and \
                        settings.LONG_INSTRUCTIONS_BEGIN in file_contents and \
                        settings.LONG_INSTRUCTIONS_END in file_contents:
                    file_type = settings.UPLOADED_FILE_TYPES[2]     # it is inst type
                elif settings.DESIGN_LAYOUT_BEGIN in file_contents and settings.DESIGN_LAYOUT_END in file_contents:
                    file_type = settings.UPLOADED_FILE_TYPES[3]     # it is layout type
            elif extension == '.csv':
                file_type = settings.UPLOADED_FILE_TYPES[1]

            if file_type == settings.UPLOADED_FILE_TYPES[1]:    # it is a data file
                obj_workflow_file = workflow_components.WorkflowFile(
                    file_type=file_type,
                    workflow_id=workflow_id,
                    project_id=project_id,
                    user_id=user_id,
                    file_path_str=str(file_path),
                    id_field_name=id_field_name
                )
            else:
                obj_workflow_file = workflow_components.WorkflowFile(
                    file_type=file_type,
                    workflow_id=workflow_id,
                    project_id=project_id,
                    user_id=user_id,
                    file_path_str=str(file_path),
                    id_field_name=None
                )
            workflow_dao.store_uploaded_file(obj_workflow_file=obj_workflow_file, f=f)

        # Create a new run
        run_type = settings.RUN_TYPES[1]  # human run
        obj_run = run_components.Run(
            workflow_id=workflow_id,
            project_id=project_id,
            user_id=user_id,
            run_name='Default Run',
            run_description='For curation runs',
            run_type=run_type,   # didn't need to supply it as a run is "human" by default already
            run_status=settings.RUN_STATUS[0],   # "IDLE",
            notification_url=notification_url
        )
        run_id = run_dao.create_run(obj_run)
        obj_run.id = run_id

        # Create new directory and copy files from workflow directory to the new directory
        run_dir_path = get_run_dir_path(obj_run=obj_run)
        run_dao.copy_source_contents_to_target_dir(source_dir=workflow_dir_path, target_dir=run_dir_path)

        # Parse run>workflow.cy and build dag
        obj_run_dag = run_dao.parse(run_dir_path)
        print(obj_run_dag)

        # Store the dag in db
        run_dao.store_dag(obj_run, obj_run_dag)

        # Get the execution order of nodes in dag (this is called linearized dag)
        # Preserve original dag before getting order
        original_dag = run_helper_functions.get_copy(obj_run_dag)
        print(original_dag)
        # Get execution order - this modifies dag as well
        nodes_execution_order = run_helper_functions.get_execution_order(obj_run_dag)  # contains data nodes as well
        print([str(i) for i in nodes_execution_order])
        # Store execution order for later use
        mapping_node_id_vs_position = dict()
        index = 0
        for node in nodes_execution_order:
            mapping_node_id_vs_position[node.id] = index
            index = index + 1
        run_dao.store_execution_order(mapping_node_id_vs_position=mapping_node_id_vs_position, obj_run=obj_run)

        # Pass over operator nodes to initialize jobs
        # Extract operator nodes from linearized dag
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

        # Check if at least one job with job name as 3a_amt
        flag_3a_amt = False
        for node_id, job_id in mapping_node_id_vs_job_id.items():
            obj_job: job_components.Job = job_dao.find_job(job_id=job_id, run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
            if obj_job.type == settings.OPERATOR_TYPES[1] and obj_job.name == settings.HUMAN_OPERATORS[1]:  # human, and 3a_amt job
                flag_3a_amt = True

        if flag_3a_amt:
            return JsonResponse({'status': 'error', 'message': '3a_amt job present'}, status=500)
        
        # No 3a_amt job present, progress dag without any interruptions
        obj_run.status = settings.RUN_STATUS[1]     # "RUNNING"
        run_dao.edit_run(obj_run=obj_run)
        # Pass over operator nodes again in execution order, this time to execute
        # This time we have the node vs job mapping as well
        explore_dag: bool = run_helper_functions.progress_dag(
            obj_run=obj_run,
            original_dag=original_dag,
            operator_nodes_in_execution_order=operator_nodes_in_execution_order,
            mapping_node_id_vs_job_id=mapping_node_id_vs_job_id,
            id_field_name=id_field_name
        )
        # Either all operators exhausted
        if explore_dag:
            # run.status = "completed" in all_runs
            obj_run.status = settings.RUN_STATUS[2]     # "COMPLETED"
            run_dao.edit_run(obj_run=obj_run)
            message = "Run completed!"
            # message = "Run completed as well because it contained all automatic operators. \n " \
            #           "You can check run results in view dashboard next to this run on the run listing page."
        # Or human operator caused a break in exploration
        else:
            message = "Run initiated. The run will complete once human workers are done annotating."
            # message = "A human (worker) dependent operator is in progress. \n " \
            #           "You can check run progress in view dashboard next to this run on the run listing page."

        # We have the user_id, project_id, workflow_id, run_id, construct the run_id that we return as a composite id
        composite_run_id = f"{user_id}.{project_id}.{workflow_id}.{run_id}"
        return JsonResponse({'status': 'success', 'message': 'Curation run created', 'run_id': composite_run_id}, status=200)
    except Exception as e:
        print(f"Error creating curation run: {e}")
        return JsonResponse({'status': 'error', 'message': f'Internal server error: {e}'}, status=500)


def drive_by_curate(request: HttpRequest):
    """
    Drive by curate the incoming data
    Inputs: (sent as JSON in the request body)
    1. Run ID (composite - user_id . project_id . workflow_id . run_id)
    2. <x, u, v> curations - each curation is a list of [x, v, u] where x is the external tuple id (coming from id_field_name of the data file), u is the worker id, and v is the annotation.

    Returns:
    1. Status of the request - success, fail, other error
    2. Message - Any details pertaining to the request
    """
    try:
        # Capture the inputs
        composite_run_id = request.POST.get('run_id')
        curations = request.POST.get('curations')

        # Parse the composite run id
        user_id, project_id, workflow_id, run_id = composite_run_id.split('.')
        print(f"User ID: {user_id}, Project ID: {project_id}, Workflow ID: {workflow_id}, Run ID: {run_id}")
        # Parse the curations
        curations = json.loads(curations)
        print(f"Curations: {curations}")

        # Figure out the sole 3a_kn(lm) job for this run
        obj_job = job_dao.find_3a_kn_job(run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
        if not obj_job:
            obj_job = job_dao.find_3a_knlm_job(run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
        if not obj_job:
            return JsonResponse({'status': 'error', 'message': 'No 3a_kn or 3a_knlm job found for this run'}, status=404)
        
        print(f"Found the 3a_kn or 3a_knlm job for this run")
        print(f"Job: {obj_job}")
        print(f"Job ID: {obj_job.id}")
        
        # Figure out the id_field_name from the workflow file
        workflow_files = workflow_dao.find_all_files(user_id=user_id, project_id=project_id, workflow_id=workflow_id)
        for workflow_file in workflow_files:
            if workflow_file.type == settings.UPLOADED_FILE_TYPES[1]:   # input file (data file)
                id_field_name = workflow_file.id_field_name
                break
        if id_field_name is None:
            return JsonResponse({'status': 'error', 'message': 'No id_field_name found for this workflows data file.'}, status=404)
        print(f"Id field name: {id_field_name}")
        
        # Insert the curations into the drive_by_curation_votes table where x goes to id_field_name and v goes to annotation and u goes to user_id
        job_dao.insert_drive_by_curation_votes(obj_job=obj_job, curations=curations, id_field_name=id_field_name)
        print(f"Inserted the curations into the drive_by_curation_votes table")

        # Replace the worker_id with the user_id in all curations in-memory
        for curation in curations:
            curation[1] = user_id
        print(f"Replaced the worker_id with the user_id in all curations in-memory")
        print(f"Curations: {curations}")
        # Create a job prefixed temp table to store the current annotations
        job_dao.create_temp_table(obj_job=obj_job, id_field_name=id_field_name)
        job_dao.insert_temp_curations(obj_job=obj_job, curations=curations, id_field_name=id_field_name)
        # Join the temp table with the tuples table to get the task_id vs worker_id vs annotation
        annotations = job_dao.join_temp_table_with_tuples(obj_job=obj_job, id_field_name=id_field_name)
        print(f"Joined the temp table with the tuples table to get the task_id vs worker_id vs annotation")
        print(f"Annotations: {annotations}")

        # Add these drive-by votes to the outputs table
        job_dao.add_drive_by_votes_to_outputs(obj_job=obj_job, curations=annotations)
        print(f"Added the drive-by votes to the outputs table")

        # Aggregate these ad-hoc curations
        job_dao.aggregate_while_drive_by_curating(obj_job=obj_job, curations=annotations)
        print(f"Aggregated the drive-by votes to the outputs table")
        # Destroy the temp table
        # job_dao.destroy_temp_table(obj_job=obj_job)

        return JsonResponse({'status': 'success', 'message': 'Drive by curations received and processed'}, status=200)

    except Exception as e:
        print(f"Error during drive by curating: {e}")
        print(f"Error details: {e.__traceback__}")
        return JsonResponse({'status': 'error', 'message': f'Internal server error: {e}'}, status=500)

def status(request: HttpRequest):
    """
    Get the status of a curation run
    Inputs: Run ID (composite - user_id . project_id . workflow_id . run_id)
    Outputs: Status of the run (COMPLETED, RUNNING, IDLE, ABORTED)
    """
    try:
        # Capture the inputs
        composite_run_id = request.GET.get('run_id', -1)
        user_id, project_id, workflow_id, run_id = composite_run_id.split('.')

        # Get the run
        obj_run: run_components.Run = run_dao.find_run(run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
        return JsonResponse({'run_status': obj_run.status}, status=200)
    except Exception as e:
        print(f"Error getting status of curation run: {e}")
        return JsonResponse({'status': 'error', 'message': f'Internal server error: {e}'}, status=500)

def download_tables(request: HttpRequest):
    """
    For the specified tables, export them from the database and return as csv files.
    Inputs: 
    1. Run ID (composite - user_id . project_id . workflow_id . run_id)
    2. Table name(s):
        a. Assignments
        b. Annotations
        c. Aggregations
    Outputs: Files - csv files for the specified tables
    """
    try:
        # Capture the inputs
        composite_run_id = request.POST.get('run_id')
        table_names = request.POST.get('table_names')   # list of table names

        # Get the run
        user_id, project_id, workflow_id, run_id = composite_run_id.split('.')
        obj_run: run_components.Run = run_dao.find_run(run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
        run_dir_path = get_run_dir_path(obj_run=obj_run)

        # Get the 3a_kn or 3a_knlm job
        obj_job = job_dao.find_3a_kn_job(run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
        if not obj_job:
            obj_job = job_dao.find_3a_knlm_job(run_id=run_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)
        if not obj_job:
            return JsonResponse({'status': 'error', 'message': 'No 3a_kn or 3a_knlm job found for this run'}, status=404)

        # Parse the table names
        table_names = json.loads(table_names)
        for table_name in table_names:
            if table_name not in ['Assignments', 'Annotations', 'Aggregations']:
                return JsonResponse({'status': 'error', 'message': 'Invalid table name'}, status=400)

        in_memory_zip = io.BytesIO()
        zip_filename = f"files.zip"
        files_found = False
        with zipfile.ZipFile(in_memory_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Get the tables
            for table_name in table_names:
                if table_name == 'Assignments':
                    export_table = get_job_prefix_table_name(obj_job=obj_job) + 'assignments'
                elif table_name == 'Annotations':
                    export_table = get_job_prefix_table_name(obj_job=obj_job) + 'outputs'
                elif table_name == 'Aggregations':
                    export_table = get_job_prefix_table_name(obj_job=obj_job) + 'final_labels'
                # Export the table as a csv file
                dest_file_name = table_name + '.csv'
                dest_file_path = run_dir_path.joinpath(dest_file_name)
                job_dao.create_file_from_table(source_table_name=export_table, destination_file_path=dest_file_path)

                zf.write(dest_file_path, arcname=dest_file_name) # Add file to zip
                files_found = True

        # Return these files from the run directory
        if not files_found:
            return JsonResponse({'status': 'error', 'message': 'No tables found!'}, status=404)    
        in_memory_zip.seek(0) # Rewind to the beginning of the stream
        response = FileResponse(in_memory_zip, content_type='application/zip')
        response['Content-Disposition'] = f'attachment; filename="{zip_filename}"'
        return response
    except Exception as e:
        print(f"Error downloading tables: {e}")
        return JsonResponse({'status': 'error', 'message': f'Internal server error: {e}'}, status=500)