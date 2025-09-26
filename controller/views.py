from django.http import HttpResponse
from django.conf import settings
from django.contrib.auth.decorators import login_required

from controller.logic.project import business_logic as project_logic
from controller.logic.workflow import business_logic as workflow_logic
from controller.logic.run import business_logic as run_logic
from controller.logic.simulated_run import business_logic as simulated_run_logic
from controller.logic.pipelined_simulated_run import business_logic as pipelined_simulated_run_logic
from controller.logic.job import business_logic as job_logic

from controller.enums import UserType


@login_required
def process(request):
    """
    entry point of all non-account requests
    :param request: incoming request
    :return: outgoing response
    """
    action_category = request.GET.get('category', 'na')
    action = request.GET.get('action', 'na')

    if action_category == 'project':
        if action == 'index':   # list all projects under this user
            return project_logic.index(request)
        elif action == 'create':    # create new project
            return project_logic.create(request)
        elif action == 'view':  # view a specific project
            return project_logic.view(request)
        elif action == 'edit':   # edit a specific project
            return project_logic.edit(request)
        elif action == 'delete':    # delete a specific project
            return project_logic.delete(request)
    elif action_category == 'workflow':
        if action == 'index':   # list all workflows of a specific user project
            return workflow_logic.index(request)
        elif action == 'create':    # create new workflow
            return workflow_logic.create(request)
        elif action == 'delete':    # delete a specific workflow
            return workflow_logic.delete(request)
        elif action == 'edit':    # edit a specific workflow
            return workflow_logic.edit(request)
        elif action == 'edit_workflow_basic_info':    # edit basic info of workflow
            return workflow_logic.edit_basic_info(request)
        elif action == 'edit_workflow_upload_files':    # upload file into the workflow
            return workflow_logic.edit_workflow_upload_files(request)
        elif action == 'edit_workflow_delete_file':    # delete an uploaded file
            return workflow_logic.edit_workflow_delete_file(request)
        elif action == 'view':    # create new workflow
            return workflow_logic.view(request)
    elif action_category == 'run':
        if action == 'index':   # list all human runs of a workflow
            return run_logic.index(request)
        elif action == 'create':    # create new human run
            return run_logic.create(request)
        elif action == 'view':  # view the run's details
            return run_logic.view(request)
        elif action == 'download_file':     # download a file pertaining to the run
            return run_logic.download_file(request)
    elif action_category == 'job_3a_kn':    # worker interactions with 3a_kn human jobs
        if action == 'index':   # list all 3a_kn jobs for worker
            return job_logic.index_3a_kn(request)
        elif action == 'work':    # selected a job to work on
            return job_logic.work_3a_kn(request)
        elif action == 'process_annotation':    # submitted an annotation
            return job_logic.process_annotation_3a_kn(request)
        elif action == 'skip': # skip the current task
            return job_logic.skip(request)
        elif action == 'quit':  # quit working on this 3a_kn job
            return job_logic.quit(request)
    elif action_category == 'job_3a_knlm':    # worker interactions with 3a_knlm human jobs
        # Add user type as a request attribute for use in business logic
        request.user_type = UserType.from_user_id(request.user.id)
        if action == 'index':   # list all 3a_knlm jobs for worker
            return job_logic.index_3a_knlm(request)
        elif action == 'work':    # selected a job to work on
            return job_logic.work_3a_knlm(request)
        elif action == 'process_annotation':    # submitted an annotation
            return job_logic.process_annotation_3a_knlm(request)
        elif action == 'skip': # skip the current task
            return job_logic.skip_3a_knlm(request)
        elif action == 'quit':  # quit working on this 3a_knlm job
            return job_logic.quit_3a_knlm(request)
    elif action_category == 'simulated_run':    # handled separately at top level but uses "run" code wherever possible
        if action == 'index':   # list all simulated runs of a workflow
            return simulated_run_logic.index(request)
        elif action == 'create':    # create new simulated run
            return simulated_run_logic.create(request)
        elif action == 'view_statistics':   # view statistics pertaining to the simulation
            return simulated_run_logic.view_statistics(request)
        elif action == 'view':  # view details of the simulated run
            return simulated_run_logic.view(request)
        elif action == 'download_file':     # download a file pertaining to the simulated run
            return simulated_run_logic.download_file(request)
    elif action_category == 'pipelined_simulated_run':  # simulated run with pipelining instead of default batch mode
        if action == 'index':
            return pipelined_simulated_run_logic.index(request)
        elif action == 'create':
            return pipelined_simulated_run_logic.create(request)
        elif action == 'view_statistics':
            return simulated_run_logic.view_statistics(request)
        elif action == 'view':
            return simulated_run_logic.view(request)
        elif action == 'download_file':
            pass

    return HttpResponse('Invalid request: ', request.path + action_category + action)
