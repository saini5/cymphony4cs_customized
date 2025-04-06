from django.http import HttpResponse, HttpRequest, JsonResponse
from django.template import loader
from django.conf import settings

import controller.logic.workflow.components as workflow_components
import controller.logic.project.helper_functions as project_helper_functions
import controller.logic.workflow.helper_functions as workflow_helper_functions
import controller.logic.workflow.data_access_operations as workflow_dao
import controller.logic.project.data_access_operations as project_dao
from controller.logic.common_logic_operations import get_workflow_dir_path


def index(request:HttpRequest):
    """Return the workflow listing under this project and options to manipulate them"""

    # get this user and the selected project id
    user_id, project_id = project_helper_functions.get_project_identifiers(request)

    # get the project
    obj_project = project_dao.find_project(project_id=project_id, user_id=user_id)

    # get the constituent workflows under this project and user
    list_workflows = workflow_dao.find_all_workflows(user_id=user_id, project_id=project_id)

    # show on screen via the response
    context = {
        'section': 'requester',
        'obj_project': obj_project,
        'list_workflows': list_workflows
    }
    template = loader.get_template('controller/workflow/workflow_management_index.html')
    response = HttpResponse(template.render(context, request))
    return response


def create(request: HttpRequest):
    """Return a workflow creation form, or create workflow based on filled form"""

    # get this user and the selected project id
    user_id, project_id = project_helper_functions.get_project_identifiers(request)

    if request.method == 'GET':    # return a form if the user intends to create a new workflow
        context = {
            'section': 'requester',
            'project_id': project_id
        }
        template = loader.get_template('controller/workflow/create_workflow.html')
        response = HttpResponse(template.render(context, request))
        return response
    elif request.method == 'POST':  # create workflow based on form response
        # encapsulate the workflow details
        obj_workflow = workflow_components.Workflow(
            project_id=project_id,
            user_id=request.user.id,
            workflow_name=request.POST['wname'],
            workflow_description=request.POST['wdesc']
        )

        # store in db
        workflow_id = workflow_dao.create_workflow(obj_workflow)
        obj_workflow.id = workflow_id

        # return response
        context = {
            'section': 'requester',
            'workflow_id': obj_workflow.id,
            'workflow_name': obj_workflow.name,
            'project_id': obj_workflow.project_id
        }
        # for api requests such as by external web client
        if 'python' in request.headers.get('User-Agent'):
            return JsonResponse(context)
        # usual case: for requests via GUI (browser)
        template = loader.get_template('controller/workflow/workflow_created.html')
        response = HttpResponse(template.render(context, request))
        return response


def edit(request:HttpRequest):
    """Return workflow basic info and file uploads for this workflow"""

    user_id, project_id, workflow_id = workflow_helper_functions.get_workflow_identifiers(request)

    # get the workflow
    obj_workflow = workflow_dao.find_workflow(workflow_id=workflow_id, project_id=project_id, user_id=user_id)

    # get the constituent files under this workflow
    list_files = workflow_dao.find_all_files(user_id=user_id, project_id=project_id, workflow_id=workflow_id)
    
    # return screen with two sections
    context = {
        'section': 'requester',
        'obj_workflow': obj_workflow,
        'list_files': list_files
    }
    template = loader.get_template('controller/workflow/edit_workflow.html')
    response = HttpResponse(template.render(context, request))
    return response
    

def edit_basic_info(request:HttpRequest):
    """Return workflow edit form or edit the workflow based on filled form"""

    user_id, project_id, workflow_id = workflow_helper_functions.get_workflow_identifiers(request)

    # get the workflow
    obj_workflow = workflow_dao.find_workflow(workflow_id=workflow_id, project_id=project_id, user_id=user_id)

    if request.method == 'GET':
        # pre-fill the form with the retrieved project and send back the form screen
        context = {
            'section': 'requester',
            'obj_workflow': obj_workflow
        }
        template = loader.get_template('controller/workflow/edit_workflow_basic_info.html')
        response = HttpResponse(template.render(context, request))
        return response
    elif request.method == 'POST':
        # a user-filled form came
        # encapsulate the project details
        obj_workflow.name = request.POST['wname']
        obj_workflow.description = request.POST['wdesc']

        # update in db
        workflow_dao.edit_workflow_basic_info(obj_workflow)

        # return response - project edited successfully
        context = {'section': 'requester', 'obj_workflow': obj_workflow}
        template = loader.get_template('controller/workflow/workflow_basic_info_edited.html')
        response = HttpResponse(template.render(context, request))
        return response


def edit_workflow_upload_files(request: HttpRequest):
    """Return upload form or upload the files based on filled form"""

    user_id, project_id, workflow_id = workflow_helper_functions.get_workflow_identifiers(request)

    # get the workflow
    obj_workflow = workflow_dao.find_workflow(workflow_id=workflow_id, project_id=project_id, user_id=user_id)

    # get the constituent files under this workflow
    list_files = workflow_dao.find_all_files(user_id=user_id, project_id=project_id, workflow_id=workflow_id)

    if request.method == 'GET':
        # pre-fill the form with the retrieved project and send back the form screen
        context = {
            'section': 'requester',
            'obj_workflow': obj_workflow,
            'list_files': list_files,
        }
        template = loader.get_template('controller/workflow/edit_workflow_upload_files.html')
        response = HttpResponse(template.render(context, request))
        return response
    elif request.method == 'POST':
        # a user-filled form came

        # encapsulate file details into the file object
        f = request.FILES['fname']
        workflow_dir_path = get_workflow_dir_path(obj_workflow=obj_workflow)
        file_path = workflow_dir_path.joinpath(
            f.name
        )
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

        obj_workflow_file = workflow_components.WorkflowFile(
            file_type=file_type,
            workflow_id=workflow_id,
            project_id=project_id,
            user_id=user_id,
            file_path_str=str(file_path)
        )

        workflow_dao.store_uploaded_file(obj_workflow_file=obj_workflow_file, f=f)

        # return response - file uploaded successfully
        context = {
            'section': 'requester',
            'workflow_id': obj_workflow.id,
            'workflow_name': obj_workflow.name,
            'project_id': obj_workflow.project_id,
        }
        # for api requests such as by external web client
        if 'python' in request.headers.get('User-Agent'):
            return JsonResponse(context)
        # usual case: for requests via GUI (browser)
        template = loader.get_template('controller/workflow/workflow_files_upload_successful.html')
        response = HttpResponse(template.render(context, request))
        return response


def edit_workflow_delete_file(request:HttpRequest):
    """Delete the specific file in the specified workflow"""

    # file type will identify the table to query
    file_type = request.GET.get('ftype', '')

    # the below four ids together will identify the file in the table
    user_id, project_id, workflow_id = workflow_helper_functions.get_workflow_identifiers(request)
    file_id = request.GET.get('fid', '')

    # delete the resource
    workflow_dao.delete_uploaded_file(file_type=file_type, file_id=file_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)

    # return the response
    context = {
        'section': 'requester',
        'workflow_id': workflow_id,
        'project_id': project_id
    }
    template = loader.get_template('controller/workflow/workflow_files_deleted_success.html')
    response = HttpResponse(template.render(context, request))
    return response


def delete(request:HttpRequest):
    """Delete the specific workflow in the specified project for this user"""

    user_id, project_id, workflow_id = workflow_helper_functions.get_workflow_identifiers(request)

    # delete the resource
    workflow_dao.delete_workflow(workflow_id=workflow_id, project_id=project_id, user_id=user_id)

    # return the response
    context = {
        'section': 'requester',
        'project_id': project_id
    }
    template = loader.get_template('controller/workflow/workflow_deleted.html')
    response = HttpResponse(template.render(context, request))
    return response


def view(request:HttpRequest):
    """Return the details of the specific workflow"""

    user_id, project_id, workflow_id = workflow_helper_functions.get_workflow_identifiers(request)

    # get the workflow
    obj_workflow = workflow_dao.find_workflow(workflow_id=workflow_id, project_id=project_id, user_id=user_id)

    # get the constituent files under this workflow
    list_files = workflow_dao.find_all_files(user_id=user_id, project_id=project_id, workflow_id=workflow_id)

    # return screen with two sections
    context = {
        'section': 'requester',
        'obj_workflow': obj_workflow,
        'list_files': list_files
    }
    template = loader.get_template('controller/workflow/view_workflow.html')
    response = HttpResponse(template.render(context, request))
    return response

