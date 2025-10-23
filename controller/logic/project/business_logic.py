from django.http import HttpResponse, HttpRequest, JsonResponse
from django.template import loader

import controller.logic.project.components as project_components
import controller.logic.project.helper_functions as project_helper_functions
import controller.logic.project.data_access_operations as project_dao


def index(request: HttpRequest):
    """Return project listing under this user"""

    # get this user
    user_id = request.user.id

    # get all projects under this user
    user_projects_list = project_dao.find_all_projects(user_id=user_id)

    # return screen showing the above listing
    context = {
        'section': 'requester',
        'user_projects_list': user_projects_list
    }
    template = loader.get_template('controller/project/project_management_index.html')
    response = HttpResponse(template.render(context, request))
    return response


def create(request: HttpRequest):
    """Return a project creation form, or create project based on filled form"""

    if request.method == 'GET':    # return a form if the user intends to create a new project
        context = {
            'section': 'requester'
        }
        template = loader.get_template('controller/project/detail_project.html')
        response = HttpResponse(template.render(context, request))
        return response
    elif request.method == 'POST':  # create project based on form response
        # encapsulate the project details
        obj_project = project_components.Project(
            user_id=request.user.id,
            project_name=request.POST['pname'],
            project_description=request.POST['pdesc']
        )

        # store in db
        project_id = project_dao.create_project(obj_project)
        obj_project.id = project_id

        # return response
        context = {
            'section': 'requester',
            'project_id': obj_project.id,
            'project_name': obj_project.name,
            'user_id': obj_project.user_id
        }
        # for api requests such as by external web client
        if 'python' in request.headers.get('User-Agent'):
            return JsonResponse(context)
        # usual case: for requests via GUI
        template = loader.get_template('controller/project/project_created.html')
        response = HttpResponse(template.render(context, request))
        return response


def view(request:HttpRequest):
    """Return the details of the specific project"""

    # get this user and the selected project id
    user_id, project_id = project_helper_functions.get_project_identifiers(request)

    # get the project
    obj_project = project_dao.find_project(project_id=project_id, user_id=user_id)

    # show on screen via the response
    context = {
        'section': 'requester',
        'obj_project': obj_project
    }
    template = loader.get_template('controller/project/view_project.html')
    response = HttpResponse(template.render(context, request))
    return response


def edit(request:HttpRequest):
    """Return project edit form or edit the project based on filled form"""

    # get this user and the selected project id
    user_id, project_id = project_helper_functions.get_project_identifiers(request)

    # get the project
    obj_project = project_dao.find_project(project_id=project_id, user_id=user_id)

    if request.method == 'GET':
        # pre-fill the form with the retrieved project and send back the form screen
        context = {
            'section': 'requester',
            'obj_project': obj_project
        }
        template = loader.get_template('controller/project/edit_project.html')
        response = HttpResponse(template.render(context, request))
        return response
    elif request.method == 'POST':
        # a user-filled form came
        # encapsulate the project details
        obj_project.name = request.POST['pname']
        obj_project.description = request.POST['pdesc']

        # update in db
        project_dao.edit_project(obj_project)

        # return response - project edited successfully
        context = {'section': 'requester', 'obj_project': obj_project}
        template = loader.get_template('controller/project/project_edited.html')
        response = HttpResponse(template.render(context, request))
        return response


def delete(request:HttpRequest):
    """Delete the specific project"""

    # get this user and the selected project id
    user_id, project_id = project_helper_functions.get_project_identifiers(request)

    # delete the resource
    project_dao.delete_project(project_id=project_id, user_id=user_id)

    # return the response
    context = {'section': 'requester'}
    template = loader.get_template('controller/project/project_deleted.html')
    response = HttpResponse(template.render(context, request))
    return response
