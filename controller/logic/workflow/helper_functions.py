

def get_workflow_identifiers(request):
    """
    Return the set of identifiers which identify a project
    :param
        request: HttpRequest
    :return:
        user_id: int
        project_id: int
        workflow_id: int
    """
    user_id = request.user.id
    project_id = request.GET.get('pid', -1)
    workflow_id = request.GET.get('wid', -1)
    return user_id, project_id, workflow_id

