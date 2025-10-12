from django.utils import timezone


class Workflow:
    """
        A class to represent a workflow.

        Attributes
        ----------
        id : int
            workflow id
        project_id : int
            parent project
        user_id : int
            workflow (and obviously project's) owner's user id
        name: str
            workflow name
        description : str
            Description of workflow
        date_creation : datetime.datetime
            time of workflow creation
    """
    def __init__(self, project_id, user_id, workflow_name, workflow_description, date_creation=timezone.now(), workflow_id=-1):
        """
            Constructs all the necessary attributes for the workflow object.

            Parameters
            ----------
            id : int (optional)
                workflow id
            project_id : int
                parent project
            user_id : int
                workflow (and obviously project's) owner's user id
            name: str
                workflow name
            description : str
                Description of workflow
            date_creation : datetime.datetime (optional)
                time of workflow creation
        """
        self.id = workflow_id
        self.project_id = project_id
        self.user_id = user_id
        self.name = workflow_name
        self.description = workflow_description
        self.date_creation = date_creation

    def __str__(self):
        return 'Project({0}, {1}, {2}, {3}, {4}, {5})'.format(
            self.id, self.project_id, self.user_id, self.name, self.description, self.date_creation
        )


class WorkflowFile:
    """
        A class to represent a file of a workflow.

        Attributes
        ----------
        type : str
            file type (out of 'cy', 'inst', or 'input')
        id : int
            file id
        workflow_id : int
            parent workflow
        project_id : int
            parent project of the workflow
        user_id : int
            owner's user id
        file_path_str: str
            file path in str representation
        date_creation : datetime.datetime
            time of file creation
        id_field_name : str (optional)
            name of the id field in the data file
    """
    def __init__(self, workflow_id, project_id, user_id, file_path_str, date_creation=timezone.now(), file_id=-1, file_type='na', id_field_name=None):
        """
            Constructs all the necessary attributes for the workflow object.

            Parameters
            ----------
            type : str (optional)
                file type (out of 'cy', 'inst', or 'input')
            id : int (optional)
                file id
            workflow_id : int
                parent workflow
            project_id : int
                parent project of the workflow
            user_id : int
                owner's user id
            file_path_str: str
                file path in str representation
            date_creation : datetime.datetime (optional)
                time of workflow creation
            id_field_name : str (optional)
                name of the id field in the data file
        """
        self.type = file_type
        self.id = file_id
        self.workflow_id = workflow_id
        self.project_id = project_id
        self.user_id = user_id
        self.file_path_str = file_path_str
        self.date_creation = date_creation
        self.id_field_name = id_field_name
    def __str__(self):
        return 'Workflow({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7})'.format(
            self.type, self.id, self.workflow_id, self.project_id, self.user_id, self.file_path_str, self.date_creation, self.id_field_name
        )
