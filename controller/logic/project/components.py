from django.utils import timezone


class Project:
    """
        A class to represent a project.

        Attributes
        ----------
        id : int
            project id
        user_id : int
            project owner's user id
        name: str
            project name
        description : str
            Description of project
        date_creation : datetime.datetime
            time of project creation
    """
    def __init__(self, user_id, project_name, project_description, date_creation=timezone.now(), project_id=-1):
        """
            Constructs all the necessary attributes for the project object.

            Parameters
            ----------
            id : int (optional)
                project id
            user_id : int
                project owner's user id
            name: str
                project name
            description : str
                Description of project
            date_creation : datetime.datetime (optional)
                time of project creation
        """
        self.id = project_id
        self.user_id = user_id
        self.name = project_name
        self.description = project_description
        self.date_creation = date_creation

    def __str__(self):
        return 'Project({0}, {1}, {2}, {3}, {4})'.format(self.id, self.user_id, self.name, self.description, self.date_creation)


