from django.utils import timezone
from django.conf import settings


class Job:
    """
        A class to represent a job.

        Attributes
        ----------
        id : int
            job id
        run_id: int
            parent run
        workflow_id : int
            parent workflow
        project_id : int
            workflow's parent project
        user_id : int
            project's owner
        name: str
            job name (operator name)
        type : str
            Type of job (human or automatic)
        status : str
            Status of job
        date_creation : datetime.datetime
            time of job creation
    """
    def __init__(self, run_id, workflow_id, project_id, user_id, job_name, job_type, job_status=settings.JOB_STATUS[0], date_creation=timezone.now(), job_id=-1):
        """
            Constructs all the necessary attributes for the job object.

            Parameters
            ----------
            id : int (optional)
                job id
            run_id: int
                parent run
            workflow_id : int
                parent workflow
            project_id : int
                workflow's parent project
            user_id : int
                project's owner
            name: str
                job name
            type : str
                type of job
            status : str (optional)
                Status of job
            date_creation : datetime.datetime (optional)
                time of run creation
        """
        self.id = job_id
        self.run_id = run_id
        self.workflow_id = workflow_id
        self.project_id = project_id
        self.user_id = user_id
        self.name = job_name
        self.type = job_type
        self.status = job_status
        self.date_creation = date_creation

    def __str__(self):
        return 'Job({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8})'.format(
            self.id, self.run_id, self.workflow_id, self.project_id, self.user_id, self.name, self.type, self.status, self.date_creation
        )

