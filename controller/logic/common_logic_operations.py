import re
from pathlib import Path
from django.conf import settings

import controller.logic.workflow.components as workflow_components
import controller.logic.run.components as run_components
import controller.logic.job.components as job_components


def multiple_replace(string, rep_dict):
    """Logic for multiple simultaneous string replacements"""
    # source: https://stackoverflow.com/questions/6116978/how-to-replace-multiple-substrings-of-a-string
    pattern = re.compile("|".join([re.escape(k) for k in sorted(rep_dict,key=len,reverse=True)]), flags=re.DOTALL)
    return pattern.sub(lambda x: rep_dict[x.group(0)], string)


def cantor_pairing(a: int, b: int):
    """Pairing function from https://en.wikipedia.org/wiki/Pairing_function"""
    result = (0.5)*(a+b)*(a+b+1) + b
    return int(result)


def parse_string_to_list_of_strings(x: str):
    """Break '["abc","xyz",...]' string into list of strings [abc, xyz, ... ]"""
    x = x.lstrip('[')
    x = x.rstrip(']')
    list_str = x.split(',')
    list_str = [x.strip() for x in list_str]
    list_str = [x[1:-1] for x in list_str]  # remove the extra " at the start and end of each element
    return list_str


def get_workflow_dir_path(obj_workflow: workflow_components.Workflow):
    """Get directory path of workflow related files"""
    return Path(settings.MEDIA_ROOT).joinpath(
        'u' + str(obj_workflow.user_id),
        'p' + str(obj_workflow.project_id),
        'w' + str(obj_workflow.id)
    )


def get_run_dir_path(obj_run: run_components.Run):
    """Get directory path of run related files"""
    return Path(settings.MEDIA_ROOT).joinpath(
        'u' + str(obj_run.user_id),
        'p' + str(obj_run.project_id),
        'w' + str(obj_run.workflow_id),
        'r' + str(obj_run.id)
    )


def get_run_prefix_table_name(obj_run: run_components.Run):
    """Get string representing prefix of run level db tables"""
    run_prefix_table_name = "u" + str(obj_run.user_id) + "_" + \
                            "p" + str(obj_run.project_id) + "_" + \
                            "w" + str(obj_run.workflow_id) + "_" + \
                            "r" + str(obj_run.id) + "_"
    return run_prefix_table_name


def get_job_prefix_table_name(obj_job: job_components.Job):
    """Get string representing prefix of job level db tables"""
    job_prefix_table_name = "u" + str(obj_job.user_id) + "_" + \
                            "p" + str(obj_job.project_id) + "_" + \
                            "w" + str(obj_job.workflow_id) + "_" + \
                            "r" + str(obj_job.run_id) + "_" + \
                            "j" + str(obj_job.id) + "_"
    return job_prefix_table_name
