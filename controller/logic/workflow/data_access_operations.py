from django.core.files.uploadedfile import UploadedFile
from django.db import connection
from django.utils import timezone
from django.conf import settings

import controller.logic.workflow.components as workflow_components
from controller.logic.common_data_access_operations import dict_fetchall, dict_fetchone

from pathlib import Path


def find_all_workflows(user_id: int, project_id: int):
    """Return all workflows under this user's projects, from the db"""

    cursor = connection.cursor()
    list_all_workflows = []
    try:
        # get all workflows for this project and user from the all_workflows table
        table_all_workflows = "all_workflows"
        cursor.execute(
            "SELECT w_id, u_id, p_id, w_name, w_desc, date_creation FROM " +
            table_all_workflows +
            " WHERE u_id = %s AND p_id = %s",
            [user_id, project_id]
        )
        workflows = dict_fetchall(cursor)
        for row in workflows:
            obj_workflow = workflow_components.Workflow(
                workflow_id=row['w_id'],
                project_id=row['p_id'],
                user_id=row['u_id'],
                workflow_name=row['w_name'],
                workflow_description=row['w_desc'],
                date_creation=row['date_creation']
            )
            list_all_workflows.append(obj_workflow)
        return list_all_workflows

    except ValueError as err:
        print('Data access exception in find all workflows')
        print(err.args)

    finally:
        cursor.close()


def find_workflow(workflow_id: int, project_id: int, user_id: int):
    """Return the specified workflow from the db"""

    cursor = connection.cursor()
    workflow = None
    try:
        # get the specific workflow for this user project from the all_workflows table
        table_all_workflows = "all_workflows"
        cursor.execute(
            "SELECT w_id, p_id, u_id, w_name, w_desc, date_creation FROM " +
            table_all_workflows +
            " WHERE w_id = %s AND p_id = %s AND u_id = %s",
            [workflow_id, project_id, user_id]
        )
        workflow_row = dict_fetchone(cursor)

        obj_workflow = workflow_components.Workflow(
            workflow_id=workflow_row['w_id'],
            project_id=workflow_row['p_id'],
            user_id=workflow_row['u_id'],
            workflow_name=workflow_row['w_name'],
            workflow_description=workflow_row['w_desc'],
            date_creation=workflow_row['date_creation']
        )

        workflow = obj_workflow
        return workflow

    except ValueError as err:
        print('Data access exception in find workflow')
        print(err.args)

    finally:
        cursor.close()


def create_workflow(obj_workflow: workflow_components.Workflow):
    """Insert the incoming workflow into db"""

    cursor = connection.cursor()
    try:
        # insert workflow entry into all_workflows table
        table_all_workflows = "all_workflows"
        cursor.execute(
            "INSERT into " +
            table_all_workflows +
            " (p_id, u_id, w_name, w_desc, date_creation) VALUES (%s, %s, %s, %s, %s)" +
            " RETURNING w_id;",
            [obj_workflow.project_id,
             obj_workflow.user_id,
             obj_workflow.name,
             obj_workflow.description,
             timezone.now()  # store this time of creation
             ]
        )
        workflow_id = cursor.fetchone()[0]

        return workflow_id

    except ValueError as err:
        print('Data access exception in create workflow')
        print(err.args)

    finally:
        cursor.close()


def delete_workflow(workflow_id: int, project_id: int, user_id: int):
    """Delete the specified workflow from the db"""

    cursor = connection.cursor()
    try:
        # get the specific workflow from the all_workflows table
        table_all_workflows = "all_workflows"
        cursor.execute(
            "DELETE FROM " +
            table_all_workflows +
            " WHERE w_id = %s AND p_id = %s AND u_id = %s",
            [workflow_id, project_id, user_id]
        )

        return

    except ValueError as err:
        print('Data access exception in delete workflow')
        print(err.args)

    finally:
        cursor.close()


def edit_workflow_basic_info(obj_workflow: workflow_components.Workflow):
    """Edit the specified workflow basic info of this user project in the db"""

    cursor = connection.cursor()
    try:
        # insert workflow entry into all_workflows table
        table_all_workflows = "all_workflows"
        cursor.execute(
            "UPDATE " +
            table_all_workflows +
            " SET " +
            "w_name = %s, w_desc = %s" +
            " WHERE u_id = %s AND p_id = %s AND w_id = %s",
            [obj_workflow.name, obj_workflow.description, obj_workflow.user_id, obj_workflow.project_id, obj_workflow.id]
        )
        return

    except ValueError as err:
        print('Data access exception in edit workflow basic info')
        print(err.args)

    finally:
        cursor.close()


def store_uploaded_file(obj_workflow_file: workflow_components.WorkflowFile, f: UploadedFile):
    """Store this file and store ledger entry"""

    cursor = connection.cursor()
    try:
        # store on fs
        file_path = Path(obj_workflow_file.file_path_str)
        file_dir_path = file_path.parent
        file_dir_path.mkdir(parents=True, exist_ok=True)

        if file_path.is_file():
            raise ValueError("File with this name already exists")

        with file_path.open("wb+") as destination:
            for chunk in f.chunks():
                destination.write(chunk)

        file_path_str = str(file_path)
        # store ledger entry in db
        if obj_workflow_file.type == settings.UPLOADED_FILE_TYPES[0]:
            cursor.execute(
                "INSERT into " +
                "workflow_cy_files" +
                " (w_id, p_id, u_id, cy_file_path, date_creation) VALUES (%s, %s, %s, %s, %s)",
                [
                    obj_workflow_file.workflow_id,
                    obj_workflow_file.project_id,
                    obj_workflow_file.user_id,
                    file_path_str,
                    timezone.now()
                ]
            )
        elif obj_workflow_file.type == settings.UPLOADED_FILE_TYPES[2]:
            cursor.execute(
                "INSERT into " +
                "workflow_inst_files" +
                " (w_id, p_id, u_id, inst_file_path, date_creation) VALUES (%s, %s, %s, %s, %s)",
                [
                    obj_workflow_file.workflow_id,
                    obj_workflow_file.project_id,
                    obj_workflow_file.user_id,
                    file_path_str,
                    timezone.now()
                ]
            )
        elif obj_workflow_file.type == settings.UPLOADED_FILE_TYPES[1]:
            cursor.execute(
                "INSERT into " +
                "workflow_input_files" +
                " (w_id, p_id, u_id, input_file_path, date_creation) VALUES (%s, %s, %s, %s, %s)",
                [
                    obj_workflow_file.workflow_id,
                    obj_workflow_file.project_id,
                    obj_workflow_file.user_id,
                    file_path_str,
                    timezone.now()
                ]
            )
        elif obj_workflow_file.type == settings.UPLOADED_FILE_TYPES[3]:
            cursor.execute(
                "INSERT into " +
                "workflow_layout_files" +
                " (w_id, p_id, u_id, layout_file_path, date_creation) VALUES (%s, %s, %s, %s, %s)",
                [
                    obj_workflow_file.workflow_id,
                    obj_workflow_file.project_id,
                    obj_workflow_file.user_id,
                    file_path_str,
                    timezone.now()
                ]
            )

        return

    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in store file')

    finally:
        cursor.close()


def delete_uploaded_file(file_type: str, file_id: int, workflow_id: int, project_id: int, user_id: int):
    """Delete the specified file from the fs and db"""

    cursor = connection.cursor()
    try:
        # get the file in question
        obj_workflow_file = find_workflow_file(file_type=file_type, file_id=file_id, workflow_id=workflow_id, project_id=project_id, user_id=user_id)

        file_path_str = obj_workflow_file.file_path_str
        file_path = Path(file_path_str)
        if not file_path.is_file():
            raise ValueError("File to be deleted does not even exist")
        # remove file
        file_path.unlink()

        if obj_workflow_file.type == settings.UPLOADED_FILE_TYPES[0]:
            cursor.execute(
                "DELETE FROM " +
                "workflow_cy_files" +
                " WHERE f_id = %s AND w_id = %s AND p_id = %s AND u_id = %s",
                [
                    obj_workflow_file.id,
                    obj_workflow_file.workflow_id,
                    obj_workflow_file.project_id,
                    obj_workflow_file.user_id
                ]
            )
        elif obj_workflow_file.type == settings.UPLOADED_FILE_TYPES[2]:
            cursor.execute(
                "DELETE FROM " +
                "workflow_inst_files" +
                " WHERE f_id = %s AND w_id = %s AND p_id = %s AND u_id = %s",
                [
                    obj_workflow_file.id,
                    obj_workflow_file.workflow_id,
                    obj_workflow_file.project_id,
                    obj_workflow_file.user_id
                ]
            )
        elif obj_workflow_file.type == settings.UPLOADED_FILE_TYPES[1]:
            cursor.execute(
                "DELETE FROM " +
                "workflow_input_files" +
                " WHERE f_id = %s AND w_id = %s AND p_id = %s AND u_id = %s",
                [
                    obj_workflow_file.id,
                    obj_workflow_file.workflow_id,
                    obj_workflow_file.project_id,
                    obj_workflow_file.user_id
                ]
            )
        elif obj_workflow_file.type == settings.UPLOADED_FILE_TYPES[3]:
            cursor.execute(
                "DELETE FROM " +
                "workflow_layout_files" +
                " WHERE f_id = %s AND w_id = %s AND p_id = %s AND u_id = %s",
                [
                    obj_workflow_file.id,
                    obj_workflow_file.workflow_id,
                    obj_workflow_file.project_id,
                    obj_workflow_file.user_id
                ]
            )

        return

    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in delete uploaded file')

    finally:
        cursor.close()


def find_all_files(user_id: int, project_id: int, workflow_id: int):
    """Return all files under this workflow, from the db"""

    cursor = connection.cursor()
    list_all_files = []
    try:
        # get all files for this workflow from three tables
        table_workflow_input_files = "workflow_input_files"
        table_workflow_inst_files = "workflow_inst_files"
        table_workflow_cy_files = "workflow_cy_files"
        table_workflow_layout_files = "workflow_layout_files"

        cursor.execute(
            "SELECT input_file_path, f_id, w_id, p_id, u_id, date_creation FROM " +
            table_workflow_input_files +
            " WHERE w_id = %s AND p_id = %s AND u_id = %s",
            [workflow_id, project_id, user_id]
        )
        input_files = dict_fetchall(cursor)

        cursor.execute(
            "SELECT inst_file_path, f_id, w_id, p_id, u_id, date_creation FROM " +
            table_workflow_inst_files +
            " WHERE w_id = %s AND p_id = %s AND u_id = %s",
            [workflow_id, project_id, user_id]
        )
        inst_files = dict_fetchall(cursor)

        cursor.execute(
            "SELECT cy_file_path, f_id, w_id, p_id, u_id, date_creation FROM " +
            table_workflow_cy_files +
            " WHERE w_id = %s AND p_id = %s AND u_id = %s",
            [workflow_id, project_id, user_id]
        )
        cy_files = dict_fetchall(cursor)

        cursor.execute(
            "SELECT layout_file_path, f_id, w_id, p_id, u_id, date_creation FROM " +
            table_workflow_layout_files +
            " WHERE w_id = %s AND p_id = %s AND u_id = %s",
            [workflow_id, project_id, user_id]
        )
        layout_files = dict_fetchall(cursor)

        for row in cy_files:
            obj_workflow_file = workflow_components.WorkflowFile(
                file_type=settings.UPLOADED_FILE_TYPES[0],
                file_id=row['f_id'],
                workflow_id=row['w_id'],
                project_id=row['p_id'],
                user_id=row['u_id'],
                file_path_str=row['cy_file_path'],
                date_creation=row['date_creation']
            )
            list_all_files.append(obj_workflow_file)
        for row in inst_files:
            obj_workflow_file = workflow_components.WorkflowFile(
                file_type=settings.UPLOADED_FILE_TYPES[2],
                file_id=row['f_id'],
                workflow_id=row['w_id'],
                project_id=row['p_id'],
                user_id=row['u_id'],
                file_path_str=row['inst_file_path'],
                date_creation=row['date_creation']
            )
            list_all_files.append(obj_workflow_file)
        for row in input_files:
            obj_workflow_file = workflow_components.WorkflowFile(
                file_type=settings.UPLOADED_FILE_TYPES[1],
                file_id=row['f_id'],
                workflow_id=row['w_id'],
                project_id=row['p_id'],
                user_id=row['u_id'],
                file_path_str=row['input_file_path'],
                date_creation=row['date_creation']
            )
            list_all_files.append(obj_workflow_file)
        for row in layout_files:
            obj_workflow_file = workflow_components.WorkflowFile(
                file_type=settings.UPLOADED_FILE_TYPES[3],
                file_id=row['f_id'],
                workflow_id=row['w_id'],
                project_id=row['p_id'],
                user_id=row['u_id'],
                file_path_str=row['layout_file_path'],
                date_creation=row['date_creation']
            )
            list_all_files.append(obj_workflow_file)

        return list_all_files

    except ValueError as err:
        print('Data access exception in find all workflow files')
        print(err.args)

    finally:
        cursor.close()


def find_workflow_file(file_type: str, file_id: int, workflow_id: int, project_id: int, user_id: int):
    """Return the specified file object from the db"""

    cursor = connection.cursor()
    workflow_file = None
    try:
        # the three ledger tables
        table_workflow_input_files = "workflow_input_files"
        table_workflow_inst_files = "workflow_inst_files"
        table_workflow_cy_files = "workflow_cy_files"
        table_workflow_layout_files = "workflow_layout_files"

        if file_type == settings.UPLOADED_FILE_TYPES[1]:
            cursor.execute(
                "SELECT input_file_path, f_id, w_id, p_id, u_id, date_creation FROM " +
                table_workflow_input_files +
                " WHERE f_id = %s AND w_id = %s AND p_id = %s AND u_id = %s",
                [file_id, workflow_id, project_id, user_id]
            )
            workflow_file_row = dict_fetchone(cursor)
            workflow_file_path = workflow_file_row['input_file_path']
        elif file_type == settings.UPLOADED_FILE_TYPES[2]:
            cursor.execute(
                "SELECT inst_file_path, f_id, w_id, p_id, u_id, date_creation FROM " +
                table_workflow_inst_files +
                " WHERE f_id = %s AND w_id = %s AND p_id = %s AND u_id = %s",
                [file_id, workflow_id, project_id, user_id]
            )
            workflow_file_row = dict_fetchone(cursor)
            workflow_file_path = workflow_file_row['inst_file_path']
        elif file_type == settings.UPLOADED_FILE_TYPES[0]:
            cursor.execute(
                "SELECT cy_file_path, f_id, w_id, p_id, u_id, date_creation FROM " +
                table_workflow_cy_files +
                " WHERE f_id = %s AND w_id = %s AND p_id = %s AND u_id = %s",
                [file_id, workflow_id, project_id, user_id]
            )
            workflow_file_row = dict_fetchone(cursor)
            workflow_file_path = workflow_file_row['cy_file_path']
        elif file_type == settings.UPLOADED_FILE_TYPES[3]:
            cursor.execute(
                "SELECT layout_file_path, f_id, w_id, p_id, u_id, date_creation FROM " +
                table_workflow_layout_files +
                " WHERE f_id = %s AND w_id = %s AND p_id = %s AND u_id = %s",
                [file_id, workflow_id, project_id, user_id]
            )
            workflow_file_row = dict_fetchone(cursor)
            workflow_file_path = workflow_file_row['layout_file_path']

        obj_workflow_file = workflow_components.WorkflowFile(
            file_type=file_type,
            file_id=workflow_file_row['f_id'],
            workflow_id=workflow_file_row['w_id'],
            project_id=workflow_file_row['p_id'],
            user_id=workflow_file_row['u_id'],
            file_path_str=workflow_file_path,
            date_creation=workflow_file_row['date_creation']
        )

        workflow_file = obj_workflow_file
        return workflow_file

    except ValueError as err:
        print('Data access exception in find workflow file')
        print(err.args)

    finally:
        cursor.close()
