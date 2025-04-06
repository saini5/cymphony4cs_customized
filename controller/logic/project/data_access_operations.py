from django.db import connection
from django.utils import timezone

import controller.logic.project.components as project_components
from controller.logic.common_data_access_operations import dict_fetchall, dict_fetchone


def create_project(obj_project: project_components.Project):
    """Insert the incoming project into db"""

    cursor = connection.cursor()
    try:
        # insert project entry into all_projects table
        table_all_projects = "all_projects"
        cursor.execute(
            "INSERT into " +
            table_all_projects +
            " (u_id, p_name, p_desc, date_creation) VALUES (%s, %s, %s, %s)" +
            " RETURNING p_id;",
            [obj_project.user_id,
             obj_project.name,
             obj_project.description,
             timezone.now()  # store this time of creation
             ]
        )
        project_id = cursor.fetchone()[0]
        return project_id

    except ValueError as err:
        print('Data access exception in create project')
        print(err.args)

    finally:
        cursor.close()


def find_all_projects(user_id: int):
    """Return all projects for this user from the db"""

    cursor = connection.cursor()
    list_all_projects = []
    try:
        # get all projects for this user from the all_projects table
        table_all_projects = "all_projects"
        cursor.execute(
            "SELECT u_id, p_id, p_name, p_desc, date_creation FROM " +
            table_all_projects +
            " WHERE u_id = %s",
            [user_id]
        )
        projects = dict_fetchall(cursor)
        for row in projects:
            obj_project = project_components.Project(
                user_id=row['u_id'],
                project_name=row['p_name'],
                project_description=row['p_desc'],
                date_creation=row['date_creation'],
                project_id=row['p_id']
            )
            list_all_projects.append(obj_project)
        return list_all_projects

    except ValueError as err:
        print('Data access exception in find all projects')
        print(err.args)

    finally:
        cursor.close()


def find_project(project_id: int, user_id: int):
    """Return the specified project for this user from the db"""

    cursor = connection.cursor()
    project = None
    try:
        # get the specific project for this user from the all_projects table
        table_all_projects = "all_projects"
        cursor.execute(
            "SELECT u_id, p_id, p_name, p_desc, date_creation FROM " +
            table_all_projects +
            " WHERE u_id = %s AND p_id = %s",
            [user_id, project_id]
        )
        project_row = dict_fetchone(cursor)

        obj_project = project_components.Project(
            user_id=project_row['u_id'],
            project_name=project_row['p_name'],
            project_description=project_row['p_desc'],
            date_creation=project_row['date_creation'],
            project_id=project_row['p_id']
        )

        project = obj_project
        return project

    except ValueError as err:
        print('Data access exception in find project')
        print(err.args)

    finally:
        cursor.close()


def delete_project(project_id: int, user_id: int):
    """Delete the specified project for this user from the db"""

    cursor = connection.cursor()
    try:
        # get the specific project for this user from the all_projects table
        table_all_projects = "all_projects"
        cursor.execute(
            "DELETE FROM " +
            table_all_projects +
            " WHERE u_id = %s AND p_id = %s",
            [user_id, project_id]
        )

        return

    except ValueError as err:
        print('Data access exception in delete project')
        print(err.args)

    finally:
        cursor.close()


def edit_project(obj_project: project_components.Project):
    """Edit the specified project of this user in the db"""

    cursor = connection.cursor()
    try:
        # insert project entry into all_projects table
        table_all_projects = "all_projects"
        cursor.execute(
            "UPDATE " +
            table_all_projects +
            " SET " +
            "p_name = %s, p_desc = %s" +
            " WHERE u_id = %s AND p_id = %s",
            [obj_project.name, obj_project.description, obj_project.user_id, obj_project.id]
        )
        return

    except ValueError as err:
        print('Data access exception in create project')
        print(err.args)

    finally:
        cursor.close()
