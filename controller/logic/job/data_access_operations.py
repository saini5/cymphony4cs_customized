from django.db import connection, transaction
from django.utils import timezone
from django.conf import settings

import controller.logic.job.components as job_components
from controller.logic.common_data_access_operations import dict_fetchall, dict_fetchone, is_steward
from controller.logic.common_logic_operations import get_job_prefix_table_name

from pathlib import Path
from datetime import datetime, timedelta
import pytz, psycopg2, time, csv, random

import time

from controller.enums import UserType


def find_all_jobs(job_name: str, job_type: str, job_status: str):
    """Return all jobs from the db with the name, type, status"""

    cursor = connection.cursor()
    list_all_jobs = []
    try:
        # print(job_name, job_type, job_status)
        # get all projects for this user from the all_projects table
        table_all_jobs = "all_jobs"
        cursor.execute(
            "SELECT u_id, p_id, w_id, r_id, j_id, j_name, j_type, j_status, date_creation FROM " +
            table_all_jobs +
            " WHERE j_name = %s AND j_type = %s AND j_status = %s",
            [job_name, job_type, job_status]
        )
        jobs = dict_fetchall(cursor)
        for row in jobs:
            obj_job = job_components.Job(
                user_id=row['u_id'],
                project_id=row['p_id'],
                workflow_id=row['w_id'],
                run_id=row['r_id'],
                job_id=row['j_id'],
                job_name=row['j_name'],
                job_type=row['j_type'],
                job_status=row['j_status'],
                date_creation=row['date_creation']
            )
            # print(obj_job)
            list_all_jobs.append(obj_job)
        return list_all_jobs

    except ValueError as err:
        print('Data access exception in find all jobs')
        print(err.args)

    finally:
        cursor.close()


def find_all_jobs_under_run(job_type: str, run_id: int, workflow_id: int, project_id: int, user_id: int):
    """Return all jobs under a run"""

    cursor = connection.cursor()
    list_all_jobs = []
    try:
        # print(run_id, job_type)
        # get all jobs for this user from the all_jobs table
        table_all_jobs = "all_jobs"
        cursor.execute(
            "SELECT u_id, p_id, w_id, r_id, j_id, j_name, j_type, j_status, date_creation FROM " +
            table_all_jobs +
            " WHERE u_id = %s AND p_id = %s AND w_id = %s AND r_id = %s AND j_type = %s",
            [user_id, project_id, workflow_id, run_id, job_type]
        )
        jobs = dict_fetchall(cursor)
        for row in jobs:
            obj_job = job_components.Job(
                user_id=row['u_id'],
                project_id=row['p_id'],
                workflow_id=row['w_id'],
                run_id=row['r_id'],
                job_id=row['j_id'],
                job_name=row['j_name'],
                job_type=row['j_type'],
                job_status=row['j_status'],
                date_creation=row['date_creation']
            )
            list_all_jobs.append(obj_job)
        return list_all_jobs

    except ValueError as err:
        print('Data access exception in find all jobs')
        print(err.args)

    finally:
        cursor.close()


def find_job(job_id: int, run_id: int, workflow_id: int, project_id: int, user_id: int):
    """Return the specified job from the db"""

    cursor = connection.cursor()
    job = None
    try:
        # get the specific workflow for this user project from the all_workflows table
        table_all_jobs = "all_jobs"
        cursor.execute(
            "SELECT u_id, p_id, w_id, r_id, j_id, j_name, j_type, j_status, date_creation FROM " +
            table_all_jobs +
            " WHERE u_id = %s AND p_id = %s AND w_id = %s AND r_id = %s AND j_id = %s",
            [user_id, project_id, workflow_id, run_id, job_id]
        )
        job_row = dict_fetchone(cursor)

        obj_job = job_components.Job(
            user_id=job_row['u_id'],
            project_id=job_row['p_id'],
            workflow_id=job_row['w_id'],
            run_id=job_row['r_id'],
            job_id=job_row['j_id'],
            job_name=job_row['j_name'],
            job_type=job_row['j_type'],
            job_status=job_row['j_status'],
            date_creation=job_row['date_creation']
        )

        job = obj_job
        return job

    except ValueError as err:
        print('Data access exception in find job')
        print(err.args)

    finally:
        cursor.close()


def create_job(obj_job: job_components.Job):
    """Insert the incoming job into db"""
    cursor = connection.cursor()
    try:
        # insert job entry into all_jobs table
        table_all_jobs = "all_jobs"
        cursor.execute(
            "INSERT into " +
            table_all_jobs +
            " (r_id, w_id, p_id, u_id, j_name, j_type, j_status, date_creation) " +
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)" +
            " RETURNING j_id;",
            [
                obj_job.run_id,
                obj_job.workflow_id,
                obj_job.project_id,
                obj_job.user_id,
                obj_job.name,
                obj_job.type,
                obj_job.status,
                timezone.now()  # store this time of creation
            ]
        )
        job_id = cursor.fetchone()[0]

        return job_id

    except ValueError as err:
        print('Data access exception in create job')
        print(err.args)

    finally:
        cursor.close()


def edit_job(obj_job: job_components.Job):
    """Edit the specified job in the db"""

    cursor = connection.cursor()
    try:
        # update job entry into all_jobs table
        table_all_jobs = "all_jobs"
        cursor.execute(
            "UPDATE " +
            table_all_jobs +
            " SET " +
            "j_name = %s, j_type = %s, j_status = %s" +
            " WHERE u_id = %s AND p_id = %s AND w_id = %s AND r_id = %s AND j_id = %s",
            [
                obj_job.name,
                obj_job.type,
                obj_job.status,
                obj_job.user_id,
                obj_job.project_id,
                obj_job.workflow_id,
                obj_job.run_id,
                obj_job.id,
            ]
        )
        return

    except ValueError as err:
        print('Data access exception in edit job')
        print(err.args)

    finally:
        cursor.close()


def create_table_from_file(source_file_path: Path, target_table_name: str):
    """Create a table from a csv file"""
    # Remarks:
    # Since open() is used to open a CSV file for reading,
    # the file will by default be decoded into unicode using the system default encoding.
    # To decode a file using a different encoding, use the encoding argument of open:
    # import csv
    # with open('some.csv', newline='', encoding='utf-8') as f:
    #   reader = csv.reader(f)`
    cursor = connection.cursor()
    try:
        if source_file_path.is_file():
            # 1. create target table: open the csv, read the headers, pass to callproc alongwith _id header
            with source_file_path.open() as data_file:
                csv_reader = csv.reader(data_file)
                headers = next(csv_reader)
            main_header = "_id"
            column_list = ""
            for header in headers:
                column_list = column_list + ", " + header.strip() + " " + "text"
            query_string = "CREATE TABLE " + \
                           target_table_name + "(" + \
                           main_header + " integer"+ \
                           column_list + \
                           ", date_creation TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP" + \
                           ")"
            # print(query_string)
            cursor.execute(query_string, [])

            # 2. fill target table: open the csv, read the data, insert into the table (TODO: bulk insert)
            column_list = ""
            placeholder_list = ""
            for header in headers:
                column_list = column_list + ", " + header.strip()
                placeholder_list = placeholder_list + ", " + "%s"
            query_string = "INSERT INTO " + \
                           target_table_name + \
                           "(" + main_header + column_list + ")" + \
                           " VALUES (" + "%s" + placeholder_list + ")"
            with source_file_path.open() as data_file:
                csv_reader = csv.reader(data_file)
                headers = next(csv_reader)  # to get header row out of the way
                id_value = 0
                for row in csv_reader:     # traverse non-header rows
                    id_value = id_value + 1
                    value_list = []
                    value_list.append(id_value)
                    value_list.extend(row)
                    # print('size =' + str(len(value_list)) + ': ', value_list)
                    cursor.execute(query_string, value_list)
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in create table from file')
    finally:
        cursor.close()


def do_bookkeeping_3a_kn(
        data_table_name: str,
        instructions: dict,
        layout: dict,
        configuration: dict,
        obj_job: job_components.Job,
        annotations_per_tuple_per_worker_table_name: str,
        aggregated_annotations_table_name: str
):
    """Do some bookkeeping operations for 3a_kn job"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)

        # 1. create and populate the parsed instructions, parsed layout, and config in j_instructions, j_layout, and j_config table
        table_instructions = job_prefix_table_name + "worker_instructions"
        cursor.callproc('create_table_instructions', [table_instructions])
        for key in instructions.keys():
            value = instructions.get(key)
            cursor.execute(
                "INSERT into " + table_instructions +
                " (type, content) VALUES (%s, %s)",
                [key, value]
            )
        table_layout = job_prefix_table_name + "layout"
        cursor.callproc('create_table_layout', [table_layout])
        for key in layout.keys():
            value = layout.get(key)
            cursor.execute(
                "INSERT into " + table_layout +
                " (type, content) VALUES (%s, %s)",
                [key, value]
            )
        table_configuration = job_prefix_table_name + "config_parameters"
        cursor.callproc('create_table_configuration', [table_configuration])
        for key in configuration.keys():
            value = configuration.get(key)
            cursor.execute(
                "INSERT into " + table_configuration +
                " (key, value, value_data_type) VALUES (%s, %s, %s)",
                [key, value, str(type(value))]
            )

        # 2. create empty j_tables
        # create j_tasks table
        table_tasks = job_prefix_table_name + "tasks"
        cursor.callproc('create_table_tasks', [table_tasks])
        # add defaults before insert
        cursor.execute("ALTER TABLE ONLY " + table_tasks + " ALTER COLUMN total_assigned SET DEFAULT 0", [])
        cursor.execute("ALTER TABLE ONLY " + table_tasks + " ALTER COLUMN abandoned SET DEFAULT 0", [])
        cursor.execute("ALTER TABLE ONLY " + table_tasks + " ALTER COLUMN pending_annotations SET DEFAULT 0", [])
        cursor.execute("ALTER TABLE ONLY " + table_tasks + " ALTER COLUMN done SET DEFAULT False", [])
        # populate j_tasks table
        cursor.execute(
            "INSERT INTO " + table_tasks + "(_id) SELECT _id FROM " + data_table_name,
            []
        )
        # cursor.execute("SELECT _id FROM " + data_table_name, [])
        # list_ids = cursor.fetchall()
        # print('data table ids fetched.')
        #
        # for id in list_ids:
        #     cursor.execute(
        #         "INSERT into " + table_tasks +
        #         " (_id, total_assigned, abandoned, pending_annotations, done) VALUES (%s, %s, %s, %s, %s)",
        #         [id, 0, 0, 0, False]
        #     )
        cursor.execute("CREATE INDEX " + table_tasks + "_id_done ON " + table_tasks + " (_id, done)")
        # TODO: date_creation also getting copied here. replace with updated timestamp of copying
        table_tuples = job_prefix_table_name + "tuples"
        cursor.execute(
            "CREATE TABLE " +
            table_tuples +
            " AS TABLE " +
            data_table_name,
            []
        )
        cursor.execute(
            "ALTER TABLE " +
            table_tuples +
            " ADD PRIMARY KEY (_id)",
            []
        )
        # create p1_w1_task_assignments table
        table_assignments = job_prefix_table_name + "assignments"
        cursor.callproc('create_table_assignments', [table_assignments])
        cursor.execute("CREATE INDEX " + table_assignments + "_worker_id ON " + table_assignments + " (worker_id)")

        # 2. create output B and C tables at the run level
        # create p1_w1_task_outputs table
        # table_outputs = annotations_per_tuple_per_worker_table_name
        table_outputs = job_prefix_table_name + "outputs"
        cursor.callproc('create_table_outputs', [table_outputs])
        cursor.execute("CREATE INDEX " + table_outputs + "_worker_id ON " + table_outputs + " (worker_id)")
        # create p1_w1_final_labels table
        # table_final_labels = aggregated_annotations_table_name
        table_final_labels = job_prefix_table_name + "final_labels"
        cursor.callproc('create_table_final_labels', [table_final_labels])
        return
    except ValueError as err:
        print('Data access exception in bookkeeping 3a_kn job')
        print(err.args)
    finally:
        cursor.close()


def create_file_from_table(source_table_name: str, destination_file_path: Path):
    """Create csv file from table"""
    # cursor = connection.cursor()
    db_params = settings.DATABASES['default']
    con = psycopg2.connect(
        database=db_params['NAME'],
        user=db_params['USER'],
        password=db_params['PASSWORD'],
        host=db_params['HOST'],
        port=db_params['PORT']
    )
    cursor = con.cursor()
    try:
        # query = "COPY " + source_table_name + " TO " + "'" + str(destination_file_path) + "'" + " WITH CSV HEADER"
        # cursor.execute(query, [])
        custom_query = "COPY " + source_table_name + " TO STDOUT WITH CSV HEADER"
        with destination_file_path.open(mode='w') as f:
            cursor.copy_expert(sql=custom_query, file=f)
            con.commit()
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in create file from table')
    finally:
        cursor.close()
        con.close()


def get_instructions(requester_id: int, project_id: int, workflow_id: int, run_id: int, job_id: int):
    """Retrieve instructions"""
    cursor = connection.cursor()
    instructions = dict()
    try:
        obj_job: job_components.Job = find_job(
            job_id=job_id,
            run_id=run_id,
            workflow_id=workflow_id,
            project_id=project_id,
            user_id=requester_id
        )
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_instructions = job_prefix_table_name + "worker_instructions"
        cursor.execute(
            "SELECT type, content, date_creation FROM " +
            table_instructions,
            []
        )
        rows = dict_fetchall(cursor)
        for row in rows:
            instructions[row['type']] = row['content']
        return instructions
    except ValueError as err:
        print('Data access exception in get instructions')
        print(err.args)
    finally:
        cursor.close()


def get_configuration(requester_id: int, project_id: int, workflow_id: int, run_id: int, job_id: int):
    """Retrieve configuration"""
    cursor = connection.cursor()
    configuration = dict()
    try:
        obj_job: job_components.Job = find_job(
            job_id=job_id,
            run_id=run_id,
            workflow_id=workflow_id,
            project_id=project_id,
            user_id=requester_id
        )
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_configuration = job_prefix_table_name + "config_parameters"
        cursor.execute(
            "SELECT key, value, value_data_type, date_creation FROM " +
            table_configuration,
            []
        )
        rows = dict_fetchall(cursor)
        for row in rows:
            configuration[row['key']] = row['value']
        return configuration
    except ValueError as err:
        print('Data access exception in get configuration')
        print(err.args)
    finally:
        cursor.close()


def select_candidate(candidate_tasks, k, n):
    """Select a candidate task for assignment, based on below prioritization logic"""
    # find t* out of tf
    # # 1. in progress == n i.e. {t from candidates | t.done = no, t.total_assigned - t.abandoned == n}
    # set_n_in_progress = set()
    # for task in candidate_tasks:
    #     if task[1] - task[2] == n:  # task[1] is t.total_assigned, task[2] is t.abandoned
    #         set_n_in_progress.add(task)
    # for task in set_n_in_progress:
    #     print("Should never have reached here, since submit + aggregate would have already labeled undecided")
    #     candidate_tasks.discard(task)
    #
    # # 2. in_progress >= k i.e. { t from candidates | done = no, t.total_assigned - t.abandoned >= k}
    # set_k_or_more_in_progress = set()
    # for task in candidate_tasks:
    #     if task[1] - task[2] >= k:  # task[1] is t.total_assigned, task[2] is t.abandoned
    #         set_k_or_more_in_progress.add(task)
    # for task in set_k_or_more_in_progress:
    #     if task[3] > 0:  # if t.pending_annotations > 0:
    #         print('Should never have reached here, since done will be already yes')
    #         candidate_tasks.discard(task)  # remove t from candidates
    #     else:  # t.pending_annotations == 0
    #         # try to assign t to w
    #         # print('k or more')
    #         candidate_task = task
    #         return candidate_task
    #
    # # 3. in_progress > 0 (& < k) i.e. { t from candidates | t.done = no, t.total_assigned - t.abandoned > 0}
    # set_more_than_0_in_progress = set()
    # for task in candidate_tasks:
    #     if task[1] - task[2] > 0:  # task[1] is t.total_assigned, task[2] is t.abandoned
    #         set_more_than_0_in_progress.add(task)
    # for task in set_more_than_0_in_progress:
    #     # try to assign t to w
    #     # print('greater than 0')
    #     candidate_task = task
    #     return candidate_task
    #
    # # 4. 0_in_progress = { t from candidates | t.done = no, t.total_assigned - t.abandoned == 0}
    # set_0_in_progress = set()
    # for task in candidate_tasks:
    #     if task[1] - task[2] == 0:  # task[1] is t.total_assigned, task[2] is t.abandoned
    #         set_0_in_progress.add(task)
    # for task in set_0_in_progress:
    #     # try to assign t to w
    #     # print('zero in progress')
    #     candidate_task = task
    #     return candidate_task
    #
    # print("Should never reach here, since all cases are covered above where done = no")
    # # for safety, try to assign first candidate task
    return candidate_tasks[0]


def assign_3a_kn(worker_id: int, obj_job: job_components.Job, job_k: int, job_n: int, task_annotation_time_limit: int):
    """Assign task to worker"""
    cursor = connection.cursor()
    try:
        with transaction.atomic():
            # This code executes inside a transaction.
            task = None
            # while True:
            #
            #     # print('worker ', worker_id, ' attempting to get candidate tasks')
            #     candidate_tasks_for_this_worker = get_candidate_tasks_of_job_for_worker(cursor=cursor, worker_id=worker_id, obj_job=obj_job)
            #
            #     if not candidate_tasks_for_this_worker:
            #         # print('worker ', worker_id, ' did not get any candidate tasks despite some label aggregations remaining')
            #         print('return No available tasks for you')
            #         return 0
            #
            #     # print('worker ', worker_id, ' selecting candidate task out of candidate tasks based on some criteria')
            #     # select task out of this
            #     candidate_task = select_candidate(candidate_tasks_for_this_worker, job_k, job_n)
            #
            #     # lock task and see if it is still done
            #     # print('worker ', worker_id, ' locking candidate task ', candidate_task[0], 'to see if it still done')
            #     task = lock_task_of_job_for_worker(cursor=cursor, task_id=candidate_task[0], obj_job=obj_job, worker_id=worker_id)
            #
            #     # null will be returned from above if the task is not acceptable (i.e. it is already done)
            #     if not task:
            #         # print('worker ', worker_id,
            #         #       ' had to drop candidate task', candidate_task[0], ' because it was already done')
            #         time.sleep(random.uniform(0, 0.25))  # to prevent too much looping
            #         continue
            #     else:
            #         # this row has been locked, will be released automatically when t is updated
            #         # print('worker ', worker_id,
            #         #       ' locked the candidate task', candidate_task[0], ' as the assigned task in tasks table')
            #         task = task
            #         break
            task = get_and_lock_task_of_job_for_worker(cursor=cursor, worker_id=worker_id, obj_job=obj_job)
            if not task:
                # print('worker ', worker_id, ' did not get any candidate tasks despite some label aggregations remaining')
                # either all tasks were done, or all tasks had been annotated by this worker or a combination of the previous two
                # or some tasks had been locked by other workers (for assign or submit annotation) so this worker skipped over them
                print('return No available tasks for you')
                return 0

            task_id = task[0]

            # assign t to w
            # print('worker ', worker_id, ' getting assignment of ', task_id, ' in assignments table')
            add_assignment_for_task_in_assign(cursor=cursor, task_id=task_id, obj_job=obj_job, worker_id=worker_id, annotation_time_limit=task_annotation_time_limit)

            #  t.total_assigned++, t.pending_annotation+
            task_total_assigned = task[1] + 1
            task_pending_annotations = task[3] + 1
            task_abandoned = task[2]
            task_done = task[4]
            if task_total_assigned - task_abandoned >= job_k:
                task_done = True

            # this will unlock the task as well
            # print('worker ', worker_id,
            #       ' updating task stats', task_id, ' in tasks table and hence, releasing lock on task')
            update_tasks_for_task_in_assign(cursor=cursor, obj_job=obj_job, task_id=task_id, task_total_assigned=task_total_assigned, task_pending_annotations=task_pending_annotations, task_done=task_done)

            return task_id

    except ValueError as err:
        print('Data access exception in assign_3a_kn')
        print(err.args)
        raise

    finally:
        cursor.close()
        # break


def assign_3a_lm(worker_id: int, obj_job: job_components.Job, job_l: int, job_m: int, task_annotation_time_limit: int):
    """Assign task to worker"""
    cursor = connection.cursor()
    try:
        with transaction.atomic():
            task = None
            task = get_and_lock_task_of_job_for_worker(cursor=cursor, worker_id=worker_id, obj_job=obj_job)
            if not task:
                # worker did not get any candidate tasks despite some label aggregations remaining'
                # either all tasks were done, or all tasks had been annotated by this worker or a combination of the previous two
                # or some tasks had been locked by other workers (for assign or submit annotation) so this worker skipped over them
                print('return No available tasks for you')
                return 0

            # For the steward, I am only concerned about the task id and the done status in the task table
            task_id = task[0]
            task_done = task[4]

            # assign t to w
            # print('worker ', worker_id, ' getting assignment of ', task_id, ' in assignments table')
            add_assignment_for_task_in_assign(cursor=cursor, task_id=task_id, obj_job=obj_job, worker_id=worker_id, annotation_time_limit=task_annotation_time_limit)

            #  Since it is a steward, we don't change these values since they are relevant for regular workers.
            task_total_assigned = task[1]
            task_pending_annotations = task[3] 
            task_abandoned = task[2]
            
            # Instead of having and comparing fields like total_assigned and abandoned for steward workers, we check based on the number of active assignments by stewards in assignments table.
            task_active_assignments_by_stewards = get_active_assignments_by_stewards(cursor=cursor, task_id=task_id, obj_job=obj_job)
            # assignments in progress for this task, by stewards
            if task_active_assignments_by_stewards >= job_l:
                task_done = True

            # this will unlock the task as well
            # print('worker ', worker_id,
            #       ' updating task stats', task_id, ' in tasks table and hence, releasing lock on task')
            update_tasks_for_task_in_assign(cursor=cursor, obj_job=obj_job, task_id=task_id, task_total_assigned=task_total_assigned, task_pending_annotations=task_pending_annotations, task_done=task_done)

            return task_id

    except ValueError as err:
        print('Data access exception in assign_3a_knm')
        print(err.args)
        raise

    finally:
        cursor.close()
        # break


def get_and_lock_task_of_job_for_worker(
        cursor,
        worker_id: int,
        obj_job: job_components.Job
):
    """Get task and lock it"""
    job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)

    # create j_tasks table
    table_tasks = job_prefix_table_name + "tasks"
    table_outputs = job_prefix_table_name + "outputs"

    # print('worker ', worker_id, ' querying tasks not annotated by him which are not done yet')
    cursor.execute(
        "SELECT _id, total_assigned, abandoned, pending_annotations, done, date_creation FROM " +
        # "SELECT * FROM " +
        table_tasks +
        " WHERE done = %s AND _id NOT IN " +
        "(SELECT _id FROM " +
        table_outputs +
        # " WHERE worker_id = %s) LIMIT 1 FOR UPDATE OF TABLE " + table_tasks,    # fastest possible serial execution of workers picking up tasks
        " WHERE worker_id = %s) LIMIT 1 FOR UPDATE OF " + table_tasks + " SKIP LOCKED",    # truly parallel execution of workers picking up tasks parallely
        [False, worker_id]
    )
    task = cursor.fetchone()

    return task


def get_candidate_tasks_of_job_for_worker(
        cursor,
        worker_id: int,
        obj_job: job_components.Job     # ,
        # tasks_already_annotated_by_worker: list
):
    """Get candidate tasks"""
    job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)

    # create j_tasks table
    table_tasks = job_prefix_table_name + "tasks"
    table_outputs = job_prefix_table_name + "outputs"

    # tasks_already_annotated_by_worker_string: str = ""
    # if len(tasks_already_annotated_by_worker) > 0:
    #     for x in tasks_already_annotated_by_worker:
    #         tasks_already_annotated_by_worker_string = tasks_already_annotated_by_worker_string + str(x) + ", "
    #     tasks_already_annotated_by_worker_string = tasks_already_annotated_by_worker_string[:-2]
    # else:
    #     # since the NOT IN () brackets cannot be left empty by syntax, put in an impossible value here
    #     # -1 is impossible for _id column
    #     tasks_already_annotated_by_worker_string = "-1"
    # # print(tasks_already_annotated_by_worker_string)

    # print('worker ', worker_id, ' querying tasks not annotated by him which are not done yet')
    cursor.execute(
        "SELECT _id, total_assigned, abandoned, pending_annotations, done, date_creation FROM " +
        table_tasks +
        " WHERE done = %s AND _id NOT IN " +
        "(SELECT _id FROM " +
        table_outputs +
        " WHERE worker_id = %s) LIMIT 1",   # FOR SHARE SKIP LOCKED",
        [False, worker_id]
        #
        # " WHERE done = %s AND _id NOT IN " +
        # "(" +
        # tasks_already_annotated_by_worker_string +
        # ") ",
        # [False]
    )
    candidate_tasks_for_this_worker = cursor.fetchall()
    # print(candidate_tasks_for_this_worker)

    return candidate_tasks_for_this_worker


def lock_task_of_job_for_worker(cursor, task_id: int, obj_job: job_components.Job, worker_id: int):
    """Lock task"""
    job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
    # create j_tasks table
    table_tasks = job_prefix_table_name + "tasks"
    cursor.execute(
        "SELECT _id, total_assigned, abandoned, pending_annotations, done, date_creation FROM " +
        table_tasks +
        " WHERE _id = %s AND done = %s " +
        "FOR UPDATE" +
        " SKIP LOCKED",
        [task_id, False]
    )
    task = cursor.fetchone()
    return task


def add_assignment_for_task_in_assign(
        cursor,
        task_id: int,
        obj_job: job_components.Job,
        worker_id: int,
        annotation_time_limit: int
):
    """Store the worker-task assignment in db"""
    job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
    table_assignments = job_prefix_table_name + "assignments"
    # add entry to table_assignments
    cursor.execute(
        "INSERT into " + table_assignments +
        " (_id, worker_id, timeout_threshold_at, status) VALUES (%s, %s, %s, %s)",
        [
            task_id,
            worker_id,
            datetime.utcnow().replace(tzinfo=pytz.UTC) + timedelta(seconds=annotation_time_limit),
            settings.ASSIGNMENT_STATUS[0]   # 'PENDING_ANNOTATION'
        ]
    )
    return


def get_qualifying_annotation_for_task(cursor, task_id: int, obj_job: job_components.Job, worker_type: UserType, threshold: int):
    """Get qualifying annotation for a task filtered by worker type (steward or regular)"""
    job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
    table_outputs = job_prefix_table_name + "outputs"
    
    worker_type_name = worker_type.value
    
    cursor.execute(f"""
        SELECT VOTES_PER_ANNOTATION.annotation 
        FROM ( 
            SELECT TASK_ANNOTATIONS.annotation, count(*) AS n_votes 
            FROM ( 
                SELECT O._id, O.worker_id, O.annotation 
                FROM {table_outputs} O
                INNER JOIN auth_user_groups AUG ON O.worker_id = AUG.user_id
                INNER JOIN auth_group AG ON AUG.group_id = AG.id
                WHERE O._id = %s 
                AND AG.name = %s
            ) AS TASK_ANNOTATIONS 
            GROUP BY TASK_ANNOTATIONS.annotation 
        ) AS VOTES_PER_ANNOTATION 
        WHERE VOTES_PER_ANNOTATION.n_votes >= %s
        """,
        [task_id, worker_type_name, threshold]
    )
    return cursor.fetchone()

def get_total_annotations_for_task(cursor, task_id: int, obj_job: job_components.Job, worker_type: UserType):
    """Get total annotations for a task filtered by worker type (steward or regular)"""
    job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
    table_outputs = job_prefix_table_name + "outputs"
    
    worker_type_name = worker_type.value
    
    cursor.execute(f"""
        SELECT COUNT(*) AS total_annotations
        FROM {table_outputs} O
        INNER JOIN auth_user_groups AUG ON O.worker_id = AUG.user_id
        INNER JOIN auth_group AG ON AUG.group_id = AG.id
        WHERE O._id = %s 
        AND AG.name = %s
        """,
        [task_id, worker_type_name]
    )
    return cursor.fetchone()

def get_active_assignments_by_stewards(cursor, task_id: int, obj_job: job_components.Job):
    """Get active assignments (pending or completed) for this task, by stewards"""
    
    job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
    table_assignments = job_prefix_table_name + "assignments"
    
    steward_group_name = UserType.STEWARD.value
    
    cursor.execute(f"""
        SELECT COUNT(*) AS active_assignments
        FROM {table_assignments} A
        INNER JOIN auth_user_groups AUG ON A.worker_id = AUG.user_id
        INNER JOIN auth_group AG ON AUG.group_id = AG.id
        WHERE A._id = %s 
        AND AG.name = %s
        AND (A.status = %s OR A.status = %s)
        """,
        [task_id, steward_group_name, settings.ASSIGNMENT_STATUS[0], settings.ASSIGNMENT_STATUS[1]]
    )
    active_assignments_by_stewards = cursor.fetchone()[0]
    return active_assignments_by_stewards


def update_tasks_for_task_in_assign(cursor, obj_job: job_components.Job, task_id: int, task_total_assigned: int, task_pending_annotations:int, task_done: bool):
    """Update tasks table"""
    job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
    table_tasks = job_prefix_table_name + "tasks"
    # update table_tasks with these new values
    cursor.execute(
        "UPDATE " + table_tasks +
        " SET total_assigned = %s , pending_annotations = %s, done = %s WHERE _id = %s",
        [task_total_assigned, task_pending_annotations, task_done, task_id]
    )
    return


def get_data_headers_for_job(obj_job: job_components.Job):
    """Get all headers (except _id and date_creation) for this 3a_kn job"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_tuples = job_prefix_table_name + "tuples"
        cursor.execute(
            "SELECT * FROM " +
            table_tuples +
            " LIMIT 0 ",
            []
        )
        columns = [col[0] for col in cursor.description]
        columns.remove('_id')
        if 'date_creation' in columns:
            columns.remove('date_creation')
        return columns
    except ValueError as err:
        print('Data access exception in get data headers for job')
        print(err.args)
    finally:
        cursor.close()


def get_data_row_for_job(obj_job: job_components.Job, tuple_id: int):
    """Get a particular data row for this 3a_kn job"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_tuples = job_prefix_table_name + "tuples"
        cursor.execute(
            "SELECT * FROM " +
            table_tuples +
            " WHERE _id = %s",
            [tuple_id]
        )
        tuple_row = dict_fetchone(cursor)
        return tuple_row
    except ValueError as err:
        print('Data access exception in get data row for job')
        print(err.args)
    finally:
        cursor.close()


def get_data_rows_for_job(obj_job: job_components.Job):
    """Get all rows of data for this 3a_kn job"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_tuples = job_prefix_table_name + "tuples"
        cursor.execute(
            "SELECT * FROM " +
            table_tuples,
            []
        )
        tuple_rows = dict_fetchall(cursor)
        return tuple_rows
    except ValueError as err:
        print('Data access exception in get data rows for job')
        print(err.args)
    finally:
        cursor.close()


def update_assignment_for_task_in_aggregate(obj_job: job_components.Job, task_id: int, worker_id: int):
    """Update assignments table"""
    cursor = connection.cursor()
    try:
        with transaction.atomic():
            job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
            table_assignments = job_prefix_table_name + "assignments"
            cursor.execute(
                "UPDATE " +
                table_assignments +
                " SET status = %s, completed_at = %s "
                "WHERE _id = %s AND worker_id = %s AND status = %s "
                "RETURNING status;",
                [
                    settings.ASSIGNMENT_STATUS[1],   # 'COMPLETED'
                    datetime.utcnow().replace(tzinfo=pytz.UTC),
                    task_id,
                    worker_id,
                    settings.ASSIGNMENT_STATUS[0]   # 'PENDING_ANNOTATION'
                ]
            )
            if cursor.rowcount == 0:
                # this means: must be already abandoned
                # it cannot have been completed or unassigned, since in those cases, this function call would not have been possible.
                # TODO: notify w that vote didn't' get recorded and we are assigning you something else
                return True
            elif cursor.rowcount > 1:
                # <_id, worker_id> had multiple rows in assignments table that had status "pending_annotation"
                raise ValueError(
                    'Illegal updates happened. <_id, worker_id> had multiple rows in assignments table that had status "pending_annotation"',
                    (task_id, worker_id)
                )
            elif cursor.rowcount == 1:
                # the update was successful on the one (task_id, worker_id, status) row that was pending
                pass
            return False
    except ValueError as err:
        print('Data access exception in update assignment for task in aggregate')
        print(err.args)
        raise
    finally:
        cursor.close()


def bookkeeping_and_aggregate_3a_kn(obj_job: job_components.Job, task_id: int, worker_id: int, job_k: int, job_n: int, answer: str):
    """Aggregate task's annotations"""
    cursor = connection.cursor()
    try:
        with transaction.atomic():

            job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
            table_tasks = job_prefix_table_name + "tasks"
            table_outputs = job_prefix_table_name + "outputs"

            # print('worker ', worker_id, ' fetching task', task_id, ' in lock from tasks table')
            cursor.execute(
                "SELECT _id, total_assigned, abandoned, pending_annotations, done FROM " +
                table_tasks +
                " WHERE _id = %s FOR UPDATE",
                [task_id]
            )
            task = cursor.fetchone()

            # print('worker ', worker_id, ' inserting to outputs within task ', task_id, ' lock')
            # Push annotation to p1_w1_task_outputs (O)
            cursor.execute(
                "INSERT into " +
                table_outputs +
                " (_id, annotation, worker_id) VALUES (%s, %s, %s)",
                [task_id, answer, worker_id]
            )

            # aggregate start
            task_id = task[0]
            task_done = task[4]

            flag_k_votes_agree = False  # if k votes agree out of n
            final_annotation = settings.DEFAULT_AGGREGATION_LABEL  # 'undecided'
            cursor.execute(
                "SELECT VOTES_PER_ANNOTATION.annotation " +
                "FROM ( " +
                "   SELECT TASK_ANNOTATIONS.annotation, count(*) AS n_votes " +
                "   FROM ( " +
                "       SELECT _id, worker_id, annotation " +
                "       FROM " + table_outputs + " " +
                "       WHERE _id = %s " +
                "   ) AS TASK_ANNOTATIONS " +
                "   GROUP BY TASK_ANNOTATIONS.annotation " +
                ") AS VOTES_PER_ANNOTATION " +
                "WHERE VOTES_PER_ANNOTATION.n_votes >= %s",
                [task_id, job_k]
            )
            final_annotation_row = cursor.fetchone()
            if not final_annotation_row:
                pass    # flag_k_votes_agree remains False
            else:
                flag_k_votes_agree = True
                final_annotation = final_annotation_row[0]

            cursor.execute(
                "SELECT count(*) AS n_task_annotations " +
                "FROM " + table_outputs + " " +
                "WHERE _id = %s ",
                [task_id]
            )
            n_task_annotations_row = cursor.fetchone()
            n_task_annotations = int(n_task_annotations_row[0])

            # main logic
            if n_task_annotations > job_n:
                print('ERROR: Should never have reached here')
            elif n_task_annotations == job_n:
                if flag_k_votes_agree:
                    aggregate(cursor, task_id, final_annotation, obj_job)
                else:
                    # final_annotation is still 'undecided'.
                    aggregate(cursor, task_id, final_annotation, obj_job)
            elif n_task_annotations >= job_k:  # votes >=k and < n
                if flag_k_votes_agree:
                    aggregate(cursor, task_id, final_annotation, obj_job)
                else:
                    # votes >= k but k don't agree out of them.
                    # so wait for votes to become n but signal to assign that it should open this task for assignment again
                    if task_done:
                        # print('task was done. changing it back.')
                        task_done = False
            # else: votes < k, so just wait for enough votes.

            # # pushed_to_final_labels = False
            #
            # # fetch votes of task_id in task_outputs
            # cursor.execute(
            #     "SELECT _id, annotation, worker_id FROM " +
            #     table_outputs +
            #     " WHERE _id = %s",
            #     [task_id]
            # )
            # task_annotations_rows = cursor.fetchall()
            #
            # # number of votes per vote type
            # task_annotations_dict = {}
            # task_annotations_list = []
            # for task_annotation_row in task_annotations_rows:
            #     annotation = task_annotation_row[1]
            #     task_annotations_list.append(annotation)
            #     if task_annotations_dict.get(annotation):
            #         val = task_annotations_dict.get(annotation)
            #         task_annotations_dict[annotation] = val + 1
            #     else:
            #         task_annotations_dict[annotation] = 1
            # flag_k_votes_agree = False  # if k votes agree out of n
            # final_annotation = settings.DEFAULT_AGGREGATION_LABEL   # 'undecided'
            # for key in task_annotations_dict.keys():
            #     if task_annotations_dict.get(key) >= job_k:
            #         flag_k_votes_agree = True
            #         final_annotation = key

            # # main logic
            # if len(task_annotations_list) > job_n:
            #     print('ERROR: Should never have reached here')
            # elif len(task_annotations_list) == job_n:
            #     if flag_k_votes_agree:
            #         aggregate(cursor, task_id, final_annotation, obj_job)
            #         # pushed_to_final_labels = True
            #     else:
            #         # final_annotation is still 'undecided'.
            #         aggregate(cursor, task_id, final_annotation, obj_job)
            #         # pushed_to_final_labels = True
            # elif len(task_annotations_list) >= job_k:  # votes >=k
            #     if flag_k_votes_agree:
            #         aggregate(cursor, task_id, final_annotation, obj_job)
            #         # pushed_to_final_labels = True
            #     else:
            #         # votes >= k but k don't agree out of them.
            #         # so wait for votes to become n but signal to assign that it should open this task for assignment again
            #         if task_done:
            #             # print('task was done. changing it back.')
            #             task_done = False
            # # else: votes < k, so just wait for enough votes.

            task_done = task_done

            # aggregate ends
            # print('worker ', worker_id, ' updating task ', task_id, ' in tasks table and releasing lock')
            cursor.execute(
                "UPDATE " +
                table_tasks +
                " SET pending_annotations = %s, done = %s  WHERE _id = %s",
                [task[3] - 1, task_done, task_id]
            )

            return

    except ValueError as err:
        print('Data access exception in bookkeeping and aggregate')
        print(err.args)
        raise

    finally:
        cursor.close()
        # break

def bookkeeping_and_aggregate_for_regular_workers(obj_job: job_components.Job, task_id: int, worker_id: int, job_k: int, job_n: int, answer: str):
    """Aggregate task's annotations for regular workers"""
    cursor = connection.cursor()
    try:
        with transaction.atomic():

            job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
            table_tasks = job_prefix_table_name + "tasks"
            table_outputs = job_prefix_table_name + "outputs"

            # print('worker ', worker_id, ' fetching task', task_id, ' in lock from tasks table')
            cursor.execute(
                "SELECT _id, total_assigned, abandoned, pending_annotations, done FROM " +
                table_tasks +
                " WHERE _id = %s FOR UPDATE",
                [task_id]
            )
            task = cursor.fetchone()

            # print('worker ', worker_id, ' inserting to outputs within task ', task_id, ' lock')
            # Push annotation to p1_w1_task_outputs (O)
            cursor.execute(
                "INSERT into " +
                table_outputs +
                " (_id, annotation, worker_id) VALUES (%s, %s, %s)",
                [task_id, answer, worker_id]
            )

            # aggregate start
            task_id = task[0]
            task_done = task[4]

            flag_k_votes_agree = False  # if k votes agree out of n
            final_annotation = settings.DEFAULT_AGGREGATION_LABEL  # 'undecided'
            final_annotation_row = get_qualifying_annotation_for_task(cursor, task_id, obj_job, UserType.REGULAR, job_k)
            if not final_annotation_row:
                pass    # flag_k_votes_agree remains False
            else:
                flag_k_votes_agree = True
                final_annotation = final_annotation_row[0]

            n_task_annotations_row = get_total_annotations_for_task(cursor, task_id, obj_job, UserType.REGULAR)
            n_task_annotations = int(n_task_annotations_row[0])

            # main logic
            if n_task_annotations > job_n:
                print('ERROR: Should never have reached here')
            elif n_task_annotations == job_n:
                if flag_k_votes_agree:
                    aggregate(cursor, task_id, final_annotation, obj_job)
                else:
                    # final_annotation is still 'undecided'.
                    aggregate(cursor, task_id, final_annotation, obj_job)
            elif n_task_annotations >= job_k:  # votes >=k and < n
                if flag_k_votes_agree:
                    aggregate(cursor, task_id, final_annotation, obj_job)
                else:
                    # votes >= k but k don't agree out of them.
                    # so wait for votes to become n but signal to assign that it should open this task for assignment again
                    if task_done:
                        # print('task was done. changing it back.')
                        task_done = False
            # else: votes < k, so just wait for enough votes.

            # aggregate ends
            # print('worker ', worker_id, ' updating task ', task_id, ' in tasks table and releasing lock')
            cursor.execute(
                "UPDATE " +
                table_tasks +
                " SET pending_annotations = %s, done = %s  WHERE _id = %s",
                [task[3] - 1, task_done, task_id]
            )

            return

    except ValueError as err:
        print('Data access exception in bookkeeping and aggregate')
        print(err.args)
        raise

    finally:
        cursor.close()
        # break

def bookkeeping_and_aggregate_for_steward_workers(obj_job: job_components.Job, task_id: int, worker_id: int, job_l: int, job_m: int, answer: str):
    """Aggregate task's annotations for steward workers"""
    cursor = connection.cursor()
    try:
        with transaction.atomic():

            job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
            table_tasks = job_prefix_table_name + "tasks"
            table_outputs = job_prefix_table_name + "outputs"

            # print('worker ', worker_id, ' fetching task', task_id, ' in lock from tasks table')
            # Steward case only cares about _id and done in the tasks table, other fields are solely for regular workers.
            cursor.execute(
                "SELECT _id, done FROM " +
                table_tasks +
                " WHERE _id = %s FOR UPDATE",
                [task_id]
            )
            task = cursor.fetchone()

            # print('worker ', worker_id, ' inserting to outputs within task ', task_id, ' lock')
            # Push annotation to p1_w1_task_outputs (O)
            cursor.execute(
                "INSERT into " +
                table_outputs +
                " (_id, annotation, worker_id) VALUES (%s, %s, %s)",
                [task_id, answer, worker_id]
            )

            # aggregate start
            task_id = task[0]
            task_done = task[1]

            flag_l_votes_agree = False  # if l votes agree out of m
            final_annotation = settings.DEFAULT_AGGREGATION_LABEL  # 'undecided'
            final_annotation_row = get_qualifying_annotation_for_task(cursor, task_id, obj_job, UserType.STEWARD, job_l)
            if not final_annotation_row:
                pass    # flag_l_votes_agree remains False
            else:
                flag_l_votes_agree = True
                final_annotation = final_annotation_row[0]

            n_task_annotations_row = get_total_annotations_for_task(cursor, task_id, obj_job, UserType.STEWARD)
            n_task_annotations = int(n_task_annotations_row[0])

            # main logic
            if n_task_annotations > job_m:
                print('ERROR: Should never have reached here')
            elif n_task_annotations == job_m:
                if flag_l_votes_agree:
                    aggregate(cursor, task_id, final_annotation, obj_job)
                else:
                    # final_annotation is still 'undecided'.
                    aggregate(cursor, task_id, final_annotation, obj_job)
            elif n_task_annotations >= job_l:  # votes >=l and < m
                if flag_l_votes_agree:
                    aggregate(cursor, task_id, final_annotation, obj_job)
                else:
                    # votes >= l but l don't agree out of them.
                    # so wait for votes to become m but signal to assign that it should open this task for assignment again
                    if task_done:
                        # print('task was done. changing it back.')
                        task_done = False
            # else: votes < l, so just wait for enough votes.

            # aggregate ends
            # print('worker ', worker_id, ' updating task ', task_id, ' in tasks table and releasing lock')
            cursor.execute(
                "UPDATE " +
                table_tasks +
                " SET done = %s  WHERE _id = %s",
                [task_done, task_id]
            )
            # we dont need to update pending_annotations because steward case only cares about id and done in the tasks table, other fields are solely for regular workers.

            return

    except ValueError as err:
        print('Data access exception in bookkeeping and aggregate')
        print(err.args)
        raise

    finally:
        cursor.close()
        # break

def aggregate(cursor, task_id, final_annotation, obj_job):
    """Store the aggregated label against the task in final_labels table"""
    job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
    # aggregate
    table_tasks = job_prefix_table_name + "tasks"
    table_final_labels = job_prefix_table_name + "final_labels"
    # push t, label to F (final_labels)
    # print('inserting to final labels within task ', task_id, ' lock')
    cursor.execute(
        "INSERT into " +
        table_final_labels +
        " (_id, label) VALUES (%s, %s)",
        [task_id, final_annotation]
    )
    return


def check_all_tasks_have_aggregated_labels(obj_job: job_components.Job, count_tasks: int):
    """Check if all tasks of the job have had their annotations aggregated"""
    cursor = connection.cursor()
    all_tasks_have_aggregated_labels = False
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_final_labels = job_prefix_table_name + "final_labels"
        count_tasks_in_job = int(count_tasks)
        cursor.execute(
            "SELECT count(*) FROM " + table_final_labels,
            []
        )
        count_final_labels_row = cursor.fetchone()
        count_final_labels_in_job = int(count_final_labels_row[0])
        # print('Number of rows in tasks: ', count_tasks_in_job, ' : ', type(count_tasks_in_job))
        # print('Number of rows in final labels: ', count_final_labels_in_job, ' : ', type(count_final_labels_in_job))
        if count_tasks_in_job == count_final_labels_in_job:
            all_tasks_have_aggregated_labels = True
        # print('All tasks have aggregated labels? = ', all_tasks_have_aggregated_labels)
        return all_tasks_have_aggregated_labels
    except ValueError as err:
        print('Data access exception in check if all tasks have aggregated labels')
        print(err.args)
    finally:
        cursor.close()


def skip_3a_kn(obj_job: job_components.Job, task_id: int, worker_id: int):
    """The worker (annotator) wants to quit annotating"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_assignments = job_prefix_table_name + "assignments"
        cursor.execute(
            "SELECT _id, worker_id, status FROM " +
            table_assignments +
            " WHERE _id = %s and worker_id = %s and status = %s FOR UPDATE",
            [
                task_id,
                worker_id,
                settings.ASSIGNMENT_STATUS[0]       # 'PENDING_ANNOTATION'
            ]
        )
        task_assignments = cursor.fetchall()
        if not task_assignments or not task_assignments[0]:
            # <_id, worker_id> not in pending status
            # cannot be completed or unassigned since otherwise, this function call would not have been possible
            # therefore, must be abandoned already
            pass
        elif len(task_assignments) > 1:
            # <_id, worker_id> had multiple rows in assignments table that had status "pending_annotation"
            raise ValueError(
                '<_id, worker_id> had multiple rows in assignments table that had status "pending_annotation"',
                (task_id, worker_id)
            )
        elif len(task_assignments) == 1:
            # <_id, w_id> is in pending status
            print('Voluntarily abandoning task ', task_id, ' for worker ', worker_id)
            abandon(cursor, obj_job, task_id, worker_id)

        return
    # try:
    #     with transaction.atomic():
    #
    #         job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
    #
    #         table_tasks = job_prefix_table_name + "tasks"
    #         table_assignments = job_prefix_table_name + "assignments"
    #
    #         cursor.execute(
    #             "SELECT _id, total_assigned, abandoned, pending_annotations, done FROM " +
    #             table_tasks +
    #             " WHERE _id = %s FOR UPDATE",
    #             [task_id]
    #         )
    #         task = cursor.fetchone()
    #
    #         # increment abandoned, decrement pending annotations, make task done to false if it was previously true
    #         task_abandoned = task[2] + 1
    #         task_pending_annotations = task[3] - 1
    #         task_done = task[4]
    #         if task_done:
    #             print('task was done. changing it back, because now we abandoned this task')
    #             task_done = False
    #
    #         # update assignments table
    #         cursor.execute(
    #             "UPDATE " +
    #             table_assignments +
    #             " SET status = %s, abandoned_at = %s WHERE _id = %s AND worker_id = %s AND status = %s",
    #             [
    #                 settings.ASSIGNMENT_STATUS[2],      # 'ABANDONED',
    #                 datetime.utcnow().replace(tzinfo=pytz.UTC),
    #                 task_id,
    #                 worker_id,
    #                 settings.ASSIGNMENT_STATUS[0]       # 'PENDING_ANNOTATION'
    #             ]
    #         )
    #
    #         # update the tasks table (and unlock it)
    #         cursor.execute(
    #             "UPDATE " +
    #             table_tasks +
    #             " SET abandoned = %s, pending_annotations = %s, done = %s  WHERE _id = %s",
    #             [task_abandoned, task_pending_annotations, task_done, task_id]
    #         )
    #
    #         return

    except ValueError as err:
        print('Data access exception in skip_3a_kn')
        print(err.args)
        raise

    finally:
        cursor.close()

def skip_3a_lm(obj_job: job_components.Job, task_id: int, worker_id: int):
    """The worker (annotator) wants to quit annotating"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_assignments = job_prefix_table_name + "assignments"
        cursor.execute(
            "SELECT _id, worker_id, status FROM " +
            table_assignments +
            " WHERE _id = %s and worker_id = %s and status = %s FOR UPDATE",
            [
                task_id,
                worker_id,
                settings.ASSIGNMENT_STATUS[0]       # 'PENDING_ANNOTATION'
            ]
        )
        task_assignments = cursor.fetchall()
        if not task_assignments or not task_assignments[0]:
            # <_id, worker_id> not in pending status
            # cannot be completed or unassigned since otherwise, this function call would not have been possible
            # therefore, must be abandoned already
            pass
        elif len(task_assignments) > 1:
            # <_id, worker_id> had multiple rows in assignments table that had status "pending_annotation"
            raise ValueError(
                '<_id, worker_id> had multiple rows in assignments table that had status "pending_annotation"',
                (task_id, worker_id)
            )
        elif len(task_assignments) == 1:
            # <_id, w_id> is in pending status
            print('Voluntarily abandoning task ', task_id, ' for worker ', worker_id)
            abandon_lm(cursor, obj_job, task_id, worker_id)

        return
    except ValueError as err:
        print('Data access exception in skip_3a_lm')
        print(err.args)
        raise

    finally:
        cursor.close()


def abandon_tasks_3a_kn(obj_job: job_components.Job):
    """Find tasks to abandon, and then abandon them"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_tasks = job_prefix_table_name + "tasks"
        table_assignments = job_prefix_table_name + "assignments"

        # TODO: atomic maybe not needed since I am already doing A before T

        # 1. select from p1_w1_A where status = pending and timeout_threshold_at < now() for update
        cursor.execute(
            "SELECT _id, worker_id FROM " +
            table_assignments +
            " WHERE status = %s AND timeout_threshold_at < %s " +
            "FOR UPDATE",
            [
                settings.ASSIGNMENT_STATUS[0],  # 'PENDING_ANNOTATION'
                datetime.utcnow().replace(tzinfo=pytz.UTC)
            ]
        )
        task_assignments = cursor.fetchall()

        # 2. for each entry in result set:
        for task_assignment in task_assignments:
            task_id = task_assignment[0]
            worker_id = task_assignment[1]
            print('Abandoning task ', task_id, ' for worker ', worker_id)
            abandon(cursor, obj_job, task_id, worker_id)

        return

    except ValueError as err:
        print('Data access exception in abandon_tasks')
        print(err.args)

    finally:
        cursor.close()

def abandon_tasks_3a_knlm(obj_job: job_components.Job):
    """Find tasks to abandon, and then abandon them, for 3a_knlm jobs"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_tasks = job_prefix_table_name + "tasks"
        table_assignments = job_prefix_table_name + "assignments"

        # TODO: atomic maybe not needed since I am already doing A before T

        # 1. select from p1_w1_A where status = pending and timeout_threshold_at < now() for update
        cursor.execute(f"""
            SELECT A._id, A.worker_id, AG.name as worker_type
            FROM {table_assignments} A
            INNER JOIN auth_user_groups AUG ON A.worker_id = AUG.user_id
            INNER JOIN auth_group AG ON AUG.group_id = AG.id
            WHERE A.status = %s AND A.timeout_threshold_at < %s 
            FOR UPDATE
            """,
            [
                settings.ASSIGNMENT_STATUS[0],  # 'PENDING_ANNOTATION'
                datetime.utcnow().replace(tzinfo=pytz.UTC)
            ]
        )
        task_assignments = cursor.fetchall()

        # 2. for each entry in result set:
        for task_assignment in task_assignments:
            task_id = task_assignment[0]
            worker_id = task_assignment[1]
            worker_type = task_assignment[2]
            print(f'Abandoning task {task_id} for worker {worker_id} (type: {worker_type})')
            
            # Use appropriate abandon function based on worker type
            if worker_type == UserType.STEWARD.value:
                abandon_lm(cursor, obj_job, task_id, worker_id)
            elif worker_type == UserType.REGULAR.value:
                abandon(cursor, obj_job, task_id, worker_id)

        return

    except ValueError as err:
        print('Data access exception in abandon_tasks_3a_knlm')
        print(err.args)

    finally:
        cursor.close()


def abandon(cursor, obj_job: job_components.Job, task_id: int, worker_id: int):
    # 1. create table_tasks variable, and table_task_assignments variable.
    job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
    table_tasks = job_prefix_table_name + "tasks"
    table_assignments = job_prefix_table_name + "assignments"

    # 2. select from p1_w1_A where task = task and worker = worker and job_id = job_id for update
    cursor.execute("UPDATE " +
                   table_assignments +
                   " SET status = %s, abandoned_at = %s WHERE _id = %s AND worker_id = %s AND status = %s",
                   [
                       settings.ASSIGNMENT_STATUS[2],      # 'ABANDONED',
                       datetime.utcnow().replace(tzinfo=pytz.UTC),
                       task_id,
                       worker_id,
                       settings.ASSIGNMENT_STATUS[0]       # 'PENDING_ANNOTATION'
                   ]
    )
    # 3. select from p1_w1_T where task = task and job = job for update
    cursor.execute(
        "SELECT _id, total_assigned, abandoned, pending_annotations, done FROM " +
        table_tasks +
        " WHERE _id = %s FOR UPDATE",
        [task_id]
    )
    task = cursor.fetchone()

    # increment abandoned, decrement pending annotations, make task done to false if it was previously true
    task_abandoned = task[2] + 1
    task_pending_annotations = task[3] - 1
    task_done = task[4]
    if task_done:
        print('task was done. changing it back, because now we abandoned this task')
        task_done = False

    # update the tasks table (and unlock it)
    cursor.execute(
        "UPDATE " +
        table_tasks +
        " SET abandoned = %s, pending_annotations = %s, done = %s  WHERE _id = %s",
        [task_abandoned, task_pending_annotations, task_done, task_id]
    )

    return

def abandon_lm(cursor, obj_job: job_components.Job, task_id: int, worker_id: int):
    # 1. create table_tasks variable, and table_task_assignments variable.
    job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
    table_tasks = job_prefix_table_name + "tasks"
    table_assignments = job_prefix_table_name + "assignments"

    # 2. select from p1_w1_A where task = task and worker = worker and job_id = job_id for update
    cursor.execute("UPDATE " +
                   table_assignments +
                   " SET status = %s, abandoned_at = %s WHERE _id = %s AND worker_id = %s AND status = %s",
                   [
                       settings.ASSIGNMENT_STATUS[2],      # 'ABANDONED',
                       datetime.utcnow().replace(tzinfo=pytz.UTC),
                       task_id,
                       worker_id,
                       settings.ASSIGNMENT_STATUS[0]       # 'PENDING_ANNOTATION'
                   ]
    )
    # 3. select from p1_w1_T where task = task and job = job for update
    # Steward case only cares about _id and done in the tasks table, other fields are solely for regular workers.
    cursor.execute(
        "SELECT _id, done FROM " +
        table_tasks +
        " WHERE _id = %s FOR UPDATE",
        [task_id]
    )
    task = cursor.fetchone()

    # make task done to false if it was previously true
    """TODO: 
    Can be a fundamental problem if :
    If 2 regulars, and 1 steward has the annotation page, where k=2 and l=2,
    So done = True because of the 2 regular assignments.
    If one regular skips, then no problem making done = False.
    If one steward skips, then done will be made False according to this function.
    So 1 other regular or steward can get an assignment, which is not right.
    But we will mostly not run into this problem because generally l will be 1.
    """
    task_done = task[1]
    if task_done:
        print('task was done. changing it back, because now we abandoned this task')
        task_done = False

    # update the tasks table (and unlock it)
    cursor.execute(
        "UPDATE " +
        table_tasks +
        " SET done = %s  WHERE _id = %s",
        [task_done, task_id]
    )

    return

def do_bookkeeping_3a_kn_part_2(
        obj_job: job_components.Job,
        annotations_per_tuple_per_worker_table_name: str,
        aggregated_annotations_table_name: str
):
    """Do bookkeeping to mark the 3a_kn job as completed"""
    processed_3a_kn_part_2 = False
    cursor = connection.cursor()
    try:
        with transaction.atomic():

            table_all_jobs = "all_jobs"
            job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)

            # 1. lock the job in all_jobs, for update
            cursor.execute(
                "SELECT u_id, p_id, w_id, r_id, j_id, j_name, j_type, j_status, date_creation FROM " +
                table_all_jobs +
                " WHERE u_id = %s AND p_id = %s AND w_id = %s AND r_id = %s AND j_id = %s" +
                " FOR UPDATE",
                [obj_job.user_id, obj_job.project_id, obj_job.workflow_id, obj_job.run_id, obj_job.id]
            )
            job_row = dict_fetchone(cursor)
            obj_job = job_components.Job(
                user_id=job_row['u_id'],
                project_id=job_row['p_id'],
                workflow_id=job_row['w_id'],
                run_id=job_row['r_id'],
                job_id=job_row['j_id'],
                job_name=job_row['j_name'],
                job_type=job_row['j_type'],
                job_status=job_row['j_status'],
                date_creation=job_row['date_creation']
            )

            if obj_job.status == settings.JOB_STATUS[2]:    # "COMPLETED"
                # the job was already completed, so should not process/bookkeep it again
                # return false
                return processed_3a_kn_part_2

            # 2. copy tables from job level to run level
            # create the annotations per tuple per worker table
            table_outputs = job_prefix_table_name + "outputs"
            cursor.execute(
                "CREATE TABLE " +
                annotations_per_tuple_per_worker_table_name +
                " AS TABLE " +
                table_outputs, []
            )
            # # 1. query for tuples
            # table_tuples = job_prefix_table_name + "tuples"
            # cursor.execute(
            #     "SELECT * FROM " +
            #     table_tuples +
            #     " LIMIT 0 ",
            #     []
            # )
            # columns = [col[0] for col in cursor.description]
            # if 'date_creation' in columns:
            #     columns.remove('date_creation')
            # # TODO: while joining, I am removing worker_id and annotation column from tuples so as not to conflict with newly assigned columns.
            # # ideally, this removal should have been done by the requester in the workflow via exec_sql before passign the results of previ-
            # # ous 3a_kn (B_1) as input (tuples) to the new 3a_kn.
            # # Cause of error that has been handled here: 3akn1: B_1 (with two cols) -> 3akn2: tuples (with same named cols) ->
            # # 3akn2: join tuples (with two cols) with outputs (with same two cols) to produce B_2 (with same two cols)
            # # Error producing line: "3akn2: join tuples (with two cols) with outputs (with same two cols) "
            # # Error was: column specified more than once
            # if 'worker_id' in columns:
            #     columns.remove('worker_id')
            # if 'annotation' in columns:
            #     columns.remove('annotation')
            # columns.remove('_id')
            # column_list = "_id"
            # for column in columns:
            #     column_list = column_list + ", " + column
            # query_string_tuples = "SELECT " + \
            #                       column_list + \
            #                       " FROM " + \
            #                       table_tuples
            #
            # # 2. query for outputs table
            # table_outputs = job_prefix_table_name + "outputs"
            # cursor.execute(
            #     "SELECT * FROM " +
            #     table_outputs +
            #     " LIMIT 0 ",
            #     []
            # )
            # columns = [col[0] for col in cursor.description]
            # if 'date_creation' in columns:
            #     columns.remove('date_creation')
            # columns.remove('_id')
            # column_list = "_id"
            # for column in columns:
            #     column_list = column_list + ", " + column
            # query_string_outputs = "SELECT " + \
            #                             column_list + \
            #                             " FROM " + \
            #                             table_outputs
            #
            # # 3. prepare aggregated annotations tuple by joining above two
            # cursor.execute(
            #     "CREATE TABLE " + annotations_per_tuple_per_worker_table_name + " AS (" +
            #     "SELECT " +
            #     "*"
            #     " FROM " +
            #     "(" +
            #     query_string_tuples +
            #     ") AS T1" +
            #     " INNER JOIN " +
            #     "(" +
            #     query_string_outputs +
            #     ") AS T2" +
            #     " USING(_id)" +
            #     ")",
            #     []
            # )

            # create the aggregated annotations table

            table_final_labels = job_prefix_table_name + "final_labels"
            cursor.execute(
                "CREATE TABLE " +
                aggregated_annotations_table_name +
                " AS TABLE " +
                table_final_labels, []
            )
            # # 1. query for tuples
            # table_tuples = job_prefix_table_name + "tuples"
            # cursor.execute(
            #     "SELECT * FROM " +
            #     table_tuples +
            #     " LIMIT 0 ",
            #     []
            # )
            # columns = [col[0] for col in cursor.description]
            # if 'date_creation' in columns:
            #     columns.remove('date_creation')
            # # TODO: while joining, I am removing label column from tuples so as not to conflict with newly assigned labels.
            # # ideally, this removal should have been done by the requester in the workflow via exec_sql before passign the results of previ-
            # # ous 3a_kn (C_1) as input (tuples) to the new 3a_kn.
            # # Cause of error that has been handled here: 3akn1: C_1 (with label col) -> 3akn2: tuples (with label col now) ->
            # # 3akn2: join tuples (with label col) with final_labels (with label col) to produce C_2 (with label col)
            # # Error producing line: "3akn2: join tuples (with label col) with final_labels (with label col) "
            # # Error was: column "label" specified more than once
            # if 'label' in columns:
            #     columns.remove('label')
            # columns.remove('_id')
            # column_list = "_id"
            # for column in columns:
            #     column_list = column_list + ", " + column
            # query_string_tuples = "SELECT " + \
            #                       column_list + \
            #                       " FROM " + \
            #                       table_tuples
            #
            # # 2. query for final labels
            # table_final_labels = job_prefix_table_name + "final_labels"
            # cursor.execute(
            #     "SELECT * FROM " +
            #     table_final_labels +
            #     " LIMIT 0 ",
            #     []
            # )
            # columns = [col[0] for col in cursor.description]
            # if 'date_creation' in columns:
            #     columns.remove('date_creation')
            # columns.remove('_id')
            # column_list = "_id"
            # for column in columns:
            #     column_list = column_list + ", " + column
            # query_string_final_labels = "SELECT " + \
            #                       column_list + \
            #                       " FROM " + \
            #                       table_final_labels
            #
            # # 3. prepare aggregated annotations tuple by joining above two
            # cursor.execute(
            #     "CREATE TABLE " + aggregated_annotations_table_name + " AS (" +
            #         "SELECT " +
            #             "*"
            #         " FROM " +
            #             "(" +
            #                 query_string_tuples +
            #             ") AS T1" +
            #         " INNER JOIN " +
            #             "(" +
            #             query_string_final_labels +
            #             ") AS T2" +
            #         " USING(_id)" +
            #     ")",
            #     []
            # )

            table_tasks = job_prefix_table_name + "tasks"
            table_assignments = job_prefix_table_name + "assignments"
            # delete indexes from job level tasks table and outputs table
            cursor.execute("DROP INDEX IF EXISTS " + table_tasks + "_id_done")
            cursor.execute("DROP INDEX IF EXISTS " + table_assignments + "_worker_id")
            cursor.execute("DROP INDEX IF EXISTS " + table_outputs + "_worker_id")

            # 3. update job in all_jobs (and release lock)
            obj_job.status = settings.JOB_STATUS[2]     # "COMPLETED"
            cursor.execute(
                "UPDATE " +
                table_all_jobs +
                " SET " +
                "j_status = %s" +
                " WHERE u_id = %s AND p_id = %s AND w_id = %s AND r_id = %s AND j_id = %s",
                [
                    obj_job.status,
                    obj_job.user_id,
                    obj_job.project_id,
                    obj_job.workflow_id,
                    obj_job.run_id,
                    obj_job.id,
                ]
            )

            processed_3a_kn_part_2 = True
            return processed_3a_kn_part_2

    except ValueError as err:
        print('Data access exception in bookkeeping 3a_kn job part 2')
        print(err.args)
        raise

    finally:
        cursor.close()
        # break


def sample_table(source_table_name: str, sample_size: int, output_table_name: str):
    """Take a sample from the table"""
    cursor = connection.cursor()
    try:
        # cursor.execute("SELECT count(*) FROM " +
        #                source_table_name,
        #                [])
        # total_size_row = cursor.fetchone()
        # total_size = total_size_row[0]
        # print('total_size: ', total_size)
        #
        # fraction = float(sample_size)/float(total_size)
        # print(fraction)
        # SELECT * FROM ts_test TABLESAMPLE BERNOULLI(percentage)
        # SELECT * FROM u5_p14_w12_r79_c_1 order by random() limit 2
        cursor.execute(
            "CREATE TABLE " + output_table_name + " AS " +
            "( SELECT * FROM " + source_table_name + " ORDER BY random() LIMIT %s )",
            [sample_size]
        )
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in sample table')
    finally:
        cursor.close()


def materialize_query_as_table(output_table: str, query: str):
    """
    Execute the specified query.
    Please note that there is a big security flaw if this query is executed without analyzing,
    since the requester can issue any query, and we are just executing on their behalf as is.
    Warning: Currently, we are executing query as is without analyzing them. This operator needs to be improved.
    """
    cursor = connection.cursor()
    try:
        # print('going to execute: ')
        # print(query)
        # print('---------')
        cursor.execute(
            "CREATE TABLE " + output_table + " AS " +
            query,[])
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in materialize_query_as_table')
    finally:
        cursor.close()


def do_bookkeeping_3a_amt(
        data_table_name: str,
        instructions: dict,
        layout: dict,
        configuration: dict,
        obj_job: job_components.Job,
        annotations_per_tuple_per_worker_table_name: str,
        aggregated_annotations_table_name: str
):
    """Do bookkeeping operations for 3a_amt job"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)

        # create and populate the parsed instructions in j_instructions table
        table_instructions = job_prefix_table_name + "amt_worker_instructions"
        cursor.callproc('create_table_instructions', [table_instructions])
        for key in instructions.keys():
            value = instructions.get(key)
            cursor.execute(
                "INSERT into " + table_instructions +
                " (type, content) VALUES (%s, %s)",
                [key, value]
            )

        table_layout = job_prefix_table_name + "layout"
        cursor.callproc('create_table_layout', [table_layout])
        for key in layout.keys():
            value = layout.get(key)
            cursor.execute(
                "INSERT into " + table_layout +
                " (type, content) VALUES (%s, %s)",
                [key, value]
            )

        # create and populate the config in j_config table
        table_configuration = job_prefix_table_name + "amt_config_parameters"
        cursor.callproc('create_table_configuration', [table_configuration])
        for key in configuration.keys():
            value = configuration.get(key)
            cursor.execute(
                "INSERT into " + table_configuration +
                " (key, value, value_data_type) VALUES (%s, %s, %s)",
                [key, value, str(type(value))]
            )

        # TODO: date_creation also getting copied here. replace with updated timestamp of copying
        table_tuples = job_prefix_table_name + "amt_tuples"
        cursor.execute(
            "CREATE TABLE " +
            table_tuples +
            " AS TABLE " +
            data_table_name,
            []
        )
        cursor.execute(
            "ALTER TABLE " +
            table_tuples +
            " ADD PRIMARY KEY (_id)",
            []
        )

        # create table related to storing tracking information about amt annotations
        table_tasks = job_prefix_table_name + "amt_tasks"
        cursor.callproc('create_table_amt_tasks', [table_tasks])

        # create table related to storing amt annotations of data
        table_outputs = job_prefix_table_name + "amt_outputs"
        cursor.callproc('create_table_amt_outputs', [table_outputs])

        # create table related to storing aggregated amt annotations of data
        table_final_labels = job_prefix_table_name + "amt_final_labels"
        cursor.callproc('create_table_amt_final_labels', [table_final_labels])

        return

    except ValueError as err:
        print('Data access exception in bookkeeping 3a_amt job')
        print(err.args)

    finally:
        cursor.close()


def get_data_headers_for_3a_amt_job(obj_job: job_components.Job):
    """Get all column names of data for this 3a_amt job (including _id, but not date_creation)"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_tuples = job_prefix_table_name + "amt_tuples"
        cursor.execute(
            "SELECT * FROM " +
            table_tuples +
            " LIMIT 0 ",
            []
        )
        columns = [col[0] for col in cursor.description]
        if 'date_creation' in columns:
            columns.remove('date_creation')
        return columns
    except ValueError as err:
        print('Data access exception in get data headers for 3a_amt job')
        print(err.args)
    finally:
        cursor.close()


def get_data_rows_for_3a_amt_job(obj_job: job_components.Job):
    """Get all rows of data for this 3a_amt job"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_tuples = job_prefix_table_name + "amt_tuples"
        cursor.execute(
            "SELECT * FROM " +
            table_tuples,
            []
        )
        tuple_rows = dict_fetchall(cursor)
        return tuple_rows
    except ValueError as err:
        print('Data access exception in get data rows for 3a_amt job')
        print(err.args)
    finally:
        cursor.close()


def populate_tasks_table_for_amt_job(obj_job: job_components.Job, mapping_task_id_vs_hit_info: dict):
    """Store the tasks pertaining to amt job"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_tasks = job_prefix_table_name + "amt_tasks"
        for task_id, hit_info in mapping_task_id_vs_hit_info.items():
            hit_id = hit_info.get('hit_id')
            hit_group_id = hit_info.get('hit_group_id')
            hit_max_assignments = hit_info.get('hit_max_assignments')
            cursor.execute(
                "INSERT into " + table_tasks +
                " (_id, hit_id, hit_group_id, num_responses_submitted, max_responses, all_responses_submitted) VALUES (%s, %s, %s, %s, %s, %s)",
                [task_id, hit_id, hit_group_id, 0, hit_max_assignments , False]
            )
        return
    except ValueError as err:
        print('Data access exception in populate tasks table for 3a_amt job')
        print(err.args)
    finally:
        cursor.close()


def get_unfinished_amt_tasks(obj_job: job_components.Job):
    """Get those tasks for amt job, which are not yet finished"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)

        table_tasks = job_prefix_table_name + "amt_tasks"
        cursor.execute(
            "SELECT _id, hit_id, hit_group_id, num_responses_submitted, max_responses, all_responses_submitted FROM " +
            table_tasks +
            " WHERE all_responses_submitted = %s " ,
            [False]
        )
        task_rows = dict_fetchall(cursor)
        return task_rows
    except ValueError as err:
        print('Data access exception in get unfinished amt tasks')
        print(err.args)
    finally:
        cursor.close()


def get_amt_responses_for_tasks(obj_job: job_components.Job, task_ids: list):
    """Get the already stored responses for the specified tasks"""
    cursor = connection.cursor()
    responses: dict = {}
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        num_tasks = len(task_ids)
        table_outputs = job_prefix_table_name + "amt_outputs"
        query_string = "SELECT _id, annotation, worker_id, date_annotation, amt_assignment_id, date_creation FROM " +\
            table_outputs +\
            " WHERE _id IN ("
        for i in range(0, num_tasks):
            query_string = query_string + "%s, "
        query_string = query_string.rstrip(', ')
        query_string = query_string + ')'
        cursor.execute(
            query_string,
            task_ids
        )
        responses_for_tasks = dict_fetchall(cursor)
        for response in responses_for_tasks:
            task_id = response['_id']
            worker_id = response['worker_id']
            answer_data_for_task_id = {
                'answer_for_task_id': response['annotation'],
                'answer_submit_time': response['date_annotation'],
                'amt_assignment_id': response['amt_assignment_id']
            }
            if task_id not in responses:
                responses[task_id] = {}
            responses[task_id][worker_id] = answer_data_for_task_id
            # print(task_id, ':', worker_id, ' : ', answer_data_for_task_id)
        return responses
    except ValueError as err:
        print('Data access exception in get amt responses for tasks')
        print(err.args)
    finally:
        cursor.close()


def update_cymphony_with_new_amt_responses(obj_job: job_components.Job, new_responses: dict):
    """
    Insert the new responses into task outputs table
    Then, update the tasks table to track response statistics so far
    """
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)

        # insert worker responses for each task into table outputs
        table_outputs = job_prefix_table_name + "amt_outputs"
        for task_id in new_responses.keys():
            dict_worker_vs_response: dict = new_responses.get(task_id)
            for worker_id, answer_data in dict_worker_vs_response.items():
                annotation = answer_data.get('answer_for_input_field')
                date_annotation = answer_data.get('answer_submit_time')
                amt_assignment_id = answer_data.get('amt_assignment_id')
                cursor.execute(
                    "INSERT into " + table_outputs +
                    " (_id, worker_id, annotation, date_annotation, amt_assignment_id) VALUES (%s, %s, %s, %s, %s)",
                    [task_id, worker_id, annotation, date_annotation, amt_assignment_id]
                )

        # update statistical response information for each task
        table_tasks = job_prefix_table_name + "amt_tasks"
        for task_id in new_responses.keys():
            dict_worker_vs_response: dict = new_responses.get(task_id)
            with transaction.atomic():
                cursor.execute(
                    "SELECT _id, hit_id, hit_group_id, num_responses_submitted, max_responses, all_responses_submitted, date_creation FROM " +
                    table_tasks +
                    " WHERE _id = %s " +
                    "FOR UPDATE",
                    [task_id]
                )
                task = dict_fetchone(cursor)
                task_num_responses_submitted: int = task['num_responses_submitted'] + len(dict_worker_vs_response)
                task_all_responses_submitted: bool = task['all_responses_submitted']
                if task_num_responses_submitted == task['max_responses']:
                    task_all_responses_submitted = True
                cursor.execute(
                    "UPDATE " +
                    table_tasks +
                    " SET num_responses_submitted = %s, all_responses_submitted = %s WHERE _id = %s",
                    [task_num_responses_submitted, task_all_responses_submitted, task_id]
                )
        return
    except ValueError as err:
        print('Data access exception in update cymphony with new amt responses')
        print(err.args)
    finally:
        cursor.close()


def aggregate_3a_amt(obj_job: job_components.Job, k: int):
    """Aggregate annotations for tasks in 3a_amt job"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)

        table_outputs = job_prefix_table_name + "amt_outputs"

        # for the same id, retrieve annotations in chronological order of submit times
        cursor.execute(
            "SELECT _id, worker_id, annotation, date_annotation, amt_assignment_id, date_creation FROM " +
            table_outputs +
            " ORDER BY _id, date_annotation",
            []
        )
        annotations_tasks = dict_fetchall(cursor)

        # annotations for each task will be ordered chronologically
        dict_task_vs_annotations = dict()
        for annotation_row in annotations_tasks:
            task_id = annotation_row['_id']
            annotation = annotation_row['annotation']
            if task_id not in dict_task_vs_annotations:
                dict_task_vs_annotations[task_id] = []
            dict_task_vs_annotations.get(task_id).append(annotation)
            # print(task_id, ': ', annotation_row['worker_id'], ': ', annotation, ': ', annotation_row['date_annotation'])

        dict_task_vs_final_label = dict()
        # for each task, determine the first k votes that agree
        for task_id, time_ordered_annotations in dict_task_vs_annotations.items():
            final_label = settings.AMT_DEFAULT_AGGREGATION_LABEL    # 'undecided'

            # number of votes per vote type
            task_annotations_dict = {}
            for annotation in time_ordered_annotations:
                if annotation not in task_annotations_dict:
                    task_annotations_dict[annotation] = 0
                task_annotations_dict[annotation] = task_annotations_dict[annotation] + 1
                # print(task_id, ': ', annotation, ': ', task_annotations_dict[annotation], ' votes')

                # we just added one count to an annotation for the task, did the count reach k
                if task_annotations_dict[annotation] >= k:
                    # this annotation is the first (chronologically) annotation that has agreed k times
                    # make this the final label
                    final_label = annotation
                    break

            # either there was a break in for loop (we found a final label for this task),
            # or there was no single annotation that occurred k times, leaving the final label still undecided
            dict_task_vs_final_label[task_id] = final_label

        # transfer the decided final labels for each task into the final labels table
        table_final_labels = job_prefix_table_name + "amt_final_labels"
        for task_id, final_label in dict_task_vs_final_label.items():
            cursor.execute(
                "INSERT into " +
                table_final_labels +
                " (_id, label) VALUES (%s, %s)",
                [task_id, final_label]
            )

        return

    except ValueError as err:
        print('Data access exception in aggregate 3a_amt task outputs to final labels')
        print(err.args)

    finally:
        cursor.close()


def get_configuration_for_3a_amt_job(obj_job: job_components.Job):
    """Retrieve configuration for the 3a_amt job"""
    cursor = connection.cursor()
    configuration = dict()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_configuration = job_prefix_table_name + "amt_config_parameters"
        cursor.execute(
            "SELECT key, value, value_data_type, date_creation FROM " +
            table_configuration,
            []
        )
        rows = dict_fetchall(cursor)
        for row in rows:
            configuration[row['key']] = row['value']
        return configuration
    except ValueError as err:
        print('Data access exception in get configuration for 3a_amt job')
        print(err.args)
    finally:
        cursor.close()


def do_bookkeeping_3a_amt_part_2(
        obj_job: job_components.Job,
        annotations_per_tuple_per_worker_table_name: str,
        aggregated_annotations_table_name: str
):
    """Do bookkeeping to mark the 3a_amt job's completion"""
    processed_3a_amt_part_2 = False
    cursor = connection.cursor()
    try:
        with transaction.atomic():
            table_all_jobs = "all_jobs"
            job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)

            # 1. lock the job in all_jobs, for update
            cursor.execute(
                "SELECT u_id, p_id, w_id, r_id, j_id, j_name, j_type, j_status, date_creation FROM " +
                table_all_jobs +
                " WHERE u_id = %s AND p_id = %s AND w_id = %s AND r_id = %s AND j_id = %s" +
                " FOR UPDATE",
                [obj_job.user_id, obj_job.project_id, obj_job.workflow_id, obj_job.run_id, obj_job.id]
            )
            job_row = dict_fetchone(cursor)
            obj_job = job_components.Job(
                user_id=job_row['u_id'],
                project_id=job_row['p_id'],
                workflow_id=job_row['w_id'],
                run_id=job_row['r_id'],
                job_id=job_row['j_id'],
                job_name=job_row['j_name'],
                job_type=job_row['j_type'],
                job_status=job_row['j_status'],
                date_creation=job_row['date_creation']
            )

            if obj_job.status == settings.JOB_STATUS[2]:    # "COMPLETED"
                # the job was already completed, so should not process/bookkeep it again
                # return false
                return processed_3a_amt_part_2

            # 2. copy tables from job level to run level
            # create the annotations per tuple per worker table
            table_outputs = job_prefix_table_name + "amt_outputs"
            cursor.execute(
                "CREATE TABLE " +
                annotations_per_tuple_per_worker_table_name +
                " AS TABLE " +
                table_outputs, []
            )

            # create the aggregated annotations table
            table_final_labels = job_prefix_table_name + "amt_final_labels"
            cursor.execute(
                "CREATE TABLE " +
                aggregated_annotations_table_name +
                " AS TABLE " +
                table_final_labels, []
            )

            # 3. update job in all_jobs (and release lock)
            obj_job.status = settings.JOB_STATUS[2]     # "COMPLETED"
            cursor.execute(
                "UPDATE " +
                table_all_jobs +
                " SET " +
                "j_status = %s" +
                " WHERE u_id = %s AND p_id = %s AND w_id = %s AND r_id = %s AND j_id = %s",
                [
                    obj_job.status,
                    obj_job.user_id,
                    obj_job.project_id,
                    obj_job.workflow_id,
                    obj_job.run_id,
                    obj_job.id,
                ]
            )

            processed_3a_amt_part_2 = True
            return processed_3a_amt_part_2

    except ValueError as err:
        print('Data access exception in bookkeeping 3a_amt job part 2')
        print(err.args)

    finally:
        cursor.close()


def get_layout(requester_id: int, project_id: int, workflow_id: int, run_id: int, job_id: int):
    """Retrieve layout of how task is to be represented to user (worker)"""
    cursor = connection.cursor()
    layout = dict()
    try:
        obj_job: job_components.Job = find_job(
            job_id=job_id,
            run_id=run_id,
            workflow_id=workflow_id,
            project_id=project_id,
            user_id=requester_id
        )
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)

        table_layout = job_prefix_table_name + "layout"
        cursor.execute(
            "SELECT type, content, date_creation FROM " +
            table_layout,
            []
        )
        rows = dict_fetchall(cursor)
        for row in rows:
            layout[row['type']] = row['content']

        return layout

    except ValueError as err:
        print('Data access exception in get layout')
        print(err.args)

    finally:
        cursor.close()


def get_count_tasks(obj_job: job_components.Job):
    """Get count of the tasks in the tasks table for this job"""
    cursor = connection.cursor()
    count_tasks_in_job: int = -1
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_tasks = job_prefix_table_name + "tasks"
        cursor.execute("SELECT count(*) FROM " + table_tasks, [])
        count_tasks_row = cursor.fetchone()
        count_tasks_in_job = int(count_tasks_row[0])
        # print("Count of tasks: ", count_tasks_in_job)
        return count_tasks_in_job
    except ValueError as err:
        print('Data access exception in get count of tasks')
        print(err.args)
    finally:
        cursor.close()
