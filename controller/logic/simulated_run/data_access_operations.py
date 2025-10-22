from django.db import connection, close_old_connections
from django.conf import settings

import controller.logic.run.components as run_components
import controller.logic.job.components as job_components
import controller.logic.job.data_access_operations as job_dao
from controller.logic.common_data_access_operations import dict_fetchall, dict_fetchone
from controller.logic.common_logic_operations import get_run_prefix_table_name, get_job_prefix_table_name

import psycopg2


def store_run_simulation_parameters(simulation_parameters: dict, obj_run: run_components.Run):
    """Store run level simulation parameters for a simulated run in db"""
    cursor = connection.cursor()
    try:
        # create empty simulation parameters table
        table_prefix = get_run_prefix_table_name(obj_run=obj_run)
        table_simulation_parameters = table_prefix + "simulation_parameters"
        cursor.callproc('create_table_run_simulation_parameters',
                        [table_simulation_parameters])
        for key, value in simulation_parameters.items():
            cursor.execute(
                "INSERT into " + table_simulation_parameters +
                " (key, value, value_data_type) VALUES (%s, %s, %s)",
                [key, value, str(type(value))]
            )
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in store simulation params of simulated run')
    finally:
        cursor.close()


def store_job_simulation_parameters(simulation_parameters: dict, obj_job: job_components.Job):
    """Store job specific simulation parameters for a simulated run in db"""
    cursor = connection.cursor()
    try:
        # create empty simulation parameters table
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_simulation_parameters = table_prefix + "simulation_parameters"
        cursor.callproc('create_table_job_simulation_parameters',
                        [table_simulation_parameters])
        for key, value in simulation_parameters.items():
            cursor.execute(
                "INSERT into " + table_simulation_parameters +
                " (key, value, value_data_type) VALUES (%s, %s, %s)",
                [key, value, str(type(value))]
            )
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in store simulation params of job in simulated run')
    finally:
        cursor.close()


def load_run_simulation_parameters(obj_run: run_components.Run):
    """Load run level simulation parameters for a simulated run from db"""
    cursor = connection.cursor()
    simulation_parameters = dict()
    try:
        table_prefix = get_run_prefix_table_name(obj_run=obj_run)
        table_simulation_parameters = table_prefix + "simulation_parameters"
        cursor.execute(
            "SELECT key, value, value_data_type, date_creation FROM " +
            table_simulation_parameters,
            []
        )
        rows = dict_fetchall(cursor)
        for row in rows:
            simulation_parameters[row['key']] = row['value']
        return simulation_parameters
    except ValueError as err:
        print('Data access exception in load simulation parameters for the simulated run')
        print(err.args)
    finally:
        cursor.close()


def load_job_simulation_parameters(obj_job: job_components.Job):
    """Load job specific simulation parameters for a simulated run from db"""
    cursor = connection.cursor()
    simulation_parameters = dict()
    try:
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_simulation_parameters = table_prefix + "simulation_parameters"
        cursor.execute(
            "SELECT key, value, value_data_type, date_creation FROM " +
            table_simulation_parameters,
            []
        )
        rows = dict_fetchall(cursor)
        for row in rows:
            simulation_parameters[row['key']] = row['value']
        return simulation_parameters
    except ValueError as err:
        print('Data access exception in load simulation parameters of job in the simulated run')
        print(err.args)
    finally:
        cursor.close()


def create_table_parameters_simulation_workers_job(obj_job: job_components.Job):
    """Create job level table for storing parameters of simulation of workers"""
    cursor = connection.cursor()
    try:
        # create empty parameters_simulation_workers table
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_parameters_simulation_workers = table_prefix + "parameters_simulation_workers"
        cursor.callproc('create_table_parameters_simulation_workers_job',
                        [table_parameters_simulation_workers])
        return
    except ValueError as err:
        print(err.args)
        raise ValueError(
            'Data access exception in creating table, for parameters corresponding to simulation of workers, against a job'
        )
    finally:
        cursor.close()


def create_table_statistics_simulation_workers_job(obj_job: job_components.Job):
    """Create job level table for storing statistics of simulation of workers"""
    cursor = connection.cursor()
    try:
        # create empty statistics_simulation_workers table
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_statistics_simulation_workers = table_prefix + "statistics_simulation_workers"
        cursor.callproc('create_table_statistics_simulation_workers_job',
                        [table_statistics_simulation_workers])
        return
    except ValueError as err:
        print(err.args)
        raise ValueError(
            'Data access exception in creating table, for statistics corresponding to simulation of workers, against a job'
        )
    finally:
        cursor.close()


def store_parameters_simulation_workers_job(parameters_simulation_workers: list, obj_job: job_components.Job):
    """
    Store following parameters of simulation of workers in db
    identifier: loop point (burst identifier)
    number_workers: number of workers in this loop point (burst)
    time_gap: the decided time gap between this loop point (burst) and the next loop point (burst)
    """
    cursor = connection.cursor()
    try:
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_parameters_simulation_workers = table_prefix + "parameters_simulation_workers"
        for info in parameters_simulation_workers:
            # add entry to table_parameters_simulation_workers
            cursor.execute(
                "INSERT into " + table_parameters_simulation_workers +
                " (identifier, number_workers, time_gap) VALUES (%s, %s, %s)",
                [
                    info['identifier'],
                    info['number_workers'],
                    info['time_gap']
                ]
            )
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in storing parameters of simulation of workers, against a job')
    finally:
        cursor.close()


def load_parameters_simulation_workers_job(obj_job: job_components.Job):
    """Load parameters corresponding to simulation of workers"""
    cursor = connection.cursor()
    parameters_simulation_workers: list = []
    try:
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_parameters_simulation_workers = table_prefix + "parameters_simulation_workers"
        cursor.execute(
            "SELECT identifier, number_workers, time_gap, date_creation FROM " +
            table_parameters_simulation_workers +
            " ORDER BY identifier",
            []
        )
        rows = dict_fetchall(cursor)
        for row in rows:
            info: dict = {}
            info['identifier'] = row['identifier']
            info['number_workers'] = row['number_workers']
            info['time_gap'] = row['time_gap']
            info['date_creation'] = row['date_creation']
            parameters_simulation_workers.append(info)
        return parameters_simulation_workers
    except ValueError as err:
        print('Data access exception in loading parameters of simulation of workers, against a job')
        print(err.args)
    finally:
        cursor.close()


def store_statistics_simulation_workers_job(statistics_simulation_workers: list, obj_job: job_components.Job):
    """
    Store statistics corresponding to simulation of workers in db
    identifier: loop point (burst identifier)
    number_workers: workers that actually got spawned (signed up) against this job, within this loop point (burst)
    time_gap: the actual time spent between this and the next loop point (burst)
    """
    cursor = connection.cursor()
    try:
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_statistics_simulation_workers = table_prefix + "statistics_simulation_workers"
        for info in statistics_simulation_workers:
            # add entry to table_statistics_simulation_workers
            cursor.execute(
                "INSERT into " + table_statistics_simulation_workers +
                " (identifier, number_workers, time_gap) VALUES (%s, %s, %s)",
                [
                    info['identifier'],
                    info['number_workers'],
                    info['time_gap']
                ]
            )
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in storing statistics of simulation of workers, against a job')
    finally:
        cursor.close()


def load_statistics_simulation_workers_job(obj_job: job_components.Job):
    """Load statistics corresponding to simulation of workers"""
    cursor = connection.cursor()
    statistics_simulation_workers: list = []
    try:
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_statistics_simulation_workers = table_prefix + "statistics_simulation_workers"
        cursor.execute(
            "SELECT identifier, number_workers, time_gap, date_creation FROM " +
            table_statistics_simulation_workers +
            " ORDER BY identifier",
            []
        )
        rows = dict_fetchall(cursor)
        for row in rows:
            info: dict = {}
            info['identifier'] = row['identifier']
            info['number_workers'] = row['number_workers']
            info['time_gap'] = row['time_gap']
            info['date_creation'] = row['date_creation']
            statistics_simulation_workers.append(info)
        return statistics_simulation_workers
    except ValueError as err:
        print('Data access exception in loading statistics of simulation of workers, against a job')
        print(err.args)
    finally:
        cursor.close()


def create_table_parameters_workers_job(obj_job: job_components.Job):
    """Create job level table for storing parameters of individual workers"""
    cursor = connection.cursor()
    try:
        # create empty parameters_workers table
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_parameters_workers = table_prefix + "parameters_workers"
        cursor.callproc(
            'create_table_parameters_workers_job',
            [table_parameters_workers]
        )
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in creating table for workers parameters in simulating job')
    finally:
        cursor.close()


def create_table_statistics_workers_job(obj_job: job_components.Job):
    """Create job level table for storing statistics of individual workers hitting the simulated job"""
    cursor = connection.cursor()
    try:
        # create empty statistics_workers table
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_statistics_workers = table_prefix + "statistics_workers"
        cursor.callproc(
            'create_table_statistics_workers_job',
            [table_statistics_workers]
        )
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in creating table for workers statistics in simulating job')
    finally:
        cursor.close()


def store_parameters_worker_job(
        worker_username: str,
        worker_annotation_time: int,
        worker_reliability: float,
        obj_job: job_components.Job
):
    """Store the worker parameters in db i.e. store parameters of a worker hitting a simulated job"""
    # cursor, con = get_client_side_cursor_and_connection()
    cursor = connection.cursor()
    try:
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_parameters_workers = table_prefix + "parameters_workers"
        # add entry to table_parameters_workers
        cursor.execute(
            "INSERT into " + table_parameters_workers +
            " (worker_username, worker_reliability, worker_annotation_time) VALUES (%s, %s, %s)",
            [
                worker_username,
                worker_reliability,
                worker_annotation_time
            ]
        )
        # con.commit()
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in storing parameters of a worker, hitting a job in a simulated run')
    finally:
        cursor.close()
        close_old_connections()
        # con.close()


def load_parameters_worker_job(obj_job: job_components.Job, worker_username: str):
    """Load parameters of a worker hitting a simulated job"""
    cursor = connection.cursor()
    worker_parameters = dict()
    try:
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_parameters_workers = table_prefix + "parameters_workers"
        cursor.execute(
            "SELECT worker_username, worker_reliability, worker_annotation_time, date_creation FROM " +
            table_parameters_workers +
            " WHERE worker_username = %s",
            [worker_username]
        )
        row = dict_fetchone(cursor)
        # TODO: this should ideally go through a component maybe called WorkerParameters
        #  (worker is technically a user but we want to differentiate a normal user from a simulated worker,
        #  so a simulated worker will also have some worker parameters)
        # Here, I did not want to fetch by position either, for sake of readability, so making a dict out of it.
        worker_parameters['worker_username'] = row['worker_username']
        worker_parameters['worker_reliability'] = row['worker_reliability']
        worker_parameters['worker_annotation_time'] = row['worker_annotation_time']
        worker_parameters['date_creation'] = row['date_creation']
        return worker_parameters
    except ValueError as err:
        print('Data access exception in loading parameters of a worker, hitting a job in a simulated run')
        print(err.args)
    finally:
        cursor.close()


def store_statistics_worker_job(worker_username: str, worker_precision: float, worker_recall: float, total_matches_so_far: int, total_annotated_so_far: int, total_size_tuples: int, obj_job: job_components.Job):
    """Store the worker parameters in db i.e. store statistics of a worker hitting a simulated job"""
    # cursor, con = get_client_side_cursor_and_connection()
    cursor = connection.cursor()
    try:
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_statistics_workers = table_prefix + "statistics_workers"
        # add entry to table_statistics_workers
        cursor.execute(
            "INSERT into " + table_statistics_workers +
            " (worker_username, worker_precision, worker_recall, total_matches, total_annotated, total_tuples) VALUES (%s, %s, %s, %s, %s, %s)",
            [
                worker_username,
                worker_precision,
                worker_recall,
                total_matches_so_far,
                total_annotated_so_far,
                total_size_tuples
            ]
        )
        # con.commit()
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in storing statistics of a worker, hitting a job in a simulated run')
    finally:
        cursor.close()
        close_old_connections()
        # con.close()


def load_statistics_worker_job(obj_job: job_components.Job, worker_username: str):
    """Load statistics of a worker hitting a simulated job"""
    cursor = connection.cursor()
    worker_statistics = dict()
    try:
        table_prefix = get_job_prefix_table_name(obj_job=obj_job)
        table_statistics_workers = table_prefix + "statistics_workers"
        cursor.execute(
            "SELECT worker_username, worker_precision, worker_recall, date_creation FROM " +
            table_statistics_workers +
            " WHERE worker_username = %s",
            [worker_username]
        )
        row = dict_fetchone(cursor)
        # TODO: this should ideally go through a component maybe called WorkerStatistics
        #  (worker is technically a user but we want to differentiate a normal user from a simulated worker,
        #  so a simulated worker will also have some worker statistics)
        # Here, I did not want to fetch by position either, for sake of readability, so making a dict out of it.
        worker_statistics['worker_username'] = row['worker_username']
        worker_statistics['worker_precision'] = row['worker_precision']
        worker_statistics['worker_recall'] = row['worker_recall']
        worker_statistics['date_creation'] = row['date_creation']
        return worker_statistics
    except ValueError as err:
        print('Data access exception in loading statistics of a worker, hitting a job in a simulated run')
        print(err.args)
    finally:
        cursor.close()


def get_size_data(data_table_name: str):
    """Get count of data table"""
    # intended usage:
    # data table to be the (run prefixed) input table to a 3a_kn job, for simulating accuracy of workers on this job
    # returns the number of rows in the data table
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT count(*) FROM " + data_table_name, [])
        count_rows = cursor.fetchone()[0]
        print('data table number of rows: ', count_rows)
        return count_rows
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in get size in data table')
    finally:
        cursor.close()


def get_stats_of_worker_annotations(obj_job: job_components.Job):
    """Calculate worker statistics"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_outputs = job_prefix_table_name + "outputs"
        cursor.execute(
            "select " +
                "min(t.n_annotations), " +
                "max(t.n_annotations), " +
                "avg(t.n_annotations), " +
                "count(t.n_annotations) " +
            "from" +
                "( " +
                    "select " +
                        "count(*) as n_annotations " +
                    "from " +
                        table_outputs + " " +
                    "group by worker_id "
                ") as t ",
            []
        )
        statistics_row = cursor.fetchone()
        min_annotations = statistics_row[0]
        max_annotations = statistics_row[1]
        avg_annotations = statistics_row[2]
        num_workers_annotated = statistics_row[3]
        return min_annotations, max_annotations, avg_annotations, num_workers_annotated
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in get stats of worker annotations')
    finally:
        cursor.close()


def get_accuracy(obj_job: job_components.Job):
    """Get accuracy of worker labels"""
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_tuples = job_prefix_table_name + "tuples"
        table_final_labels = job_prefix_table_name + "final_labels"

        # 1. query for tuples
        cursor.execute(
            "SELECT * FROM " +
            table_tuples +
            " LIMIT 0 ",
            []
        )
        columns = [col[0] for col in cursor.description]
        if 'date_creation' in columns:
            columns.remove('date_creation')
        # while joining, I am removing label column from tuples so as not to conflict with newly assigned labels.
        # ideally, this removal should have been done by the requester in the workflow via exec_sql before passign the results of previ-
        # ous 3a_kn (C_1) as input (tuples) to the new 3a_kn.
        # Cause of error that has been handled here: 3akn1: C_1 (with label col) -> 3akn2: tuples (with label col now) ->
        # 3akn2: join tuples (with label col) with final_labels (with label col) to produce C_2 (with label col)
        # Error producing line: "3akn2: join tuples (with label col) with final_labels (with label col) "
        # Error was: column "label" specified more than once
        if 'label' in columns:
            columns.remove('label')
        columns.remove('_id')
        column_list = "_id"
        for column in columns:
            column_list = column_list + ", " + column
        query_string_tuples = "SELECT " + \
                              column_list + \
                              " FROM " + \
                              table_tuples

        # 2. query for final labels
        cursor.execute(
            "SELECT * FROM " +
            table_final_labels +
            " LIMIT 0 ",
            []
        )
        columns = [col[0] for col in cursor.description]
        if 'date_creation' in columns:
            columns.remove('date_creation')
        columns.remove('_id')
        column_list = "_id"
        for column in columns:
            column_list = column_list + ", " + column
        query_string_final_labels = "SELECT " + \
                              column_list + \
                              " FROM " + \
                              table_final_labels

        # 3. prepare aggregated annotations tuple by joining above two
        gold_label_column = settings.GOLD_LABEL_COLUMN_NAME
        cursor.execute(
            "SELECT " +
                "T1._id, " +
                "T1." + gold_label_column + ", " +
                "T2.label "
            "FROM " +
                "(" +
                    query_string_tuples +
                ") AS T1 " +
            "INNER JOIN " +
                "(" +
                query_string_final_labels +
                ") AS T2" +
            " USING(_id)",
            []
        )
        tuples_joined_final_labels = dict_fetchall(cursor)
        matches = 0
        number_aggregated_labels = 0
        for row in tuples_joined_final_labels:
            gold_label = row[gold_label_column]
            aggregated_label = row['label']
            if gold_label == aggregated_label:
                matches = matches + 1
            # size(final_labels table) <= size(tuples table),
            # so size(join table) will be equal to size(final_labels table)
            number_aggregated_labels = number_aggregated_labels + 1
        # count the number of tuples in the job
        cursor.execute("SELECT count(*) FROM " + table_tuples, [])
        count_tuples = cursor.fetchone()[0]
        # note that since computing the join of tuples and final labels, the count of tuples would not have changed since it is job's input data
        # this prevents the below calculations from becoming inaccurate in case the job is running (and annotations are continuing to be aggregated)
        accuracy = -1
        if number_aggregated_labels == 0:
            # could not compute accuracy since no annotations have been aggregated yet to compute gold match against
            # i.e. either no one has started annotating on the job (job being running or aborted),
            # or none of the annotations collected so far in job ( job being running or aborted) have been aggregated
            pass
        elif number_aggregated_labels < count_tuples:
            # job still running (collecting and aggregating annotations in progress) or aborted, and
            # aggregation of annotations is in progress
            accuracy = float(matches) / number_aggregated_labels    # accuracy as of the time of above computed join
        elif number_aggregated_labels == count_tuples:
            # job completed (no longer collecting or aggregating annotations)
            accuracy = float(matches) / number_aggregated_labels
        return accuracy
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in get accuracy')
    finally:
        cursor.close()


def get_e2e_time(obj_job: job_components.Job):
    """Last final label aggregation time minus(-) first assignment time"""
    time_interval = -1
    cursor = connection.cursor()
    try:
        job_prefix_table_name = get_job_prefix_table_name(obj_job=obj_job)
        table_assignments = job_prefix_table_name + "assignments"
        table_final_labels = job_prefix_table_name + "final_labels"
        cursor.execute(
            "select "
            "(SELECT date_creation FROM " +
            table_final_labels +
            " ORDER BY date_creation DESC LIMIT 1)" +
            " - " +
            "(SELECT date_creation FROM " +
            table_assignments +
            " ORDER BY date_creation ASC LIMIT 1)" +
            " AS time_interval",
            []
        )
        time_interval_row = cursor.fetchone()
        if time_interval_row:
            time_interval = time_interval_row[0]
        return time_interval
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in get e2e time')
    finally:
        cursor.close()


def get_client_side_cursor_and_connection():
    """Get client side cursor and connection"""
    db_params = settings.DATABASES['default']
    con = psycopg2.connect(
        database=db_params['NAME'],
        user=db_params['USER'],
        password=db_params['PASSWORD'],
        host=db_params['HOST'],
        port=db_params['PORT']
    )
    cursor = con.cursor()
    return cursor, connection
