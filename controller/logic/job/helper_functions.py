from django.conf import settings

import controller.logic.job.components as job_components
import controller.logic.job.data_access_operations as job_dao
import controller.logic.job.helper_functions as job_helper_functions
import controller.logic.run.components as run_components
import controller.logic.run.data_access_operations as run_dao
import controller.logic.run.helper_functions as run_helper_functions
from controller.logic.common_logic_operations import multiple_replace, parse_string_to_list_of_strings
from controller.logic.common_logic_operations import get_run_dir_path, get_job_prefix_table_name

import xmltodict, copy, collections, boto3, re, time
from pathlib import Path
from distutils.util import strtobool


def process_3a_kn(
        data_table_name: str,
        instructions_file_path: Path,
        layout_file_path: Path,
        configuration: dict,
        obj_job: job_components.Job,
        annotations_per_tuple_per_worker_table_name: str,
        aggregated_annotations_table_name: str
):
    """Prepare 3a_kn job so workers can work on it."""
    # job submitted to cymphony jobs dashboard for workers to annotate
    submitted: bool = True
    # 1. do bookkeeping
    dict_instructions = parse_instruction_file(instruction_file_path=instructions_file_path)
    dict_layout = parse_layout_file(layout_file_path=layout_file_path)
    # create empty j_tables, populate j_tasks table, populate j_instructions j_layout and j_config table
    job_dao.do_bookkeeping_3a_kn(
        data_table_name=data_table_name,
        instructions=dict_instructions,
        layout=dict_layout,
        configuration=configuration,
        obj_job=obj_job,
        annotations_per_tuple_per_worker_table_name=annotations_per_tuple_per_worker_table_name,
        aggregated_annotations_table_name=aggregated_annotations_table_name
    )
    # check if data is present
    tuple_header = job_dao.get_data_headers_for_job(obj_job=obj_job)
    tuple_rows = job_dao.get_data_rows_for_job(obj_job=obj_job)
    if not tuple_rows:
        """
        there is no data, so no submitting to cymphony jobs dashboard, 
            so no workers submitting annotations to cymphony, so no aggregations by cymphony
        instead, process second part of 3a_kn (copy job tables to run level tables and mark job as completed)
        return False, signifying we did not submit job to jobs dashboard and 
            hence dag should be progressed forward as if 3a_kn was an automatic operator.
        """
        job_helper_functions.process_3a_kn_part_2(
            obj_job=obj_job,
            annotations_per_tuple_per_worker_table_name=annotations_per_tuple_per_worker_table_name,
            aggregated_annotations_table_name=aggregated_annotations_table_name
        )
        submitted = False
        return submitted

    # 2. mark this node's corresponding job to "running" (this will make the job appear on jobs dashboard)
    obj_job.status = settings.JOB_STATUS[1]     # "RUNNING"
    job_dao.edit_job(obj_job=obj_job)
    submitted = True
    return submitted


def process_read_table(
        input_file_name: str,
        obj_job: job_components.Job,
        output_data_table_name: str
):
    """Process the read_table job"""
    """
    TODO: select job from all_jobs for update, do processing, mark job in all_jobs as completed (and release lock).
    This all should be done under one cursor in one function of job_dao.
    Although this piece of code won't be entered twice, and hence this job is unlikely to be updated in a conflict, 
        thinking like that introduces coupling.
    You should make every piece of code robust assuming there would be conflicting schedules.
    For reference, see process_3a_kn_part_2.
    You should do the above for all other process_.. as well.
    """
    # mark this job to "running"
    obj_job.status = settings.JOB_STATUS[1]     # "RUNNING"
    job_dao.edit_job(obj_job=obj_job)
    # run directory containing data file
    obj_run: run_components.Run = run_dao.find_run(
        user_id=obj_job.user_id,
        project_id=obj_job.project_id,
        workflow_id=obj_job.workflow_id,
        run_id=obj_job.run_id
    )
    run_dir_path = get_run_dir_path(obj_run=obj_run)
    # get the input file from the run directory
    input_file_path = None
    for file_path in run_dir_path.iterdir():
        if file_path.is_file():
            if file_path.name == input_file_name:
                input_file_path = file_path
    job_dao.create_table_from_file(source_file_path=input_file_path, target_table_name=output_data_table_name)
    # mark this node's corresponding job to "completed"
    obj_job.status = settings.JOB_STATUS[2]     # "COMPLETED"
    job_dao.edit_job(obj_job=obj_job)
    return


def process_sample_random(
        input_table_name: str, sample_size: int,
        obj_job: job_components.Job,
        output_table_name: str
):
    """Process the sample_random job"""
    # mark this job to "running"
    obj_job.status = settings.JOB_STATUS[1]     # "RUNNING"
    job_dao.edit_job(obj_job=obj_job)
    # sample_table
    job_dao.sample_table(
        source_table_name=input_table_name,
        sample_size=sample_size,
        output_table_name=output_table_name
    )
    # mark this node's corresponding job to "completed"
    obj_job.status = settings.JOB_STATUS[2]     # "COMPLETED"
    job_dao.edit_job(obj_job=obj_job)


def process_write_table(
        input_table_name: str,
        obj_job: job_components.Job,
        output_file_path: Path
):
    """Process the write table job"""
    # mark this job to "running"
    obj_job.status = settings.JOB_STATUS[1]     # "RUNNING"
    job_dao.edit_job(obj_job=obj_job)
    job_dao.create_file_from_table(source_table_name=input_table_name, destination_file_path=output_file_path)
    # mark this node's corresponding job to "completed"
    obj_job.status = settings.JOB_STATUS[2]     # "COMPLETED"
    job_dao.edit_job(obj_job=obj_job)


def parse_instruction_file(instruction_file_path: Path):
    """Parse the instruction file supplied as part of 3a_kn or 3a_amt job"""
    # instruction_loc file contains 1. Short instructions, 2. Long instructions
    instructions_intermediate_representation: dict = {}
    # parse instruction file and put in table
    with instruction_file_path.open() as file:
        contents = file.read()
        a, b = contents.find(settings.SHORT_INSTRUCTIONS_BEGIN), contents.find(settings.SHORT_INSTRUCTIONS_END)
        x, y = contents.find(settings.LONG_INSTRUCTIONS_BEGIN), contents.find(settings.LONG_INSTRUCTIONS_END)
        short_instructions = contents[a + len(settings.SHORT_INSTRUCTIONS_BEGIN):b]
        long_instructions = contents[x + len(settings.LONG_INSTRUCTIONS_BEGIN):y]
        instructions_intermediate_representation['short_instructions'] = short_instructions
        instructions_intermediate_representation['long_instructions'] = long_instructions
    return instructions_intermediate_representation


def assign_3a_kn(worker_id: int, obj_job: job_components.Job, job_k: int, job_n: int, task_annotation_time_limit: int, count_tasks: int):
    """Assign task to worker"""
    all_tasks_have_aggregated_labels: bool = job_dao.check_all_tasks_have_aggregated_labels(obj_job, count_tasks)
    if not all_tasks_have_aggregated_labels:
        # possibility of task being assigned to worker
        return job_dao.assign_3a_kn(worker_id, obj_job, job_k, job_n, task_annotation_time_limit)
    else:
        # job is no more collecting annotations for any tasks
        return -1


def get_annotation_page_3a_kn(
        obj_job: job_components.Job,
        task_id: int,
        task_annotation_time_limit: int,
        task_question: str,
        task_option_list,
        task_short_instructions: str,
        task_long_instructions: str,
        task_design_layout: str
):
    """Prepare annotation page of task for worker"""
    # find the header
    tuple_header = job_dao.get_data_headers_for_job(obj_job=obj_job)
    tuple_id = task_id
    tuple_row = job_dao.get_data_row_for_job(obj_job=obj_job, tuple_id=tuple_id)
    header_value_dict = collections.OrderedDict()
    for header in tuple_header:
        header_value_dict[header] = tuple_row.get(header)
    # prepare question
    task_question = task_question.strip()
    for key, value in header_value_dict.items():
        variable_to_replace = '${' + key + '}'
        task_question = task_question.replace(variable_to_replace, value)
    # prepare tabular representation of the row (excluding id)
    if task_design_layout == settings.DEFAULT_LAYOUT:
        task_representation: str = "<table border=\"1px black\">"
        for key, value in header_value_dict.items():
            task_representation = task_representation + "<th>" + key + "</th>"
        task_representation = task_representation + "<tr>"
        for key, value in header_value_dict.items():
            task_representation = task_representation + "<td>" + value + "</td>"
        task_representation = task_representation + "</tr>"
        task_representation = task_representation + "</table>"
    else:
        task_representation: str = task_design_layout
        for key, value in header_value_dict.items():
            variable_to_replace = '${' + key + '}'
            task_representation = task_representation.replace(variable_to_replace, value)
    # materialized task representation
    task_option_list = task_option_list
    page_contents = {
        'task_question': task_question,
        'task_representation': task_representation,
        'task_option_list': task_option_list,
        'task_short_instructions': task_short_instructions,
        'task_long_instructions': task_long_instructions,
        'header_value_dict': header_value_dict,
        'timer': task_annotation_time_limit
    }
    return page_contents


def aggregate_3a_kn(obj_job: job_components.Job, job_k: int, job_n: int, worker_id: int, task_id: int, answer: str):
    """Aggregate task annotations as part of 3a_kn job"""
    # updating assignment to completed
    already_abandoned = job_dao.update_assignment_for_task_in_aggregate(
        obj_job=obj_job,
        task_id=task_id,
        worker_id=worker_id
    )

    if already_abandoned:
        return

    # putting to outputs and aggregating
    job_dao.bookkeeping_and_aggregate(
        obj_job=obj_job,
        task_id=task_id,
        worker_id=worker_id,
        job_k=job_k,
        job_n=job_n,
        answer=answer
    )


def process_3a_kn_part_2(
        obj_job: job_components.Job,
        annotations_per_tuple_per_worker_table_name: str,
        aggregated_annotations_table_name: str
):
    """Do bookkeeping to mark up completion of the 3a_kn job"""
    """
    do bookkeeping i.e.
    select job in all_jobs for update
    copy ...j_outputs and ...j_final_labels to annotations... and aggregated... tables respectively.
    mark job in all_jobs as completed
    """
    processed_3a_kn_part_2: bool = job_dao.do_bookkeeping_3a_kn_part_2(
        obj_job=obj_job,
        annotations_per_tuple_per_worker_table_name=annotations_per_tuple_per_worker_table_name,
        aggregated_annotations_table_name=aggregated_annotations_table_name
    )
    return processed_3a_kn_part_2


def process_exec_sql(
        query: str,
        input_tables: list,
        output_table: str,
        obj_job: job_components.Job
):
    """Process the exec_sql job"""
    # 1. put the job in running through job.dao call
    # mark this job to "running"
    obj_job.status = settings.JOB_STATUS[1]     # "RUNNING"
    job_dao.edit_job(obj_job=obj_job)

    # 2. materialize the query as a table
    job_dao.materialize_query_as_table(output_table, query)

    # 3. put the job to completed through job.dao call
    # mark this node's corresponding job to "completed"
    obj_job.status = settings.JOB_STATUS[2]     # "COMPLETED"
    job_dao.edit_job(obj_job=obj_job)
    return


def process_3a_amt(
        amt_credentials: dict,
        data_table_name: str,
        instructions_file_path: Path,
        layout_file_path: Path,
        configuration: dict,
        obj_job: job_components.Job,
        annotations_per_tuple_per_worker_table_name: str,
        aggregated_annotations_table_name: str
):
    """Prepare 3a_amt job and publish to amt, so amt workers can work on it."""
    # 1. do bookkeeping
    dict_instructions = parse_instruction_file(instruction_file_path=instructions_file_path)
    dict_layout = parse_layout_file(layout_file_path=layout_file_path)
    job_dao.do_bookkeeping_3a_amt(
        data_table_name=data_table_name,
        instructions=dict_instructions,
        layout=dict_layout,
        configuration=configuration,
        obj_job=obj_job,
        annotations_per_tuple_per_worker_table_name=annotations_per_tuple_per_worker_table_name,
        aggregated_annotations_table_name=aggregated_annotations_table_name
    )
    # check if data is present
    tuple_header = job_dao.get_data_headers_for_3a_amt_job(obj_job=obj_job)
    tuple_rows = job_dao.get_data_rows_for_3a_amt_job(obj_job=obj_job)
    if not tuple_rows:
        """
        there is no data, so no submitting to amt, or retrieval from amt
        instead, process second part of 3a_amt (copy job tables to run level tables and mark job as completed)
        return None, signifying we did not connect with amt and hence, 
            dag should be progressed forward as if 3a_amt was an automatic operator.
        """
        job_helper_functions.process_3a_amt_part_2(
            obj_job=obj_job,
            annotations_per_tuple_per_worker_table_name=annotations_per_tuple_per_worker_table_name,
            aggregated_annotations_table_name=aggregated_annotations_table_name
        )
        return None

    # 2. convert specification
    configuration_amt_specification = convert_specification_from_cymphony_to_amt(configuration=configuration)
    short_instructions = dict_instructions['short_instructions']
    long_instructions = dict_instructions['long_instructions']
    design_layout = dict_layout['design_layout']

    # 3. connect to amt
    use_sandbox: bool = configuration_amt_specification['Sandbox']
    if use_sandbox:
        endpoint_url = settings.MTURK_SANDBOX
    else:
        endpoint_url = settings.MTURK_PRODUCTION
    mturk = boto3.client(
        'mturk',
        aws_access_key_id=amt_credentials['access_key_id'],
        aws_secret_access_key=amt_credentials['secret_access_key'],
        region_name=settings.MTURK_REGION_NAME,
        endpoint_url=endpoint_url
    )
    print("I have $", mturk.get_account_balance()['AvailableBalance'], " in my account")

    # 4. for each batch of tasks, publish a hit to amt
    # 4.1 lay down basic scaffolding for hit
    scaffolding_annotation_page = settings.PATH_ANNOTATION_PAGE_AMT
    file_scaffolding_annotation_page = open(scaffolding_annotation_page, "r")
    html_question_scaffolding: str = file_scaffolding_annotation_page.read()
    html_question_scaffolding: str = html_question_scaffolding.replace(
        settings.ANNOTATION_PAGE_AMT_SHORT_INSTRUCTIONS_PLACEHOLDER,
        short_instructions
    )
    html_question_scaffolding: str = html_question_scaffolding.replace(
        settings.ANNOTATION_PAGE_AMT_LONG_INSTRUCTIONS_PLACEHOLDER,
        long_instructions
    )
    # 4.2 put in tasks in the scaffolding and publish each hit
    categories = configuration_amt_specification['Categories']
    question_header = configuration_amt_specification['Header']
    dict_task_id_vs_question = prepare_question_representations(question_header, tuple_header, tuple_rows)
    dict_task_id_vs_representation = prepare_task_representations(design_layout, tuple_header, tuple_rows)
    tasks_per_hit: int = configuration_amt_specification['TasksPerHit'] # number of tasks clubbed together for one hit
    mapping_task_id_vs_hit_info = {}
    batch_task_ids: list = []
    counter_for_divisibility = 1
    for task_id in dict_task_id_vs_representation.keys():
        batch_task_ids.append(task_id)
        if counter_for_divisibility % tasks_per_hit == 0:   # just hit the tasks limit for this hit
            hit_info: dict = prepare_and_push_hit(
                batch_task_ids=batch_task_ids,
                dict_task_id_vs_representation=dict_task_id_vs_representation,
                dict_task_id_vs_question=dict_task_id_vs_question,
                categories=categories,
                html_question_scaffolding=html_question_scaffolding,
                mturk_client=mturk,
                configuration_amt_specification=configuration_amt_specification
            )
            add_to_mapping_task_id_vs_hit_info(
                mapping_task_id_vs_hit_info=mapping_task_id_vs_hit_info,
                batch_task_ids=batch_task_ids,
                hit_info=hit_info
            )
            batch_task_ids = [] # empty batch since it has been pushed as a hit
        counter_for_divisibility = counter_for_divisibility + 1
    # if there are still some tasks in the batch_task_ids list, it means that they haven't been pushed yet.
    if len(batch_task_ids) > 0:     # will also be less than tasks_per_hit
        # prepare hit for this remaining batch of tasks
        hit_info: dict = prepare_and_push_hit(
            batch_task_ids=batch_task_ids,
            dict_task_id_vs_representation=dict_task_id_vs_representation,
            dict_task_id_vs_question=dict_task_id_vs_question,
            categories=categories,
            html_question_scaffolding=html_question_scaffolding,
            mturk_client=mturk,
            configuration_amt_specification=configuration_amt_specification
        )
        add_to_mapping_task_id_vs_hit_info(
            mapping_task_id_vs_hit_info=mapping_task_id_vs_hit_info,
            batch_task_ids=batch_task_ids,
            hit_info=hit_info
        )
        batch_task_ids = []  # empty batch since it has been pushed as a hit

    # 5. initialize tasks table
    job_dao.populate_tasks_table_for_amt_job(
        obj_job=obj_job,
        mapping_task_id_vs_hit_info=mapping_task_id_vs_hit_info
    )

    # 6. mark this node's corresponding job to "running"
    obj_job.status = settings.JOB_STATUS[1]     # "RUNNING"
    job_dao.edit_job(obj_job=obj_job)
    return mturk


def convert_specification_from_cymphony_to_amt(configuration: dict):
    """Convert cymphony level amt parameters (supplied by requester) to amt level parameters"""
    configuration_amt_specification = {}
    # setting defaults for optional parameters of 3a_amt (except qualifications)
    # qualifications are not assigned defaults, since either they are present, or they are not.
    key = "publish_to_sandbox"
    if key in configuration:
        configuration_amt_specification['Sandbox'] = bool(
            strtobool(
                configuration[key]
            )
        )
    else:
        configuration_amt_specification['Sandbox'] = settings.AMT_DEFAULTS.get(key)
    print("Target will be sandbox: ", configuration_amt_specification['Sandbox'])
    key = "tasks_per_hit"
    if key in configuration:
        configuration_amt_specification['TasksPerHit'] = int(configuration[key])
    else:
        configuration_amt_specification['TasksPerHit'] = settings.AMT_DEFAULTS.get(key)
    key = "auto_approve_and_pay_workers_in"
    if key in configuration:
        configuration_amt_specification['AutoApprovalDelayInSeconds'] = int(configuration[key])
    else:
        configuration_amt_specification['AutoApprovalDelayInSeconds'] = settings.AMT_DEFAULTS.get(key)
    # for all other configurations, converted to amt format below
    qualifications: list = []
    for key, value in configuration.items():
        # pre-processing
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        # relating to presentation of job outline to workers
        if key == "title":
            configuration_amt_specification['Title'] = value
        elif key == "description":
            configuration_amt_specification['Description'] = value
        elif key == "keywords":
            list_keywords: list = parse_string_to_list_of_strings(value)
            keywords_amt_format: str = ', '.join(list_keywords)
            configuration_amt_specification['Keywords'] = keywords_amt_format
        elif key == "lifetime":
            configuration_amt_specification['LifetimeInSeconds'] = int(value)
        # relating to worker recruitment
        elif key == "workers_are_masters":
            masters = bool(strtobool(value))
            print(masters, type(masters))
            if masters:
                sandbox: bool = configuration_amt_specification['Sandbox']
                if sandbox:
                    masters_target = 'sandbox'
                else:
                    masters_target = 'production'
                settings_qualifications_masters: dict = settings.AMT_QUALIFICATIONS.get(key)
                if settings_qualifications_masters:
                    settings_qualifications_masters_environment: dict = settings_qualifications_masters.get(masters_target)
                else:
                    settings_qualifications_masters_environment: dict = {}
                    raise ValueError("should not have reached here")
                qualification_type_id: str = settings_qualifications_masters_environment.get('QUALIFICATION_TYPE_ID')
                qualification_comparator: str = settings_qualifications_masters_environment.get('COMPARATOR')
                qualification_dict_amt_format: dict = {}
                qualification_dict_amt_format['QualificationTypeId'] = qualification_type_id
                qualification_dict_amt_format['Comparator'] = qualification_comparator
                qualifications.append(qualification_dict_amt_format)
            else:
                # qualification not added to qualifications
                pass
        elif key == "workers_from_location":
            list_locations: list = parse_string_to_list_of_strings(value)
            settings_qualifications_location: dict = settings.AMT_QUALIFICATIONS.get(key)
            qualification_type_id: str = settings_qualifications_location.get('QUALIFICATION_TYPE_ID')
            qualification_comparator: str = settings_qualifications_location.get('COMPARATOR')
            locale_values: list = []
            for location in list_locations:
                locale_value: dict = {}
                if '-' in location:
                    country_subdivision = location.split('-')
                    locale_value['Country'] = country_subdivision[0]
                    locale_value['Subdivision'] = country_subdivision[1]
                else:
                    locale_value['Country'] = location
                locale_values.append(locale_value)
            qualification_dict_amt_format: dict = {}
            qualification_dict_amt_format['QualificationTypeId'] = qualification_type_id
            qualification_dict_amt_format['Comparator'] = qualification_comparator
            qualification_dict_amt_format['LocaleValues'] = locale_values
            qualifications.append(qualification_dict_amt_format)
        elif key == "workers_with_min_hits_approved":
            min_hits_approved: int = int(value)
            settings_qualifications_min_hits_approved: dict = settings.AMT_QUALIFICATIONS.get(key)
            qualification_type_id: str = settings_qualifications_min_hits_approved.get('QUALIFICATION_TYPE_ID')
            qualification_comparator: str = settings_qualifications_min_hits_approved.get('COMPARATOR')
            qualification_dict_amt_format: dict = {}
            qualification_dict_amt_format['QualificationTypeId'] = qualification_type_id
            qualification_dict_amt_format['Comparator'] = qualification_comparator
            qualification_dict_amt_format['IntegerValues'] = [min_hits_approved]
            qualifications.append(qualification_dict_amt_format)
        elif key == "workers_with_max_hits_approved":
            max_hits_approved: int = int(value)
            settings_qualifications_max_hits_approved: dict = settings.AMT_QUALIFICATIONS.get(key)
            qualification_type_id: str = settings_qualifications_max_hits_approved.get('QUALIFICATION_TYPE_ID')
            qualification_comparator: str = settings_qualifications_max_hits_approved.get('COMPARATOR')
            qualification_dict_amt_format: dict = {}
            qualification_dict_amt_format['QualificationTypeId'] = qualification_type_id
            qualification_dict_amt_format['Comparator'] = qualification_comparator
            qualification_dict_amt_format['IntegerValues'] = [max_hits_approved]
            qualifications.append(qualification_dict_amt_format)
        elif key == "workers_with_min_approval_rate":
            min_approval_rate: int = int(value)
            settings_qualifications_min_approval_rate: dict = settings.AMT_QUALIFICATIONS.get(key)
            qualification_type_id: str = settings_qualifications_min_approval_rate.get('QUALIFICATION_TYPE_ID')
            qualification_comparator: str = settings_qualifications_min_approval_rate.get('COMPARATOR')
            qualification_dict_amt_format: dict = {}
            qualification_dict_amt_format['QualificationTypeId'] = qualification_type_id
            qualification_dict_amt_format['Comparator'] = qualification_comparator
            qualification_dict_amt_format['IntegerValues'] = [min_approval_rate]
            qualifications.append(qualification_dict_amt_format)
        elif key == "workers_with_max_approval_rate":
            max_approval_rate: int = int(value)
            settings_qualifications_max_approval_rate: dict = settings.AMT_QUALIFICATIONS.get(key)
            qualification_type_id: str = settings_qualifications_max_approval_rate.get('QUALIFICATION_TYPE_ID')
            qualification_comparator: str = settings_qualifications_max_approval_rate.get('COMPARATOR')
            qualification_dict_amt_format: dict = {}
            qualification_dict_amt_format['QualificationTypeId'] = qualification_type_id
            qualification_dict_amt_format['Comparator'] = qualification_comparator
            qualification_dict_amt_format['IntegerValues'] = [max_approval_rate]
            qualifications.append(qualification_dict_amt_format)
        # relating to hit body
        elif key == "question":
            configuration_amt_specification['Header'] = value
        elif key == "answers":
            if value == settings.FREE_TEXT_ANSWER:    # free text
                configuration_amt_specification['Categories'] = None
            else:   # list of choices
                list_answers: list = parse_string_to_list_of_strings(value)
                configuration_amt_specification['Categories'] = list_answers
        # relating to worker interaction with hit
        elif key == "n":
            configuration_amt_specification['MaxAssignments'] = int(value)
        elif key == "k":
            pass    # k is not an amt specific parameter (cymphony uses this for aggregating amt annotations later on)
        elif key == "annotation_time_limit":
            configuration_amt_specification['AssignmentDurationInSeconds'] = int(value)
        # relating to payments to workers
        elif key == "reward_per_hit":
            configuration_amt_specification['Reward'] = value
        """
        some remarks:
        for auto_approve_and_pay_workers_in, set at the start of this function by setting
        configuration_amt_specification['AutoApprovalDelayInSeconds']
        relating to miscellaneous use cases -
        for target being sandbox/production and tasks per hit, both are taken care of at the start by
        setting configuration_amt_specification['Sandbox'] and configuration_amt_specification['TasksPerHit']
        """

    configuration_amt_specification['Qualifications'] = qualifications
    return configuration_amt_specification


def prepare_question_representations(question, tuple_header, tuple_rows):
    """Prepare task id vs it's corresponding question representation"""
    # task id vs question's representation in string format
    dict_task_id_vs_question_representation: dict = {}
    # for each row
    for tuple_row in tuple_rows:
        # column name vs value, for the row
        header_value_dict = collections.OrderedDict()
        for header in tuple_header:
            header_value_dict[header] = tuple_row.get(header)
        # retrieve the task id of this row
        task_id_key = '_id'
        task_id = -1
        if task_id_key in header_value_dict:
            # remove the id key, value from the dict and store the id
            task_id = header_value_dict.pop(task_id_key)
        # prepare question representation of the row (excluding _id)
        question_representation: str = question
        for key, value in header_value_dict.items():
            variable_to_replace = '${' + key + '}'
            question_representation = question_representation.replace(variable_to_replace, value)
        # materialized question representation
        dict_task_id_vs_question_representation[task_id] = question_representation
    return dict_task_id_vs_question_representation


def prepare_task_representations(design_layout, tuple_header, tuple_rows):
    """Prepare task id vs how the task data is represented in tabular format"""
    # task id vs task's tabular representation in string format
    dict_task_id_vs_representation: dict = {}
    if design_layout == settings.DEFAULT_LAYOUT:
        # for each row
        for tuple_row in tuple_rows:
            # column name vs value, for the row
            header_value_dict = collections.OrderedDict()
            for header in tuple_header:
                header_value_dict[header] = tuple_row.get(header)
            # retrieve the task id of this row
            task_id_key = '_id'
            task_id = -1
            if task_id_key in header_value_dict:
                # remove the id key, value from the dict and store the id
                task_id = header_value_dict.pop(task_id_key)
            # prepare tabular representation of the row (excluding _id)
            task_representation: str = "<table border=\"1px black\">"
            for key, value in header_value_dict.items():
                task_representation = task_representation + "<th>" + key + "</th>"
            task_representation = task_representation + "<tr>"
            for key, value in header_value_dict.items():
                task_representation = task_representation + "<td>" + value + "</td>"
            task_representation = task_representation + "</tr>"
            task_representation = task_representation + "</table>"
            dict_task_id_vs_representation[task_id] = task_representation
    else:
        # for each row
        for tuple_row in tuple_rows:
            # column name vs value, for the row
            header_value_dict = collections.OrderedDict()
            for header in tuple_header:
                header_value_dict[header] = tuple_row.get(header)
            # retrieve the task id of this row
            task_id_key = '_id'
            task_id = -1
            if task_id_key in header_value_dict:
                # remove the id key, value from the dict and store the id
                task_id = header_value_dict.pop(task_id_key)
            # prepare tabular representation of the row (excluding _id)
            task_representation: str = design_layout
            for key, value in header_value_dict.items():
                variable_to_replace = '${' + key + '}'
                task_representation = task_representation.replace(variable_to_replace, value)
            # materialized task representation
            dict_task_id_vs_representation[task_id] = task_representation
    return dict_task_id_vs_representation


def prepare_and_push_hit(
        batch_task_ids,
        dict_task_id_vs_representation,
        dict_task_id_vs_question,
        categories,
        html_question_scaffolding,
        mturk_client,
        configuration_amt_specification
):
    """Prepare hit for this batch of tasks, and push to amt"""
    # prepare hit for this batch of tasks
    html_question: str = prepare_html_question_for_hit(
        batch_task_ids=batch_task_ids,
        dict_task_id_vs_representation=dict_task_id_vs_representation,
        dict_task_id_vs_question=dict_task_id_vs_question,
        categories=categories,
        html_question_scaffolding=html_question_scaffolding,
    )
    # push hit to amt
    hit_info: dict = push_hit(
        mturk_client=mturk_client,
        html_question=html_question,
        configuration_amt_specification=configuration_amt_specification
    )
    return hit_info


def prepare_html_question_for_hit(
        batch_task_ids,
        dict_task_id_vs_representation,
        dict_task_id_vs_question,
        categories,
        html_question_scaffolding
):
    """Wrap the batch of tasks in the form of html form"""
    # prepare html question for this hit (batch of tasks)
    # wrap html page with question and answers around each tuple, so as to push as a hit
    tasks_form_data: str = ""
    for task_id in batch_task_ids:
        task_representation: str = dict_task_id_vs_representation[task_id]
        question: str = dict_task_id_vs_question[task_id]
        # a custom element for each task in hit
        tasks_form_data: str = tasks_form_data + (
            """<div class="task-container">"""
        )
        tasks_form_data: str = tasks_form_data + (
                """<br><br>""" +
                """<div style="overflow-x:auto;"> <br> """ +
                """<h2>""" + question + """</h2> <br>""" +
                task_representation +
                """</div><br><br>"""
        )
        if categories is None:  # free text
            tasks_form_data: str = tasks_form_data + (
                    """<input """ +
                    """type="text" """ +
                    """name=""" + '"' + str(task_id) + '" ' +
                    """id=""" + '"' + str(task_id) + '" ' +
                    """placeholder=""" + '"' + 'Type in your answer here' + '" ' +
                    """ required>""" +
                    """<br> <br> """
            )
        else:
            for choice in categories:
                tasks_form_data: str = tasks_form_data + (
                    """<input """ +
                        """type="radio" """ +
                        """name=""" + '"' + str(task_id) + '" ' +
                        """id=""" + '"' + str(task_id) + str(choice) + '" ' +
                        """value=""" + '"' + str(choice) + '" ' +
                    """ required>""" +
                    """<label """ +
                        """for=""" + '"' + str(task_id) + str(choice) + '" ' +
                    """>""" +
                    str(choice) +
                    """</label> <br> <br> """
                )
        tasks_form_data: str = tasks_form_data + (
            """</div>"""
        )
    html_question: str = html_question_scaffolding.replace(
        settings.ANNOTATION_PAGE_AMT_FORM_DATA_PLACEHOLDER,
        tasks_form_data
    )
    # remove any garbage at start
    html_question = re.sub("^([\\W]+)<", "<", html_question.strip(), 1)
    return html_question


def push_hit(
        mturk_client: boto3.client,
        html_question: str,
        configuration_amt_specification:dict
):
    """Publish the hit to amt"""
    new_hit = mturk_client.create_hit(
        Title=configuration_amt_specification.get('Title'),
        Description=configuration_amt_specification.get('Description'),
        Keywords=configuration_amt_specification.get('Keywords'),
        LifetimeInSeconds=configuration_amt_specification.get('LifetimeInSeconds'),
        QualificationRequirements=configuration_amt_specification.get('Qualifications'),
        Question=html_question,
        MaxAssignments=configuration_amt_specification.get('MaxAssignments'),
        AssignmentDurationInSeconds=configuration_amt_specification.get('AssignmentDurationInSeconds'),
        Reward=configuration_amt_specification.get('Reward'),
        AutoApprovalDelayInSeconds=configuration_amt_specification.get('AutoApprovalDelayInSeconds')
    )
    print("A new HIT has been created. You can preview it here: ")
    print("https://workersandbox.mturk.com/mturk/preview?groupId=", new_hit['HIT']['HITGroupId'])
    print("HITID = " + new_hit['HIT']['HITId'] + " (Use to Get Results)")
    hit_id = new_hit['HIT']['HITId']
    hit_group_id = new_hit['HIT']['HITGroupId']
    return {
        'hit_id': hit_id,
        'hit_group_id': hit_group_id,
        'hit_max_assignments': configuration_amt_specification.get('MaxAssignments')
    }


def add_to_mapping_task_id_vs_hit_info(mapping_task_id_vs_hit_info: dict, batch_task_ids: list, hit_info: dict):
    """Prepare task id vs its corresponding hit info"""
    for task_id in batch_task_ids:
        mapping_task_id_vs_hit_info[task_id] = hit_info


def coordinate_cymphony_amt(obj_job: job_components.Job, mturk_client: boto3.client):
    """Periodically ping amt to fetch task responses, aggregate them, and finish the 3a_amt job if conditions are met"""
    while True:
        print('Woken up')
        start_ts = time.time()
        # get tasks that still have annotations remaining
        unfinished_tasks: list = job_dao.get_unfinished_amt_tasks(obj_job=obj_job)
        print('Number of unfinished tasks: ', len(unfinished_tasks))
        # collect all amt responses for these tasks
        # a hit can contain multiple tasks, and amt supplies results per hit
        # print('============Collect all amt responses==============')
        set_hits: set = set()
        for task in unfinished_tasks:
            hit_id = task['hit_id']
            set_hits.add(hit_id)
        print('Number of hits corresponnding to unfinished tasks: ', len(set_hits))
        amt_responses: dict = collect_amt_responses(mturk_client=mturk_client, hits=set_hits)
        # collect all responses already in db, for these tasks
        # print('============Collect all responses already in db==============')
        task_ids = []
        for task in unfinished_tasks:
            task_ids.append(task['_id'])
        old_responses: dict = job_dao.get_amt_responses_for_tasks(obj_job=obj_job, task_ids=task_ids)
        # what responses are new responses
        # print('============What responses are new responses==============')
        new_responses: dict = extract_new_responses(
            old_responses=old_responses,
            amt_responses=amt_responses,
            task_ids=task_ids
        )
        # update cymphony tables based on the new responses
        # print('============update cymphony tables based on the new responses==============')
        job_dao.update_cymphony_with_new_amt_responses(obj_job=obj_job, new_responses=new_responses)
        # have all the unfinished tasks finished now?
        unfinished_tasks: list = job_dao.get_unfinished_amt_tasks(obj_job=obj_job)
        print('Number of still remaining unfinished tasks: ', len(unfinished_tasks))
        if not unfinished_tasks:
            break
        end_ts = time.time()
        time_elapsed = int(end_ts - start_ts)
        print(float(time_elapsed)/60, 'minutes have passed. (', time_elapsed, 'seconds )')
        print('Going to sleep')
        # repeat check after some time
        time.sleep(settings.AMT_PING_FREQUENCY)
    # aggregate annotations provided by amt workers, for this job
    configuration_dict = job_dao.get_configuration_for_3a_amt_job(obj_job=obj_job)
    job_k: int = int(configuration_dict.get('k'))
    job_dao.aggregate_3a_amt(obj_job=obj_job, k=job_k)
    # human operator (here, 3a_amt) just finished, do bookkeeping and mark complete; and progress dag
    run_helper_functions.complete_processing_job_and_progress_dag(this_job=obj_job)


def collect_amt_responses(mturk_client: boto3.client, hits: set):
    """Collect responses for all tasks that have been annotated by amt workers"""
    # returns responses for each task
    responses: dict = {}
    # for each hit
    for hit_id in hits:
        # worker responses that are in submitted or approved assignment status
        worker_results = mturk_client.list_assignments_for_hit(
            HITId=hit_id,
            AssignmentStatuses=[settings.AMT_ASSIGNMENT_STATUS[0], settings.AMT_ASSIGNMENT_STATUS[1]]
        )
        # parse worker results to extract useful information
        if worker_results['NumResults'] > 0:
            assignments = worker_results['Assignments']
            # for a worker assignment
            for assignment in assignments:
                worker_id = assignment['WorkerId']  # amt returns a string
                answers = xmltodict.parse(assignment['Answer'])
                if type(answers['QuestionFormAnswers']['Answer']) is list:
                    # multiple fields in hit layout
                    for answer_field in answers['QuestionFormAnswers']['Answer']:
                        # we supplied task_id as the input_field when publishing hit earlier, so we should get that back here
                        input_field = int(answer_field['QuestionIdentifier'])    # amt returns a string
                        answer_data_for_input_field = {
                            'answer_for_input_field': answer_field['FreeText'], # amt returns a string
                            # amt returns a datetime structure in the Coordinated Universal Time (UTC, sometimes called Greenwich Mean Time) time zone
                            'answer_submit_time': assignment['SubmitTime'],
                            'amt_assignment_id': assignment['AssignmentId']    # amt returns a string
                        }
                        if input_field not in responses:
                            responses[input_field] = {}
                        responses[input_field][worker_id] = answer_data_for_input_field
                        # print(input_field, ':', worker_id, ' : ', answer_data_for_input_field)
                else:
                    # one field found in hit layout
                    input_field = int(answers['QuestionFormAnswers']['Answer']['QuestionIdentifier'])
                    answer_data_for_input_field = {
                        'answer_for_input_field': answers['QuestionFormAnswers']['Answer']['FreeText'],
                        'answer_submit_time': assignment['SubmitTime'],
                        'amt_assignment_id': assignment['AssignmentId']
                    }
                    if input_field not in responses:
                        responses[input_field] = {}
                    responses[input_field][worker_id] = answer_data_for_input_field
                    # print(input_field, ':', worker_id, ' : ', answer_data_for_input_field)
        else:   # worker_results['NumResults'] == 0:
            # no results for tasks in this hit yet
            # no task entries made in responses
            pass
    return responses


def extract_new_responses(old_responses: dict, amt_responses: dict,  task_ids: list):
    """Extract those responses that are not already saved in database"""
    new_responses: dict = {}
    # for each task id
    for task_id in task_ids:
        dict_worker_vs_amt_response: dict = amt_responses.get(task_id)  # None if no response by amt for this task
        dict_worker_vs_old_response: dict = old_responses.get(task_id)  # None if no response on record for this task
        if (dict_worker_vs_amt_response is None) and (dict_worker_vs_old_response is None):
            # no old record and no response from amt, for this task
            print(task_id, ': no old record and no response from amt, for this task')
            pass
        elif (dict_worker_vs_amt_response is None) and (dict_worker_vs_old_response is not None):
            # will never happen since amt responses collected are cumulative in nature
            print(task_id, ': SHOULD NOT HAVE REACHED HERE')
            pass
        elif (dict_worker_vs_amt_response is not None) and (dict_worker_vs_old_response is None):
            # all responses of amt are new responses, for this task_id
            print(task_id, ': all responses of amt are new responses, for this task_id')
            for worker_id, answer_data in dict_worker_vs_amt_response.items():
                if task_id not in new_responses:
                    new_responses[task_id] = {}
                new_responses[task_id][worker_id] = answer_data
                print(task_id, ':', worker_id, ' : ', answer_data)
        else:  # this task id has old responses as well as amt collected responses
            print(task_id, ': this task id has old responses as well as amt collected responses')
            # did amt collect new responses
            if len(dict_worker_vs_amt_response) > len(dict_worker_vs_old_response):
                # what are the new responses for this task_id
                print('what are the new responses for this task_id')
                set_workers_amt = set(dict_worker_vs_amt_response.keys())
                set_workers_old = set(dict_worker_vs_old_response.keys())
                set_new_workers = set_workers_amt - set_workers_old
                for worker_id in set_new_workers:
                    answer_data = dict_worker_vs_amt_response[worker_id]
                    if task_id not in new_responses:
                        new_responses[task_id] = {}
                    new_responses[task_id][worker_id] = answer_data
                    print(task_id, ':', worker_id, ' : ', answer_data)
            else:
                # amt did not collect any new response for this task id
                print('amt did not collect any new response for this task id')
                pass
    return new_responses


def process_3a_amt_part_2(
        obj_job: job_components.Job,
        annotations_per_tuple_per_worker_table_name: str, aggregated_annotations_table_name: str
):
    """Do bookkeeping to mark up completion of the 3a_amt job"""
    """
    do bookkeeping i.e.
    select job in all_jobs for update
    copy ...j_outputs and ...j_final_labels to annotations... and aggregated... tables respectively.
    mark job in all_jobs as completed
    """
    processed_3a_amt_part_2: bool = job_dao.do_bookkeeping_3a_amt_part_2(
        obj_job=obj_job,
        annotations_per_tuple_per_worker_table_name=annotations_per_tuple_per_worker_table_name,
        aggregated_annotations_table_name=aggregated_annotations_table_name
    )
    print('processed_3a_amt_part_2: ', processed_3a_amt_part_2)
    return processed_3a_amt_part_2


def parse_layout_file(layout_file_path: Path):
    """Parse file that contains information on how to represent task data"""
    # layout file contains layout for representing task
    layout_intermediate_representation: dict = {}
    if layout_file_path:
        # parse layout file and put in table
        with layout_file_path.open() as file:
            contents = file.read()
            a, b = contents.find(settings.DESIGN_LAYOUT_BEGIN), contents.find(settings.DESIGN_LAYOUT_END)
            design_layout = contents[a + len(settings.DESIGN_LAYOUT_BEGIN):b]
            layout_intermediate_representation['design_layout'] = design_layout
    else:
        layout_intermediate_representation['design_layout'] = settings.DEFAULT_LAYOUT
    return layout_intermediate_representation


def get_job_identifiers(request):
    """
    Return the set of identifiers which identify a job
    :param
        request: HttpRequest
    :return:
        user_id: int
        project_id: int
        workflow_id: int
        run_id: int
        job_id: int
    """
    requester_id = request.GET.get('uid', -1)
    project_id = request.GET.get('pid', -1)
    workflow_id = request.GET.get('wid', -1)
    run_id = request.GET.get('rid', -1)
    job_id = request.GET.get('jid', -1)
    return requester_id, project_id, workflow_id, run_id, job_id
