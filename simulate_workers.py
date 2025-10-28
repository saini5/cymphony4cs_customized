"""
Simulate workers on a job, to run synthetic drive-by exp with workers.
A. Helper files
    a. A log file for the current simulation. Create a log file for the current simulation. 
    Include the current timestamp in the file name. The file name should be of the format: steward_simulation_YYYYMMDD_HHMMSS.log
    b. A csv file to store per worker statistics.
B. Input simulation parameters.
    a. User inputs the target job's information. Store the target job info in the log file.
    b. User inputs the simulation parameters. Store the simulation parameters in the log file.
C. Run the simulation.
    a. Run the overall simulation loop.
    b. For each loop point, run the worker simulation loop. 
    c. When each worker is finished, store the work statistics in the log file.

"""

from datetime import datetime
import random
import time
import threading
import csv
from requests import Session
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import logging
from pathlib import Path


# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

file_lock = threading.Lock()

def create_log():
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_file_name = Path(f'steward_simulation_{timestamp}.log')
    log_file = open(log_file_name, 'w')
    return log_file

def create_csv():
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    csv_file_name = Path(f'worker_statistics_{timestamp}.csv')
    with open(csv_file_name, 'w') as csv_file:
        csv_file.write('worker_username,worker_precision,total_matches,total_annotated\n')
    return csv_file_name

def set_target_job_info(run_id: int, workflow_id: int, project_id: int, user_id: int, job_category: str, gold_label_column_name: str, job_id: int = -1):
    """Get the target job from the database"""
    target_job_info = {
        'job_id': job_id,
        'run_id': run_id,
        'workflow_id': workflow_id,
        'project_id': project_id,
        'user_id': user_id,
        'job_category': job_category,
        'gold_label_column_name': gold_label_column_name
    }
    return target_job_info

def store_target_job_info(target_job_info: dict, log_file=None):
    """Store the target job info in the log file"""
    if log_file is None:
        log_file = create_log()
    log_file.write(f'Target job info: {target_job_info}\n')
    return

def set_simulation_parameters(
    loop_times: int,
    workers_per_burst: list,
    time_gaps: list,
    time_durations: list,
    annotation_times: list,
    accuracies: list
):
    """Set the simulation parameters from the user"""
    simulation_parameters = {
        'loop_times': loop_times,
        'workers_per_burst': workers_per_burst,
        'time_gaps': time_gaps,
        'time_durations': time_durations,
        'annotation_times': annotation_times,
        'accuracies': accuracies
    }
    return simulation_parameters

def store_simulation_parameters(simulation_parameters: dict, log_file=None):
    """Store the simulation parameters in the log file"""
    if log_file is None:
        log_file = create_log()
    log_file.write(f'Simulation parameters: {simulation_parameters}\n')
    return

def simulate_workers_on_job(simulation_parameters: dict, job_info: dict, log_file=None, csv_file=None):
    """Generates synthetic workers and makes them work on the job based on the specified simulation params"""

    loop_times = simulation_parameters['loop_times']
    workers_per_burst = simulation_parameters['workers_per_burst']
    time_gaps = simulation_parameters['time_gaps']
    time_durations = simulation_parameters['time_durations']
    annotation_times = simulation_parameters['annotation_times']
    accuracies = simulation_parameters['accuracies']
    
    # will store these in the log file, just after simulating all workers below
    parameters_simulation_workers: list = []
    statistics_simulation_workers: list = []

    for i in range(0, loop_times):
        start_ts = time.time()
        parameters_info: dict = {}
        statistics_info: dict = {}
        # loop point (burst) identifier
        parameters_info['identifier'] = i
        statistics_info['identifier'] = i
        workers_in_this_burst = workers_per_burst[i]
        parameters_info['number_workers'] = workers_in_this_burst
        j = 0
        while j < workers_in_this_burst:
            y = threading.Thread(
                target=run_worker_pipeline,
                args=( 
                    accuracies[i],
                    annotation_times[i],
                    time_durations[i],
                    job_info, 
                    log_file,
                    csv_file
                )
            )
            y.start()
            j = j + 1
        # TODO: this should be number of actual spawned (signed up) workers in this loop point
        # hence, it is dependent on all worker pipelines merging back into this code here.
        statistics_info['number_workers'] = workers_in_this_burst
        time_gap = time_gaps[i]
        parameters_info['time_gap'] = time_gap
        parameters_simulation_workers.append(parameters_info)
        time.sleep(time_gap)
        end_ts = time.time()
        time_elapsed = int(end_ts - start_ts)    # time between this and next loop points (in seconds)
        statistics_info['time_gap'] = time_elapsed
        statistics_simulation_workers.append(statistics_info)
    # store parameters corresponding to simulation of workers
    # these parameters got decided based on the broad ranged job simulation parameters
    store_actual_simulation_parameters(parameters_simulation_workers, log_file)
    # store statistics corresponding to simulation of workers
    # these statistics were computed based on actual execution
    store_actual_simulation_statistics(statistics_simulation_workers, log_file)

def store_actual_simulation_parameters(parameters_simulation_workers: list, log_file=None):
    """Store the actual simulation parameters in the log file"""
    if log_file is None:
        log_file = create_log()
    log_file.write(f'Actual simulation parameters: {parameters_simulation_workers}\n')
    return

def store_actual_simulation_statistics(statistics_simulation_workers: list, log_file=None):
    """Store the actual simulation statistics in the log file"""
    if log_file is None:
        log_file = create_log()
    log_file.write(f'Actual simulation statistics: {statistics_simulation_workers}\n')
    return

def run_worker_pipeline(accuracy: float, annotation_time: int, time_duration: int, job_info: dict, log_file=None, csv_file=None):
    """Simulating the working of one single synthetic worker"""
    job_category = job_info['job_category']
    GOLD_LABEL_COLUMN_NAME = job_info['gold_label_column_name']
    MIN_WORKER_RETRY_DELAY = 1
    MAX_WORKER_RETRY_DELAY = 60

    # worker characteristics
    worker_annotation_time = annotation_time
    worker_accuracy = accuracy
    worker_time_duration = time_duration
    # target characteristics
    target_url = 'http://localhost:8000'
    # initializations
    s = Session()
    submit_retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS"]
    )
    s.mount(target_url, HTTPAdapter(max_retries=submit_retries))
    s.headers = {'User-Agent': 'python-requests/1.2.0'}

    user_name = 'syn_' + \
        'u_' + str(job_info['user_id']) + \
        'p_' + str(job_info['project_id']) + \
        'w_' + str(job_info['workflow_id']) + \
        'r_' + str(job_info['run_id']) + \
        '__' + str(datetime.now().strftime("%Y%m%d_%H%M%S"))
    password = 'This1sATe5t'

    # 1. signup 
    signup_url = target_url + '/one_step_register/'     # This url skips the activation link verification process
    signup_data = {
        'first_name': user_name,
        'last_name': user_name,
        # TODO (bypassed it instead): will have to disable email uniqueness and verification check
        'email': user_name + '@email.com',
        'username': user_name,
        'password1': password,
        'password2': password,
    }
    call_signup(s, signup_url, signup_data)
    
    # 2. login
    # not needed since one_step_register based signup logs the worker in automatically.
    # login_url = target_url + '/login/'
    # call_login(s, login_url, user_name, password)

    # 3. dashboard
    call_dashboard(s, target_url)

    # 4. job index
    job_index_url = target_url + '/controller/?category=' + job_category + '&action=index'
    job_response = call_job_index(s, job_index_url)
    job_response_json = job_response.json()
    # print('Job response JSON: ', job_response_json)
    job_listing = job_response_json.get('list_3a_knlm_jobs') if job_category == 'job_3a_knlm' else job_response_json.get('list_3a_kn_jobs')
    if job_listing is None:
        print('No job listing found. Exiting.')
        return
    target_jobs: list = []
    for job in job_listing:
        # print("Job: ", job, type(job.get('project_id')))
        # print("Job Info: ", job_info, type(job_info['project_id']))
        if job.get('project_id') == job_info['project_id'] and job.get('workflow_id') == job_info['workflow_id'] and job.get('run_id') == job_info['run_id']:
            target_jobs.append(job)
    if len(target_jobs) > 1:
        print('Multiple jobs found in this run. Exiting.')
        return
    elif len(target_jobs) == 0:
        print('No job found. Exiting.')
        return
    else:
        target_job = target_jobs[0]
        job_info['job_id'] = target_job.get('job_id')
        # print('My target job is: ', target_job)

    total_matches_so_far = 0
    total_annotated_so_far = 0
    
    start_ts = time.time()
    # print('Worker time duration: ', worker_time_duration)
    # print('Start time: ', start_ts)
    # print('Current time: ', time.time())
    # print('Time difference: ', time.time() - start_ts)
    worker_code = None
    while (time.time() - start_ts) < worker_time_duration:
        # 5. call work on job
        work_on_job_url = target_url + \
                          '/controller/?category=' + job_category + '&action=work&uid={0}&pid={1}&wid={2}&rid={3}&jid={4}'.format(
                              job_info['user_id'], job_info['project_id'], job_info['workflow_id'], job_info['run_id'], job_info['job_id']
                          )
        call_work_on_job_response = call_work_on_job(
            s, work_on_job_url
        )
        
        call_work_on_job_json_response = call_work_on_job_response.json()
        work_response_information: dict = {}
        for key, value in call_work_on_job_json_response.items():
            work_response_information[key] = value
        worker_code = work_response_information.get('worker_code')
        # print('Worker code: ', worker_code)
        if worker_code == 3:    # QUIT
            # store worker parameters that got decided based on simulation parameters
            store_actual_simulation_parameters_per_worker(
                worker_username=user_name, 
                worker_reliability=worker_accuracy,
                worker_annotation_time=worker_annotation_time,
                log_file=log_file
            )
            # compute statistics of worker, pertaining to worker's interaction with cymphony
            precision = compute_worker_statistics(
                total_matches_so_far=total_matches_so_far,
                total_annotated_so_far=total_annotated_so_far,
                accuracy=worker_accuracy,
                user_name=user_name
            )
            # store the above calculated worker statistics
            store_actual_simulation_statistics_per_worker(
                worker_username=user_name, 
                worker_precision=precision,
                total_matches_so_far=total_matches_so_far,
                total_annotated_so_far=total_annotated_so_far,
                log_file=log_file,
                csv_file=csv_file
            )
            # worker has finished interacting with cymphony
            # print("Thread finishing for worker with username: ", user_name)
            return
        elif worker_code == 1:  # ANNOTATE
            # get gold label and available answer options from the 'work on job' response
            header_value_dict = work_response_information.get('header_value_dict')
            gold_label = header_value_dict.get(GOLD_LABEL_COLUMN_NAME)
            # print('gold_label: ', gold_label)
            task_option_list = work_response_information.get('task_option_list')
            # print('task_option_list: ', task_option_list)
            while (time.time() - start_ts) < worker_time_duration:
                # decide on the label to annotate with
                label_to_annotate_with = None
                # worker takes some time to decided his/her choice
                time.sleep(worker_annotation_time)
                
                if random.random() < worker_accuracy:   # Tosses a coin with say 83% probability of being correct, where accuracy is the probability of being correct.
                    label_to_annotate_with = gold_label
                    total_matches_so_far = total_matches_so_far + 1
                else:
                    if task_option_list is None:  # free text answer
                        label_to_annotate_with = gold_label + str(random.randint(1, 10))
                    else:  # answer has to be from a set of choices
                        choices = task_option_list.copy()
                        choices.remove(gold_label)
                        label_list: list = random.choices(choices, k=1)
                        label_to_annotate_with = label_list[0]

                # print('Total annotated so far: ', total_annotated_so_far)
                total_annotated_so_far = total_annotated_so_far + 1
                # send the label to annotate with as well
                
                submit_annotation_url = target_url + '/controller/?category=' + job_category + '&action=process_annotation'
                call_submit_annotation_response = call_submit_annotation(
                    s,
                    submit_annotation_url,
                    label_to_annotate_with
                )
                
                # analyze the "submit" response
                call_submit_annotation_json_response = call_submit_annotation_response.json()
                submit_annotation_response_information: dict = {}
                for key, value in call_submit_annotation_json_response.items():
                    submit_annotation_response_information[key] = value
                worker_code = submit_annotation_response_information.get('worker_code')
                if worker_code == 3:    # QUIT
                    # store worker parameters that got decided based on simulation parameters
                    store_actual_simulation_parameters_per_worker(
                        worker_username=user_name, 
                        worker_reliability=worker_accuracy,
                        worker_annotation_time=worker_annotation_time,
                        log_file=log_file
                    )
                    # compute statistics of worker, pertaining to worker's interaction with cymphony
                    precision = compute_worker_statistics(
                        total_matches_so_far=total_matches_so_far,
                        total_annotated_so_far=total_annotated_so_far,
                        accuracy=worker_accuracy,
                        user_name=user_name
                    )
                    # store the above calculated worker statistics
                    store_actual_simulation_statistics_per_worker(
                        worker_username=user_name, 
                        worker_precision=precision,
                        total_matches_so_far=total_matches_so_far,
                        total_annotated_so_far=total_annotated_so_far,
                        log_file=log_file,
                        csv_file=csv_file
                    )
                    # worker has finished interacting with cymphony
                    # print("Thread finishing for worker with username: ", user_name)
                    return
                elif worker_code == 1:  # ANNOTATE
                    header_value_dict = submit_annotation_response_information.get('header_value_dict')
                    gold_label = header_value_dict.get(GOLD_LABEL_COLUMN_NAME)
                    # print('gold_label: ', gold_label)
                    task_option_list = submit_annotation_response_information.get('task_option_list')
                    # print('task_option_list: ', task_option_list)
                    # continue
                    pass
                else: # retry annotating after some time, but will have to go through work on job this time
                    break
            if (time.time() - start_ts) >= worker_time_duration:
                print('Worker time duration exceeded. Breaking out of the loop.')
                break
        else:   # retry working on job after some time
            pass
        delay = random.randint(MIN_WORKER_RETRY_DELAY, MAX_WORKER_RETRY_DELAY)   # eg: 1 minute delay window if (1, 60)
        # print('worker with username: ', user_name, ' will retry after ', delay, ' seconds')
        time.sleep(delay)
        # print('worker with username: ', user_name, ' is retrying now after a delay of ', delay, ' seconds ')
    # once this function ends we either need to pass the session up to the
    # calling function or it will be gone forever
    # Let's let it shut down forever, since every worker should have a different requests session
    if (time.time() - start_ts) >= worker_time_duration:
        if worker_code == 1: # LEaving in the middle of annotating.
            print('Leaving in the middle of annotating.')
            quit_job_url = target_url + \
                          '/controller/?category=' + job_category + '&action=quit&uid={0}&pid={1}&wid={2}&rid={3}&jid={4}'.format(
                              job_info['user_id'], job_info['project_id'], job_info['workflow_id'], job_info['run_id'], job_info['job_id']
                          )
            quit_job_response = call_quit_job(
                s, quit_job_url
            )
            print('Quit job response: ', quit_job_response.json())

        print('Worker time duration exceeded. Time duration: ', time.time() - start_ts)
        print('Storing worker parameters and statistics.')
        # store worker parameters that got decided based on simulation parameters
        store_actual_simulation_parameters_per_worker(
            worker_username=user_name, 
            worker_reliability=worker_accuracy,
            worker_annotation_time=worker_annotation_time,
            log_file=log_file
        )
        # compute statistics of worker, pertaining to worker's interaction with cymphony
        precision = compute_worker_statistics(
            total_matches_so_far=total_matches_so_far,
            total_annotated_so_far=total_annotated_so_far,
            accuracy=worker_accuracy,
            user_name=user_name
        )
        # store the above calculated worker statistics
        store_actual_simulation_statistics_per_worker(
            worker_username=user_name, 
            worker_precision=precision,
            total_matches_so_far=total_matches_so_far,
            total_annotated_so_far=total_annotated_so_far,
            log_file=log_file,
            csv_file=csv_file
        )
        # worker has finished interacting with cymphony
        print('Worker time duration exceeded. Returning.')
        return
    return

def store_actual_simulation_parameters_per_worker(worker_username: str, worker_reliability: float, worker_annotation_time: int, log_file=None):
    """Store the actual simulation parameters for a worker in the log file"""
    if log_file is None:
        log_file = create_log()
    log_file.write(f'Actual simulation parameters for worker {worker_username}: worker_reliability={worker_reliability}, worker_annotation_time={worker_annotation_time}\n')
    return

def store_actual_simulation_statistics_per_worker(worker_username: str, worker_precision: float, total_matches_so_far: int, total_annotated_so_far: int, log_file=None, csv_file=None):
    """Store the actual simulation statistics for a worker in the log file"""
    if log_file is None:
        log_file = create_log()
    log_file.write(f'Actual simulation statistics for worker {worker_username}: worker_precision={worker_precision}, total_matches_so_far={total_matches_so_far}, total_annotated_so_far={total_annotated_so_far}\n')
    
    # also append username, precision, recall, total_matches, total_annotated, total_tuples per worker in a csv file.
    if csv_file is None:
        csv_file = create_csv()
    with file_lock:
        with open(csv_file, 'a') as f:
            f.write(f'{worker_username},{worker_precision},{total_matches_so_far},{total_annotated_so_far}\n')
    
    return


# 1. signup
def call_signup(s, url, data):
    method = 'POST'
    signup_response = s.post(url, data=data)

    signup_response.raise_for_status()

    # Check if signup was successful by examining the redirect URL
    # If redirected back to signup URL, it's a failure.
    if signup_response.url == url:
        logger.error(f"Signup/Login failed. Response URL: {signup_response.url}")
        raise Exception("Signup failed. Check username/password or Cymphony logs.")

    logger.info(f"Signed up and logged in to {url} with username {data['username']} successfully.")
    return signup_response


# 2. login
def call_login(session, login_url, username, password):
    # Step 1: GET the login page to obtain the CSRF token
    logger.info(f"GETting login page to retrieve CSRF token from {login_url}")
    get_response = session.get(login_url)
    get_response.raise_for_status()
    
    # Extract CSRF token from cookies
    csrftoken = session.cookies.get('csrftoken')
    if not csrftoken:
        raise Exception("CSRF token not found in login page cookies. Login page might have changed.")
    session.headers.update({'X-CSRFToken': csrftoken})
    
    # Step 2: POST login credentials with the obtained CSRF token
    logger.info(f"POSTing login credentials to {login_url}")
    response = session.post(
        login_url, 
        data={
            'username': username, 
            'password': password,
            'csrfmiddlewaretoken': csrftoken # Include the CSRF token in POST data
        },
        # auth=(username, password) # Basic Auth is typically for stateless APIs, not form logins
                                    # Keep it commented for now as LoginView primarily relies on session + CSRF
    )
    response.raise_for_status()

    # Check if login was successful by examining the redirect URL
    # If redirected back to login URL, it's a failure.
    if response.url == login_url:
        logger.error(f"Login failed. Response URL: {response.url}")
        raise Exception("Login failed. Check username/password or Cymphony logs.")

    logger.info(f"Logged in {username}. CSRF Token: {session.headers.get('X-CSRFToken')}")


# 3. dashboard
def call_dashboard(s, url):
    data = None
    dashboard_response = s.get(url, data=data)
    return dashboard_response


# 4. job index
def call_job_index(s, url):
    method = 'GET'
    data = None
    jobs_listing_response = s.get(url, data=data)
    return jobs_listing_response


# 5. work on specific job
def call_work_on_job(s, url):
    method = 'GET'
    data = None
    task_to_be_annotated_response = s.get(url, data=data)
    return task_to_be_annotated_response

def call_quit_job(s, url):
    method = 'GET'
    data = None
    quit_job_response = s.get(url, data=data)
    return quit_job_response


# 6. submit annotation
def call_submit_annotation(s, url, label_to_annotate_with):
    method = 'POST'
    data = {
        'choice': label_to_annotate_with
    }
    task_annotated_response = s.post(url, data=data)
    return task_annotated_response


def compute_worker_statistics(
        total_matches_so_far,
        total_annotated_so_far,
        accuracy,
        user_name
):
    print(f'Total matches made by worker {user_name}: {total_matches_so_far}')
    print(f'Total annotated by worker {user_name}: {total_annotated_so_far}')
    # precision
    precision: float = -1.0
    print(f'Intended accuracy of worker {user_name} based on supplied parameter: {accuracy}')
    if total_annotated_so_far > 0:
        precision = float(total_matches_so_far) / total_annotated_so_far
        print(f'Actual accuracy of worker {user_name}: {precision}')
    else:
        precision = -1.0
        print(f'Could not compute actual accuracy of worker {user_name} because of 0 annotations.')
    return precision

def simulate_bulk_with_regular_workers(composite_run_id, job_category='job_3a_knlm', gold_label_column_name='gold_label'):
    # create a log
    log_file = create_log()
    csv_file = create_csv()

    user_id, project_id, workflow_id, run_id = composite_run_id.split('.')

    target_job_info = set_target_job_info(
        run_id=int(run_id),
        workflow_id=int(workflow_id),
        project_id=int(project_id),
        user_id=int(user_id),
        job_category=job_category,
        gold_label_column_name=gold_label_column_name
    )
    store_target_job_info(target_job_info, log_file)

    loop_times = 100
    workers_per_burst = [1 for _ in range(loop_times)]
    time_gaps = [120 for _ in range(loop_times)]
    time_gaps[-1] = 0
    # The usage of loop_times in the below means that we assume all workers in the same burst to have the same values for these parameters.
    # Doesn't matter right now since we are using 1 worker per burst.
    # Later, can be changed by using sum(workers_per_burst), and then changing the parameters in the thread call underneath.
    time_durations = [float('inf') for _ in range(loop_times)]
    annotation_times = [10 for _ in range(loop_times)]
    accuracies = [random.uniform(0.8, 0.85) for _ in range(loop_times)]
    
    simulation_parameters = set_simulation_parameters(
        loop_times=loop_times,
        workers_per_burst=workers_per_burst,
        time_gaps=time_gaps,
        time_durations=time_durations,
        annotation_times=annotation_times,
        accuracies=accuracies
    )
    store_simulation_parameters(simulation_parameters, log_file)

    simulate_workers_on_job(simulation_parameters, target_job_info, log_file, csv_file)

    return

if __name__ == '__main__':
    user_id = input('Enter the user ID: ')
    project_id = input('Enter the project ID: ')
    workflow_id = input('Enter the workflow ID: ')
    run_id = input('Enter the run ID: ')
    composite_run_id = f"{user_id}.{project_id}.{workflow_id}.{run_id}"
    simulate_bulk_with_regular_workers(composite_run_id)