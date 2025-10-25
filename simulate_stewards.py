"""
Simulate stewards on a job, to run synthetic bulk exp with stewards.
A. Helper files
    a. A static pool of stewards in stewards.csv which contains the username and password of the stewards.
    b. A log file for the current simulation. Create a log file for the current simulation. Include the current timestamp in the file name. The file name should be of the format: steward_simulation_YYYYMMDD_HHMMSS.log
B. Input simulation parameters.
    a. User inputs the target job's information. Store the target job info in the log file.
    b. User inputs the simulation parameters. Store the simulation parameters in the log file.
C. Run the simulation.
    a. Run the overall simulation loop.
    b. For each loop point, run the stewards simulation loop. From a pool of stewards in stewards.csv, pick the number of stewards to be used in this loop point.
    c. When each steward is finished, store the work statistics in the log file.

"""
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

def create_log():
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_file_name = Path(f'steward_simulation_{timestamp}.log')
    log_file = open(log_file_name, 'w')
    return log_file

def get_list_of_stewards():
    list_of_stewards: list = []
    with open('stewards.csv', 'r') as f:
        reader = csv.reader(f)
        # skip header row
        next(reader)
        for row in reader:
            list_of_stewards.append({
                'username': row[0],
                'password': row[1]
            })
    return list_of_stewards

def set_target_job_info(job_id: int, run_id: int, workflow_id: int, project_id: int, user_id: int, size_data_job: int, job_category: str, gold_label_column_name: str):
    """Get the target job from the database"""
    target_job_info = {
        'job_id': job_id,
        'run_id': run_id,
        'workflow_id': workflow_id,
        'project_id': project_id,
        'user_id': user_id,
        'size_data_job': size_data_job,
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
    workers_per_burst: int,
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

def simulate_stewards_on_job(simulation_parameters: dict, job_info: dict, log_file=None):
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
    
    list_of_stewards = get_list_of_stewards()
    print('List of stewards: ', list_of_stewards)

    for i in range(0, loop_times):
        start_ts = time.time()
        parameters_info: dict = {}
        statistics_info: dict = {}
        # loop point (burst) identifier
        parameters_info['identifier'] = i
        statistics_info['identifier'] = i
        workers_in_this_burst = workers_per_burst
        parameters_info['number_workers'] = workers_in_this_burst
        j = 0
        while j < workers_in_this_burst:
            steward = list_of_stewards.pop(0)
            y = threading.Thread(
                target=run_steward_pipeline,
                args=(
                    steward['username'], 
                    steward['password'], 
                    accuracies[i],
                    annotation_times[i],
                    time_durations[i],
                    job_info, 
                    log_file
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

def run_steward_pipeline(user_name: str, password: str, accuracy: float, annotation_time: int, time_duration: int, job_info: dict, log_file=None):
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

    # 1. signup (Not applicable for stewards since they are already created by admin)

    # 2. login
    login_url = target_url + '/login/'
    call_login(s, login_url, user_name, password)

    # 3. dashboard
    call_dashboard(s, target_url)

    # 4. job index
    job_index_url = target_url + '/controller/?category=' + job_category + '&action=index'
    call_job_index(s, job_index_url)

    # basic computation for determining number of labels to match in simulation
    # target_p = worker_accuracy
    total_size_tuples = int(job_info['size_data_job'])
    # target_total_matches = int(target_p * float(total_size_tuples))
    # target_total_non_matches = int(total_size_tuples) - target_total_matches
    total_matches_so_far = 0
    total_annotated_so_far = 0
    
    start_ts = time.time()
    print('Worker time duration: ', worker_time_duration)
    print('Start time: ', start_ts)
    print('Current time: ', time.time())
    print('Time difference: ', time.time() - start_ts)
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
        print('Worker code: ', worker_code)
        if worker_code == 3:    # QUIT
            # store worker parameters that got decided based on simulation parameters
            store_actual_simulation_parameters_per_worker(
                worker_username=user_name, 
                worker_reliability=worker_accuracy,
                worker_annotation_time=worker_annotation_time,
                user_name=user_name,
                log_file=log_file
            )
            # compute statistics of worker, pertaining to worker's interaction with cymphony
            (precision, recall) = compute_worker_statistics(
                total_matches_so_far=total_matches_so_far,
                total_size_tuples=total_size_tuples,
                total_annotated_so_far=total_annotated_so_far,
                accuracy=worker_accuracy,
                user_name=user_name
            )
            # store the above calculated worker statistics
            store_actual_simulation_statistics_per_worker(
                worker_username=user_name, 
                worker_precision=precision,
                worker_recall=recall,
                total_matches_so_far=total_matches_so_far,
                total_annotated_so_far=total_annotated_so_far,
                total_size_tuples=total_size_tuples,
                user_name=user_name,
                log_file=log_file
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
                # label_annotate_with is set to gold_label or not based on below computation
                # once hit intended number of matches, always go into the non-matching section of code,
                # once hit the number of non-matches, always go into the matching section.
                # This will ensure the actual worker accuracy to be exactly equal to
                #   intended accuracy in case worker annotates all tuples.
                # If worker does not get to annotate all tuples in data, probability based matching is done,
                #   which is good since it will not allow first all yes, then all no,
                #   but all no got eliminated because worker never got to annotate later tuples.
                # So, probability based matching is the best possible case already in that scenario,
                #   and will ensure worker accuracy to be closest to intended accuracy of worker.
                
                # total_non_matches_so_far = total_annotated_so_far - total_matches_so_far
                # if total_non_matches_so_far >= target_total_non_matches:
                #     # print("Non matches had exhausted.")
                #     # all intended non-matches exhausted, choose to match
                #     label_to_annotate_with = gold_label
                #     total_matches_so_far = total_matches_so_far + 1
                # elif total_matches_so_far >= target_total_matches:
                #     # print("Matches had exhausted.")
                #     # all intended matches exhausted, choose to not match
                #     # any label (out of the available options) but the gold label
                #     if task_option_list is None:    # free text answer
                #         label_to_annotate_with = gold_label + str(random.randint(1,10))
                #     else:   # answer has to be from a set of choices
                #         choices: list = task_option_list.copy()
                #         choices.remove(gold_label)
                #         label_list: list = random.choices(choices, k=1)
                #         label_to_annotate_with = label_list[0]
                # else:  # edge cases did not get hit, so go with normal case of picking based on probability.
                #     # print("Probability based choice selection")
                #     match = random.choices([0, 1], weights=(target_total_non_matches, target_total_matches), k=1)
                #     if match[0] == 1:
                #         label_to_annotate_with = gold_label
                #         total_matches_so_far = total_matches_so_far + 1
                #     else:
                #         # any label (out of the available options) but the gold label
                #         if task_option_list is None:  # free text answer
                #             label_to_annotate_with = gold_label + str(random.randint(1, 10))
                #         else:  # answer has to be from a set of choices
                #             choices = task_option_list.copy()
                #             choices.remove(gold_label)
                #             label_list: list = random.choices(choices, k=1)
                #             label_to_annotate_with = label_list[0]
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

                print('Total annotated so far: ', total_annotated_so_far)
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
                        user_name=user_name,
                        log_file=log_file
                    )
                    # compute statistics of worker, pertaining to worker's interaction with cymphony
                    (precision, recall) = compute_worker_statistics(
                        total_matches_so_far=total_matches_so_far,
                        total_size_tuples=total_size_tuples,
                        total_annotated_so_far=total_annotated_so_far,
                        accuracy=worker_accuracy,
                        user_name=user_name
                    )
                    # store the above calculated worker statistics
                    store_actual_simulation_statistics_per_worker(
                        worker_username=user_name, 
                        worker_precision=precision,
                        worker_recall=recall,
                        total_matches_so_far=total_matches_so_far,
                        total_annotated_so_far=total_annotated_so_far,
                        total_size_tuples=total_size_tuples,
                        user_name=user_name,
                        log_file=log_file
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
            user_name=user_name,
            log_file=log_file
        )
        # compute statistics of worker, pertaining to worker's interaction with cymphony
        (precision, recall) = compute_worker_statistics(
            total_matches_so_far=total_matches_so_far,
            total_size_tuples=total_size_tuples,
            total_annotated_so_far=total_annotated_so_far,
            accuracy=worker_accuracy,
            user_name=user_name
        )
        # store the above calculated worker statistics
        store_actual_simulation_statistics_per_worker(
            worker_username=user_name, 
            worker_precision=precision,
            worker_recall=recall,
            total_matches_so_far=total_matches_so_far,
            total_annotated_so_far=total_annotated_so_far,
            total_size_tuples=total_size_tuples,
            user_name=user_name,
            log_file=log_file
        )
        # worker has finished interacting with cymphony
        print('Worker time duration exceeded. Returning.')
        return
    return

def store_actual_simulation_parameters_per_worker(worker_username: str, worker_reliability: float, worker_annotation_time: int, user_name: str, log_file=None):
    """Store the actual simulation parameters for a worker in the log file"""
    if log_file is None:
        log_file = create_log()
    log_file.write(f'Actual simulation parameters for worker {worker_username}: worker_reliability={worker_reliability}, worker_annotation_time={worker_annotation_time}, user_name={user_name}\n')
    return

def store_actual_simulation_statistics_per_worker(worker_username: str, worker_precision: float, worker_recall: float, total_matches_so_far: int, total_annotated_so_far: int, total_size_tuples: int, user_name: str, log_file=None):
    """Store the actual simulation statistics for a worker in the log file"""
    if log_file is None:
        log_file = create_log()
    log_file.write(f'Actual simulation statistics for worker {worker_username}: worker_precision={worker_precision}, worker_recall={worker_recall}, total_matches_so_far={total_matches_so_far}, total_annotated_so_far={total_annotated_so_far}, total_size_tuples={total_size_tuples}, user_name={user_name}\n')
    return


# 1. signup
def call_signup(s, url, data):
    method = 'POST'
    signup_response = s.post(url, data=data)
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
        total_size_tuples, 
        total_annotated_so_far,
        accuracy,
        user_name
):
    print(f'Total matches made by worker {user_name}: {total_matches_so_far}')
    # recall
    print(f'Total annotated by worker {user_name}: {total_annotated_so_far}')
    print(f'Total size(tuples) of data belonging to the job: {total_size_tuples}')
    recall: float = -1.0
    if total_size_tuples > 0:
        recall = float(total_annotated_so_far) / total_size_tuples
        print(f'Fraction of total tuples annotated by this worker {user_name}: {recall}')
    else:
        recall = -1.0
    # precision
    precision: float = -1.0
    print(f'Intended accuracy of worker {user_name} based on supplied parameter: {accuracy}')
    if total_annotated_so_far > 0:
        precision = float(total_matches_so_far) / total_annotated_so_far
        print(f'Actual accuracy of worker {user_name}: {precision}')
    else:
        precision = -1.0
        print(f'Could not compute actual accuracy of worker {user_name} because of 0 annotations.')
    return precision, recall

def main():
    # create a log
    log_file = create_log()

    # input from user
    user_id = input('Enter the user ID: ')
    project_id = input('Enter the project ID: ')
    workflow_id = input('Enter the workflow ID: ')
    run_id = input('Enter the run ID: ')
    job_id = input('Enter the job ID: ')
    size_data_job = int(input('Enter the size of data in job: '))
    job_category = 'job_3a_knlm'
    gold_label_column_name = 'gold_label'

    target_job_info = set_target_job_info(
        job_id=job_id,
        run_id=run_id,
        workflow_id=workflow_id,
        project_id=project_id,
        user_id=user_id,
        size_data_job=size_data_job,
        job_category=job_category,
        gold_label_column_name=gold_label_column_name
    )
    store_target_job_info(target_job_info, log_file)

    loop_times = 2
    workers_per_burst = 1
    time_gaps = [1800, 0]   # 30 minutes
    time_durations = [3600, 7200]   # 1 hour, 2 hours
    annotation_times = [10, 10]
    accuracies = [0.95, 0.95]
    simulation_parameters = set_simulation_parameters(
        loop_times=loop_times,
        workers_per_burst=workers_per_burst,
        time_gaps=time_gaps,
        time_durations=time_durations,
        annotation_times=annotation_times,
        accuracies=accuracies
    )
    store_simulation_parameters(simulation_parameters, log_file)

    simulate_stewards_on_job(simulation_parameters, target_job_info, log_file)
    return

if __name__ == '__main__':
    main()