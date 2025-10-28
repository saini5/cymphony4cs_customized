# Environment installed via conda create -n dummy-smartcat python=3.12.3 flask=3.1.2 requests=2.32.5 urllib3=2.5.0
# Create Smartcat User in Cymphony via python manage.py manage_roles_and_users --create_user "smartcat" "smartcat_password"

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# The flask import is part of the conda environment setup, so it's not a direct dependency in requirements.txt
from flask import Flask, request, jsonify
import threading
import time
import json
import logging
from pathlib import Path
import shutil
import random
import csv

from simulate_workers import simulate_bulk_with_regular_workers

# --- Configuration ---
CYMPHONY_URL = 'http://127.0.0.1:8000' # Your Cymphony instance URL
SMARTCAT_WEBHOOK_URL = 'http://127.0.0.1:5000/webhook' # Fake Smartcat's webhook receiver URL
SMARTCAT_USERNAME = 'smartcat' # Cymphony user for Smartcat
SMARTCAT_PASSWORD = 'smartcat_password' # Password for Smartcat user
# WEBHOOK_SECRET = 'mys_webhook_secret' # Shared secret for webhook signature verification

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cymphony API Client ---
class CymphonyClient:
    def __init__(self, base_url):
        self.base_url = base_url
        self.session = requests.Session()
        # Set up retry mechanism
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({'User-Agent': 'FakeSmartcat-python-requests/1.0'}) # TODO: Cymphony currently returns json response only if 'python' in user-agent. How can I change it so that it returns json response whenever the request is a web api request and not from the browser.

    def _post(self, endpoint, data=None, files=None, json=None):
        if json is not None and (data or files):
            raise ValueError("Pass either json=... or (data=..., files=...), not both.")
        
        url = f"{self.base_url}{endpoint}"
        if json is not None:
            logger.info(f"POSTing to {url} with json: {json}")
            response = self.session.post(url, json=json)
        else:
            logger.info(f"POSTing to {url} with data: {data}")
            response = self.session.post(url, data=data, files=files) # For form-urlencoded, if any
        response.raise_for_status() # Raise an exception for HTTP errors
        # Can sometimes receive a zip, so branch on content-type
        if response.headers.get('Content-Type') == 'application/zip':
            return response.content
        else:
            return response.json()

    def _get(self, endpoint, params=None):
        url = f"{self.base_url}{endpoint}"
        logger.info(f"GETting from {url} with params: {params}")
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def login(self, username, password):
        login_url = f"{self.base_url}/login/"
        
        # Step 1: GET the login page to obtain the CSRF token
        logger.info(f"GETting login page to retrieve CSRF token from {login_url}")
        get_response = self.session.get(login_url)
        get_response.raise_for_status()
        
        # Extract CSRF token from cookies
        csrftoken = self.session.cookies.get('csrftoken')
        if not csrftoken:
            raise Exception("CSRF token not found in login page cookies. Login page might have changed.")
        self.session.headers.update({'X-CSRFToken': csrftoken})
        
        # Step 2: POST login credentials with the obtained CSRF token
        logger.info(f"POSTing login credentials to {login_url}")
        response = self.session.post(
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

        logger.info(f"Logged in {username}. CSRF Token: {self.session.headers.get('X-CSRFToken')}")

    def create_curation_run(self, dict_file_paths, data_file_id_field_name, notification_url):
        """Create Cymphony based curation run."""

        # The receiver expects the below
        # workflow_file = request.FILES.get('workflow_file')
        # data_file = request.FILES.get('data_file')
        # id_field_name = request.POST.get('id_field_name', None)
        # instructions_file = request.FILES.get('instructions_file')
        # layout_file = request.FILES.get('layout_file')
        # notification_url = request.POST.get('notification_url', None)

        endpoint = f'/controller/?category=curation_run&action=create'
        dict_files = dict()
        for key, value in dict_file_paths.items():
            dict_files[key] = open(value, 'rb')
        form_data = {
            'id_field_name': data_file_id_field_name,
            'notification_url': notification_url
        }
        response = self._post(endpoint, data=form_data, files=dict_files)
        print("Received the response from Cymphony: ", response)
        return response['run_id']

    def send_ad_hoc_curations(self, run_id, curations):
        endpoint = f'/controller/?category=curation_run&action=drive_by_curate'
        data = {
            'run_id': run_id,
            'curations': json.dumps(curations), # List of [external_tuple_id, annotator_worker_id, annotation]
        }
        logger.info(f"Sending {len(curations)} ad-hoc curations to Cymphony for run {run_id}")
        response = self._post(endpoint, data=data)
        print("Received the response from Cymphony: ", response)
        return response
    
    def get_run_status(self, run_id):
        endpoint = f'/controller/?category=curation_run&action=status'
        logger.info(f"Getting status of run {run_id} from Cymphony")
        response = self._get(endpoint, params={'run_id': run_id})
        logger.info(f"Received the response from Cymphony: {response}")
        return response

    def download_tables(self, run_id, table_names):
        endpoint = f'/controller/?category=curation_run&action=download_tables'
        logger.info(f"Downloading tables {table_names} from Cymphony for run {run_id}")
        contents = self._post(endpoint, data={'run_id': run_id, 'table_names': json.dumps(table_names)})
        logger.info(f"Received the zip contents from Cymphony")
        return contents

def simulate_worker(worker_id: int, data_csv_path: Path, id_field_name: str, annotation_time: int, accuracy: float, num_annotations: int, gold_label_column_name: str, domain_answer_options: list):
    """Simulates a worker's annotation of a given number of tuples from the data file."""
    logger.info(f"Simulating worker {worker_id}...")
    # logger.info(f"Worker {worker_id} data CSV path: {data_csv_path}")
    # logger.info(f"Worker {worker_id} ID field name: {id_field_name}")
    # logger.info(f"Worker {worker_id} Annotation time: {annotation_time}")
    # logger.info(f"Worker {worker_id} Accuracy: {accuracy}")
    # logger.info(f"Worker {worker_id} Number of annotations: {num_annotations}")
    # logger.info(f"Worker {worker_id} Gold label column name: {gold_label_column_name}")
    # logger.info(f"Worker {worker_id} Domain answer options: {domain_answer_options}")
    if not data_csv_path.exists():
        raise FileNotFoundError(f"Data CSV file not found at {data_csv_path}")
    
    with open(data_csv_path, 'r') as f:
        reader = csv.DictReader(f)
        all_rows = list(reader) # implicitly skips header row
        curations = []

        if all_rows and len(all_rows) >= num_annotations:
            # Select num_annotations unique rows to be annotated
            selected_rows = random.sample(all_rows, num_annotations)

            for i, row_to_annotate in enumerate(selected_rows):
                id = row_to_annotate[id_field_name]
                time.sleep(annotation_time) # Simulate the time taken by the worker to annotate the tuple
                is_correct = random.random() < accuracy # Tosses a coin with say 83% probability of being correct, where accuracy is the probability of being correct.
                if is_correct:
                    annotation = row_to_annotate[gold_label_column_name]
                else:
                    possible_wrong_annotations = [a for a in domain_answer_options if a != row_to_annotate[gold_label_column_name]]
                    # Handle case where there are no other annotation choices
                    if not possible_wrong_annotations:
                        annotation = row_to_annotate[gold_label_column_name] # Fallback to correct if no other choice
                    else:
                        annotation = random.choice(possible_wrong_annotations)
                curations.append([id, worker_id, annotation])
        else:
            raise ValueError(f"Not enough rows found in the data CSV file at {data_csv_path} for {num_annotations} annotations.")
    return curations

def simulate_worker_and_send_to_cymphony(
    worker_id, 
    data_csv_path, 
    id_field_name, 
    annotation_time, 
    accuracy, 
    num_annotations, 
    gold_label_column_name, 
    domain_answer_options,
    run_id,
    client
    ):
    curations_data = simulate_worker(
        worker_id=worker_id,
        data_csv_path=data_csv_path, 
        id_field_name=id_field_name, 
        annotation_time=annotation_time,
        accuracy=accuracy, 
        num_annotations=num_annotations,
        gold_label_column_name=gold_label_column_name, 
        domain_answer_options=domain_answer_options 
    )
    logger.info(f"Curations generated for worker {worker_id}: {curations_data}")
    
    logger.info(f"Sending ad-hoc curations for worker {worker_id} to Cymphony...")
    drive_by_response = client.send_ad_hoc_curations(run_id, curations_data)
    if drive_by_response['status'] == 'success':
        logger.info(f"Ad-hoc curations sent to Cymphony for run {run_id}")
    else:
        logger.error(f"Failed to send ad-hoc curations to Cymphony for run {run_id}")
    

def simulate_driveby_with_sc_workers(dict_file_paths, data_file_id_field_name, run_id, client, start_time_sc_operations):
    # Setting up the curations
    num_workers = 50
    time_gap_between_workers = 120 # 2 minutes
    threads = []
    for worker_id in range(num_workers):
        worker_id=worker_id
        data_csv_path=dict_file_paths['data_file']
        id_field_name=data_file_id_field_name
        annotation_time=10
        accuracy=random.uniform(0.8, 0.85)
        num_annotations=10
        gold_label_column_name="gold_label"
        domain_answer_options=['0.0', '1.0'] 
        
        logger.info(f"Starting drive-by curation for worker {worker_id}...")
        thread = threading.Thread(
            target=simulate_worker_and_send_to_cymphony,
            args=(
                worker_id,
                data_csv_path,  
                id_field_name,
                annotation_time,
                accuracy,
                num_annotations,
                gold_label_column_name,
                domain_answer_options,
                run_id,
                client
            )
        )
        thread.start()
        threads.append(thread)
        time.sleep(time_gap_between_workers)
    
    # Wait for all workers to finish
    for thread in threads:
        thread.join()
    
    end_time_sc_operations = time.time()
    time_taken_sc_operations = end_time_sc_operations - start_time_sc_operations
    logger.info(f"Time taken for SC operations: {time_taken_sc_operations} seconds")
    


# --- Main Simulation Logic ---
def simulate():
    client = CymphonyClient(CYMPHONY_URL)
    run_id = None
    try:
        logger.info("--- Starting Fake Smartcat Drive-by Curation Simulation ---")
        e2e_start_time = time.time()
        start_time_sc_operations = time.time()

        # 1. SC logs in to CN
        logger.info(f"Logging in as {SMARTCAT_USERNAME}...")
        client.login(SMARTCAT_USERNAME, SMARTCAT_PASSWORD)
        logger.info("Login successful.")

        time.sleep(2)

        # 2. SC creates a curation run
        workflow_dir = Path("fake-smartcat-exps/drive-by-curation/synthetic-workers/workflow/")
        dict_file_paths = {
            'workflow_file': workflow_dir / "workflow.cy",
            'data_file': workflow_dir / "edi_preprocessed_data.csv",
            'instructions_file': workflow_dir / "instructions.html"
        }
        data_file_id_field_name = "id"
        notification_url = SMARTCAT_WEBHOOK_URL
        run_id = client.create_curation_run(dict_file_paths, data_file_id_field_name, notification_url)
        logger.info(f"Curation run created with ID: {run_id}")

        time.sleep(2)

        # 3.1 Bulk curation via simulate_workers.py from within a thread
        thread_bulk_with_regular_workers = threading.Thread(
            target=simulate_bulk_with_regular_workers,
            args=(run_id, 'job_3a_knlm', 'gold_label')
        )
        thread_bulk_with_regular_workers.start()

        time.sleep(2)

        # 3.2 SC sends ad-hoc curations
        thread_driveby_with_sc_workers = threading.Thread(
            target=simulate_driveby_with_sc_workers,
            args=(dict_file_paths, data_file_id_field_name, run_id, client, start_time_sc_operations)
        )
        thread_driveby_with_sc_workers.start()
        
        time.sleep(2)

        # 4. SC gets the status of the run periodically or via an event.
        poll_interval = 60 * 5 # seconds (5 minutes)
        max_poll_time = 3600 * 24 # seconds (24 hours)
        start_time = time.time()
        run_completed_via_poll = False
        
        while time.time() - start_time < max_poll_time:
            # Wait for either a callback or the poll interval to pass
            if callback_received_event.wait(timeout=poll_interval):
                logger.info("Callback received! Processing callback and skipping polling.")
                callback_received_event.clear() # Reset the event for the next potential callback
                # Process the callback directly
                latest_callback = received_callbacks[-1] # Process latest callback
                if latest_callback.get('run_id') == run_id and latest_callback.get('status') == 'COMPLETED':
                    logger.info(f"Run {run_id} completed via webhook notification.")
                    run_completed_via_poll = False # Indicate completion via callback
                    break
                else:
                    logger.info("Received a callback, but it's not for completion of this run. Continuing...")

            else: # Timeout occurred, no callback received, proceed with polling
                logger.info(f"Polling Cymphony for run {run_id} status...")
                status_response = client.get_run_status(run_id)
                current_status = status_response.get('run_status') # Assuming 'status' in response
                logger.info(f"Run {run_id} current status: {current_status}")

                if current_status == 'COMPLETED': # Assuming Cymphony returns 'COMPLETED'
                    logger.info(f"Run {run_id} completed via polling.")
                    run_completed_via_poll = True
                    break
            
        else:
            logger.warning(f"Run {run_id} did not complete within {max_poll_time} seconds. Timeout.")
            exit(1)

        time.sleep(2)

        e2e_end_time = time.time()
        logger.info(f"Total time taken: {int(e2e_end_time - e2e_start_time)} seconds")

        # 5. If run completed (either via poll or callback), SC downloads the tables
        table_names = ['Assignments', 'Annotations', 'Aggregations']
        download_tables_contents = client.download_tables(run_id, table_names)
        # save the zip contents
        download_tables_dir = Path(f"fake-smartcat-exps/drive-by-curation/synthetic-workers/exp_{run_id}_{time.strftime('%Y%m%d%H%M%S')}")
        download_tables_dir.mkdir(parents=True, exist_ok=True)
        zip_file_path = download_tables_dir / f"tables_{run_id}_{int(time.time())}.zip"
        with open(zip_file_path, 'wb') as f:
            f.write(download_tables_contents)
        logger.info(f"Tables downloaded from Cymphony for run {run_id} and placed in {zip_file_path}")
        

    except requests.exceptions.HTTPError as e:
        logger.error(f"Cymphony API Error: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        pass


# --- Fake Smartcat Webhook Receiver ---
app = Flask(__name__)
received_callbacks = [] # Store received callbacks
callback_received_event = threading.Event() # Event to signal main thread

@app.route('/webhook', methods=['POST'])
def smartcat_webhook_receiver():
    try:
        payload = request.get_json()
        logger.info(f"Received webhook: {json.dumps(payload)}")
        
        # --- Optional: Verify webhook signature (commented out for now) ---
        # signature = request.headers.get('X-Webhook-Signature')
        # if signature and WEBHOOK_SECRET:
        #     import hmac, hashlib
        #     expected_signature = hmac.new(WEBHOOK_SECRET.encode(), request.data, hashlib.sha256).hexdigest()
        #     if not hmac.compare_digest(f'sha256={expected_signature}', signature):
        #         logger.warning("Webhook signature verification failed.")
        #         return jsonify({"status": "error", "message": "Invalid signature"}), 401
        
        received_callbacks.append(payload)
        callback_received_event.set() # Signal that a callback was received
        return jsonify({"status": "received", "message": "Callback processed"}), 200
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

def run_flask_app():
    # When running Flask in a separate thread, debug=True and use_reloader=True are problematic
    # Disable debug and reloader for threaded execution
    app.run(port=5000, debug=False, use_reloader=False)

if __name__ == "__main__":
    # Start the Flask app in a separate thread
    flask_thread = threading.Thread(target=run_flask_app)
    flask_thread.daemon = True # Allow main program to exit even if thread is running
    flask_thread.start()
    logger.info(f"Fake Smartcat webhook listener started on {SMARTCAT_WEBHOOK_URL}")

    # Give Flask a moment to start
    time.sleep(2) 

    simulate()
    
    logger.info("--- Fake Smartcat Simulation Complete ---")


"""

Appendix: (to keep as a git backup)


--------------------------------SC workers before run completion---------------------------------
SELECT * FROM public.u12_p90_w89_r101_j302_outputs

-- Number of workers:
SELECT COUNT(DISTINCT worker_id) FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes 
where date_creation <= '2025-10-28 12:45:27.624322-05'; -- 33		-- date creation picked as the last annotation of regular worker in outputs

-- Number of votes per worker: min, max, avg
SELECT min(O.c), max(O.c), avg(O.c) 
FROM (
	SELECT count(annotation) as c 
	FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes 
	where date_creation <= '2025-10-28 12:45:27.624322-05'
	group by worker_id
) O	-- 10,10,10

-- Worker precision (matches / annotated)
--- avg worker precision
select COUNT(*) as total_matches 
FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes D, public.u12_p90_w89_r101_j302_tuples T 
where D.id = T.id and D.annotation = T.gold_label
and D.date_creation <= '2025-10-28 12:45:27.624322-05' --268

SELECT COUNT(*) as total_annotations 
FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes D
where D.date_creation <= '2025-10-28 12:45:27.624322-05'-- (or outputs is also fine)	-- 330
-- overall precision = total_matches / total_annotations
-- min, max preicison of a worker, and how much.
SELECT 
	(A.matches_per_worker * 1.0) / (B.annotations_per_worker) as precision, A.matches_per_worker, B.annotations_per_worker, A.worker_id, B.worker_id
FROM 
	(
		select COUNT(*) as matches_per_worker, D.worker_id as worker_id 
		 FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes D, public.u12_p90_w89_r101_j302_tuples T 
		 where D.id = T.id and D.annotation = T.gold_label
		and D.date_creation <= '2025-10-28 12:45:27.624322-05'
		GROUP BY D.worker_id
	) A, 
	(
		SELECT COUNT(*) as annotations_per_worker, worker_id 
		FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes 
		where date_creation <= '2025-10-28 12:45:27.624322-05'
		GROUP BY worker_id
	) B
WHERE A.worker_id = B.worker_id
ORDER BY precision DESC	-- 10/10, 5/10

-- number of tuples labeled
--- distinct
SELECT COUNT(DISTINCT O._id) 
FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes D, public.u12_p90_w89_r101_j302_tuples T, public.u12_p90_w89_r101_j302_outputs O
where O._id=T._id and T.id=D.id
and D.date_creation <= '2025-10-28 12:45:27.624322-05'  --309
--or
select COUNT(DISTINCT _id) 
FROM public.u12_p90_w89_r101_j302_outputs 
WHERE worker_id=12
and date_creation <= '2025-10-28 12:45:27.624322-05' -- 309

-- tuples labeled by sc that had multiple votes by sc
select _id, count(annotation) 
FROM public.u12_p90_w89_r101_j302_outputs 
where worker_id=12 and date_creation <= '2025-10-28 12:45:27.624322-05'
GROUP BY _id having count(annotation) > 1  --20

-- number of tuples only partially voted (no final label)
select DISTINCT _id 
FROM public.u12_p90_w89_r101_j302_outputs 
where worker_id=12 and _id NOT IN (select _id FROM public.u12_p90_w89_r101_j302_final_labels)
and date_creation <= '2025-10-28 12:45:27.624322-05'  --0

--- tuples with no final label but multiple votes (optional)
select _id, count(annotation) 
FROM public.u12_p90_w89_r101_j302_outputs 
where worker_id = 12 and _id NOT IN (select _id FROM public.u12_p90_w89_r101_j302_final_labels)
and date_creation <= '2025-10-28 12:45:27.624322-05'
GROUP BY _id having count(annotation) > 1 
  --0


---------------------------------------SC workers all--------------------------
-- Number of workers:
SELECT COUNT(DISTINCT worker_id) FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes; -- 50

-- Number of votes per worker: min, max, avg
SELECT min(O.c), max(O.c), avg(O.c) 
FROM (
	SELECT count(annotation) as c 
	  FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes 
	group by worker_id
) O	-- 10,10,10

-- Worker precision (matches / annotated)
--- avg worker precision
select COUNT(*) as total_matches 
FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes D, public.u12_p90_w89_r101_j302_tuples T 
where D.id = T.id and D.annotation = T.gold_label	--400
SELECT COUNT(*) as total_annotations FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes D -- (or outputs is also fine)	-- 500
-- overall precision = total_matches / total_annotations
-- min, max preicison of a worker, and how much.
SELECT 
	(A.matches_per_worker * 1.0) / (B.annotations_per_worker) as precision, A.matches_per_worker, B.annotations_per_worker, A.worker_id, B.worker_id
FROM 
	(
		select COUNT(*) as matches_per_worker, D.worker_id as worker_id 
		 FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes D, public.u12_p90_w89_r101_j302_tuples T 
		 where D.id = T.id and D.annotation = T.gold_label 
		GROUP BY D.worker_id
	) A, 
	(SELECT COUNT(*) as annotations_per_worker, worker_id FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes GROUP BY worker_id) B
WHERE A.worker_id = B.worker_id
ORDER BY precision DESC	-- 10/10, 5/10

-- number of tuples labeled
--- distinct
SELECT COUNT(DISTINCT O._id) 
FROM public.u12_p90_w89_r101_j302_drive_by_curation_votes D, public.u12_p90_w89_r101_j302_tuples T, public.u12_p90_w89_r101_j302_outputs O
where O._id=T._id and T.id=D.id;	--462
--or
select COUNT(DISTINCT _id) FROM public.u12_p90_w89_r101_j302_outputs WHERE worker_id=12 -- 462
-- tuples labeled by sc that had multiple votes by sc
select _id, count(annotation) FROM public.u12_p90_w89_r101_j302_outputs where worker_id=12 GROUP BY _id having count(annotation) > 1 --37

-- number of tuples only partially voted (no final label)
select DISTINCT _id FROM public.u12_p90_w89_r101_j302_outputs where worker_id=12 and _id NOT IN (select _id FROM public.u12_p90_w89_r101_j302_final_labels)	--0

--- tuples with no final label but multiple votes (optional)
select _id, count(annotation) 
FROM public.u12_p90_w89_r101_j302_outputs 
where worker_id = 12 and _id NOT IN (select _id FROM public.u12_p90_w89_r101_j302_final_labels) GROUP BY _id having count(annotation) > 1 --0


--------------- CN regular workers
--# of regular workers, votes per regular worker
select min(t.n_annotations), max(t.n_annotations), avg(t.n_annotations), count(t.n_annotations)
from ( 
	select count(*) as n_annotations from public.u12_p90_w89_r101_j302_outputs where worker_id NOT IN (12) group by worker_id 
) as t -- 10	388	198.6060606060606061	33


-- Regular Worker precision (matches / annotated)
--- avg regular worker precision
select COUNT(*) as total_matches 
FROM public.u12_p90_w89_r101_j302_outputs O, public.u12_p90_w89_r101_j302_tuples T 
where O.worker_id not in (12) and O._id = T._id and O.annotation = T.gold_label	-- 5407
SELECT COUNT(*) as total_annotations 
FROM public.u12_p90_w89_r101_j302_outputs O 
where O.worker_id not in (12) -- (or outputs is also fine)	-- 7054-500 = 6554
-- overall precision = total_matches / total_annotations
-- min, max preicison of a worker, and how much.
SELECT 
	(A.matches_per_worker * 1.0) / (B.annotations_per_worker) as precision, A.matches_per_worker, B.annotations_per_worker, A.worker_id, B.worker_id
FROM 
	(
		select COUNT(*) as matches_per_worker, O.worker_id as worker_id 
		 FROM public.u12_p90_w89_r101_j302_outputs O, public.u12_p90_w89_r101_j302_tuples T 
		 where O.worker_id NOT in (12) and O._id=T._id and O.annotation = T.gold_label 
		GROUP BY O.worker_id
	) A, 
	(SELECT COUNT(*) as annotations_per_worker, worker_id FROM public.u12_p90_w89_r101_j302_outputs where worker_id NOT IN (12) GROUP BY worker_id) B
WHERE A.worker_id = B.worker_id
ORDER BY precision DESC	-- 20/21, 294/375
--------------- 


--------------- CN overall workers before run completion 
--# of workers, votes per worker
select min(t.n_annotations), max(t.n_annotations), avg(t.n_annotations), count(t.n_annotations)
from ( 
	select count(*) as n_annotations 
	from public.u12_p90_w89_r101_j302_outputs
	where date_creation <= '2025-10-28 12:45:27.624322-05'
	group by worker_id 
) as t -- 10	388	202.4705882352941176	34


-- Worker precision (matches / annotated)
--- avg regular worker precision
select COUNT(*) as total_matches 
FROM public.u12_p90_w89_r101_j302_outputs O, public.u12_p90_w89_r101_j302_tuples T 
where O._id = T._id and O.annotation = T.gold_label
and O.date_creation <= '2025-10-28 12:45:27.624322-05'  -- 5675

SELECT COUNT(*) as total_annotations 
FROM public.u12_p90_w89_r101_j302_outputs O 
where O.date_creation <= '2025-10-28 12:45:27.624322-05' -- 6884
-- overall precision = total_matches / total_annotations
-- min, max preicison of a worker, and how much.
SELECT 
	(A.matches_per_worker * 1.0) / (B.annotations_per_worker) as precision, A.matches_per_worker, B.annotations_per_worker, A.worker_id, B.worker_id
FROM 
	(
		select COUNT(*) as matches_per_worker, O.worker_id as worker_id 
		 FROM public.u12_p90_w89_r101_j302_outputs O, public.u12_p90_w89_r101_j302_tuples T 
		 where O._id=T._id and O.annotation = T.gold_label 
		and O.date_creation <= '2025-10-28 12:45:27.624322-05'
		GROUP BY O.worker_id
	) A, 
	(
		SELECT COUNT(*) as annotations_per_worker, worker_id 
		FROM public.u12_p90_w89_r101_j302_outputs  where date_creation <= '2025-10-28 12:45:27.624322-05'
		GROUP BY worker_id
	) B
WHERE A.worker_id = B.worker_id
ORDER BY precision DESC	-- 20/21, 294/375
--------------- 

--------------- CN overall workers
--# of workers, votes per regular worker
select min(t.n_annotations), max(t.n_annotations), avg(t.n_annotations), count(t.n_annotations)
from ( 
	select count(*) as n_annotations from public.u12_p90_w89_r101_j302_outputs group by worker_id 
) as t -- 10	500	207.4705882352941176	34


-- Worker precision (matches / annotated)
--- avg  worker precision
select COUNT(*) as total_matches 
FROM public.u12_p90_w89_r101_j302_outputs O, public.u12_p90_w89_r101_j302_tuples T 
where O._id = T._id and O.annotation = T.gold_label	-- 5807
SELECT COUNT(*) as total_annotations FROM public.u12_p90_w89_r101_j302_outputs O 	-- 7054
-- overall precision = total_matches / total_annotations
-- min, max preicison of a worker, and how much.
SELECT 
	(A.matches_per_worker * 1.0) / (B.annotations_per_worker) as precision, A.matches_per_worker, B.annotations_per_worker, A.worker_id, B.worker_id
FROM 
	(
		select COUNT(*) as matches_per_worker, O.worker_id as worker_id 
		 FROM public.u12_p90_w89_r101_j302_outputs O, public.u12_p90_w89_r101_j302_tuples T 
		 where O._id=T._id and O.annotation = T.gold_label 
		GROUP BY O.worker_id
	) A, 
	(SELECT COUNT(*) as annotations_per_worker, worker_id FROM public.u12_p90_w89_r101_j302_outputs GROUP BY worker_id) B
WHERE A.worker_id = B.worker_id
ORDER BY precision DESC	-- 20/21, 294/375
--------------- 


-- number of tuples that got final votes
select COUNT( _id) FROM public.u12_p90_w89_r101_j302_final_labels --2918 (same result with distinct of course)

-- how many final votes are yes, no, undecided?
SELECT label, COUNT(label) FROM public.u12_p90_w89_r101_j302_final_labels group by label -- 0.0: 567, 1.0: 2351

-- TP
SELECT COUNT(*) 
FROM public.u12_p90_w89_r101_j302_final_labels F, public.u12_p90_w89_r101_j302_tuples T 
where F._id = T._id and F.label = T.gold_label AND F.label = '1.0' --2319

-- FP
SELECT COUNT(*) 
FROM public.u12_p90_w89_r101_j302_final_labels F, public.u12_p90_w89_r101_j302_tuples T 
where F._id = T._id and F.label != T.gold_label AND F.label = '1.0' --32

-- FN
SELECT COUNT(*) 
FROM public.u12_p90_w89_r101_j302_final_labels F, public.u12_p90_w89_r101_j302_tuples T 
where F._id = T._id and F.label != T.gold_label AND F.label = '0.0' --189

-- TN
SELECT COUNT(*) 
FROM public.u12_p90_w89_r101_j302_final_labels F, public.u12_p90_w89_r101_j302_tuples T 
where F._id = T._id and F.label = T.gold_label AND F.label = '0.0' --378

-- overall precision, recall
--- correct preds
SELECT COUNT(*) FROM public.u12_p90_w89_r101_j302_final_labels F, public.u12_p90_w89_r101_j302_tuples T where F._id = T._id and F.label = T.gold_label; --2697
--- total preds
SELECT COUNT(*) FROM public.u12_p90_w89_r101_j302_final_labels F --2918








"""